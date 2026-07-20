defmodule Ret.BotRuntimeOutboxDispatcher do
  @moduledoc """
  Supervised, recoverable delivery of durable bot runtime desired state.

  PostgreSQL decides claim ownership and per-room order. The dispatcher only
  completes an event after the parent returns the exact terminal v2 ACK; an
  accepted request, timeout, legacy 2xx, malformed body or lost claim remains
  pending and is retried with the same operation id.
  """

  use GenServer

  @default_poll_interval_ms 250
  @default_error_interval_ms 1_000
  @default_claim_ttl_seconds 30
  @default_retry_base_seconds 1
  @default_retry_max_seconds 60
  @min_claim_ttl_seconds 10
  @max_claim_ttl_seconds 3_600
  @max_retry_delay_seconds 86_400

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc false
  def dispatch_once(opts \\ []) do
    state = build_state(opts)

    case state.store_module.claim_next(state.claim_owner,
           claim_ttl_seconds: state.claim_ttl_seconds
         ) do
      {:ok, event} -> deliver_claim(event, state)
      :empty -> :empty
      {:error, :database_unavailable} -> {:error, :database_unavailable}
      _other -> {:error, :invalid_store_result}
    end
  rescue
    _error in [DBConnection.ConnectionError, Postgrex.Error] ->
      {:error, :database_unavailable}
  catch
    :exit, _reason -> {:error, :database_unavailable}
  end

  @impl true
  def init(opts) do
    state = build_state(opts)

    if state.enabled do
      send(self(), :dispatch)
    end

    {:ok, state}
  end

  @impl true
  def handle_info(:dispatch, %{enabled: true} = state) do
    result = dispatch_once(Map.to_list(state))
    schedule_dispatch(dispatch_delay(result, state))
    {:noreply, state}
  end

  def handle_info(:dispatch, state), do: {:noreply, state}

  defp deliver_claim(event, state) do
    case state.delivery_module.deliver_runtime_event(event) do
      :ok ->
        case state.store_module.complete_claim(
               event.id,
               event.runtime_revision,
               event.claim_owner,
               event.claim_token
             ) do
          :ok -> {:ok, :completed}
          {:error, :claim_lost} -> {:error, :claim_lost_after_terminal_ack}
          {:error, :database_unavailable} -> {:error, :database_unavailable_after_terminal_ack}
          _other -> {:error, :invalid_store_result}
        end

      {:pending, reason} ->
        retry_claim(event, reason, state)

      _other ->
        retry_claim(event, :invalid_delivery_result, state)
    end
  rescue
    _error -> retry_claim(event, :delivery_exception, state)
  catch
    :exit, _reason -> retry_claim(event, :delivery_exit, state)
  end

  defp retry_claim(event, reason, state) do
    delay_seconds = retry_delay_seconds(event.attempt_count, state)
    failure_code = failure_code(reason)

    case state.store_module.retry_claim(
           event.id,
           event.runtime_revision,
           event.claim_owner,
           event.claim_token,
           failure_code,
           delay_seconds
         ) do
      :ok -> {:ok, :retry_scheduled}
      {:error, :claim_lost} -> {:error, :claim_lost}
      {:error, :database_unavailable} -> {:error, :database_unavailable}
      _other -> {:error, :invalid_store_result}
    end
  end

  defp retry_delay_seconds(attempt_count, state) do
    exponent = max(min((attempt_count || 1) - 1, 16), 0)
    delay = state.retry_base_seconds * Bitwise.bsl(1, exponent)
    min(delay, state.retry_max_seconds)
  end

  defp failure_code(reason) when is_atom(reason) do
    reason
    |> Atom.to_string()
    |> String.slice(0, 64)
  end

  defp failure_code(_reason), do: "runtime_delivery_pending"

  defp dispatch_delay(:empty, state), do: state.poll_interval_ms
  defp dispatch_delay({:ok, _result}, _state), do: 0
  defp dispatch_delay({:error, _reason}, state), do: state.error_interval_ms

  defp schedule_dispatch(delay_ms) do
    Process.send_after(self(), :dispatch, delay_ms)
  end

  defp build_state(opts) do
    config = Application.get_env(:ret, __MODULE__, [])

    %{
      enabled: option(opts, config, :enabled, true) === true,
      claim_owner:
        valid_claim_owner(
          option(opts, config, :claim_owner, Ecto.UUID.generate()),
          Ecto.UUID.generate()
        ),
      store_module: option(opts, config, :store_module, Ret.BotRuntimeOutbox.Store),
      delivery_module: option(opts, config, :delivery_module, Ret.BotOrchestrator),
      poll_interval_ms:
        positive_integer(
          option(opts, config, :poll_interval_ms, @default_poll_interval_ms),
          @default_poll_interval_ms
        ),
      error_interval_ms:
        positive_integer(
          option(opts, config, :error_interval_ms, @default_error_interval_ms),
          @default_error_interval_ms
        ),
      claim_ttl_seconds:
        bounded_integer(
          option(opts, config, :claim_ttl_seconds, @default_claim_ttl_seconds),
          @min_claim_ttl_seconds,
          @max_claim_ttl_seconds,
          @default_claim_ttl_seconds
        ),
      retry_base_seconds:
        bounded_integer(
          option(opts, config, :retry_base_seconds, @default_retry_base_seconds),
          1,
          @max_retry_delay_seconds,
          @default_retry_base_seconds
        ),
      retry_max_seconds:
        bounded_integer(
          option(opts, config, :retry_max_seconds, @default_retry_max_seconds),
          1,
          @max_retry_delay_seconds,
          @default_retry_max_seconds
        )
    }
  end

  defp option(opts, config, key, default) do
    Keyword.get(opts, key, Keyword.get(config, key, default))
  end

  defp positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, default), do: default

  defp valid_claim_owner(value, _default)
       when is_binary(value) and byte_size(value) in 1..128,
       do: value

  defp valid_claim_owner(_value, default), do: default

  defp bounded_integer(value, minimum, maximum, _default)
       when is_integer(value) and value >= minimum and value <= maximum,
       do: value

  defp bounded_integer(_value, _minimum, _maximum, default), do: default
end
