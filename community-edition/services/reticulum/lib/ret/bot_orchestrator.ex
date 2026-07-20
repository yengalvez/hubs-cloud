defmodule Ret.BotOrchestrator do
  @moduledoc false

  require Logger

  alias Ret.BotRuntimeOutbox

  @protocol "yenhubs-bot-runtime-v2"
  @request_timeout_ms 5_000
  @max_safe_integer 9_007_199_254_740_991
  @canonical_uuid_v4 ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/

  # Chat remains an interactive, approval-fenced request. Desired room state is
  # delivered exclusively by Ret.BotRuntimeOutboxDispatcher below; there are no
  # public best-effort room_config/room_stop shortcuts.
  def chat(%{} = payload), do: post_chat_json("/internal/bots/chat", payload)

  def deliver_runtime_event(%{
        event_kind: "config",
        operation_id: operation_id,
        hub_sid: hub_sid,
        runtime_revision: runtime_revision,
        bots: bots,
        runtime_chat_enabled: runtime_chat_enabled
      })
      when is_binary(operation_id) and is_binary(hub_sid) and is_integer(runtime_revision) and
             is_map(bots) and is_boolean(runtime_chat_enabled) do
    runtime_bots = BotRuntimeOutbox.runtime_bots_projection(bots, runtime_chat_enabled)

    payload = %{
      protocol: @protocol,
      operation_id: operation_id,
      hub_sid: hub_sid,
      runtime_revision: runtime_revision,
      bots: runtime_bots,
      runtime_chat_enabled: runtime_chat_enabled
    }

    with :ok <- validate_common_payload(operation_id, hub_sid, runtime_revision),
         :ok <- validate_runtime_bots_projection(bots, runtime_chat_enabled),
         {:ok, 200, response} <- post_runtime_json("/internal/bots/room-config", payload),
         true <- response === config_ack(operation_id, hub_sid, runtime_revision) do
      :ok
    else
      {:ok, status, _response} when status >= 200 and status < 300 ->
        {:pending, pending_code(status)}

      false ->
        {:pending, :non_terminal_ack}

      {:error, reason} ->
        {:pending, reason}
    end
  end

  def deliver_runtime_event(%{
        event_kind: "stop",
        operation_id: operation_id,
        hub_sid: hub_sid,
        runtime_revision: runtime_revision,
        revoke_epoch: revoke_epoch
      })
      when is_binary(operation_id) and is_binary(hub_sid) and is_integer(runtime_revision) and
             is_integer(revoke_epoch) do
    payload = %{
      protocol: @protocol,
      operation_id: operation_id,
      hub_sid: hub_sid,
      runtime_revision: runtime_revision,
      revoke_epoch: revoke_epoch
    }

    with :ok <- validate_common_payload(operation_id, hub_sid, runtime_revision),
         :ok <- validate_safe_positive_integer(revoke_epoch),
         {:ok, 200, response} <- post_runtime_json("/internal/bots/room-stop", payload),
         true <- response === stop_ack(operation_id, hub_sid, runtime_revision, revoke_epoch) do
      :ok
    else
      {:ok, status, _response} when status >= 200 and status < 300 ->
        {:pending, pending_code(status)}

      false ->
        {:pending, :non_terminal_ack}

      {:error, reason} ->
        {:pending, reason}
    end
  end

  def deliver_runtime_event(_event), do: {:pending, :invalid_runtime_event}

  def protocol, do: @protocol

  defp config_ack(operation_id, hub_sid, runtime_revision) do
    %{
      "protocol" => @protocol,
      "operation_id" => operation_id,
      "hub_sid" => hub_sid,
      "runtime_revision" => runtime_revision,
      "state" => "applied"
    }
  end

  defp stop_ack(operation_id, hub_sid, runtime_revision, revoke_epoch) do
    %{
      "protocol" => @protocol,
      "operation_id" => operation_id,
      "hub_sid" => hub_sid,
      "runtime_revision" => runtime_revision,
      "revoke_epoch" => revoke_epoch,
      "state" => "stopped",
      "target_absent" => true,
      "managed_room_pods" => 0
    }
  end

  defp validate_common_payload(operation_id, hub_sid, runtime_revision) do
    with true <- Regex.match?(@canonical_uuid_v4, operation_id),
         true <- Regex.match?(~r/\A[A-Za-z0-9_-]{1,64}\z/, hub_sid),
         :ok <- validate_safe_positive_integer(runtime_revision) do
      :ok
    else
      _ -> {:error, :invalid_runtime_event}
    end
  end

  defp validate_safe_positive_integer(value)
       when is_integer(value) and value > 0 and value <= @max_safe_integer,
       do: :ok

  defp validate_safe_positive_integer(_value), do: {:error, :invalid_runtime_event}

  defp validate_runtime_bots_projection(bots, runtime_chat_enabled) do
    if BotRuntimeOutbox.runtime_bots_projection_valid?(bots, runtime_chat_enabled) do
      :ok
    else
      {:error, :invalid_runtime_event}
    end
  end

  defp pending_code(202), do: :accepted_pending
  defp pending_code(_status), do: :legacy_or_malformed_2xx

  defp post_runtime_json(path, payload) do
    with {:ok, access_key} <- validated_access_key(),
         {:ok, response} <- request(path, payload, access_key) do
      case response do
        %HTTPoison.Response{status_code: status_code, body: body}
        when status_code >= 200 and status_code < 300 ->
          case decode_json(body) do
            {:ok, decoded} -> {:ok, status_code, decoded}
            :error -> {:ok, status_code, :malformed_json}
          end

        %HTTPoison.Response{status_code: status_code} ->
          Logger.warning(
            "Bot orchestrator request failed with status=#{status_code} path=#{path}"
          )

          {:error, http_error_code(status_code)}
      end
    end
  end

  defp post_chat_json(path, payload) do
    with {:ok, access_key} <- validated_access_key(),
         {:ok, response} <- request(path, payload, access_key) do
      case response do
        %HTTPoison.Response{status_code: status_code, body: body}
        when status_code >= 200 and status_code < 300 ->
          {:ok, decode_json_or_empty(body)}

        %HTTPoison.Response{status_code: status_code} ->
          Logger.warning(
            "Bot orchestrator request failed with status=#{status_code} path=#{path}"
          )

          {:error, {:http_error, status_code}}
      end
    end
  end

  defp request(path, payload, access_key) do
    url = "#{endpoint()}#{path}"

    headers = [
      {"Content-Type", "application/json"},
      {"x-ret-bot-orchestrator-access-key", access_key}
    ]

    case http_client().request(:post, url, Poison.encode!(payload), headers,
           timeout: @request_timeout_ms,
           recv_timeout: @request_timeout_ms
         ) do
      {:ok, %HTTPoison.Response{} = response} ->
        {:ok, response}

      {:error, %HTTPoison.Error{reason: reason}} ->
        Logger.warning("Bot orchestrator request failed path=#{path} reason=#{inspect(reason)}")
        {:error, request_error_code(reason)}

      _other ->
        Logger.warning("Bot orchestrator request returned an invalid client result path=#{path}")
        {:error, :invalid_http_client_result}
    end
  end

  defp validated_access_key do
    case access_key() do
      key when is_binary(key) and byte_size(key) >= 32 ->
        {:ok, key}

      _ ->
        Logger.error("Bot orchestrator access key is missing or too short; request blocked")
        {:error, :missing_access_key}
    end
  end

  defp decode_json(body) when is_binary(body) and byte_size(body) > 0 do
    case Poison.decode(body) do
      {:ok, decoded} when is_map(decoded) -> {:ok, decoded}
      _ -> :error
    end
  end

  defp decode_json(_body), do: :error

  defp decode_json_or_empty(nil), do: %{}
  defp decode_json_or_empty(""), do: %{}

  defp decode_json_or_empty(body) do
    case decode_json(body) do
      {:ok, decoded} -> decoded
      :error -> %{}
    end
  end

  defp http_error_code(status_code) when is_integer(status_code) and status_code >= 500,
    do: :orchestrator_server_error

  defp http_error_code(_status_code), do: :orchestrator_http_error

  defp request_error_code(:timeout), do: :orchestrator_timeout
  defp request_error_code(:recv_timeout), do: :orchestrator_timeout
  defp request_error_code(_reason), do: :orchestrator_request_error

  defp endpoint do
    (Application.get_env(:ret, __MODULE__)[:endpoint] || "http://bot-orchestrator:5001")
    |> String.trim_trailing("/")
  end

  defp http_client do
    Application.get_env(:ret, __MODULE__)[:http_client] || HTTPoison
  end

  defp access_key do
    Application.get_env(:ret, __MODULE__)[:access_key] || ""
  end
end
