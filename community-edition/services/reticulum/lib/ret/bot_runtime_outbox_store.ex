defmodule Ret.BotRuntimeOutbox.Store do
  @moduledoc """
  PostgreSQL persistence and recoverable claims for bot runtime delivery.

  Enqueue functions intentionally do not open a transaction: callers use them
  inside the approval/quarantine transaction so the runtime decision, fencing
  epoch and immutable event either commit together or all roll back. Claims use
  short transactions and never retain a database lock during HTTP delivery.
  """

  alias Ecto.Adapters.SQL
  alias Ret.{BotConfig, BotRuntimeOutbox, Repo}

  @default_claim_ttl_seconds 30
  @max_claim_ttl_seconds 3_600
  @max_retry_delay_seconds 86_400

  @type database_error :: {:error, :database_unavailable}
  @type claim_result :: {:ok, BotRuntimeOutbox.t()} | :empty | database_error()

  def enqueue_config(attrs), do: enqueue_config(Repo, attrs)

  def enqueue_config(repo, attrs) when is_map(attrs) do
    attrs
    |> normalize_attrs()
    |> put_runtime_chat_enabled()
    |> ensure_operation_id()
    |> Map.put(:event_kind, "config")
    |> then(&BotRuntimeOutbox.changeset(%BotRuntimeOutbox{}, &1))
    |> repo.insert()
  end

  def enqueue_stop(attrs), do: enqueue_stop(Repo, attrs)

  def enqueue_stop(repo, attrs) when is_map(attrs) do
    attrs
    |> normalize_attrs()
    |> ensure_operation_id()
    |> Map.put(:event_kind, "stop")
    |> then(&BotRuntimeOutbox.changeset(%BotRuntimeOutbox{}, &1))
    |> repo.insert()
  end

  def claim_next(claim_owner, opts \\ []), do: claim_next(Repo, claim_owner, opts)

  def claim_next(repo, claim_owner, opts)
      when is_binary(claim_owner) and byte_size(claim_owner) in 1..128 and is_list(opts) do
    ttl_seconds = Keyword.get(opts, :claim_ttl_seconds, @default_claim_ttl_seconds)
    claim_token = Keyword.get_lazy(opts, :claim_token, &Ecto.UUID.generate/0)

    with true <- valid_seconds?(ttl_seconds, 1, @max_claim_ttl_seconds),
         {:ok, claim_token} <- Ecto.UUID.cast(claim_token) do
      claim_next_validated(repo, claim_owner, claim_token, ttl_seconds)
    else
      false -> {:error, :invalid_claim_ttl}
      :error -> {:error, :invalid_claim_token}
    end
  end

  def claim_next(_repo, _claim_owner, _opts), do: {:error, :invalid_claim_owner}

  def complete_claim(id, runtime_revision, claim_owner, claim_token),
    do: complete_claim(Repo, id, runtime_revision, claim_owner, claim_token)

  def complete_claim(repo, id, runtime_revision, claim_owner, claim_token)
      when is_integer(id) and id > 0 and is_integer(runtime_revision) and runtime_revision > 0 and
             is_binary(claim_owner) and byte_size(claim_owner) in 1..128 do
    with {:ok, claim_token} <- Ecto.UUID.cast(claim_token) do
      case SQL.query(
             repo,
             """
             UPDATE ret0.bot_runtime_outbox
             SET delivered_at = timezone('UTC', clock_timestamp()),
                 claim_owner = NULL,
                 claim_token = NULL,
                 claim_expires_at = NULL,
                 last_failure_code = NULL,
                 updated_at = timezone('UTC', clock_timestamp())
             WHERE id = $1
               AND runtime_revision = $2
               AND delivered_at IS NULL
               AND claim_owner = $3
               AND claim_token = $4::text::uuid
               AND claim_expires_at > timezone('UTC', clock_timestamp())
             """,
             [id, runtime_revision, claim_owner, claim_token]
           ) do
        {:ok, %{num_rows: 1}} -> :ok
        {:ok, %{num_rows: 0}} -> {:error, :claim_lost}
        {:error, _reason} -> {:error, :database_unavailable}
      end
    else
      :error -> {:error, :claim_lost}
    end
  rescue
    _error -> {:error, :database_unavailable}
  catch
    :exit, _reason -> {:error, :database_unavailable}
  end

  def complete_claim(_repo, _id, _runtime_revision, _claim_owner, _claim_token),
    do: {:error, :claim_lost}

  def retry_claim(id, runtime_revision, claim_owner, claim_token, failure_code, delay_seconds),
    do:
      retry_claim(
        Repo,
        id,
        runtime_revision,
        claim_owner,
        claim_token,
        failure_code,
        delay_seconds
      )

  def retry_claim(
        repo,
        id,
        runtime_revision,
        claim_owner,
        claim_token,
        failure_code,
        delay_seconds
      )
      when is_integer(id) and id > 0 and is_integer(runtime_revision) and
             runtime_revision > 0 and is_binary(claim_owner) and
             byte_size(claim_owner) in 1..128 and is_binary(failure_code) do
    with {:ok, claim_token} <- Ecto.UUID.cast(claim_token),
         true <- BotRuntimeOutbox.valid_failure_code?(failure_code),
         true <- valid_seconds?(delay_seconds, 0, @max_retry_delay_seconds) do
      case SQL.query(
             repo,
             """
             UPDATE ret0.bot_runtime_outbox
             SET claim_owner = NULL,
                 claim_token = NULL,
                 claim_expires_at = NULL,
                 next_attempt_at = timezone('UTC', clock_timestamp()) +
                   ($6::bigint * interval '1 second'),
                 last_failure_code = $5,
                 updated_at = timezone('UTC', clock_timestamp())
             WHERE id = $1
               AND runtime_revision = $2
               AND delivered_at IS NULL
               AND claim_owner = $3
               AND claim_token = $4::text::uuid
               AND claim_expires_at > timezone('UTC', clock_timestamp())
             """,
             [id, runtime_revision, claim_owner, claim_token, failure_code, delay_seconds]
           ) do
        {:ok, %{num_rows: 1}} -> :ok
        {:ok, %{num_rows: 0}} -> {:error, :claim_lost}
        {:error, _reason} -> {:error, :database_unavailable}
      end
    else
      :error -> {:error, :claim_lost}
      false -> {:error, :invalid_retry}
    end
  rescue
    _error -> {:error, :database_unavailable}
  catch
    :exit, _reason -> {:error, :database_unavailable}
  end

  def retry_claim(
        _repo,
        _id,
        _runtime_revision,
        _claim_owner,
        _claim_token,
        _failure_code,
        _delay_seconds
      ),
      do: {:error, :invalid_retry}

  def pending_before?(hub_id, runtime_revision),
    do: pending_before?(Repo, hub_id, runtime_revision)

  def pending_before?(repo, hub_id, runtime_revision) do
    pending?(repo, hub_id, runtime_revision, "<")
  end

  def pending_through?(hub_id, runtime_revision),
    do: pending_through?(Repo, hub_id, runtime_revision)

  def pending_through?(repo, hub_id, runtime_revision) do
    pending?(repo, hub_id, runtime_revision, "<=")
  end

  defp claim_next_validated(repo, claim_owner, claim_token, ttl_seconds) do
    transaction_result =
      repo.transaction(fn ->
        case SQL.query(
               repo,
               """
               WITH candidate AS (
                 SELECT candidate.id
                 FROM ret0.bot_runtime_outbox AS candidate
                 WHERE candidate.delivered_at IS NULL
                   AND candidate.next_attempt_at <= timezone('UTC', clock_timestamp())
                   AND (
                     candidate.claim_token IS NULL
                     OR candidate.claim_expires_at <= timezone('UTC', clock_timestamp())
                   )
                   AND NOT EXISTS (
                     SELECT 1
                     FROM ret0.bot_runtime_outbox AS predecessor
                     WHERE predecessor.hub_id = candidate.hub_id
                       AND predecessor.runtime_revision < candidate.runtime_revision
                       AND predecessor.delivered_at IS NULL
                   )
                 ORDER BY candidate.next_attempt_at, candidate.id
                 FOR UPDATE OF candidate SKIP LOCKED
                 LIMIT 1
               )
               UPDATE ret0.bot_runtime_outbox AS outbox
               SET claim_owner = $1,
                   claim_token = $2::text::uuid,
                   claim_expires_at = timezone('UTC', clock_timestamp()) +
                     ($3::bigint * interval '1 second'),
                   attempt_count = outbox.attempt_count + 1,
                   updated_at = timezone('UTC', clock_timestamp())
               FROM candidate
               WHERE outbox.id = candidate.id
               RETURNING outbox.id
               """,
               [claim_owner, claim_token, ttl_seconds]
             ) do
          {:ok, %{rows: []}} ->
            :empty

          {:ok, %{rows: [[id]]}} ->
            {:ok, repo.get!(BotRuntimeOutbox, id)}

          {:error, _reason} ->
            repo.rollback(:database_unavailable)
        end
      end)

    case transaction_result do
      {:ok, result} -> result
      {:error, _reason} -> {:error, :database_unavailable}
    end
  rescue
    _error -> {:error, :database_unavailable}
  catch
    :exit, _reason -> {:error, :database_unavailable}
  end

  defp pending?(repo, hub_id, runtime_revision, operator)
       when is_integer(hub_id) and hub_id > 0 and is_integer(runtime_revision) and
              runtime_revision > 0 and operator in ["<", "<="] do
    case SQL.query(
           repo,
           """
           SELECT EXISTS (
             SELECT 1
             FROM ret0.bot_runtime_outbox
             WHERE hub_id = $1
               AND runtime_revision #{operator} $2
               AND delivered_at IS NULL
           )
           """,
           [hub_id, runtime_revision]
         ) do
      {:ok, %{rows: [[pending]]}} -> pending
      {:error, _reason} -> {:error, :database_unavailable}
    end
  rescue
    _error -> {:error, :database_unavailable}
  catch
    :exit, _reason -> {:error, :database_unavailable}
  end

  defp pending?(_repo, _hub_id, _runtime_revision, _operator),
    do: {:error, :invalid_pending_query}

  defp normalize_attrs(attrs) do
    keys = %{
      "operation_id" => :operation_id,
      "hub_id" => :hub_id,
      "hub_sid" => :hub_sid,
      "runtime_revision" => :runtime_revision,
      "bots" => :bots,
      "runtime_chat_enabled" => :runtime_chat_enabled,
      "revoke_epoch" => :revoke_epoch
    }

    Enum.reduce(attrs, %{}, fn
      {key, value}, normalized when is_atom(key) ->
        Map.put(normalized, key, value)

      {key, value}, normalized when is_binary(key) ->
        case Map.fetch(keys, key) do
          {:ok, normalized_key} -> Map.put(normalized, normalized_key, value)
          :error -> normalized
        end

      _entry, normalized ->
        normalized
    end)
  end

  defp put_runtime_chat_enabled(attrs) do
    bots = Map.get(attrs, :bots)
    runtime_chat_enabled = BotConfig.normalize(%{"bots" => bots})["chat_enabled"]
    Map.put(attrs, :runtime_chat_enabled, runtime_chat_enabled)
  end

  defp ensure_operation_id(attrs) do
    if Map.has_key?(attrs, :operation_id) or Map.has_key?(attrs, "operation_id") do
      attrs
    else
      Map.put(attrs, :operation_id, Ecto.UUID.generate())
    end
  end

  defp valid_seconds?(value, minimum, maximum) do
    is_integer(value) and value >= minimum and value <= maximum
  end
end
