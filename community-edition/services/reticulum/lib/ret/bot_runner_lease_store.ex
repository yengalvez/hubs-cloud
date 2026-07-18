defmodule Ret.BotRunnerLease.Store do
  @moduledoc false

  @type context :: term()
  @type epoch :: pos_integer()

  @callback acquire(
              context(),
              String.t(),
              String.t(),
              String.t(),
              String.t(),
              pos_integer()
            ) :: {:ok, %{epoch: epoch()}} | {:error, :unavailable | :database_unavailable}
  @callback renew(
              context(),
              String.t(),
              String.t(),
              String.t(),
              String.t(),
              epoch(),
              pos_integer()
            ) :: {:ok, %{epoch: epoch()}} | {:error, :lost | :database_unavailable}
  @callback with_authority(
              context(),
              String.t(),
              String.t(),
              String.t(),
              String.t(),
              epoch(),
              (-> term())
            ) :: {:ok, term()} | {:error, :not_authoritative | :database_unavailable}
  @callback with_current_authority(context(), String.t(), (map() -> term())) ::
              {:ok, term()} | {:error, :not_authoritative | :database_unavailable}
  @callback release(
              context(),
              String.t(),
              String.t(),
              String.t(),
              String.t(),
              epoch()
            ) :: {:ok, epoch() | nil} | {:error, :database_unavailable}
  @callback revoke(context(), String.t()) ::
              {:ok, epoch() | nil} | {:error, :database_unavailable}
  @callback current(context(), String.t()) ::
              {:ok, map() | nil} | {:error, :database_unavailable}
end

defmodule Ret.BotRunnerLease.PostgresStore do
  @moduledoc """
  PostgreSQL authority for bot-runner leases.

  Operations take the per-room transaction advisory lock before the lease row
  lock. Acquire, renew and release validate the complete holder, session,
  lease and epoch tuple with the database clock. Every new authority boundary
  consumes the global JavaScript-safe, non-cycling epoch sequence.
  """

  @behaviour Ret.BotRunnerLease.Store

  alias Ecto.Adapters.SQL

  @authority_lock_prefix "bot-runner-authority:v1:"

  @impl true
  def acquire(repo, hub_sid, lease_id, holder_instance_id, session_id, ttl_seconds) do
    transaction(repo, fn ->
      lock_authority!(repo, hub_sid)

      case hub_id(repo, hub_sid) do
        nil ->
          {:error, :unavailable}

        hub_id ->
          current = lock_current!(repo, hub_id)

          if current && current.active do
            {:error, :unavailable}
          else
            epoch = next_epoch!(repo)

            if current do
              SQL.query!(
                repo,
                """
                UPDATE ret0.bot_runner_leases
                SET lease_id = $2::text::uuid,
                    holder_instance_id = $3::text::uuid,
                    session_id = $4::text::uuid,
                    authority_epoch = $5,
                    expires_at = timezone('UTC', clock_timestamp()) +
                      ($6::bigint * INTERVAL '1 second'),
                    updated_at = timezone('UTC', clock_timestamp())
                WHERE hub_id = $1
                """,
                [hub_id, lease_id, holder_instance_id, session_id, epoch, ttl_seconds]
              )
            else
              SQL.query!(
                repo,
                """
                INSERT INTO ret0.bot_runner_leases (
                  hub_id,
                  lease_id,
                  holder_instance_id,
                  session_id,
                  authority_epoch,
                  expires_at,
                  inserted_at,
                  updated_at
                )
                VALUES (
                  $1,
                  $2::text::uuid,
                  $3::text::uuid,
                  $4::text::uuid,
                  $5,
                  timezone('UTC', clock_timestamp()) + ($6::bigint * INTERVAL '1 second'),
                  timezone('UTC', clock_timestamp()),
                  timezone('UTC', clock_timestamp())
                )
                """,
                [hub_id, lease_id, holder_instance_id, session_id, epoch, ttl_seconds]
              )
            end

            {:ok, %{epoch: epoch}}
          end
      end
    end)
  end

  @impl true
  def renew(
        repo,
        hub_sid,
        lease_id,
        holder_instance_id,
        session_id,
        epoch,
        ttl_seconds
      ) do
    transaction(repo, fn ->
      lock_authority!(repo, hub_sid)

      case hub_id(repo, hub_sid) do
        nil ->
          {:error, :lost}

        hub_id ->
          current = lock_current!(repo, hub_id)

          if exact_live?(current, lease_id, holder_instance_id, session_id, epoch) do
            case SQL.query!(
                   repo,
                   """
                   UPDATE ret0.bot_runner_leases
                   SET expires_at = timezone('UTC', clock_timestamp()) +
                         ($6::bigint * INTERVAL '1 second'),
                       updated_at = timezone('UTC', clock_timestamp())
                   WHERE hub_id = $1
                     AND lease_id = $2::text::uuid
                     AND holder_instance_id = $3::text::uuid
                     AND session_id = $4::text::uuid
                     AND authority_epoch = $5
                     AND expires_at > timezone('UTC', clock_timestamp())
                   """,
                   [hub_id, lease_id, holder_instance_id, session_id, epoch, ttl_seconds]
                 ) do
              %{num_rows: 1} -> {:ok, %{epoch: epoch}}
              %{num_rows: 0} -> {:error, :lost}
            end
          else
            {:error, :lost}
          end
      end
    end)
  end

  @impl true
  def with_authority(
        repo,
        hub_sid,
        lease_id,
        holder_instance_id,
        session_id,
        epoch,
        fun
      ) do
    transaction(repo, fn ->
      lock_authority!(repo, hub_sid)

      case hub_id(repo, hub_sid) do
        nil ->
          {:error, :not_authoritative}

        hub_id ->
          current = lock_current!(repo, hub_id)

          if exact_live?(current, lease_id, holder_instance_id, session_id, epoch) do
            {:ok, fun.()}
          else
            {:error, :not_authoritative}
          end
      end
    end)
  end

  @impl true
  def with_current_authority(repo, hub_sid, fun) do
    transaction(repo, fn ->
      lock_authority!(repo, hub_sid)

      case hub_id(repo, hub_sid) do
        nil ->
          {:error, :not_authoritative}

        hub_id ->
          current = lock_current!(repo, hub_id)

          if current && current.active do
            {:ok,
             fun.(%{
               authority_epoch: current.epoch,
               holder_instance_id: current.holder_instance_id,
               lease_id: current.lease_id,
               session_id: current.session_id
             })}
          else
            {:error, :not_authoritative}
          end
      end
    end)
  end

  @impl true
  def release(repo, hub_sid, lease_id, holder_instance_id, session_id, epoch) do
    transaction(repo, fn ->
      lock_authority!(repo, hub_sid)

      case hub_id(repo, hub_sid) do
        nil ->
          {:ok, nil}

        hub_id ->
          current = lock_current!(repo, hub_id)

          if exact?(current, lease_id, holder_instance_id, session_id, epoch) do
            fenced_epoch = next_epoch!(repo)
            clear_authority!(repo, hub_id, fenced_epoch)
            {:ok, fenced_epoch}
          else
            {:ok, nil}
          end
      end
    end)
  end

  @impl true
  def revoke(repo, hub_sid) do
    transaction(repo, fn ->
      lock_authority!(repo, hub_sid)

      case hub_id(repo, hub_sid) do
        nil ->
          {:ok, nil}

        hub_id ->
          current = lock_current!(repo, hub_id)
          fenced_epoch = next_epoch!(repo)

          if current do
            clear_authority!(repo, hub_id, fenced_epoch)
          else
            insert_empty_authority!(repo, hub_id, fenced_epoch)
          end

          {:ok, fenced_epoch}
      end
    end)
  end

  @impl true
  def current(repo, hub_sid) do
    case SQL.query(
           repo,
           """
           SELECT
             leases.lease_id::text,
             leases.holder_instance_id::text,
             leases.session_id::text,
             leases.authority_epoch
           FROM ret0.bot_runner_leases AS leases
           JOIN ret0.hubs AS hubs ON hubs.hub_id = leases.hub_id
           WHERE hubs.hub_sid = $1
             AND leases.lease_id IS NOT NULL
             AND leases.expires_at > timezone('UTC', clock_timestamp())
           """,
           [hub_sid]
         ) do
      {:ok, %{rows: [[lease_id, holder_instance_id, session_id, epoch]]}} ->
        {:ok,
         %{
           epoch: epoch,
           holder_instance_id: holder_instance_id,
           lease_id: lease_id,
           session_id: session_id
         }}

      {:ok, %{rows: []}} ->
        {:ok, nil}

      {:error, _reason} ->
        {:error, :database_unavailable}
    end
  rescue
    _error in [DBConnection.ConnectionError, Postgrex.Error] ->
      {:error, :database_unavailable}
  catch
    :exit, _reason -> {:error, :database_unavailable}
  end

  defp transaction(repo, fun) do
    case repo.transaction(fun) do
      {:ok, result} -> result
      {:error, expected_reason} when is_atom(expected_reason) -> {:error, expected_reason}
    end
  rescue
    _error in [DBConnection.ConnectionError, Postgrex.Error] ->
      {:error, :database_unavailable}
  catch
    :exit, _reason -> {:error, :database_unavailable}
  end

  defp lock_authority!(repo, hub_sid) do
    SQL.query!(
      repo,
      "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
      [@authority_lock_prefix <> hub_sid]
    )
  end

  defp hub_id(repo, hub_sid) do
    case SQL.query!(repo, "SELECT hub_id FROM ret0.hubs WHERE hub_sid = $1", [hub_sid]) do
      %{rows: [[hub_id]]} -> hub_id
      %{rows: []} -> nil
    end
  end

  defp lock_current!(repo, hub_id) do
    case SQL.query!(
           repo,
           """
           SELECT
             lease_id::text,
             holder_instance_id::text,
             session_id::text,
             authority_epoch,
             expires_at IS NOT NULL AND
               expires_at > timezone('UTC', clock_timestamp()) AS active
           FROM ret0.bot_runner_leases
           WHERE hub_id = $1
           FOR UPDATE
           """,
           [hub_id]
         ) do
      %{rows: [[lease_id, holder_instance_id, session_id, epoch, active]]} ->
        %{
          active: active === true,
          epoch: epoch,
          holder_instance_id: holder_instance_id,
          lease_id: lease_id,
          session_id: session_id
        }

      %{rows: []} ->
        nil
    end
  end

  defp exact_live?(current, lease_id, holder_instance_id, session_id, epoch) do
    current && current.active && exact?(current, lease_id, holder_instance_id, session_id, epoch)
  end

  defp exact?(current, lease_id, holder_instance_id, session_id, epoch) do
    current && current.lease_id == lease_id &&
      current.holder_instance_id == holder_instance_id && current.session_id == session_id &&
      current.epoch == epoch
  end

  defp next_epoch!(repo) do
    %{rows: [[epoch]]} =
      SQL.query!(repo, "SELECT nextval('ret0.bot_runner_authority_epoch_seq')", [])

    epoch
  end

  defp clear_authority!(repo, hub_id, fenced_epoch) do
    %{num_rows: 1} =
      SQL.query!(
        repo,
        """
        UPDATE ret0.bot_runner_leases
        SET lease_id = NULL,
            holder_instance_id = NULL,
            session_id = NULL,
            authority_epoch = $2,
            expires_at = NULL,
            updated_at = timezone('UTC', clock_timestamp())
        WHERE hub_id = $1
        """,
        [hub_id, fenced_epoch]
      )
  end

  defp insert_empty_authority!(repo, hub_id, fenced_epoch) do
    %{num_rows: 1} =
      SQL.query!(
        repo,
        """
        INSERT INTO ret0.bot_runner_leases (
          hub_id,
          authority_epoch,
          inserted_at,
          updated_at
        )
        VALUES (
          $1,
          $2,
          timezone('UTC', clock_timestamp()),
          timezone('UTC', clock_timestamp())
        )
        """,
        [hub_id, fenced_epoch]
      )
  end
end
