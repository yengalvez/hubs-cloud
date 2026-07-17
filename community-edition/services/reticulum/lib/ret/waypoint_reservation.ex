defmodule Ret.WaypointReservation do
  @moduledoc """
  Authoritative, per-hub waypoint leases.

  Every mutation is serialized by a PostgreSQL advisory transaction lock. One row per
  hub/session acts both as the active reservation and as an idempotency tombstone.
  """

  use Ecto.Schema

  import Ecto.Query

  alias Ret.{Locking, Repo, WaypointReservation}

  @schema_prefix "ret0"
  @primary_key {:waypoint_reservation_id, :id, autogenerate: true}

  @protocol 2
  @lease_seconds 15
  @request_timeout_ms 3_000
  @max_waypoint_id_bytes 512
  @max_request_seq 9_007_199_254_740_991
  @max_state_version 9_007_199_254_740_991
  @state_version_sequence "ret0.waypoint_reservation_state_version_seq"
  @rate_limit 30
  @rate_window_seconds 10
  # The pre-lock ceiling is intentionally no higher than the durable DB
  # ceiling. Exact retries remain idempotent, but every wire attempt is bounded.
  @front_rate_limit @rate_limit
  @front_rate_window_ms 10_000
  @tombstone_retention_seconds 7 * 24 * 60 * 60
  @request_keys ~w(protocol action waypoint_id operation_id reservation_id request_seq)
  @actions ~w(reserve renew release)

  schema "waypoint_reservations" do
    belongs_to :hub, Ret.Hub, references: :hub_id

    field :session_id, Ecto.UUID
    field :client_instance_id, Ecto.UUID
    field :channel_id, Ecto.UUID

    field :waypoint_id, :string
    field :operation_id, Ecto.UUID
    field :reservation_id, Ecto.UUID
    field :expires_at, :utc_datetime_usec
    field :state_version, :integer

    field :last_request_seq, :integer, default: 0
    field :last_request_action, :string
    field :last_request_waypoint_id, :string
    field :last_request_operation_id, Ecto.UUID
    field :last_request_reservation_id, Ecto.UUID
    field :last_response, :map

    field :rate_window_started_at, :utc_datetime_usec
    field :rate_request_count, :integer, default: 0

    timestamps(type: :utc_datetime_usec)
  end

  def protocol, do: @protocol

  @doc "Returns the exact public wire-semantics contract used for rollout negotiation."
  def capability_contract do
    %{
      protocol: @protocol,
      state_version: "monotonic_safe_integer",
      snapshot_state_version: "strictly_greater_than_events"
    }
  end

  def lease_ms, do: @lease_seconds * 1_000
  def request_timeout_ms, do: @request_timeout_ms
  def rate_limit, do: @rate_limit
  def front_rate_limit, do: @front_rate_limit
  def tombstone_retention_seconds, do: @tombstone_retention_seconds

  @doc "Returns the accepted client instance UUID, or `:unsupported` for legacy/invalid/bot joins."
  def join_client_instance(%{"protocol" => @protocol, "client_instance_id" => value} = request)
      when map_size(request) == 2 do
    case cast_uuid(value) do
      {:ok, uuid} -> {:ok, uuid}
      :error -> :unsupported
    end
  end

  def join_client_instance(_), do: :unsupported

  def unsupported_capability do
    %{
      protocol: @protocol,
      supported: false,
      lease_ms: lease_ms(),
      request_timeout_ms: @request_timeout_ms,
      snapshot_state_version: nil,
      active: [],
      current: nil,
      request_seq: 0
    }
  end

  @doc "Registers a fresh channel and returns public hub state plus this session's private lease."
  def register_channel(hub_id, session_id, client_instance_id, channel_id, now \\ now()) do
    with {:ok, session_id} <- cast_uuid(session_id),
         {:ok, client_instance_id} <- cast_uuid(client_instance_id),
         {:ok, channel_id} <- cast_uuid(channel_id) do
      with_hub_lock(hub_id, fn state_version ->
        {expired_states, _expired_count} = expire_leases(hub_id, now, state_version)
        purge_stale_tombstones(hub_id, now)

        row = Repo.get_by(WaypointReservation, hub_id: hub_id, session_id: session_id)

        {row, migration_states} =
          register_row(
            row,
            hub_id,
            session_id,
            client_instance_id,
            channel_id,
            now,
            state_version
          )

        %{
          active: active_for_hub(hub_id, now),
          current: private_current(row),
          request_seq: row.last_request_seq,
          states: dedupe_states(expired_states ++ migration_states),
          # A delayed pre-join broadcast has a lower version than this barrier.
          # Take it last, while the per-hub transaction lock is still held.
          snapshot_state_version: next_state_version!()
        }
      end)
    else
      :error -> {:error, :invalid}
    end
  end

  @doc "Processes a validated protocol request under the hub lock."
  def request(
        hub_id,
        session_id,
        client_instance_id,
        channel_id,
        payload,
        authorization,
        now \\ now()
      ) do
    request_with_dependencies(
      hub_id,
      session_id,
      client_instance_id,
      channel_id,
      payload,
      authorization,
      now,
      []
    )
  end

  @doc false
  def request_with_dependencies(
        hub_id,
        session_id,
        client_instance_id,
        channel_id,
        payload,
        authorization,
        now,
        options
      ) do
    parsed_request = parse_request(payload)
    front_limiter = Keyword.get(options, :front_limiter, &front_rate_check/4)
    lock = Keyword.get(options, :with_hub_lock, &with_hub_lock/2)

    case front_limiter.(hub_id, session_id, channel_id, now) do
      :limited ->
        request =
          case parsed_request do
            {:ok, value} -> value
            :error -> nil
          end

        %{response: public_error(request, "rate_limited"), states: []}

      :ok ->
        lock.(hub_id, fn state_version ->
          {expired_states, _expired_count} = expire_leases(hub_id, now, state_version)

          case parsed_request do
            {:ok, request} ->
              row = Repo.get_by(WaypointReservation, hub_id: hub_id, session_id: session_id)

              {response, request_states} =
                process_for_channel(
                  row,
                  client_instance_id,
                  channel_id,
                  request,
                  authorization,
                  now,
                  state_version
                )

              %{response: response, states: dedupe_states(expired_states ++ request_states)}

            :error ->
              %{response: public_error(nil, "invalid"), states: expired_states}
          end
        end)
    end
  end

  @doc false
  def front_rate_check(hub_id, session_id, _channel_id, %DateTime{} = now) do
    now_ms = DateTime.to_unix(now, :millisecond)
    window = div(now_ms, @front_rate_window_ms)
    expires_at = (window + 1) * @front_rate_window_ms
    # Keep the quota across channel migrations/reconnects for the same session.
    # The supervised PlugAttack ETS cleaner removes this fixed-window key at TTL.
    key = {:waypoint_reservation, hub_id, session_id, window}

    count =
      PlugAttack.Storage.Ets.increment(
        RetWeb.RateLimit.Storage,
        key,
        1,
        expires_at
      )

    if count > @front_rate_limit, do: :limited, else: :ok
  end

  @doc "Releases only when the terminating channel is still authoritative for the session."
  def terminate_channel(hub_id, session_id, client_instance_id, channel_id, now \\ now()) do
    with_hub_lock(hub_id, fn state_version ->
      {expired_states, _expired_count} = expire_leases(hub_id, now, state_version)
      row = Repo.get_by(WaypointReservation, hub_id: hub_id, session_id: session_id)

      terminate_states =
        if current_channel?(row, client_instance_id, channel_id) and active?(row, now) do
          state = state(row.waypoint_id, false, nil, state_version)
          clear_active!(row, state_version)
          [state]
        else
          []
        end

      %{states: dedupe_states(expired_states ++ terminate_states)}
    end)
  end

  def public_error(request, reason) do
    public_response(request, "error", reason, nil, nil, nil)
  end

  def unsupported_response(payload) do
    case parse_request(payload) do
      {:ok, request} -> public_error(request, "unsupported")
      :error -> public_error(nil, "unsupported")
    end
  end

  defp register_row(
         nil,
         hub_id,
         session_id,
         client_instance_id,
         channel_id,
         now,
         state_version
       ) do
    row =
      %WaypointReservation{
        hub_id: hub_id,
        session_id: session_id,
        client_instance_id: client_instance_id,
        channel_id: channel_id,
        state_version: state_version,
        rate_window_started_at: now,
        rate_request_count: 0
      }
      |> Repo.insert!()

    {row, []}
  end

  defp register_row(
         row,
         _hub_id,
         _session_id,
         client_instance_id,
         channel_id,
         now,
         state_version
       ) do
    same_client? = uuid_equal?(row.client_instance_id, client_instance_id)

    old_state =
      if !same_client? && active?(row, now),
        do: [state(row.waypoint_id, false, nil, state_version)],
        else: []

    attrs =
      if same_client? do
        %{channel_id: channel_id}
      else
        %{
          client_instance_id: client_instance_id,
          channel_id: channel_id,
          waypoint_id: nil,
          operation_id: nil,
          reservation_id: nil,
          expires_at: nil,
          state_version: state_version,
          last_request_seq: 0,
          last_request_action: nil,
          last_request_waypoint_id: nil,
          last_request_operation_id: nil,
          last_request_reservation_id: nil,
          last_response: nil,
          rate_window_started_at: now,
          rate_request_count: 0
        }
      end

    row = row |> Ecto.Changeset.change(attrs) |> Repo.update!()
    {row, old_state}
  end

  defp process_for_channel(
         row,
         client_instance_id,
         channel_id,
         request,
         authorization,
         now,
         state_version
       ) do
    cond do
      !current_channel?(row, client_instance_id, channel_id) ->
        {public_error(request, "stale_channel"), []}

      request.request_seq == row.last_request_seq && same_fingerprint?(row, request) ->
        {row.last_response, []}

      request.request_seq <= row.last_request_seq ->
        {public_error(request, "stale_request"), []}

      true ->
        process_new_request(row, request, authorization, now, state_version)
    end
  end

  defp process_new_request(row, request, authorization, now, state_version) do
    {rate_attrs, rate_limited?} = next_rate(row, now)

    cond do
      rate_limited? ->
        cache_response(row, request, public_error(request, "rate_limited"), rate_attrs, %{}, [])

      authorization[:bot_runner] == true ->
        cache_response(row, request, public_error(request, "unsupported"), rate_attrs, %{}, [])

      authorization[:allowed] != true ->
        cache_response(row, request, public_error(request, "not_entering"), rate_attrs, %{}, [])

      true ->
        apply_action(row, request, rate_attrs, now, state_version)
    end
  end

  defp apply_action(row, %{action: "reserve"} = request, rate_attrs, now, state_version) do
    conflict =
      Repo.one(
        from reservation in WaypointReservation,
          where: reservation.hub_id == ^row.hub_id,
          where: reservation.waypoint_id == ^request.waypoint_id,
          where: reservation.waypoint_reservation_id != ^row.waypoint_reservation_id,
          where: reservation.expires_at > ^now,
          limit: 1
      )

    if conflict do
      cache_response(row, request, public_error(request, "occupied"), rate_attrs, %{}, [])
    else
      expires_at = DateTime.add(now, @lease_seconds, :second)

      old_states =
        if active?(row, now) && row.waypoint_id != request.waypoint_id,
          do: [state(row.waypoint_id, false, nil, state_version)],
          else: []

      active_attrs = %{
        waypoint_id: request.waypoint_id,
        operation_id: request.operation_id,
        reservation_id: request.reservation_id,
        expires_at: expires_at,
        state_version: state_version
      }

      response = public_response(request, "ok", nil, true, expires_at, state_version)
      states = old_states ++ [state(request.waypoint_id, true, expires_at, state_version)]
      cache_response(row, request, response, rate_attrs, active_attrs, states)
    end
  end

  defp apply_action(row, %{action: "renew"} = request, rate_attrs, now, state_version) do
    if owns_requested_lease?(row, request, now) do
      expires_at = DateTime.add(now, @lease_seconds, :second)

      active_attrs = %{
        operation_id: request.operation_id,
        expires_at: expires_at,
        state_version: state_version
      }

      response = public_response(request, "ok", nil, true, expires_at, state_version)

      cache_response(
        row,
        request,
        response,
        rate_attrs,
        active_attrs,
        [state(request.waypoint_id, true, expires_at, state_version)]
      )
    else
      cache_response(row, request, public_error(request, "stale_request"), rate_attrs, %{}, [])
    end
  end

  defp apply_action(row, %{action: "release"} = request, rate_attrs, now, state_version) do
    if owns_requested_lease?(row, request, now) do
      active_attrs = %{
        waypoint_id: nil,
        operation_id: nil,
        reservation_id: nil,
        expires_at: nil,
        state_version: state_version
      }

      response = public_response(request, "ok", nil, false, nil, state_version)

      cache_response(
        row,
        request,
        response,
        rate_attrs,
        active_attrs,
        [state(request.waypoint_id, false, nil, state_version)]
      )
    else
      cache_response(row, request, public_error(request, "stale_request"), rate_attrs, %{}, [])
    end
  end

  defp cache_response(row, request, response, rate_attrs, active_attrs, states) do
    attrs =
      rate_attrs
      |> Map.merge(active_attrs)
      |> Map.merge(%{
        last_request_seq: request.request_seq,
        last_request_action: request.action,
        last_request_waypoint_id: request.waypoint_id,
        last_request_operation_id: request.operation_id,
        last_request_reservation_id: request.reservation_id,
        last_response: response
      })

    row |> Ecto.Changeset.change(attrs) |> Repo.update!()
    {response, states}
  end

  defp expire_leases(hub_id, now, state_version) do
    expired =
      Repo.all(
        from reservation in WaypointReservation,
          where: reservation.hub_id == ^hub_id,
          where: not is_nil(reservation.waypoint_id),
          where: reservation.expires_at <= ^now
      )

    Enum.each(expired, &clear_active!(&1, state_version))

    {Enum.map(expired, &state(&1.waypoint_id, false, nil, state_version)), length(expired)}
  end

  defp purge_stale_tombstones(hub_id, now) do
    cutoff = DateTime.add(now, -@tombstone_retention_seconds, :second)

    Repo.delete_all(
      from reservation in WaypointReservation,
        where: reservation.hub_id == ^hub_id,
        where: is_nil(reservation.waypoint_id),
        where: reservation.updated_at < ^cutoff
    )
  end

  defp clear_active!(row, state_version) do
    row
    |> Ecto.Changeset.change(%{
      waypoint_id: nil,
      operation_id: nil,
      reservation_id: nil,
      expires_at: nil,
      state_version: state_version
    })
    |> Repo.update!()
  end

  defp active_for_hub(hub_id, now) do
    Repo.all(
      from reservation in WaypointReservation,
        where: reservation.hub_id == ^hub_id,
        where: not is_nil(reservation.waypoint_id),
        where: reservation.expires_at > ^now,
        order_by: reservation.waypoint_id,
        select: %{
          waypoint_id: reservation.waypoint_id,
          expires_at: reservation.expires_at,
          state_version: reservation.state_version
        }
    )
    |> Enum.map(fn reservation ->
      %{
        waypoint_id: reservation.waypoint_id,
        expires_at: iso8601(reservation.expires_at),
        state_version: reservation.state_version
      }
    end)
  end

  defp private_current(row) do
    if row.waypoint_id do
      %{
        waypoint_id: row.waypoint_id,
        operation_id: row.operation_id,
        reservation_id: row.reservation_id,
        expires_at: iso8601(row.expires_at),
        state_version: row.state_version
      }
    else
      nil
    end
  end

  defp parse_request(%{} = payload) when map_size(payload) == 6 do
    if Enum.sort(Map.keys(payload)) == Enum.sort(@request_keys) do
      with @protocol <- payload["protocol"],
           action when action in @actions <- payload["action"],
           {:ok, waypoint_id} <- cast_waypoint_id(payload["waypoint_id"]),
           {:ok, operation_id} <- cast_uuid(payload["operation_id"]),
           {:ok, reservation_id} <- cast_uuid(payload["reservation_id"]),
           request_seq
           when is_integer(request_seq) and request_seq > 0 and
                  request_seq <= @max_request_seq <- payload["request_seq"] do
        {:ok,
         %{
           action: action,
           waypoint_id: waypoint_id,
           operation_id: operation_id,
           reservation_id: reservation_id,
           request_seq: request_seq
         }}
      else
        _ -> :error
      end
    else
      :error
    end
  end

  defp parse_request(_), do: :error

  defp cast_waypoint_id(value)
       when is_binary(value) and byte_size(value) > 0 and
              byte_size(value) <= @max_waypoint_id_bytes do
    if String.valid?(value) and !String.contains?(value, <<0>>), do: {:ok, value}, else: :error
  end

  defp cast_waypoint_id(_), do: :error

  defp cast_uuid(value) do
    case Ecto.UUID.cast(value) do
      {:ok, uuid} -> {:ok, uuid}
      :error -> :error
    end
  end

  defp current_channel?(%WaypointReservation{} = row, client_instance_id, channel_id) do
    uuid_equal?(row.client_instance_id, client_instance_id) &&
      uuid_equal?(row.channel_id, channel_id)
  end

  defp current_channel?(_, _, _), do: false

  defp uuid_equal?(stored, candidate) do
    case cast_uuid(candidate) do
      {:ok, uuid} -> stored == uuid
      :error -> false
    end
  end

  defp same_fingerprint?(row, request) do
    row.last_request_action == request.action &&
      row.last_request_waypoint_id == request.waypoint_id &&
      uuid_equal?(row.last_request_operation_id, request.operation_id) &&
      uuid_equal?(row.last_request_reservation_id, request.reservation_id)
  end

  defp owns_requested_lease?(row, request, now) do
    active?(row, now) && row.waypoint_id == request.waypoint_id &&
      uuid_equal?(row.reservation_id, request.reservation_id)
  end

  defp active?(%WaypointReservation{waypoint_id: waypoint_id, expires_at: expires_at}, now)
       when not is_nil(waypoint_id) and not is_nil(expires_at),
       do: DateTime.compare(expires_at, now) == :gt

  defp active?(_, _), do: false

  defp next_rate(row, now) do
    window_expired? =
      DateTime.diff(now, row.rate_window_started_at, :second) >= @rate_window_seconds

    {window_started_at, count} =
      if window_expired?,
        do: {now, 1},
        else: {row.rate_window_started_at, row.rate_request_count + 1}

    {%{rate_window_started_at: window_started_at, rate_request_count: count}, count > @rate_limit}
  end

  defp public_response(request, status, reason, occupied, expires_at, state_version) do
    %{
      "protocol" => @protocol,
      "status" => status,
      "reason" => reason,
      "action" => request && request.action,
      "waypoint_id" => request && request.waypoint_id,
      "operation_id" => request && request.operation_id,
      "reservation_id" => request && request.reservation_id,
      "request_seq" => request && request.request_seq,
      "occupied" => occupied,
      "expires_at" => iso8601(expires_at),
      "state_version" => state_version
    }
  end

  defp state(waypoint_id, occupied, expires_at, state_version) do
    %{
      "protocol" => @protocol,
      "waypoint_id" => waypoint_id,
      "occupied" => occupied,
      "expires_at" => iso8601(expires_at),
      "state_version" => state_version
    }
  end

  defp dedupe_states(states) do
    states
    |> Enum.reverse()
    |> Enum.uniq_by(& &1["waypoint_id"])
    |> Enum.reverse()
  end

  defp iso8601(nil), do: nil
  defp iso8601(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp with_hub_lock(hub_id, fun) do
    operation = fn -> fun.(next_state_version!()) end

    case Locking.exec_after_lock("waypoint-reservations:#{hub_id}", operation) do
      {:ok, result} -> result
      {:error, reason} -> raise "waypoint reservation transaction failed: #{inspect(reason)}"
    end
  end

  defp next_state_version! do
    case Ecto.Adapters.SQL.query!(
           Repo,
           "SELECT nextval('#{@state_version_sequence}')",
           []
         ) do
      %Postgrex.Result{rows: [[version]]}
      when is_integer(version) and version > 0 and version <= @max_state_version ->
        version

      result ->
        raise "invalid waypoint reservation state version: #{inspect(result)}"
    end
  end

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:microsecond)
end
