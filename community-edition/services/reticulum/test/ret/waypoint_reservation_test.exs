defmodule Ret.WaypointReservationTest do
  use Ret.DataCase

  import Ret.TestHelpers
  import Ecto.Query

  alias Ret.{Repo, WaypointReservation}

  setup [:create_account, :create_owned_file, :create_scene, :create_hub]

  @now ~U[2026-07-17 10:00:00.000000Z]
  @allowed %{allowed: true, bot_runner: false}
  @not_entering %{allowed: false, bot_runner: false}

  test "concurrent contenders produce one winner and public state contains no identity", %{
    hub: hub
  } do
    first = actor()
    second = actor()
    register(hub, first)
    register(hub, second)

    first_request = request(1, "reserve", "seat-a")
    second_request = request(1, "reserve", "seat-a")
    parent = self()

    tasks =
      [{first, first_request}, {second, second_request}]
      |> Enum.map(fn {actor, payload} ->
        Task.async(fn ->
          send(parent, {:ready, self()})

          receive do
            :go -> reserve(hub, actor, payload)
          end
        end)
      end)

    ready_pids =
      Enum.map(tasks, fn _task ->
        assert_receive {:ready, task_pid}
        task_pid
      end)

    assert MapSet.new(ready_pids) == MapSet.new(Enum.map(tasks, & &1.pid))
    Enum.each(tasks, &Ecto.Adapters.SQL.Sandbox.allow(Repo, self(), &1.pid))

    Enum.each(tasks, &send(&1.pid, :go))
    results = Enum.map(tasks, &Task.await(&1, 5_000))

    assert Enum.sort(Enum.map(results, & &1.response["status"])) == ["error", "ok"]

    occupied = Enum.find(results, &(&1.response["reason"] == "occupied"))
    assert occupied.response["expires_at"] == nil
    assert occupied.response["occupied"] == nil

    for result <- results,
        state <- result.states do
      assert Map.keys(state) |> Enum.sort() ==
               ~w(expires_at occupied protocol waypoint_id) |> Enum.sort()

      refute Map.has_key?(state, "session_id")
      refute Map.has_key?(state, "client_instance_id")
      refute Map.has_key?(state, "channel_id")
      refute Map.has_key?(state, "reservation_id")
      refute Map.has_key?(state, "operation_id")
    end

    observer = actor()
    join = register(hub, observer)

    assert [%{waypoint_id: "seat-a", expires_at: expires_at}] = join.active
    assert is_binary(expires_at)
    assert join.current == nil
    assert join.request_seq == 0
  end

  test "leases survive a stale-channel crash and expire while another operation runs", %{hub: hub} do
    owner = actor()
    contender = actor()
    register(hub, owner)
    register(hub, contender)

    owner_request = request(1, "reserve", "seat-a")
    assert %{response: %{"status" => "ok"}} = reserve(hub, owner, owner_request)

    assert %{states: []} =
             WaypointReservation.terminate_channel(
               hub.hub_id,
               owner.session_id,
               owner.client_instance_id,
               SecureRandom.uuid(),
               DateTime.add(@now, 1, :second)
             )

    assert %{"reason" => "occupied"} =
             reserve(hub, contender, request(1, "reserve", "seat-a"), 14).response

    result = reserve(hub, contender, request(2, "reserve", "seat-a"), 16)
    assert result.response["status"] == "ok"
    assert [%{"waypoint_id" => "seat-a", "occupied" => true}] = result.states

    invalid = %{request(3, "renew", "seat-a") | "protocol" => 2}

    expired =
      WaypointReservation.request(
        hub.hub_id,
        contender.session_id,
        contender.client_instance_id,
        contender.channel_id,
        invalid,
        @allowed,
        DateTime.add(@now, 32, :second)
      )

    assert expired.response["reason"] == "invalid"
    assert [%{"waypoint_id" => "seat-a", "occupied" => false}] = expired.states

    observer = actor()
    expired_join = register(hub, observer, 33)
    assert expired_join.active == []
    assert expired_join.states == []
  end

  test "same-client channel migration preserves private state and a new client invalidates it", %{
    hub: hub
  } do
    original = actor()
    register(hub, original)
    reserve_request = request(1, "reserve", "seat-a")
    reserve(hub, original, reserve_request)

    migrated = %{original | channel_id: SecureRandom.uuid()}
    join = register(hub, migrated, 1)

    assert join.request_seq == 1

    assert join.current == %{
             waypoint_id: "seat-a",
             operation_id: reserve_request["operation_id"],
             reservation_id: reserve_request["reservation_id"],
             expires_at: DateTime.add(@now, 15, :second) |> DateTime.to_iso8601()
           }

    assert reserve(hub, original, request(2, "renew", "seat-a"), 1).response["reason"] ==
             "stale_channel"

    assert WaypointReservation.terminate_channel(
             hub.hub_id,
             original.session_id,
             original.client_instance_id,
             original.channel_id,
             DateTime.add(@now, 1, :second)
           ).states == []

    renew_request =
      request(2, "renew", "seat-a", reservation_id: reserve_request["reservation_id"])

    assert reserve(hub, migrated, renew_request, 2).response["status"] == "ok"

    reloaded = %{migrated | channel_id: SecureRandom.uuid()}
    reloaded_join = register(hub, reloaded, 3)
    assert reloaded_join.request_seq == 2
    assert reloaded_join.current.operation_id == renew_request["operation_id"]

    replacement = %{
      reloaded
      | client_instance_id: SecureRandom.uuid(),
        channel_id: SecureRandom.uuid()
    }

    replacement_join = register(hub, replacement, 4)
    assert replacement_join.current == nil
    assert replacement_join.request_seq == 0
    assert replacement_join.active == []
    assert [%{"occupied" => false, "waypoint_id" => "seat-a"}] = replacement_join.states

    assert reserve(hub, reloaded, request(3, "renew", "seat-a"), 4).response["reason"] ==
             "stale_channel"
  end

  test "negative responses are exactly idempotent and same-sequence mutations are stale", %{
    hub: hub
  } do
    owner = actor()
    contender = actor()
    register(hub, owner)
    register(hub, contender)

    owner_request = request(1, "reserve", "seat-a")
    reserve(hub, owner, owner_request)

    occupied_request = request(1, "reserve", "seat-a")
    occupied = reserve(hub, contender, occupied_request).response
    assert occupied["reason"] == "occupied"

    release_request =
      request(2, "release", "seat-a", reservation_id: owner_request["reservation_id"])

    assert reserve(hub, owner, release_request, 1).response["status"] == "ok"
    assert reserve(hub, contender, occupied_request, 2).response == occupied

    changed_same_sequence = %{occupied_request | "operation_id" => SecureRandom.uuid()}

    assert reserve(hub, contender, changed_same_sequence, 2).response["reason"] ==
             "stale_request"

    assert reserve(hub, contender, request(2, "reserve", "seat-a"), 2).response["status"] ==
             "ok"
  end

  test "renew and release require the stable reservation token while operation ids rotate", %{
    hub: hub
  } do
    actor = actor()
    register(hub, actor)
    initial = request(1, "reserve", "seat-a")
    reserve(hub, actor, initial)

    renew = request(2, "renew", "seat-a", reservation_id: initial["reservation_id"])
    renewed = reserve(hub, actor, renew, 5)
    assert renewed.response["status"] == "ok"
    assert renewed.response["operation_id"] == renew["operation_id"]

    assert renewed.response["expires_at"] ==
             DateTime.add(@now, 20, :second) |> DateTime.to_iso8601()

    wrong_release = request(3, "release", "seat-a")
    stale = reserve(hub, actor, wrong_release, 6).response
    assert stale["reason"] == "stale_request"
    assert stale["expires_at"] == nil
    assert reserve(hub, actor, wrong_release, 7).response == stale

    release = request(4, "release", "seat-a", reservation_id: initial["reservation_id"])
    released = reserve(hub, actor, release, 8)
    assert released.response["status"] == "ok"
    assert released.response["occupied"] == false
    assert released.response["expires_at"] == nil
  end

  test "authorization failures are cached and bot runners are always unsupported", %{hub: hub} do
    actor = actor()
    register(hub, actor)
    first = request(1, "reserve", "seat-a")

    denied =
      WaypointReservation.request(
        hub.hub_id,
        actor.session_id,
        actor.client_instance_id,
        actor.channel_id,
        first,
        @not_entering,
        @now
      ).response

    assert denied["reason"] == "not_entering"
    assert reserve(hub, actor, first, 1).response == denied
    assert reserve(hub, actor, request(2, "reserve", "seat-a"), 1).response["status"] == "ok"

    bot = actor()
    register(hub, bot)

    bot_response =
      WaypointReservation.request(
        hub.hub_id,
        bot.session_id,
        bot.client_instance_id,
        bot.channel_id,
        request(1, "reserve", "seat-b"),
        %{allowed: true, bot_runner: true},
        @now
      ).response

    assert bot_response["reason"] == "unsupported"
  end

  test "rate limiting is per tombstone and exact retries do not consume more quota", %{hub: hub} do
    actor = actor()
    register(hub, actor)

    Enum.each(1..WaypointReservation.rate_limit(), fn sequence ->
      response =
        WaypointReservation.request(
          hub.hub_id,
          actor.session_id,
          actor.client_instance_id,
          actor.channel_id,
          request(sequence, "reserve", "seat-a"),
          @not_entering,
          @now
        ).response

      assert response["reason"] == "not_entering"
    end)

    limited_request = request(WaypointReservation.rate_limit() + 1, "reserve", "seat-a")

    limited =
      WaypointReservation.request(
        hub.hub_id,
        actor.session_id,
        actor.client_instance_id,
        actor.channel_id,
        limited_request,
        @not_entering,
        @now
      ).response

    assert limited["reason"] == "rate_limited"

    assert WaypointReservation.request(
             hub.hub_id,
             actor.session_id,
             actor.client_instance_id,
             actor.channel_id,
             limited_request,
             @allowed,
             @now
           ).response == limited

    reset =
      WaypointReservation.request(
        hub.hub_id,
        actor.session_id,
        actor.client_instance_id,
        actor.channel_id,
        request(WaypointReservation.rate_limit() + 2, "reserve", "seat-a"),
        @not_entering,
        DateTime.add(@now, 10, :second)
      ).response

    assert reset["reason"] == "not_entering"
  end

  test "strict validation rejects malformed and oversized requests without advancing sequence", %{
    hub: hub
  } do
    actor = actor()
    register(hub, actor)

    invalid_requests = [
      %{request(1, "reserve", "seat-a") | "protocol" => 2},
      Map.put(request(1, "reserve", "seat-a"), "extra", true),
      %{request(1, "reserve", "seat-a") | "operation_id" => "not-a-uuid"},
      %{request(1, "reserve", "seat-a") | "waypoint_id" => String.duplicate("x", 513)},
      %{request(1, "reserve", "seat-a") | "request_seq" => 9_007_199_254_740_992}
    ]

    Enum.each(invalid_requests, fn payload ->
      result =
        WaypointReservation.request(
          hub.hub_id,
          actor.session_id,
          actor.client_instance_id,
          actor.channel_id,
          payload,
          @allowed,
          @now
        )

      assert result.response["reason"] == "invalid"
      assert result.states == []
    end)

    row = Repo.get_by!(WaypointReservation, hub_id: hub.hub_id, session_id: actor.session_id)
    assert row.last_request_seq == 0

    assert WaypointReservation.join_client_instance(%{
             "protocol" => 1,
             "client_instance_id" => actor.client_instance_id
           }) == {:ok, actor.client_instance_id}

    assert WaypointReservation.join_client_instance(%{
             "protocol" => 1,
             "client_instance_id" => actor.client_instance_id,
             "extra" => true
           }) == :unsupported
  end

  test "current-channel termination releases the lease but preserves the sequence tombstone", %{
    hub: hub
  } do
    actor = actor()
    register(hub, actor)
    reserve(hub, actor, request(1, "reserve", "seat-a"))

    result =
      WaypointReservation.terminate_channel(
        hub.hub_id,
        actor.session_id,
        actor.client_instance_id,
        actor.channel_id,
        DateTime.add(@now, 1, :second)
      )

    assert [%{"waypoint_id" => "seat-a", "occupied" => false}] = result.states

    reloaded = %{actor | channel_id: SecureRandom.uuid()}
    join = register(hub, reloaded, 2)
    assert join.active == []
    assert join.current == nil
    assert join.request_seq == 1
  end

  test "registration lazily purges only inactive tombstones older than seven days", %{hub: hub} do
    stale = actor()
    recent = actor()
    active = actor()
    register(hub, stale)
    register(hub, recent)
    register(hub, active)
    reserve(hub, active, request(1, "reserve", "seat-a"))

    stale_before =
      DateTime.add(@now, -WaypointReservation.tombstone_retention_seconds() - 1, :second)

    from(row in WaypointReservation,
      where:
        row.hub_id == ^hub.hub_id and row.session_id in ^[stale.session_id, active.session_id]
    )
    |> Repo.update_all(set: [updated_at: stale_before])

    observer = actor()
    join = register(hub, observer)

    refute Repo.get_by(WaypointReservation, hub_id: hub.hub_id, session_id: stale.session_id)
    assert Repo.get_by(WaypointReservation, hub_id: hub.hub_id, session_id: recent.session_id)
    assert Repo.get_by(WaypointReservation, hub_id: hub.hub_id, session_id: active.session_id)
    assert [%{waypoint_id: "seat-a"}] = join.active
  end

  defp actor do
    %{
      session_id: SecureRandom.uuid(),
      client_instance_id: SecureRandom.uuid(),
      channel_id: SecureRandom.uuid()
    }
  end

  defp register(hub, actor, seconds \\ 0) do
    WaypointReservation.register_channel(
      hub.hub_id,
      actor.session_id,
      actor.client_instance_id,
      actor.channel_id,
      DateTime.add(@now, seconds, :second)
    )
  end

  defp reserve(hub, actor, payload, seconds \\ 0) do
    WaypointReservation.request(
      hub.hub_id,
      actor.session_id,
      actor.client_instance_id,
      actor.channel_id,
      payload,
      @allowed,
      DateTime.add(@now, seconds, :second)
    )
  end

  defp request(sequence, action, waypoint_id, opts \\ []) do
    %{
      "protocol" => 1,
      "action" => action,
      "waypoint_id" => waypoint_id,
      "operation_id" => Keyword.get(opts, :operation_id, SecureRandom.uuid()),
      "reservation_id" => Keyword.get(opts, :reservation_id, SecureRandom.uuid()),
      "request_seq" => sequence
    }
  end
end
