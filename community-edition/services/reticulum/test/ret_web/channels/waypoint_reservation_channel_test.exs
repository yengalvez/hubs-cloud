defmodule RetWeb.WaypointReservationChannelTest do
  use RetWeb.ChannelCase

  import Ret.TestHelpers

  alias Ret.{Repo, WaypointReservation}
  alias RetWeb.{HubChannel, SessionSocket}

  setup [:create_account, :create_owned_file, :create_scene, :create_hub]

  setup do
    {:ok, socket} = connect(SessionSocket, %{})
    {:ok, socket: socket}
  end

  test "legacy joins advertise unsupported and reject the event with public correlation", %{
    socket: socket,
    hub: hub
  } do
    {:ok, response, socket} =
      subscribe_and_join(socket, "hub:#{hub.hub_sid}", join_params())

    assert response.waypoint_reservation == %{
             protocol: 1,
             supported: false,
             lease_ms: 15_000,
             request_timeout_ms: 3_000,
             active: [],
             current: nil,
             request_seq: 0
           }

    payload = request(1, "reserve", "seat-a")

    assert_reply push(socket, "waypoint_reservation:request", payload), :error, reply
    assert reply["reason"] == "unsupported"
    assert reply["operation_id"] == payload["operation_id"]
    assert reply["request_seq"] == 1
    refute Map.has_key?(reply, "session_id")
    assert Repo.aggregate(WaypointReservation, :count, :waypoint_reservation_id) == 0
  end

  test "only entering or room presence can reserve and state broadcasts are anonymous", %{
    socket: socket,
    hub: hub
  } do
    client_instance_id = SecureRandom.uuid()

    {:ok, response, socket} =
      subscribe_and_join(
        socket,
        "hub:#{hub.hub_sid}",
        join_params(client_instance_id)
      )

    assert response.waypoint_reservation.supported
    assert response.waypoint_reservation.active == []
    assert response.waypoint_reservation.current == nil
    assert response.waypoint_reservation.request_seq == 0

    first = request(1, "reserve", "seat-a")
    assert_reply push(socket, "waypoint_reservation:request", first), :error, denied
    assert denied["reason"] == "not_entering"

    push(socket, "events:entering", %{})

    assert_reply push(socket, "waypoint_reservation:request", first), :error, retried
    assert retried == denied

    reserve = request(2, "reserve", "seat-a")
    assert_reply push(socket, "waypoint_reservation:request", reserve), :ok, accepted
    assert accepted["occupied"] == true
    assert accepted["expires_at"] != nil

    assert_broadcast "waypoint_reservation:state", state
    assert state["waypoint_id"] == "seat-a"
    assert state["occupied"] == true

    assert Map.keys(state) |> Enum.sort() ==
             ~w(expires_at occupied protocol waypoint_id) |> Enum.sort()

    {:ok, observer_socket} = connect(SessionSocket, %{})

    {:ok, observer_response, _observer_socket} =
      subscribe_and_join(
        observer_socket,
        "hub:#{hub.hub_sid}",
        join_params(SecureRandom.uuid())
      )

    assert [%{waypoint_id: "seat-a", expires_at: expires_at}] =
             observer_response.waypoint_reservation.active

    assert is_binary(expires_at)
    assert observer_response.waypoint_reservation.current == nil
    assert observer_response.waypoint_reservation.request_seq == 0
  end

  test "same-runtime reconnect receives current tokens and invalidates the old channel", %{
    socket: socket,
    hub: hub
  } do
    client_instance_id = SecureRandom.uuid()

    {:ok, first_join, first_socket} =
      subscribe_and_join(
        socket,
        "hub:#{hub.hub_sid}",
        join_params(client_instance_id)
      )

    push(first_socket, "events:entering", %{})
    initial = request(1, "reserve", "seat-a")
    assert_reply push(first_socket, "waypoint_reservation:request", initial), :ok, _reply
    assert_broadcast "waypoint_reservation:state", %{"occupied" => true}

    {:ok, reloaded_socket} =
      connect(SessionSocket, %{"session_token" => first_join.session_token})

    {:ok, reload_join, reloaded_socket} =
      subscribe_and_join(
        reloaded_socket,
        "hub:#{hub.hub_sid}",
        join_params(client_instance_id)
      )

    assert reload_join.session_id == first_join.session_id
    assert reload_join.waypoint_reservation.request_seq == 1
    assert reload_join.waypoint_reservation.current.waypoint_id == "seat-a"
    assert reload_join.waypoint_reservation.current.operation_id == initial["operation_id"]
    assert reload_join.waypoint_reservation.current.reservation_id == initial["reservation_id"]

    stale_renew =
      request(2, "renew", "seat-a", reservation_id: initial["reservation_id"])

    assert_reply push(first_socket, "waypoint_reservation:request", stale_renew), :error, stale
    assert stale["reason"] == "stale_channel"

    push(reloaded_socket, "events:entering", %{})
    assert_reply push(reloaded_socket, "waypoint_reservation:request", stale_renew), :ok, renewed
    assert renewed["operation_id"] == stale_renew["operation_id"]
  end

  test "authenticated bot runners never negotiate reservation capability", %{
    socket: socket,
    hub: hub
  } do
    bot_key = String.duplicate("test-bot-key-", 3)
    old_key = Application.get_env(:ret, :bot_access_key)
    Application.put_env(:ret, :bot_access_key, bot_key)

    on_exit(fn ->
      if old_key == nil,
        do: Application.delete_env(:ret, :bot_access_key),
        else: Application.put_env(:ret, :bot_access_key, old_key)
    end)

    params =
      join_params(SecureRandom.uuid())
      |> Map.put("bot_access_key", bot_key)
      |> Map.put("context", %{"bot_runner" => true})

    {:ok, response, bot_socket} =
      subscribe_and_join(socket, "hub:#{hub.hub_sid}", params)

    refute response.waypoint_reservation.supported
    assert response.waypoint_reservation.current == nil

    assert_reply push(
                   bot_socket,
                   "waypoint_reservation:request",
                   request(1, "reserve", "seat-a")
                 ),
                 :error,
                 %{"reason" => "unsupported"}

    assert Repo.aggregate(WaypointReservation, :count, :waypoint_reservation_id) == 0
  end

  test "a requested bot runner with an invalid key cannot negotiate reservations", %{
    socket: socket,
    hub: hub
  } do
    params =
      join_params(SecureRandom.uuid())
      |> Map.put("bot_access_key", "invalid")
      |> Map.put("context", %{"bot_runner" => true})

    assert {:error, %{reason: "invalid_bot_access_key"}} =
             subscribe_and_join(socket, "hub:#{hub.hub_sid}", params)

    assert Repo.aggregate(WaypointReservation, :count, :waypoint_reservation_id) == 0
  end

  test "terminate releases only the current channel and emits public state", %{
    socket: socket,
    hub: hub
  } do
    {:ok, _response, socket} =
      subscribe_and_join(
        socket,
        "hub:#{hub.hub_sid}",
        join_params(SecureRandom.uuid())
      )

    push(socket, "events:entering", %{})

    assert_reply push(socket, "waypoint_reservation:request", request(1, "reserve", "seat-a")),
                 :ok,
                 _reply

    assert_broadcast "waypoint_reservation:state", %{"occupied" => true}

    previous = Application.get_env(:ret, HubChannel)

    result =
      try do
        Application.put_env(
          :ret,
          HubChannel,
          Keyword.put(previous, :enable_terminate_actions, true)
        )

        HubChannel.terminate(:normal, socket)
      after
        Application.put_env(:ret, HubChannel, previous)
      end

    assert result == :ok

    assert_broadcast "waypoint_reservation:state", state

    assert state == %{
             "protocol" => 1,
             "waypoint_id" => "seat-a",
             "occupied" => false,
             "expires_at" => nil
           }

    row = Repo.one!(WaypointReservation)
    assert row.waypoint_id == nil
    assert row.last_request_seq == 1
  end

  defp join_params(client_instance_id \\ nil) do
    base = %{"profile" => %{}, "context" => %{}}

    if client_instance_id do
      Map.put(base, "waypoint_reservation", %{
        "protocol" => 1,
        "client_instance_id" => client_instance_id
      })
    else
      base
    end
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
