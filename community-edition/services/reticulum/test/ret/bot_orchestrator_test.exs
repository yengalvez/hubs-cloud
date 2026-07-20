defmodule Ret.BotOrchestratorTest do
  use ExUnit.Case, async: false

  alias Ret.{BotOrchestrator, BotRuntimeOutbox}

  @access_key "test-bot-orchestrator-key-at-least-32-bytes"
  @operation_id "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  setup do
    previous_config = Application.get_env(:ret, BotOrchestrator)
    previous_client = Application.get_env(:ret, Ret.BotOrchestratorTestHttpClient)

    Application.put_env(:ret, BotOrchestrator,
      endpoint: "http://bot-orchestrator.test:5001",
      access_key: @access_key,
      http_client: Ret.BotOrchestratorTestHttpClient
    )

    on_exit(fn ->
      restore_env(BotOrchestrator, previous_config)
      restore_env(Ret.BotOrchestratorTestHttpClient, previous_client)
    end)

    :ok
  end

  test "config completes only on the exact terminal v2 acknowledgement" do
    bots = %{
      "enabled" => true,
      "count" => 1,
      "chat_enabled" => 1.0,
      "future_contract" => %{"mode" => "kept"}
    }

    response = %{
      protocol: BotOrchestrator.protocol(),
      operation_id: @operation_id,
      hub_sid: "Room_1",
      runtime_revision: 7,
      state: "applied"
    }

    respond(200, Poison.encode!(response))

    assert :ok =
             BotOrchestrator.deliver_runtime_event(%{
               event_kind: "config",
               operation_id: @operation_id,
               hub_sid: "Room_1",
               runtime_revision: 7,
               bots: bots,
               runtime_chat_enabled: false
             })

    assert_receive {:bot_orchestrator_request, :post,
                    "http://bot-orchestrator.test:5001/internal/bots/room-config", body, headers,
                    options}

    assert Poison.decode!(body) == %{
             "protocol" => BotOrchestrator.protocol(),
             "operation_id" => @operation_id,
             "hub_sid" => "Room_1",
             "runtime_revision" => 7,
             "bots" => Map.put(bots, "chat_enabled", false),
             "runtime_chat_enabled" => false
           }

    assert bots["chat_enabled"] === 1.0

    assert headers == [
             {"Content-Type", "application/json"},
             {"x-ret-bot-orchestrator-access-key", @access_key}
           ]

    assert options[:timeout] == 5_000
    assert options[:recv_timeout] == 5_000
  end

  test "config delivery enforces the normalized runtime projection byte boundary" do
    boundary_bots = %{
      "enabled" => true,
      "count" => 1,
      "padding" => String.duplicate("x", 16_324)
    }

    response = %{
      protocol: BotOrchestrator.protocol(),
      operation_id: @operation_id,
      hub_sid: "Room_1",
      runtime_revision: 7,
      state: "applied"
    }

    respond(200, Poison.encode!(response))

    assert :ok =
             BotOrchestrator.deliver_runtime_event(%{
               event_kind: "config",
               operation_id: @operation_id,
               hub_sid: "Room_1",
               runtime_revision: 7,
               bots: boundary_bots,
               runtime_chat_enabled: false
             })

    assert_receive {:bot_orchestrator_request, :post, _url, body, _headers, _options}
    runtime_bots = Poison.decode!(body)["bots"]
    assert byte_size(Poison.encode!(runtime_bots)) == BotRuntimeOutbox.max_config_bytes()
    refute Map.has_key?(boundary_bots, "chat_enabled")
    refute runtime_bots["chat_enabled"]

    overflow_bots = Map.put(boundary_bots, "padding", String.duplicate("x", 16_325))

    assert {:pending, :invalid_runtime_event} =
             BotOrchestrator.deliver_runtime_event(%{
               event_kind: "config",
               operation_id: @operation_id,
               hub_sid: "Room_1",
               runtime_revision: 7,
               bots: overflow_bots,
               runtime_chat_enabled: false
             })

    refute_receive {:bot_orchestrator_request, _, _, _, _, _}
  end

  test "stop completes only after exact target and managed Pod absence" do
    response = %{
      protocol: BotOrchestrator.protocol(),
      operation_id: @operation_id,
      hub_sid: "room-2",
      runtime_revision: 8,
      revoke_epoch: 19,
      state: "stopped",
      target_absent: true,
      managed_room_pods: 0
    }

    respond(200, Poison.encode!(response))

    assert :ok =
             BotOrchestrator.deliver_runtime_event(%{
               event_kind: "stop",
               operation_id: @operation_id,
               hub_sid: "room-2",
               runtime_revision: 8,
               revoke_epoch: 19
             })
  end

  test "numeric type drift never completes config or stop acknowledgements" do
    respond(
      200,
      Poison.encode!(%{
        protocol: BotOrchestrator.protocol(),
        operation_id: @operation_id,
        hub_sid: "room-1",
        runtime_revision: 1.0,
        state: "applied"
      })
    )

    assert {:pending, :non_terminal_ack} =
             BotOrchestrator.deliver_runtime_event(config_event())

    stop_event = %{
      event_kind: "stop",
      operation_id: @operation_id,
      hub_sid: "room-2",
      runtime_revision: 1,
      revoke_epoch: 19
    }

    exact_stop_ack = %{
      protocol: BotOrchestrator.protocol(),
      operation_id: @operation_id,
      hub_sid: "room-2",
      runtime_revision: 1,
      revoke_epoch: 19,
      state: "stopped",
      target_absent: true,
      managed_room_pods: 0
    }

    for drifted_ack <- [
          %{exact_stop_ack | runtime_revision: 1.0},
          %{exact_stop_ack | revoke_epoch: 19.0},
          %{exact_stop_ack | managed_room_pods: 0.0}
        ] do
      respond(200, Poison.encode!(drifted_ack))

      assert {:pending, :non_terminal_ack} =
               BotOrchestrator.deliver_runtime_event(stop_event)
    end
  end

  test "202, legacy 2xx, malformed and non-exact 200 all remain pending" do
    event = config_event()

    respond(202, Poison.encode!(%{state: "pending"}))
    assert {:pending, :accepted_pending} = BotOrchestrator.deliver_runtime_event(event)

    respond(204, "")

    assert {:pending, :legacy_or_malformed_2xx} =
             BotOrchestrator.deliver_runtime_event(event)

    respond(200, "not-json")
    assert {:pending, :non_terminal_ack} = BotOrchestrator.deliver_runtime_event(event)

    respond(
      200,
      Poison.encode!(%{
        protocol: BotOrchestrator.protocol(),
        operation_id: @operation_id,
        hub_sid: "room-1",
        runtime_revision: 1,
        state: "applied",
        extra: true
      })
    )

    assert {:pending, :non_terminal_ack} = BotOrchestrator.deliver_runtime_event(event)
  end

  test "network failures and non-2xx responses remain bounded pending states" do
    Application.put_env(
      :ret,
      Ret.BotOrchestratorTestHttpClient,
      {self(), {:error, %HTTPoison.Error{reason: :timeout}}}
    )

    assert {:pending, :orchestrator_timeout} =
             BotOrchestrator.deliver_runtime_event(config_event())

    respond(503, "")

    assert {:pending, :orchestrator_server_error} =
             BotOrchestrator.deliver_runtime_event(config_event())
  end

  test "manually constructed events reject non-canonical or non-v4 operation ids" do
    Enum.each(
      [
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
        "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa",
        "not-a-uuid"
      ],
      fn invalid_id ->
        assert {:pending, :invalid_runtime_event} =
                 BotOrchestrator.deliver_runtime_event(%{
                   config_event()
                   | operation_id: invalid_id
                 })
      end
    )

    refute_receive {:bot_orchestrator_request, _, _, _, _, _}
  end

  defp config_event do
    %{
      event_kind: "config",
      operation_id: @operation_id,
      hub_sid: "room-1",
      runtime_revision: 1,
      bots: %{"enabled" => true, "count" => 1, "chat_enabled" => 1.0},
      runtime_chat_enabled: false
    }
  end

  defp respond(status, body) do
    Application.put_env(
      :ret,
      Ret.BotOrchestratorTestHttpClient,
      {self(), {:ok, %HTTPoison.Response{status_code: status, body: body}}}
    )
  end

  defp restore_env(key, nil), do: Application.delete_env(:ret, key)
  defp restore_env(key, value), do: Application.put_env(:ret, key, value)
end
