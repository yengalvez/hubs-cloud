defmodule RetWeb.BotControllerTest do
  use RetWeb.ConnCase
  import Ret.TestHelpers

  alias Ret.{AppConfig, BotChatPresence, BotConfigApproval, BotRunnerLease, Repo}

  defmodule SuccessfulHttpClient do
    def request(method, url, body, headers, options) do
      test_pid = Application.fetch_env!(:ret, __MODULE__)
      send(test_pid, {:successful_bot_chat_request, method, url, body, headers, options})

      {:ok,
       %HTTPoison.Response{
         status_code: 200,
         body:
           Poison.encode!(%{
             reply: "voy al escenario",
             action: %{type: "go_to_waypoint", waypoint: "spawbot-stage"}
           })
       }}
    end
  end

  defmodule QuarantiningHttpClient do
    def request(method, url, body, headers, options) do
      %{admin: admin, hub_sid: hub_sid, test_pid: test_pid} =
        Application.fetch_env!(:ret, __MODULE__)

      send(test_pid, {:quarantining_bot_chat_request, method, url, body, headers, options})
      {:ok, _hub} = Ret.BotConfigApproval.quarantine(hub_sid, admin)

      {:ok,
       %HTTPoison.Response{
         status_code: 200,
         body:
           Poison.encode!(%{
             reply: "stale reply",
             action: %{type: "go_to_waypoint", waypoint: "spawbot-stage"}
           })
       }}
    end
  end

  setup [:create_account, :create_owned_file, :create_scene, :create_hub]

  setup do
    AppConfig.set_config_value("features|enable_bot_chat", true)
    :ok
  end

  @tag :authenticated
  test "an account allowed to join cannot chat without an entered channel", %{
    account: account,
    conn: conn,
    hub: hub
  } do
    hub =
      hub
      |> Ecto.Changeset.change(%{
        user_data: %{
          "bots" => %{
            "enabled" => true,
            "chat_enabled" => true,
            "count" => 1,
            "mobility" => "static"
          }
        }
      })
      |> Repo.update!()

    approve_config!(hub, account)

    conn
    |> post("/api/v1/hubs/#{hub.hub_sid}/bots/bot-1/chat", %{message: "hola"})
    |> response(403)
  end

  @tag :authenticated
  test "the exact entered-channel capability passes presence before bot validation", %{
    account: account,
    conn: conn,
    hub: hub
  } do
    capability = Ecto.UUID.generate()
    assert :ok = BotChatPresence.track(self(), hub.hub_sid, account.account_id, capability)

    hub =
      hub
      |> Ecto.Changeset.change(%{
        user_data: %{
          "bots" => %{
            "enabled" => true,
            "chat_enabled" => true,
            "count" => 1,
            "mobility" => "static"
          }
        }
      })
      |> Repo.update!()

    approve_config!(hub, account)

    conn
    |> post("/api/v1/hubs/#{hub.hub_sid}/bots/bot-99/chat", %{
      message: "hola",
      bot_chat_capability: Ecto.UUID.generate()
    })
    |> response(403)

    conn
    |> recycle()
    |> auth_with_account(account)
    |> post("/api/v1/hubs/#{hub.hub_sid}/bots/bot-99/chat", %{
      message: "hola",
      bot_chat_capability: capability
    })
    |> response(400)

    assert :ok = BotChatPresence.untrack(self())
  end

  @tag :authenticated
  test "an entered account cannot chat with a valid but unapproved config", %{
    account: account,
    conn: conn,
    hub: hub
  } do
    capability = Ecto.UUID.generate()
    assert :ok = BotChatPresence.track(self(), hub.hub_sid, account.account_id, capability)

    hub
    |> Ecto.Changeset.change(%{
      user_data: %{
        "bots" => %{
          "enabled" => true,
          "chat_enabled" => true,
          "count" => 1,
          "mobility" => "static"
        }
      }
    })
    |> Repo.update!()

    conn
    |> post("/api/v1/hubs/#{hub.hub_sid}/bots/bot-1/chat", %{
      message: "hola",
      bot_chat_capability: capability
    })
    |> response(403)

    assert :ok = BotChatPresence.untrack(self())
  end

  @tag :authenticated
  test "a quarantine during the provider call discards the stale reply and action", %{
    account: account,
    conn: conn,
    hub: hub
  } do
    capability = Ecto.UUID.generate()
    assert :ok = BotChatPresence.track(self(), hub.hub_sid, account.account_id, capability)

    hub =
      hub
      |> Ecto.Changeset.change(%{
        user_data: %{
          "bots" => %{
            "enabled" => true,
            "chat_enabled" => true,
            "count" => 1,
            "mobility" => "static",
            "prompt" => ""
          }
        }
      })
      |> Repo.update!()

    approve_config!(hub, account)
    admin = create_account("bot-chat-race-admin-#{System.unique_integer([:positive])}", true)
    previous_orchestrator = Application.get_env(:ret, Ret.BotOrchestrator)
    previous_client = Application.get_env(:ret, QuarantiningHttpClient)

    Application.put_env(
      :ret,
      Ret.BotOrchestrator,
      endpoint: "http://bot-orchestrator.test",
      access_key: String.duplicate("k", 32),
      http_client: QuarantiningHttpClient
    )

    Application.put_env(:ret, QuarantiningHttpClient, %{
      admin: admin,
      hub_sid: hub.hub_sid,
      test_pid: self()
    })

    on_exit(fn ->
      restore_application_env(:ret, Ret.BotOrchestrator, previous_orchestrator)
      restore_application_env(:ret, QuarantiningHttpClient, previous_client)
    end)

    :ok = RetWeb.Endpoint.subscribe("hub:#{hub.hub_sid}")

    conn
    |> post("/api/v1/hubs/#{hub.hub_sid}/bots/bot-1/chat", %{
      message: "ve al escenario",
      bot_chat_capability: capability
    })
    |> response(403)

    assert_receive {:quarantining_bot_chat_request, :post,
                    "http://bot-orchestrator.test/internal/bots/chat", _body, _headers, _options}

    refute_receive %Phoenix.Socket.Broadcast{event: "message"}, 100
    assert Repo.get!(BotConfigApproval, hub.hub_id).state == "quarantined"
    assert :ok = BotChatPresence.untrack(self())
  end

  @tag :authenticated
  test "a delivered bot command carries the exact current database fence", %{
    account: account,
    conn: conn,
    hub: hub
  } do
    capability = Ecto.UUID.generate()
    assert :ok = BotChatPresence.track(self(), hub.hub_sid, account.account_id, capability)

    hub =
      hub
      |> Ecto.Changeset.change(%{
        user_data: %{
          "bots" => %{
            "enabled" => true,
            "chat_enabled" => true,
            "count" => 1,
            "mobility" => "static",
            "prompt" => ""
          }
        }
      })
      |> Repo.update!()

    approve_config!(hub, account)

    {:ok, lease} =
      BotRunnerLease.register_for_session(hub.hub_sid, Ecto.UUID.generate())

    previous_orchestrator = Application.get_env(:ret, Ret.BotOrchestrator)
    previous_client = Application.get_env(:ret, SuccessfulHttpClient)

    Application.put_env(
      :ret,
      Ret.BotOrchestrator,
      endpoint: "http://bot-orchestrator.test",
      access_key: String.duplicate("k", 32),
      http_client: SuccessfulHttpClient
    )

    Application.put_env(:ret, SuccessfulHttpClient, self())

    on_exit(fn ->
      restore_application_env(:ret, Ret.BotOrchestrator, previous_orchestrator)
      restore_application_env(:ret, SuccessfulHttpClient, previous_client)
    end)

    :ok = RetWeb.Endpoint.subscribe("hub:#{hub.hub_sid}")

    conn
    |> post("/api/v1/hubs/#{hub.hub_sid}/bots/bot-1/chat", %{
      message: "ve al escenario",
      bot_chat_capability: capability
    })
    |> json_response(200)

    assert_receive {:successful_bot_chat_request, :post,
                    "http://bot-orchestrator.test/internal/bots/chat", _body, _headers, _options}

    assert_receive %Phoenix.Socket.Broadcast{
      event: "message",
      payload: %{
        type: "bot_command",
        bot_runner_lease_id: lease_id,
        bot_runner_authority_epoch: authority_epoch
      }
    }

    assert lease_id == lease.lease_id
    assert authority_epoch == lease.authority_epoch
    assert :ok = BotRunnerLease.unregister(hub.hub_sid, lease.lease_id)
    assert :ok = BotChatPresence.untrack(self())
  end

  defp approve_config!(hub, account) do
    bots = hub.user_data["bots"]

    Repo.insert!(%BotConfigApproval{
      hub_id: hub.hub_id,
      state: "approved",
      candidate_bots: bots,
      approved_bots: bots,
      approved_by_account_id: account.account_id,
      approved_at: DateTime.utc_now()
    })
  end

  defp restore_application_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_application_env(app, key, value), do: Application.put_env(app, key, value)
end
