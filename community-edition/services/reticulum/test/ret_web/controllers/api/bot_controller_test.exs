defmodule RetWeb.BotControllerTest do
  use RetWeb.ConnCase
  import Ret.TestHelpers

  alias Ret.{AppConfig, BotChatPresence, Repo}

  setup [:create_account, :create_owned_file, :create_scene, :create_hub]

  setup do
    AppConfig.set_config_value("features|enable_bot_chat", true)
    :ok
  end

  @tag :authenticated
  test "an account allowed to join cannot chat without an entered channel", %{
    conn: conn,
    hub: hub
  } do
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
end
