defmodule RetWeb.HubChannelTest do
  use RetWeb.ChannelCase
  import Ret.TestHelpers

  alias RetWeb.{Presence, SessionSocket}

  alias Ret.{
    AppConfig,
    Account,
    BotChatPresence,
    BotConfigAdmission,
    BotRunnerLease,
    Repo,
    Hub,
    HubInvite
  }

  @default_join_params %{"profile" => %{}, "context" => %{}}

  setup [:create_account, :create_owned_file, :create_scene, :create_hub, :create_account]

  setup do
    {:ok, socket} = connect(SessionSocket, %{})
    {:ok, socket: socket}
  end

  setup do
    original_orchestrator_config = Application.get_env(:ret, Ret.BotOrchestrator)
    original_test_receiver = Application.get_env(:ret, Ret.BotOrchestratorTestHttpClient)

    Application.put_env(
      :ret,
      Ret.BotOrchestrator,
      endpoint: "http://bot-orchestrator.test",
      access_key: String.duplicate("k", 32),
      http_client: Ret.BotOrchestratorTestHttpClient
    )

    Application.put_env(:ret, Ret.BotOrchestratorTestHttpClient, self())

    on_exit(fn ->
      restore_application_env(:ret, Ret.BotOrchestrator, original_orchestrator_config)
      restore_application_env(:ret, Ret.BotOrchestratorTestHttpClient, original_test_receiver)
    end)

    :ok
  end

  describe "authorization" do
    test "joining hub works", %{socket: socket, hub: hub} do
      {:ok, %{session_id: _session_id}, _socket} =
        subscribe_and_join(socket, "hub:#{hub.hub_sid}", @default_join_params)
    end

    test "joining hub does not work if sign in required", %{socket: socket, scene: scene} do
      AppConfig.set_config_value("features|require_account_for_join", true)
      {:ok, hub} = %Hub{} |> Hub.changeset(scene, %{name: "Test Hub"}) |> Repo.insert()
      {:error, _reason} = subscribe_and_join(socket, "hub:#{hub.hub_sid}", @default_join_params)
      AppConfig.set_config_value("features|require_account_for_join", false)
    end

    test "joining hub does not work if account is disabled", %{socket: socket, hub: hub} do
      disabled_account = create_account("disabled_account")
      disabled_account |> Ecto.Changeset.change(state: :disabled) |> Ret.Repo.update!()

      {:error, %{reason: "join_denied"}} =
        subscribe_and_join(
          socket,
          "hub:#{hub.hub_sid}",
          join_params_for_account(disabled_account)
        )
    end

    test "a room owner cannot activate bots through update_hub", %{
      socket: socket,
      account: account,
      scene: scene
    } do
      {:ok, owned_hub} =
        %Hub{}
        |> Hub.changeset(scene, %{name: "Owned bot room"})
        |> Hub.add_account_to_changeset(account)
        |> Repo.insert()

      {:ok, _response, socket} =
        subscribe_and_join(
          socket,
          "hub:#{owned_hub.hub_sid}",
          join_params_for_account(account)
        )

      payload = %{
        "name" => owned_hub.name,
        "description" => owned_hub.description,
        "room_size" => owned_hub.room_size,
        "member_permissions" => Hub.member_permissions_for_hub(owned_hub),
        "allow_promotion" => owned_hub.allow_promotion,
        "entry_mode" => nil,
        "user_data" => %{
          "bots" => %{
            "enabled" => true,
            "count" => 1,
            "mobility" => "static",
            "chat_enabled" => true,
            "prompt" => ""
          }
        }
      }

      assert_reply push(socket, "update_hub", payload), :error, %{
        reason: "bot_config_rejected"
      }

      refute Repo.get!(Hub, owned_hub.hub_id).user_data["bots"]["enabled"]
    end

    test "closing a room disables its approved bots in the same update", %{
      socket: socket,
      account: account,
      scene: scene
    } do
      admin = create_account("channel-close-bot-admin", true)

      {:ok, owned_hub} =
        %Hub{}
        |> Hub.changeset(scene, %{name: "Close bot room"})
        |> Hub.add_account_to_changeset(account)
        |> Repo.insert()

      {:ok, active_hub} =
        owned_hub
        |> Hub.add_attrs_to_changeset(%{
          user_data: %{
            "bots" => %{
              "enabled" => true,
              "count" => 1,
              "mobility" => "static",
              "chat_enabled" => true,
              "prompt" => ""
            }
          }
        })
        |> BotConfigAdmission.update(admin)

      {:ok, _response, socket} =
        subscribe_and_join(
          socket,
          "hub:#{active_hub.hub_sid}",
          join_params_for_account(account)
        )

      push(socket, "close_hub", %{})
      :sys.get_state(socket.channel_pid)

      closed = Repo.get!(Hub, active_hub.hub_id)
      assert closed.entry_mode == :deny
      refute closed.user_data["bots"]["enabled"]
      assert closed.user_data["bots"]["count"] == 1

      assert_receive {:bot_orchestrator_request, :post,
                      "http://bot-orchestrator.test/internal/bots/room-stop", body, _headers,
                      _options}

      assert Poison.decode!(body) == %{"hub_sid" => active_hub.hub_sid}
    end
  end

  describe "presence" do
    test "joining hub registers in presence", %{socket: socket, hub: hub} do
      {:ok, %{session_id: session_id}, socket} =
        subscribe_and_join(socket, "hub:#{hub.hub_sid}", @default_join_params)

      :timer.sleep(100)
      presence = socket |> Presence.list()
      assert presence[session_id]
    end

    test "joining hub with an account with an identity registers identity in presence", %{
      socket: socket,
      hub: hub,
      account: account
    } do
      account |> Account.set_identity!("Test User")

      {:ok, %{session_id: session_id}, socket} =
        subscribe_and_join(socket, "hub:#{hub.hub_sid}", join_params_for_account(account))

      :timer.sleep(100)
      presence = socket |> Presence.list()
      meta = presence[session_id][:metas] |> Enum.at(0)
      assert meta[:profile]["identityName"] === "Test User"
    end

    test "authenticated chat authority starts only after the channel enters the room", %{
      socket: socket,
      hub: hub,
      account: account
    } do
      {:ok, response, socket} =
        subscribe_and_join(socket, "hub:#{hub.hub_sid}", join_params_for_account(account))

      assert response.bot_chat_capability =~ ~r/\A[A-Za-z0-9_-]{32}\z/

      refute BotChatPresence.present?(
               hub.hub_sid,
               account.account_id,
               response.bot_chat_capability
             )

      push(socket, "events:entered", %{})
      :sys.get_state(socket.channel_pid)

      assert BotChatPresence.present?(
               hub.hub_sid,
               account.account_id,
               response.bot_chat_capability
             )
    end

    test "sign_out revokes chat authority immediately", %{
      socket: socket,
      hub: hub,
      account: account
    } do
      {:ok, response, socket} =
        subscribe_and_join(socket, "hub:#{hub.hub_sid}", join_params_for_account(account))

      push(socket, "events:entered", %{})
      :sys.get_state(socket.channel_pid)

      assert BotChatPresence.present?(
               hub.hub_sid,
               account.account_id,
               response.bot_chat_capability
             )

      assert_reply push(socket, "sign_out", %{}), :ok, %{bot_chat_capability: nil}

      refute BotChatPresence.present?(
               hub.hub_sid,
               account.account_id,
               response.bot_chat_capability
             )
    end

    test "sign_in after anonymous entry creates and tracks a private capability", %{
      socket: socket,
      hub: hub,
      account: account
    } do
      {:ok, %{bot_chat_capability: nil}, socket} =
        subscribe_and_join(socket, "hub:#{hub.hub_sid}", @default_join_params)

      push(socket, "events:entered", %{})
      :sys.get_state(socket.channel_pid)
      {:ok, token, _claims} = Ret.Guardian.encode_and_sign(account)

      assert_reply push(socket, "sign_in", %{"token" => token}), :ok, %{
        bot_chat_capability: capability
      }

      assert capability =~ ~r/\A[A-Za-z0-9_-]{32}\z/
      assert BotChatPresence.present?(hub.hub_sid, account.account_id, capability)
    end

    test "switching accounts rotates authority without retaining the previous account", %{
      socket: socket,
      hub: hub,
      account: account,
      account2: account2
    } do
      {:ok, first, socket} =
        subscribe_and_join(socket, "hub:#{hub.hub_sid}", join_params_for_account(account))

      push(socket, "events:entered", %{})
      :sys.get_state(socket.channel_pid)
      {:ok, token, _claims} = Ret.Guardian.encode_and_sign(account2)

      assert_reply push(socket, "sign_in", %{"token" => token}), :ok, %{
        bot_chat_capability: second_capability
      }

      refute second_capability == first.bot_chat_capability

      refute BotChatPresence.present?(
               hub.hub_sid,
               account.account_id,
               first.bot_chat_capability
             )

      assert BotChatPresence.present?(hub.hub_sid, account2.account_id, second_capability)
    end
  end

  describe "bot runner presence context" do
    setup do
      original_bot_access_key = Application.get_env(:ret, :bot_runner_access_key)
      bot_access_key = String.duplicate("trusted-bot-access-key-", 2)

      Application.put_env(:ret, :bot_runner_access_key, bot_access_key)

      on_exit(fn ->
        if is_nil(original_bot_access_key) do
          Application.delete_env(:ret, :bot_runner_access_key)
        else
          Application.put_env(:ret, :bot_runner_access_key, original_bot_access_key)
        end
      end)

      {:ok, bot_access_key: bot_access_key}
    end

    test "publishes bot_runner=true only for a boolean request with a valid access key", %{
      socket: socket,
      hub: hub,
      bot_access_key: bot_access_key
    } do
      params =
        join_params(%{
          "bot_access_key" => bot_access_key,
          "context" => %{"bot_runner" => true}
        })

      {:ok, %{session_id: session_id, bot_runner: true}, socket} = join_hub(socket, hub, params)

      assert presence_context(socket, session_id)["bot_runner"] === true

      network_id = bot_network_id(hub, 1)

      assert_reply push(socket, "naf", bot_spawn_payload(network_id)), :ok, %{
        bot_spawn_accepted: true,
        network_id: ^network_id
      }
    end

    test "keeps the first registered lease authoritative during runner overlap", %{
      socket: first_transport,
      hub: hub,
      bot_access_key: bot_access_key
    } do
      params =
        join_params(%{
          "bot_access_key" => bot_access_key,
          "context" => %{"bot_runner" => true}
        })

      {:ok, first_join, first_socket} = join_hub(first_transport, hub, params)
      assert is_binary(first_join.bot_runner_lease_id)
      assert first_join.bot_runner_authoritative
      assert is_integer(first_join.bot_runner_authority_epoch)
      assert presence_context(first_socket, first_join.session_id)["bot_runner"] === true

      {:ok, second_transport} = connect(SessionSocket, %{})
      {:ok, second_join, second_socket} = join_hub(second_transport, hub, params)
      assert is_binary(second_join.bot_runner_lease_id)
      refute second_join.bot_runner_lease_id == first_join.bot_runner_lease_id
      refute second_join.bot_runner_authoritative
      assert second_join.bot_runner_authority_epoch == first_join.bot_runner_authority_epoch
      assert presence_context(second_socket, second_join.session_id)["bot_runner"] === true

      assert RetWeb.HubChannel.authoritative_bot_runner_lease_id(Presence.list(second_socket)) ==
               first_join.bot_runner_lease_id

      network_id = bot_network_id(hub, 1)

      assert_reply push(second_socket, "naf", bot_spawn_payload(network_id)), :error, %{
        reason: "bot_runner_not_authoritative"
      }

      assert_reply push(second_socket, "naf", bot_update_payload(network_id)), :error, %{
        reason: "bot_runner_not_authoritative"
      }

      remove = %{"dataType" => "r", "data" => %{"networkId" => network_id}}

      assert_reply push(second_socket, "naf", remove), :error, %{
        reason: "bot_runner_not_authoritative"
      }

      assert_reply push(first_socket, "naf", bot_spawn_payload(network_id)), :ok, %{
        bot_spawn_accepted: true,
        network_id: ^network_id
      }

      Process.unlink(first_socket.channel_pid)
      Process.exit(first_socket.channel_pid, :kill)
      assert wait_for_runner_lease(second_socket, second_join.bot_runner_lease_id)

      promoted_epoch =
        BotRunnerLease.snapshot(hub.hub_sid)
        |> Map.fetch!(:authority_epoch)

      assert promoted_epoch > first_join.bot_runner_authority_epoch

      refute BotRunnerLease.authorized?(
               hub.hub_sid,
               second_join.bot_runner_lease_id,
               first_join.bot_runner_authority_epoch
             )

      assert_reply push(second_socket, "naf", bot_spawn_payload(network_id)), :ok, %{
        bot_spawn_accepted: true,
        network_id: ^network_id,
        bot_runner_authority_epoch: ^promoted_epoch
      }

      :ok = Presence.untrack(second_socket, second_join.session_id)

      assert_reply push(second_socket, "naf", bot_update_payload(network_id)), :error, %{
        reason: "bot_runner_not_authoritative"
      }
    end

    test "selects the Presence leader deterministically regardless of map/meta order" do
      older = %{
        context: %{"bot_runner" => true},
        bot_runner_lease_id: "older-lease",
        bot_runner_join_order: 10,
        bot_runner_authority_epoch: 7,
        bot_runner_authoritative: true
      }

      newer = %{
        context: %{"bot_runner" => true},
        bot_runner_lease_id: "newer-lease",
        bot_runner_join_order: 11,
        bot_runner_authority_epoch: 7,
        bot_runner_authoritative: false
      }

      assert RetWeb.HubChannel.authoritative_bot_runner_lease_id(%{
               "new-session" => %{metas: [newer]},
               "old-session" => %{metas: [older]}
             }) == "older-lease"

      assert RetWeb.HubChannel.authoritative_bot_runner_lease_id(%{
               "old-session" => %{metas: [older]},
               "new-session" => %{metas: [newer]}
             }) == "older-lease"

      promoted = %{
        newer
        | bot_runner_authority_epoch: 8,
          bot_runner_authoritative: true
      }

      assert RetWeb.HubChannel.authoritative_bot_runner_lease_id(%{
               "old-session" => %{metas: [older]},
               "new-session" => %{metas: [promoted]}
             }) == "newer-lease"
    end

    test "rejects bot_runner=true when the access key is omitted", %{
      socket: socket,
      hub: hub
    } do
      params = join_params(%{"context" => %{"bot_runner" => true}})

      assert {:error, %{reason: "invalid_bot_access_key"}} = join_hub(socket, hub, params)
    end

    test "rejects bot_runner=true when the access key is incorrect", %{
      socket: socket,
      hub: hub
    } do
      params =
        join_params(%{
          "bot_access_key" => String.duplicate("wrong-bot-access-key-", 2),
          "context" => %{"bot_runner" => true}
        })

      assert {:error, %{reason: "invalid_bot_access_key"}} = join_hub(socket, hub, params)
    end

    test "rejects a string bot_runner value even with a valid access key", %{
      socket: socket,
      hub: hub,
      bot_access_key: bot_access_key
    } do
      params =
        join_params(%{
          "bot_access_key" => bot_access_key,
          "context" => %{"bot_runner" => "true"}
        })

      assert {:error, %{reason: "invalid_bot_runner_request"}} = join_hub(socket, hub, params)
    end

    test "normalizes a non-map context instead of crashing the join", %{
      socket: socket,
      hub: hub,
      bot_access_key: bot_access_key
    } do
      params =
        join_params(%{
          "bot_access_key" => bot_access_key,
          "context" => "bot_runner"
        })

      {:ok, %{session_id: session_id, bot_runner: false}, socket} = join_hub(socket, hub, params)

      assert presence_context(socket, session_id) === %{"bot_runner" => false}
    end

    test "does not allow a normal client to spawn the dedicated bot avatar template", %{
      socket: socket,
      hub: hub
    } do
      {:ok, %{bot_runner: false}, socket} = join_hub(socket, hub, @default_join_params)

      assert_reply push(socket, "naf", bot_spawn_payload(bot_network_id(hub, 1))), :error, %{
        reason: "bot_runner_required"
      }
    end

    test "does not let a bot avatar update fall through the generic NAF handler", %{
      socket: socket,
      hub: hub
    } do
      {:ok, %{bot_runner: false}, socket} = join_hub(socket, hub, @default_join_params)

      assert_reply push(socket, "naf", bot_update_payload(bot_network_id(hub, 1))), :error, %{
        reason: "bot_runner_required"
      }
    end

    test "does not let a bot avatar multi-update bypass authorization", %{
      socket: socket,
      hub: hub
    } do
      {:ok, %{bot_runner: false}, socket} = join_hub(socket, hub, @default_join_params)

      payload = %{
        "dataType" => "um",
        "data" => %{"d" => [bot_update_payload(bot_network_id(hub, 1))["data"]]}
      }

      assert_reply push(socket, "naf", payload), :error, %{reason: "bot_runner_required"}

      raw_payload = put_in(payload, ["data", "template"], "#remote-avatar")

      assert_reply push(socket, "nafr", %{"naf" => Jason.encode!(raw_payload)}), :error, %{
        reason: "bot_runner_required"
      }
    end

    test "does not let an escaped bot template bypass authorization through raw NAF", %{
      socket: socket,
      hub: hub
    } do
      {:ok, %{bot_runner: false}, socket} = join_hub(socket, hub, @default_join_params)

      raw_payload =
        ~S({"dataType":"u","data":{"networkId":"BOT_NETWORK_ID","template":"#remote-bot-\u0061vatar","persistent":false,"\u0069sFirstSync":true}})
        |> String.replace("BOT_NETWORK_ID", bot_network_id(hub, 1))

      assert_reply push(socket, "nafr", %{"naf" => raw_payload}), :error, %{
        reason: "bot_runner_required"
      }
    end

    test "does not allow a compound selector to reach the dedicated bot template", %{
      socket: socket,
      hub: hub
    } do
      {:ok, %{bot_runner: false}, socket} = join_hub(socket, hub, @default_join_params)
      {:ok, observer_socket} = connect(SessionSocket, %{})
      {:ok, _response, _observer_socket} = join_hub(observer_socket, hub, @default_join_params)

      template = "#remote-bot-avatar, #nonexistent-avatar"

      payload =
        bot_spawn_payload("room-bot-selector")
        |> put_in(["data", "template"], template)

      push(socket, "naf", payload)

      refute_push "naf", %{"data" => %{"template" => ^template}}, 100

      push(socket, "nafr", %{"naf" => Jason.encode!(payload)})

      refute_push "nafr", %{"naf" => _raw_payload}, 100
    end

    test "rejects a bot spawn without a usable network id", %{
      socket: socket,
      hub: hub,
      bot_access_key: bot_access_key
    } do
      params =
        join_params(%{
          "bot_access_key" => bot_access_key,
          "context" => %{"bot_runner" => true}
        })

      {:ok, %{bot_runner: true}, socket} = join_hub(socket, hub, params)

      assert_reply push(socket, "naf", bot_spawn_payload("")), :error, %{
        reason: "invalid_network_id"
      }

      malformed =
        bot_update_payload(bot_network_id(hub, 1))
        |> update_in(["data"], &Map.delete(&1, "persistent"))

      assert_reply push(socket, "naf", malformed), :error, %{
        reason: "invalid_bot_spawn_payload"
      }
    end

    test "reserves the exact per-hub bot network namespace for bot-1 through bot-10", %{
      socket: socket,
      hub: hub,
      bot_access_key: bot_access_key
    } do
      params =
        join_params(%{
          "bot_access_key" => bot_access_key,
          "context" => %{"bot_runner" => true}
        })

      {:ok, %{bot_runner: true}, socket} = join_hub(socket, hub, params)

      for network_id <- [
            bot_network_id(hub, 11),
            "room-bot-other-hub-bot-1",
            "arbitrary-bot-id"
          ] do
        assert_reply push(socket, "naf", bot_spawn_payload(network_id)), :error, %{
          reason: "invalid_network_id"
        }
      end
    end

    test "normal clients cannot precreate, update, or remove a protected bot network id", %{
      socket: socket,
      hub: hub
    } do
      {:ok, %{bot_runner: false}, socket} = join_hub(socket, hub, @default_join_params)
      network_id = bot_network_id(hub, 1)

      generic_spawn =
        bot_spawn_payload(network_id)
        |> put_in(["data", "template"], "#remote-avatar")

      assert_reply push(socket, "naf", generic_spawn), :error, %{
        reason: "reserved_bot_network_id"
      }

      for update <- [
            bot_update_payload(network_id) |> update_in(["data"], &Map.delete(&1, "template")),
            bot_update_payload(network_id) |> put_in(["data", "template"], "#remote-avatar")
          ] do
        assert_reply push(socket, "naf", update), :error, %{
          reason: "invalid_bot_spawn_payload"
        }

        assert_reply push(socket, "nafr", %{"naf" => Jason.encode!(update)}), :error, %{
          reason: "invalid_bot_spawn_payload"
        }
      end

      remove = %{"dataType" => "r", "data" => %{"networkId" => network_id}}
      assert_reply push(socket, "naf", remove), :error, %{reason: "bot_runner_required"}

      assert_reply push(socket, "nafr", %{"naf" => Jason.encode!(remove)}), :error, %{
        reason: "bot_runner_required"
      }
    end

    test "rejects array network ids before JavaScript can coerce them into the bot namespace", %{
      socket: socket,
      hub: hub
    } do
      {:ok, %{bot_runner: false}, socket} = join_hub(socket, hub, @default_join_params)
      protected_network_id = bot_network_id(hub, 1)
      coercible_network_id = [protected_network_id]

      generic_first_sync =
        bot_spawn_payload(coercible_network_id)
        |> put_in(["data", "template"], "#remote-avatar")

      for {event, payload} <- [
            {"naf", generic_first_sync},
            {"nafr", %{"naf" => Jason.encode!(generic_first_sync)}}
          ] do
        assert_reply push(socket, event, payload), :error, %{reason: "invalid_network_id"}
      end

      update_without_template = %{
        "dataType" => "u",
        "data" => %{
          "networkId" => coercible_network_id,
          "persistent" => false,
          "owner" => "attacker",
          "lastOwnerTime" => 9_007_199_254_740_991,
          "components" => %{"0" => %{"x" => 999}}
        }
      }

      remove = %{"dataType" => "r", "data" => %{"networkId" => coercible_network_id}}

      multi_update = %{
        "dataType" => "um",
        "data" => %{
          "d" => [
            %{
              "networkId" => "ordinary-avatar",
              "template" => "#remote-avatar",
              "persistent" => false
            },
            update_without_template["data"]
          ]
        }
      }

      for malformed <- [update_without_template, remove, multi_update],
          {event, payload} <- [
            {"naf", malformed},
            {"nafr", %{"naf" => Jason.encode!(malformed)}}
          ] do
        assert_reply push(socket, event, payload), :error, %{reason: "invalid_network_id"}
      end
    end

    test "multi-update authorization follows protected network ids even without a template", %{
      socket: socket,
      hub: hub
    } do
      {:ok, %{bot_runner: false}, socket} = join_hub(socket, hub, @default_join_params)

      update = %{
        "networkId" => bot_network_id(hub, 1),
        "persistent" => false,
        "components" => %{"0" => %{"x" => 1}}
      }

      payload = %{"dataType" => "um", "data" => %{"d" => [update]}}

      assert_reply push(socket, "naf", payload), :error, %{
        reason: "invalid_bot_spawn_payload"
      }

      assert_reply push(socket, "nafr", %{"naf" => Jason.encode!(payload)}), :error, %{
        reason: "invalid_bot_spawn_payload"
      }
    end
  end

  describe "hub invites" do
    test "join is denied when joining without an invite", %{socket: socket} do
      %{hub: hub} = create_invite_only_hub()

      {:error, %{reason: "join_denied"}} = join_hub(socket, hub, @default_join_params)
    end

    test "join is denied when joining with an invalid invite", %{socket: socket} do
      %{hub: hub} = create_invite_only_hub()

      {:error, %{reason: "join_denied"}} =
        join_hub(socket, hub, join_params_for_hub_invite_id("invalid_invite_id"))
    end

    test "join is denied when joining with a revoked invite", %{socket: socket} do
      %{hub: hub, hub_invite: hub_invite} = create_invite_only_hub()

      Ret.HubInvite.revoke_invite(hub, hub_invite.hub_invite_sid)

      {:error, %{reason: "join_denied"}} =
        join_hub(socket, hub, join_params_for_hub_invite(hub_invite))
    end

    test "join is denied when joining with a mismatched invite", %{socket: socket} do
      %{hub: hub_one} = create_invite_only_hub()
      %{hub: _hub_two, hub_invite: hub_two_invite} = create_invite_only_hub()

      # Attempt to join hub_one, using invite associated with hub_two
      {:error, %{reason: "join_denied"}} =
        join_hub(
          socket,
          hub_one,
          join_params_for_hub_invite(hub_two_invite)
        )
    end

    test "revoke cannot be performed by an anonymous user", %{socket: socket} do
      %{hub: hub, hub_invite: hub_invite} = create_invite_only_hub()

      # Join hub as anonymous user
      {:ok, _context, socket} = join_hub(socket, hub, join_params_for_hub_invite(hub_invite))

      # Attempt to revoke invite
      assert_reply push(socket, "revoke_invite", %{hub_invite_id: hub_invite.hub_invite_sid}),
                   :ok,
                   %{}

      # Join is still denied with invalid invite
      {:error, %{reason: "join_denied"}} =
        join_hub(socket, hub, join_params_for_hub_invite_id("invalid_invite_id"))

      # Join is still allowed with original invite
      {:ok, _context, _socket} = join_hub(socket, hub, join_params_for_hub_invite(hub_invite))
    end

    test "revoke is keyed to hub", %{socket: socket} do
      %{hub: hub_one} = create_invite_only_hub()
      %{hub: hub_two, hub_invite: hub_two_invite} = create_invite_only_hub()

      account_one = create_account("account_one")
      assign_creator(hub_one, account_one)

      # Join hub_one
      {:ok, _context, socket} = join_hub(socket, hub_one, join_params_for_account(account_one))

      # Attempt to revoke invite associated with hub_two, using channel associated with hub_one
      %{payload: response_payload} =
        assert_reply push(socket, "revoke_invite", %{hub_invite_id: hub_two_invite.hub_invite_sid}),
                     :ok,
                     %{}

      # Response payload should be empty when a revoke is ignored
      assert response_payload |> Map.keys() |> length === 0

      # Joining hub_two is still denied with invalid invite
      {:error, %{reason: "join_denied"}} =
        join_hub(socket, hub_two, join_params_for_hub_invite_id("invalid_invite_id"))

      # Joining hub_two still allowed with original invite
      {:ok, _context, _socket} =
        join_hub(socket, hub_two, join_params_for_hub_invite(hub_two_invite))
    end

    test "revoke can be performed by hub creator", %{socket: socket} do
      %{hub: hub, hub_invite: hub_invite} = create_invite_only_hub()

      account = create_account("test_account")
      assign_creator(hub, account)

      {:ok, _context, socket} = join_hub(socket, hub, join_params_for_account(account))

      push(socket, "revoke_invite", %{hub_invite_id: hub_invite.hub_invite_sid})
      |> assert_reply(:ok, %{hub_invite_id: new_hub_invite_id})

      assert new_hub_invite_id !== hub_invite.hub_invite_sid
    end

    test "join is allowed on an invite-only hub with a correct invite id", %{socket: socket} do
      %{hub: hub, hub_invite: hub_invite} = create_invite_only_hub()

      {:ok, _context, _socket} = join_hub(socket, hub, join_params_for_hub_invite(hub_invite))
    end
  end

  defp join_params_for_hub_invite(%HubInvite{} = hub_invite) do
    join_params_for_hub_invite_id(hub_invite.hub_invite_sid)
  end

  defp join_params_for_hub_invite_id(hub_invite_id) do
    join_params(%{"hub_invite_id" => hub_invite_id})
  end

  defp join_params_for_account(account) do
    {:ok, token, _params} = account |> Ret.Guardian.encode_and_sign()
    join_params(%{"auth_token" => token})
  end

  defp join_params(%{} = params) do
    Map.merge(@default_join_params, params)
  end

  defp join_hub(socket, %Hub{} = hub, params) do
    subscribe_and_join(socket, "hub:#{hub.hub_sid}", params)
  end

  defp presence_context(socket, session_id) do
    :timer.sleep(100)

    socket
    |> Presence.list()
    |> Map.fetch!(session_id)
    |> Map.fetch!(:metas)
    |> List.first()
    |> Map.fetch!(:context)
  end

  defp wait_for_runner_lease(socket, expected, attempts \\ 25)
  defp wait_for_runner_lease(_socket, _expected, 0), do: false

  defp wait_for_runner_lease(socket, expected, attempts) do
    if RetWeb.HubChannel.authoritative_bot_runner_lease_id(Presence.list(socket)) == expected do
      true
    else
      :timer.sleep(20)
      wait_for_runner_lease(socket, expected, attempts - 1)
    end
  end

  defp bot_spawn_payload(network_id) do
    %{
      "dataType" => "u",
      "data" => %{
        "isFirstSync" => true,
        "persistent" => false,
        "template" => "#remote-bot-avatar",
        "networkId" => network_id
      }
    }
  end

  defp bot_update_payload(network_id) do
    %{
      "dataType" => "u",
      "data" => %{
        "persistent" => false,
        "template" => "#remote-bot-avatar",
        "networkId" => network_id
      }
    }
  end

  defp bot_network_id(%Hub{hub_sid: hub_sid}, bot_number) do
    "room-bot-#{hub_sid}-bot-#{bot_number}"
  end

  defp create_invite_only_hub() do
    {:ok, hub: hub} = create_hub(%{scene: nil})

    hub
    |> Ret.Hub.changeset_for_entry_mode(:invite)
    |> Ret.Repo.update!()

    hub_invite = Ret.HubInvite.find_or_create_invite_for_hub(hub)

    %{hub: hub, hub_invite: hub_invite}
  end

  defp restore_application_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_application_env(app, key, value), do: Application.put_env(app, key, value)
end
