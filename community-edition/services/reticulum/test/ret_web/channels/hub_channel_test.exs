defmodule RetWeb.HubChannelTest do
  use RetWeb.ChannelCase
  import Ret.TestHelpers

  alias RetWeb.{Presence, SessionSocket}
  alias Ret.{AppConfig, Account, Repo, Hub, HubInvite}

  @default_join_params %{"profile" => %{}, "context" => %{}}

  setup [:create_account, :create_owned_file, :create_scene, :create_hub, :create_account]

  setup do
    {:ok, socket} = connect(SessionSocket, %{})
    {:ok, socket: socket}
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
  end

  describe "bot runner presence context" do
    setup do
      original_bot_access_key = Application.get_env(:ret, :bot_access_key)
      bot_access_key = String.duplicate("trusted-bot-access-key-", 2)

      Application.put_env(:ret, :bot_access_key, bot_access_key)

      on_exit(fn ->
        if is_nil(original_bot_access_key) do
          Application.delete_env(:ret, :bot_access_key)
        else
          Application.put_env(:ret, :bot_access_key, original_bot_access_key)
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
end
