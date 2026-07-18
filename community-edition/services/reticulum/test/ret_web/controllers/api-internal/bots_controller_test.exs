defmodule RetWeb.ApiInternal.V1.BotsControllerTest do
  use RetWeb.ConnCase

  import Ret.TestHelpers

  alias Ret.{BotConfigApproval, Repo}

  @bot_runner_access_header "x-ret-bot-orchestrator-access-key"
  @bot_runner_access_key "test-bot-orchestrator-access-key-32bytes"
  @dashboard_access_header "x-ret-dashboard-access-key"
  @dashboard_access_key "test-dashboard-access-key-32bytes"

  setup_all do
    original_bot_runner_access_key = Application.get_env(:ret, :bot_orchestrator_access_key)
    Application.put_env(:ret, :bot_orchestrator_access_key, @bot_runner_access_key)

    merge_module_config(:ret, RetWeb.Plugs.DashboardHeaderAuthorization, %{
      dashboard_access_key: @dashboard_access_key
    })

    on_exit(fn ->
      if is_nil(original_bot_runner_access_key) do
        Application.delete_env(:ret, :bot_orchestrator_access_key)
      else
        Application.put_env(:ret, :bot_orchestrator_access_key, original_bot_runner_access_key)
      end

      Ret.TestHelpers.merge_module_config(:ret, RetWeb.Plugs.DashboardHeaderAuthorization, %{
        dashboard_access_key: nil
      })
    end)
  end

  setup [:create_account, :create_owned_file, :create_scene, :create_hub]

  test "valid active config is excluded without approval and after an exact-match drift", %{
    conn: conn,
    hub: hub
  } do
    hub =
      hub
      |> Ecto.Changeset.change(%{
        user_data: %{"bots" => %{"enabled" => true, "count" => 1}}
      })
      |> Repo.update!()

    assert conn
           |> put_req_header(@bot_runner_access_header, @bot_runner_access_key)
           |> get("/api-internal/v1/hubs/configured_with_bots")
           |> json_response(200) == %{"hubs" => []}

    approve_config!(hub)

    drifted =
      hub
      |> Ecto.Changeset.change(%{
        user_data: %{"bots" => %{"enabled" => true, "count" => 2}}
      })
      |> Repo.update!()

    refute drifted.user_data["bots"] == Repo.get!(BotConfigApproval, hub.hub_id).approved_bots

    assert conn
           |> recycle()
           |> put_req_header(@bot_runner_access_header, @bot_runner_access_key)
           |> get("/api-internal/v1/hubs/configured_with_bots")
           |> json_response(200) == %{"hubs" => []}
  end

  test "configured bot discovery tolerates malformed legacy JSON without unsafe casts", %{
    conn: conn,
    hub: valid_hub,
    scene: scene
  } do
    unicode_prompt =
      "\u0085\uFEFF" <> String.duplicate("😀", 2_000) <> "\uFEFF\u0085"

    valid_hub =
      valid_hub
      |> Ecto.Changeset.change(%{
        user_data: %{
          "bots" => %{
            "enabled" => "true",
            "count" => "2",
            "mobility" => "static",
            "chat_enabled" => false,
            "prompt" => unicode_prompt
          }
        }
      })
      |> Repo.update!()

    approve_config!(valid_hub)

    {:ok, hub: malformed_count_hub} = create_hub(%{scene: scene})

    malformed_count_hub
    |> Ecto.Changeset.change(%{
      user_data: %{"bots" => %{"enabled" => true, "count" => "not-an-integer"}}
    })
    |> Repo.update!()

    {:ok, hub: malformed_boolean_hub} = create_hub(%{scene: scene})

    malformed_boolean_hub
    |> Ecto.Changeset.change(%{
      user_data: %{"bots" => %{"enabled" => "not-a-boolean", "count" => 4}}
    })
    |> Repo.update!()

    response =
      conn
      |> put_req_header(@bot_runner_access_header, @bot_runner_access_key)
      |> get("/api-internal/v1/hubs/configured_with_bots")
      |> json_response(200)

    assert [%{"hub_sid" => hub_sid, "bots" => bots}] = response["hubs"]
    assert hub_sid == valid_hub.hub_sid
    assert bots["enabled"] == true
    assert bots["count"] == 2
    assert bots["mobility"] == "static"
    refute String.contains?(bots["prompt"], ["\u0085", "\uFEFF"])
    assert bots["prompt"] |> String.codepoints() |> length() == 1_500
    assert byte_size(bots["prompt"]) == 6_000
  end

  test "configured bot discovery accepts the legacy numeric enabled flag", %{
    conn: conn,
    hub: hub
  } do
    hub =
      hub
      |> Ecto.Changeset.change(%{
        user_data: %{
          "bots" => %{
            "enabled" => 1,
            "count" => 1,
            "mobility" => "low",
            "chat_enabled" => false,
            "prompt" => ""
          }
        }
      })
      |> Repo.update!()

    approve_config!(hub)

    response =
      conn
      |> put_req_header(@bot_runner_access_header, @bot_runner_access_key)
      |> get("/api-internal/v1/hubs/configured_with_bots")
      |> json_response(200)

    assert [%{"hub_sid" => hub_sid, "bots" => %{"count" => 1}}] = response["hubs"]
    assert hub_sid == hub.hub_sid
  end

  test "configured bot discovery fails closed above the hard ten-room bound", %{
    conn: conn,
    hub: hub,
    scene: scene
  } do
    hubs =
      [hub] ++
        Enum.map(1..10, fn _index ->
          {:ok, hub: extra_hub} = create_hub(%{scene: scene})
          extra_hub
        end)

    Enum.each(hubs, fn configured_hub ->
      configured_hub =
        configured_hub
        |> Ecto.Changeset.change(%{
          user_data: %{
            "bots" => %{
              "enabled" => true,
              "count" => 1,
              "mobility" => "static",
              "chat_enabled" => false,
              "prompt" => ""
            }
          }
        })
        |> Repo.update!()

      approve_config!(configured_hub)
    end)

    response =
      conn
      |> put_req_header(@bot_runner_access_header, @bot_runner_access_key)
      |> get("/api-internal/v1/hubs/configured_with_bots")
      |> json_response(409)

    assert response == %{
             "error" => "configured_room_limit_exceeded",
             "max_configured_rooms" => 10
           }
  end

  test "enabled rooms normalized to zero bots do not consume the hard room bound", %{
    conn: conn,
    hub: hub,
    scene: scene
  } do
    hub =
      hub
      |> Ecto.Changeset.change(%{
        user_data: %{"bots" => %{"enabled" => true, "count" => 1}}
      })
      |> Repo.update!()

    approve_config!(hub)

    Enum.each(1..11, fn index ->
      {:ok, hub: zero_hub} = create_hub(%{scene: scene})
      count = if rem(index, 2) == 0, do: "not-an-integer", else: 0

      zero_hub =
        zero_hub
        |> Ecto.Changeset.change(%{
          user_data: %{"bots" => %{"enabled" => true, "count" => count}}
        })
        |> Repo.update!()

      approve_config!(zero_hub)
    end)

    response =
      conn
      |> put_req_header(@bot_runner_access_header, @bot_runner_access_key)
      |> get("/api-internal/v1/hubs/configured_with_bots")
      |> json_response(200)

    assert [%{"hub_sid" => hub_sid, "bots" => %{"count" => 1}}] = response["hubs"]
    assert hub_sid == hub.hub_sid
  end

  test "non-integer numeric enabled flags do not consume the hard room bound", %{
    conn: conn,
    hub: hub,
    scene: scene
  } do
    hub =
      hub
      |> Ecto.Changeset.change(%{
        user_data: %{"bots" => %{"enabled" => 1, "count" => 1}}
      })
      |> Repo.update!()

    approve_config!(hub)

    Enum.each(1..11, fn index ->
      {:ok, hub: disabled_hub} = create_hub(%{scene: scene})
      enabled = if rem(index, 2) == 0, do: 1.0, else: 1.5

      disabled_hub =
        disabled_hub
        |> Ecto.Changeset.change(%{
          user_data: %{"bots" => %{"enabled" => enabled, "count" => 1}}
        })
        |> Repo.update!()

      approve_config!(disabled_hub)
    end)

    response =
      conn
      |> put_req_header(@bot_runner_access_header, @bot_runner_access_key)
      |> get("/api-internal/v1/hubs/configured_with_bots")
      |> json_response(200)

    assert [%{"hub_sid" => hub_sid, "bots" => %{"count" => 1}}] = response["hubs"]
    assert hub_sid == hub.hub_sid
  end

  test "oversized bot configuration fails before Reticulum returns its JSON", %{
    conn: conn,
    hub: hub
  } do
    hub =
      hub
      |> Ecto.Changeset.change(%{
        user_data: %{
          "unrelated" => String.duplicate("x", 100_000),
          "bots" => %{
            "enabled" => true,
            "count" => 1,
            "prompt" => String.duplicate("p", 20_000)
          }
        }
      })
      |> Repo.update!()

    approve_config!(hub)

    response =
      conn
      |> put_req_header(@bot_runner_access_header, @bot_runner_access_key)
      |> get("/api-internal/v1/hubs/configured_with_bots")
      |> json_response(409)

    assert response == %{
             "error" => "bot_config_too_large",
             "max_bot_config_bytes" => 16_384
           }
  end

  test "closed legacy rooms are excluded even if their stored bot config is still active", %{
    conn: conn,
    hub: hub,
    scene: scene
  } do
    hub =
      hub
      |> Ecto.Changeset.change(%{
        entry_mode: :deny,
        user_data: %{"bots" => %{"enabled" => true, "count" => 1}}
      })
      |> Repo.update!()

    approve_config!(hub)

    {:ok, hub: open_hub} = create_hub(%{scene: scene})

    open_hub =
      open_hub
      |> Ecto.Changeset.change(%{
        entry_mode: :allow,
        user_data: %{"bots" => %{"enabled" => true, "count" => 1}}
      })
      |> Repo.update!()

    approve_config!(open_hub)

    response =
      conn
      |> put_req_header(@bot_runner_access_header, @bot_runner_access_key)
      |> get("/api-internal/v1/hubs/configured_with_bots")
      |> json_response(200)

    assert [%{"hub_sid" => hub_sid}] = response["hubs"]
    assert hub_sid == open_hub.hub_sid
  end

  test "active discovery excludes a closed legacy room even while its channel presence is stale",
       %{
         conn: conn,
         hub: closed_hub,
         scene: scene
       } do
    closed_hub =
      closed_hub
      |> Ecto.Changeset.change(%{
        entry_mode: :deny,
        user_data: %{"bots" => %{"enabled" => true, "count" => 1}}
      })
      |> Repo.update!()

    approve_config!(closed_hub)

    {:ok, hub: open_hub} = create_hub(%{scene: scene})

    open_hub =
      open_hub
      |> Ecto.Changeset.change(%{
        entry_mode: :allow,
        user_data: %{"bots" => %{"enabled" => true, "count" => 1}}
      })
      |> Repo.update!()

    approve_config!(open_hub)

    {:ok, _closed_ref} =
      RetWeb.Presence.track(self(), "ret", "closed-legacy-presence", %{
        hub_id: closed_hub.hub_sid
      })

    {:ok, _open_ref} =
      RetWeb.Presence.track(self(), "ret", "open-room-presence", %{hub_id: open_hub.hub_sid})

    response =
      conn
      |> put_req_header(@bot_runner_access_header, @bot_runner_access_key)
      |> get("/api-internal/v1/hubs/active_with_bots")
      |> json_response(200)

    assert [%{"hub_sid" => hub_sid}] = response["hubs"]
    assert hub_sid == open_hub.hub_sid
  end

  test "bot orchestrator access is restricted to bot snapshots", %{conn: conn} do
    assert conn
           |> put_req_header(@dashboard_access_header, @dashboard_access_key)
           |> get("/api-internal/v1/hubs/configured_with_bots")
           |> response(401)

    assert conn
           |> put_req_header(@bot_runner_access_header, @bot_runner_access_key)
           |> get("/api-internal/v1/storage")
           |> response(401)

    assert conn
           |> put_req_header("x-ret-bot-access-key", @bot_runner_access_key)
           |> post("/api/v1/hub_bindings", %{})
           |> response(401)
  end

  defp approve_config!(hub) do
    bots = hub.user_data["bots"]
    now = DateTime.utc_now()

    Repo.insert!(%BotConfigApproval{
      hub_id: hub.hub_id,
      state: "approved",
      candidate_bots: bots,
      approved_bots: bots,
      approved_by_account_id: 1,
      approved_at: now
    })
  end
end
