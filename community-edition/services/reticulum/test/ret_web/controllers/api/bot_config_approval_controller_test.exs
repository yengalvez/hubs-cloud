defmodule RetWeb.BotConfigApprovalControllerTest do
  use RetWeb.ConnCase

  import Ret.TestHelpers

  alias Ret.{BotConfigAdmission, BotConfigApproval, BotRuntimeOutbox, Hub, Repo}

  setup [:create_account, :create_owned_file, :create_scene]

  setup do
    admin = create_account("bot-approval-api-admin-#{System.unique_integer([:positive])}", true)
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

    {:ok, admin: admin}
  end

  test "the inventory and decisions require a current global administrator", %{
    account: account,
    conn: conn
  } do
    conn
    |> auth_with_account(account)
    |> get("/api/v1/bot_config_approvals")
    |> response(401)

    conn
    |> recycle()
    |> auth_with_account(account)
    |> post("/api/v1/bot_config_approvals/missing/quarantine", %{})
    |> response(401)
  end

  test "inventory is paginated, no-store and never returns prompt or config JSON", %{
    admin: admin,
    conn: conn,
    scene: scene
  } do
    secret_prompt = "api-inventory-secreto-秘密-🔒"

    {:ok, hub} =
      insert_active_hub(
        scene,
        admin,
        "API inventory",
        put_in(active_user_data(), ["bots", "prompt"], secret_prompt)
      )

    {:ok, _hub} = BotConfigApproval.quarantine(hub.hub_sid, admin)

    conn =
      conn
      |> auth_with_account(admin)
      |> get("/api/v1/bot_config_approvals?limit=1")

    assert get_resp_header(conn, "cache-control") == ["no-store"]

    response = json_response(conn, 200)
    assert response["protocol"] == 1
    assert [%{"hub_sid" => hub_sid} = entry] = response["approvals"]
    assert hub_sid == hub.hub_sid
    assert entry["state"] == "quarantined"
    assert entry["candidate_config_fingerprint"] =~ ~r/^v1:[0-9a-f]{64}$/

    assert entry["candidate_summary"] == %{
             "chat_enabled" => true,
             "count" => 1,
             "enabled" => true,
             "mobility" => "static",
             "prompt_bytes" => byte_size(secret_prompt),
             "prompt_codepoints" => secret_prompt |> String.codepoints() |> length(),
             "prompt_present" => true
           }

    assert entry["current_summary"] == %{
             "chat_enabled" => true,
             "count" => 1,
             "enabled" => false,
             "mobility" => "static",
             "prompt_bytes" => byte_size(secret_prompt),
             "prompt_codepoints" => secret_prompt |> String.codepoints() |> length(),
             "prompt_present" => true
           }

    assert entry["created_by_account_id"] == admin.account_id
    assert entry["entry_mode"] == "allow"
    refute Map.has_key?(entry, "candidate_bots")
    refute Map.has_key?(entry, "approved_bots")
    refute Map.has_key?(entry, "current_bots")
    refute entry |> Poison.encode!() |> String.contains?(secret_prompt)
    refute conn.resp_body =~ secret_prompt
  end

  test "quarantine and approve use an expected fingerprint and enqueue ordered runtime", %{
    admin: admin,
    conn: conn,
    scene: scene
  } do
    {:ok, hub} = insert_active_hub(scene, admin, "API decision")

    quarantine_conn =
      conn
      |> auth_with_account(admin)
      |> post("/api/v1/bot_config_approvals/#{hub.hub_sid}/quarantine", %{})

    assert get_resp_header(quarantine_conn, "cache-control") == ["no-store"]
    assert %{"status" => "quarantined"} = json_response(quarantine_conn, 200)

    stop = Repo.get_by!(BotRuntimeOutbox, hub_id: hub.hub_id, runtime_revision: 2)
    assert stop.event_kind == "stop"
    assert stop.hub_sid == hub.hub_sid
    assert is_integer(stop.revoke_epoch) and stop.revoke_epoch > 0

    approval = Repo.get!(BotConfigApproval, hub.hub_id)
    assert {:ok, fingerprint} = BotConfigApproval.fingerprint(approval.candidate_bots)

    mismatch_conn =
      conn
      |> recycle()
      |> auth_with_account(admin)
      |> post("/api/v1/bot_config_approvals/#{hub.hub_sid}/approve", %{
        expected_config_fingerprint: "v1:" <> String.duplicate("0", 64)
      })

    assert get_resp_header(mismatch_conn, "cache-control") == ["no-store"]
    assert %{"error" => "fingerprint_mismatch"} = json_response(mismatch_conn, 409)
    refute Repo.get!(Hub, hub.hub_id).user_data["bots"]["enabled"]

    approve_conn =
      conn
      |> recycle()
      |> auth_with_account(admin)
      |> post("/api/v1/bot_config_approvals/#{hub.hub_sid}/approve", %{
        expected_config_fingerprint: fingerprint
      })

    assert get_resp_header(approve_conn, "cache-control") == ["no-store"]
    assert %{"status" => "approved"} = json_response(approve_conn, 200)

    config = Repo.get_by!(BotRuntimeOutbox, hub_id: hub.hub_id, runtime_revision: 3)
    assert config.event_kind == "config"
    assert config.hub_sid == hub.hub_sid
    assert config.bots["enabled"]
    assert is_nil(config.delivered_at)
    refute_receive {:bot_orchestrator_request, _, _, _, _, _}
    assert BotConfigApproval.runtime_enabled?(Repo.get!(Hub, hub.hub_id))
  end

  test "approve reports a stable conflict for a closed room and changes no revision", %{
    admin: admin,
    conn: conn,
    scene: scene
  } do
    {:ok, hub} = insert_active_hub(scene, admin, "Closed API approval")

    assert {:ok, _closed} =
             hub
             |> Ecto.Changeset.change(entry_mode: :deny)
             |> BotConfigAdmission.update(admin)

    before_hub = Repo.get!(Hub, hub.hub_id)
    before_approval = Repo.get!(BotConfigApproval, hub.hub_id)
    assert before_approval.runtime_revision == 2
    assert {:ok, fingerprint} = BotConfigApproval.fingerprint(before_approval.candidate_bots)

    approve_conn =
      conn
      |> auth_with_account(admin)
      |> post("/api/v1/bot_config_approvals/#{hub.hub_sid}/approve", %{
        expected_config_fingerprint: fingerprint
      })

    assert get_resp_header(approve_conn, "cache-control") == ["no-store"]
    assert %{"error" => "room_closed"} = json_response(approve_conn, 409)
    assert Repo.get!(Hub, hub.hub_id) == before_hub
    assert Repo.get!(BotConfigApproval, hub.hub_id) == before_approval
    assert Repo.get_by(BotRuntimeOutbox, hub_id: hub.hub_id, runtime_revision: 3) == nil
    refute_receive {:bot_orchestrator_request, _, _, _, _, _}
  end

  defp insert_active_hub(scene, admin, name, user_data \\ active_user_data()) do
    %Hub{}
    |> Hub.changeset(scene, %{name: name, user_data: user_data})
    |> Hub.add_account_to_changeset(admin)
    |> BotConfigAdmission.insert(admin)
  end

  defp active_user_data do
    %{
      "bots" => %{
        "enabled" => true,
        "count" => 1,
        "mobility" => "static",
        "chat_enabled" => true,
        "prompt" => ""
      }
    }
  end

  defp restore_application_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_application_env(app, key, value), do: Application.put_env(app, key, value)
end
