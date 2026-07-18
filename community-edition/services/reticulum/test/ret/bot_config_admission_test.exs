defmodule Ret.BotConfigAdmissionTest do
  use Ret.DataCase

  import Ret.TestHelpers

  alias Ret.{BotConfigAdmission, BotConfigApproval, Hub, Repo}
  alias Ret.Api.{Credentials, Scopes}

  setup [:create_account, :create_owned_file, :create_scene]

  setup do
    previous = Application.get_env(:ret, :max_active_bot_rooms)
    Application.put_env(:ret, :max_active_bot_rooms, 2)

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(:ret, :max_active_bot_rooms)
      else
        Application.put_env(:ret, :max_active_bot_rooms, previous)
      end
    end)

    admin = create_account("bot-admission-admin-#{System.unique_integer([:positive])}", true)
    {:ok, admin: admin}
  end

  test "only a global administrator can create an active bot room", %{
    account: account,
    admin: admin,
    scene: scene
  } do
    assert {:error, denied} =
             scene
             |> new_hub_changeset("Denied", account, active_user_data())
             |> BotConfigAdmission.insert(account)

    assert "only a global administrator may create or modify room bot configuration" in errors_on(
             denied
           ).user_data

    refute Repo.get_by(Hub, name: "Denied")

    assert {:ok, accepted} =
             scene
             |> new_hub_changeset("Accepted", admin, active_user_data())
             |> BotConfigAdmission.insert(admin)

    assert accepted.user_data["bots"]["count"] == 1

    inactive_staging = put_in(active_user_data(), ["bots", "enabled"], false)

    assert {:error, staged_denied} =
             scene
             |> new_hub_changeset("Staged", account, inactive_staging)
             |> BotConfigAdmission.insert(account)

    assert "only a global administrator may create or modify room bot configuration" in errors_on(
             staged_denied
           ).user_data

    refute Repo.get_by(Hub, name: "Staged")
  end

  test "a stale administrator struct cannot authorize after the account is disabled", %{
    admin: stale_admin,
    scene: scene
  } do
    {:ok, approved_hub} = insert_active_hub(scene, stale_admin, "Approved before disable")

    stale_admin
    |> Ecto.Changeset.change(state: :disabled)
    |> Repo.update!()

    assert {:error, denied} =
             scene
             |> new_hub_changeset("Disabled admin", stale_admin, active_user_data())
             |> BotConfigAdmission.insert(stale_admin)

    assert "the authenticated account is no longer active for bot configuration changes" in errors_on(
             denied
           ).user_data

    refute Repo.get_by(Hub, name: "Disabled admin")

    for intended <- [
          Map.put(active_user_data(), "theme", "preserved"),
          put_in(active_user_data(), ["bots", "enabled"], false)
        ] do
      assert {:error, stale_denied} =
               approved_hub
               |> Hub.add_attrs_to_changeset(%{user_data: intended})
               |> BotConfigAdmission.update(stale_admin)

      assert "the authenticated account is no longer active for bot configuration changes" in errors_on(
               stale_denied
             ).user_data
    end

    assert Repo.get!(Hub, approved_hub.hub_id).user_data["bots"]["enabled"]
  end

  test "a deleted stale actor cannot use a previously loaded account struct", %{
    admin: admin,
    scene: scene
  } do
    {:ok, approved_hub} = insert_active_hub(scene, admin, "Approved before delete")
    stale_actor = create_account("deleted-bot-actor-#{System.unique_integer([:positive])}", true)
    Repo.delete!(stale_actor)

    assert {:error, denied} =
             approved_hub
             |> Hub.add_attrs_to_changeset(%{
               user_data: put_in(active_user_data(), ["bots", "enabled"], false)
             })
             |> BotConfigAdmission.update(stale_actor)

    assert "the authenticated account is no longer active for bot configuration changes" in errors_on(
             denied
           ).user_data

    assert Repo.get!(Hub, approved_hub.hub_id).user_data["bots"]["enabled"]
  end

  test "a non-admin may preserve or disable approved bots but cannot modify them", %{
    account: account,
    admin: admin,
    scene: scene
  } do
    {:ok, hub} =
      scene
      |> new_hub_changeset("Approved", admin, active_user_data())
      |> BotConfigAdmission.insert(admin)

    preserved = Map.put(active_user_data(), "theme", "aurora")

    assert {:ok, preserved_hub} =
             hub
             |> Hub.add_attrs_to_changeset(%{user_data: preserved})
             |> BotConfigAdmission.update(account)

    assert preserved_hub.user_data["theme"] == "aurora"

    modified = put_in(preserved, ["bots", "count"], 2)

    assert {:error, denied} =
             preserved_hub
             |> Hub.add_attrs_to_changeset(%{user_data: modified})
             |> BotConfigAdmission.update(account)

    assert "only a global administrator may create or modify room bot configuration" in errors_on(
             denied
           ).user_data

    assert Repo.get!(Hub, hub.hub_id).user_data["bots"]["count"] == 1

    raw_equivalent = put_in(preserved, ["bots", "count"], "1")

    assert {:error, raw_denied} =
             preserved_hub
             |> Ecto.Changeset.change(user_data: raw_equivalent)
             |> BotConfigAdmission.update(account)

    assert "only a global administrator may create or modify room bot configuration" in errors_on(
             raw_denied
           ).user_data

    disabled = put_in(preserved, ["bots", "enabled"], false)
    disabled_and_modified = put_in(disabled, ["bots", "prompt"], "staged by owner")

    assert {:error, staged_denied} =
             preserved_hub
             |> Hub.add_attrs_to_changeset(%{user_data: disabled_and_modified})
             |> BotConfigAdmission.update(account)

    assert "only a global administrator may create or modify room bot configuration" in errors_on(
             staged_denied
           ).user_data

    assert {:ok, disabled_hub} =
             preserved_hub
             |> Hub.add_attrs_to_changeset(%{user_data: disabled})
             |> BotConfigAdmission.update(account)

    refute disabled_hub.user_data["bots"]["enabled"]
  end

  test "ordinary updates cannot strip future approved bot fields through normalization", %{
    account: account,
    admin: admin,
    scene: scene
  } do
    {:ok, hub} = insert_active_hub(scene, admin, "Future bot field")
    future_user_data = put_in(hub.user_data, ["bots", "future_contract"], %{"version" => 2})

    assert {:ok, future_hub} =
             hub
             |> Ecto.Changeset.change(user_data: future_user_data)
             |> BotConfigAdmission.update(admin)

    assert BotConfigApproval.runtime_enabled?(future_hub)

    assert {:error, denied} =
             future_hub
             |> Hub.add_attrs_to_changeset(%{
               user_data: Map.put(future_hub.user_data, "theme", "aurora")
             })
             |> BotConfigAdmission.update(account)

    assert "only a global administrator may create or modify room bot configuration" in errors_on(
             denied
           ).user_data

    persisted_hub = Repo.get!(Hub, hub.hub_id)
    assert persisted_hub.user_data["bots"]["future_contract"] == %{"version" => 2}
    assert BotConfigApproval.runtime_enabled?(persisted_hub)
  end

  test "the configured ceiling rejects N plus one without persisting it and reopens after disable",
       %{
         account: account,
         admin: admin,
         scene: scene
       } do
    {:ok, first} = insert_active_hub(scene, admin, "First")
    {:ok, _second} = insert_active_hub(scene, admin, "Second")

    assert {:error, denied} = insert_active_hub(scene, admin, "Third")
    assert "the configured active bot room limit has been reached" in errors_on(denied).user_data
    refute Repo.get_by(Hub, name: "Third")

    assert {:ok, _disabled} =
             first
             |> Hub.add_attrs_to_changeset(%{
               user_data: put_in(active_user_data(), ["bots", "enabled"], false)
             })
             |> BotConfigAdmission.update(account)

    assert {:ok, third} = insert_active_hub(scene, admin, "Third")
    assert third.name == "Third"
  end

  test "closing disables bots atomically, preserves their settings and frees the slot", %{
    account: account,
    admin: admin,
    scene: scene
  } do
    {:ok, first} = insert_active_hub(scene, admin, "Close first")
    {:ok, _second} = insert_active_hub(scene, admin, "Close second")

    assert {:ok, closed} =
             first
             |> Hub.changeset_for_entry_mode(:deny)
             |> BotConfigAdmission.update(account)

    assert closed.entry_mode == :deny
    refute closed.user_data["bots"]["enabled"]
    assert closed.user_data["bots"]["count"] == 1
    assert closed.user_data["bots"]["mobility"] == "static"

    assert {:ok, _replacement} = insert_active_hub(scene, admin, "Replacement after close")

    assert {:ok, reopened} =
             closed
             |> Hub.changeset_for_entry_mode(:allow)
             |> BotConfigAdmission.update(account)

    assert reopened.entry_mode == :allow
    refute reopened.user_data["bots"]["enabled"]
  end

  test "a legacy closed active room requires admin re-admission on reopen", %{
    account: account,
    admin: admin,
    scene: scene
  } do
    {:ok, active_hub} = insert_active_hub(scene, admin, "Legacy closed")

    legacy_closed =
      active_hub
      |> Ecto.Changeset.change(entry_mode: :deny)
      |> Repo.update!()

    assert legacy_closed.user_data["bots"]["enabled"]

    assert {:error, denied} =
             legacy_closed
             |> Hub.changeset_for_entry_mode(:allow)
             |> BotConfigAdmission.update(account)

    assert "only a global administrator may create or modify room bot configuration" in errors_on(
             denied
           ).user_data

    assert {:ok, reopened} =
             legacy_closed
             |> Hub.changeset_for_entry_mode(:allow)
             |> BotConfigAdmission.update(admin)

    assert reopened.entry_mode == :allow
    assert reopened.user_data["bots"]["enabled"]
  end

  test "a stale reopen changeset is revalidated after bots are staged while closed", %{
    account: account,
    admin: admin,
    scene: scene
  } do
    {:ok, ordinary_hub} =
      %Hub{}
      |> Hub.changeset(scene, %{name: "Stale reopen"})
      |> Hub.add_account_to_changeset(account)
      |> Repo.insert()

    {:ok, closed_hub} =
      ordinary_hub
      |> Hub.changeset_for_entry_mode(:deny)
      |> BotConfigAdmission.update(account)

    stale_reopen = Hub.changeset_for_entry_mode(closed_hub, :allow)

    assert {:ok, staged_hub} =
             closed_hub
             |> Hub.add_attrs_to_changeset(%{user_data: active_user_data()})
             |> BotConfigAdmission.update(admin)

    assert staged_hub.entry_mode == :deny
    assert staged_hub.user_data["bots"]["enabled"]

    assert {:error, denied} = BotConfigAdmission.update(stale_reopen, account)

    assert "only a global administrator may create or modify room bot configuration" in errors_on(
             denied
           ).user_data

    assert {:ok, reopened} =
             staged_hub
             |> Hub.changeset_for_entry_mode(:allow)
             |> BotConfigAdmission.update(admin)

    assert reopened.entry_mode == :allow
    assert reopened.user_data["bots"]["enabled"]
  end

  test "concurrent admissions persist exactly the configured ceiling", %{
    admin: _admin,
    scene: _scene
  } do
    prefix = "bot-admission-race-#{System.unique_integer([:positive])}"

    Ecto.Adapters.SQL.Sandbox.unboxed_run(Repo, fn ->
      admin = Repo.insert!(%Ret.Account{is_admin: true})
      parent = self()

      try do
        tasks =
          Enum.map(1..3, fn index ->
            Task.async(fn ->
              :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo, sandbox: false)

              try do
                send(parent, {:ready, self()})

                receive do
                  :go ->
                    %Hub{}
                    |> Hub.changeset(nil, %{
                      name: "#{prefix}-#{index}",
                      user_data: active_user_data()
                    })
                    |> BotConfigAdmission.insert(admin)
                end
              after
                Ecto.Adapters.SQL.Sandbox.checkin(Repo)
              end
            end)
          end)

        ready_pids =
          Enum.map(tasks, fn _task ->
            assert_receive {:ready, task_pid}, 2_000
            task_pid
          end)

        assert MapSet.new(ready_pids) == MapSet.new(Enum.map(tasks, & &1.pid))
        Enum.each(tasks, &send(&1.pid, :go))
        results = Enum.map(tasks, &Task.await(&1, 5_000))

        assert Enum.count(results, &match?({:ok, %Hub{}}, &1)) == 2
        assert Enum.count(results, &match?({:error, %Ecto.Changeset{}}, &1)) == 1

        assert Repo.aggregate(
                 from(h in Hub, where: like(h.name, ^"#{prefix}%")),
                 :count,
                 :hub_id
               ) == 2
      after
        Repo.delete_all(from h in Hub, where: like(h.name, ^"#{prefix}%"))
        Repo.delete!(admin)
      end
    end)
  end

  test "Reticulum clamps the same hard ceiling as the orchestrator" do
    Application.put_env(:ret, :max_active_bot_rooms, 1)
    assert BotConfigAdmission.max_active_rooms() == 1

    Application.put_env(:ret, :max_active_bot_rooms, 999)
    assert BotConfigAdmission.max_active_rooms() == 10

    Application.put_env(:ret, :max_active_bot_rooms, "invalid")
    assert BotConfigAdmission.max_active_rooms() == 5
  end

  test "GraphQL room create and update credentials cannot bypass admission", %{
    account: account,
    admin: admin
  } do
    non_admin_credentials = room_credentials(account)
    admin_credentials = room_credentials(admin)

    assert {:error, create_denied} =
             Ret.Api.Rooms.authed_create_room(non_admin_credentials, %{
               name: "GraphQL denied",
               host: "test-janus",
               user_data: active_user_data()
             })

    assert "only a global administrator may create or modify room bot configuration" in errors_on(
             create_denied
           ).user_data

    assert {:ok, admin_hub} =
             Ret.Api.Rooms.authed_create_room(admin_credentials, %{
               name: "GraphQL admin",
               host: "test-janus",
               user_data: active_user_data()
             })

    assert admin_hub.user_data["bots"]["count"] == 1

    assert {:ok, ordinary_hub} =
             Ret.Api.Rooms.authed_create_room(non_admin_credentials, %{
               name: "GraphQL ordinary",
               host: "test-janus"
             })

    assert {:error, update_denied} =
             Ret.Api.Rooms.authed_update_room(
               ordinary_hub.hub_sid,
               non_admin_credentials,
               %{user_data: active_user_data()}
             )

    assert "only a global administrator may create or modify room bot configuration" in errors_on(
             update_denied
           ).user_data

    refute Repo.get!(Hub, ordinary_hub.hub_id).user_data["bots"]["enabled"]
  end

  test "GraphQL app credentials cannot create or stage bot configuration", %{
    admin: admin
  } do
    app_credentials = %Credentials{
      subject_type: :app,
      scopes: [Scopes.write_rooms()],
      is_revoked: false
    }

    assert %Ecto.Association.NotLoaded{} = app_credentials.account

    assert {:error, create_denied} =
             Ret.Api.Rooms.authed_create_room(app_credentials, %{
               name: "App staged",
               host: "test-janus",
               user_data: put_in(active_user_data(), ["bots", "enabled"], false)
             })

    assert "only a global administrator may create or modify room bot configuration" in errors_on(
             create_denied
           ).user_data

    assert {:ok, ordinary_hub} =
             Ret.Api.Rooms.authed_create_room(app_credentials, %{
               name: "App ordinary",
               host: "test-janus"
             })

    admin_bound_app_credentials = %{app_credentials | account: admin}

    assert {:error, admin_bound_create_denied} =
             Ret.Api.Rooms.authed_create_room(admin_bound_app_credentials, %{
               name: "App admin association",
               host: "test-janus",
               user_data: active_user_data()
             })

    assert "only a global administrator may create or modify room bot configuration" in errors_on(
             admin_bound_create_denied
           ).user_data

    assert {:error, update_denied} =
             Ret.Api.Rooms.authed_update_room(
               ordinary_hub.hub_sid,
               admin_bound_app_credentials,
               %{user_data: active_user_data()}
             )

    assert "only a global administrator may create or modify room bot configuration" in errors_on(
             update_denied
           ).user_data

    {:ok, approved_hub} =
      ordinary_hub
      |> Hub.add_attrs_to_changeset(%{user_data: active_user_data()})
      |> BotConfigAdmission.update(admin)

    assert {:ok, disabled_hub} =
             Ret.Api.Rooms.authed_update_room(
               approved_hub.hub_sid,
               admin_bound_app_credentials,
               %{user_data: put_in(active_user_data(), ["bots", "enabled"], false)}
             )

    refute disabled_hub.user_data["bots"]["enabled"]
  end

  defp insert_active_hub(scene, admin, name) do
    scene
    |> new_hub_changeset(name, admin, active_user_data())
    |> BotConfigAdmission.insert(admin)
  end

  defp new_hub_changeset(scene, name, account, user_data) do
    %Hub{}
    |> Hub.changeset(scene, %{name: name, user_data: user_data})
    |> Hub.add_account_to_changeset(account)
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

  defp room_credentials(account) do
    %Credentials{
      account: account,
      subject_type: :account,
      scopes: [Scopes.write_rooms()],
      is_revoked: false
    }
  end
end
