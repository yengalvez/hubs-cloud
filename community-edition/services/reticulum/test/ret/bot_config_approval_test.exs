defmodule Ret.BotConfigApprovalTest do
  use Ret.DataCase

  import Ret.TestHelpers

  alias Ecto.Adapters.SQL
  alias Ecto.Adapters.SQL.Sandbox

  alias Ret.{
    BotConfigAdmission,
    BotConfigApproval,
    BotRunnerLease,
    BotRuntimeOutbox,
    Hub,
    Repo
  }

  alias Ret.BotRunnerLease.PostgresStore

  setup [:create_account, :create_owned_file, :create_scene]

  setup do
    admin = create_account("bot-approval-admin-#{System.unique_integer([:positive])}", true)
    {:ok, admin: admin}
  end

  test "an active admin create auto-approves from the absence of a row", %{
    admin: admin,
    scene: scene
  } do
    refute Repo.exists?(BotConfigApproval)

    assert {:ok, hub} = insert_active_hub(scene, admin, "Fresh approved room")

    approval = Repo.get!(BotConfigApproval, hub.hub_id)
    assert approval.state == "approved"
    assert approval.candidate_bots == hub.user_data["bots"]
    assert approval.approved_bots == hub.user_data["bots"]
    assert approval.approved_by_account_id == admin.account_id
    assert approval.approved_at
    refute approval.last_quarantined_at
    refute approval.last_quarantine_reason
    assert BotConfigApproval.runtime_enabled?(hub)
  end

  test "fingerprints canonically identify exact JSON and are bounded at 16 KiB" do
    exact = Map.put(active_bots(), "prompt", "private prompt")
    same_exact_json = exact |> Enum.reverse() |> Map.new()

    assert {:ok, "v1:" <> digest} = BotConfigApproval.fingerprint(exact)
    assert byte_size(digest) == 64
    assert BotConfigApproval.exact_json_equal?(same_exact_json, exact)
    assert BotConfigApproval.fingerprint(same_exact_json) == BotConfigApproval.fingerprint(exact)

    refute BotConfigApproval.fingerprint(Map.put(exact, "future_field", true)) ==
             BotConfigApproval.fingerprint(exact)

    refute BotConfigApproval.fingerprint(Map.put(exact, "count", "1")) ==
             BotConfigApproval.fingerprint(exact)

    refute BotConfigApproval.fingerprint(Map.put(exact, "future_number", 1.0)) ==
             BotConfigApproval.fingerprint(Map.put(exact, "future_number", 1))

    refute BotConfigApproval.exact_json_equal?(
             Map.put(exact, "future_number", 1.0),
             Map.put(exact, "future_number", 1)
           )

    assert {:error, :config_too_large} =
             BotConfigApproval.fingerprint(%{
               "enabled" => true,
               "count" => 1,
               "prompt" => String.duplicate("p", BotConfigApproval.max_config_bytes())
             })
  end

  test "inventory suppresses fingerprints for candidates over the persisted JSONB limit", %{
    admin: admin,
    scene: scene
  } do
    candidate =
      active_bots()
      |> Map.put("future_values", List.duplicate(0, 6_000))

    assert {:ok, _compact_fingerprint} = BotConfigApproval.fingerprint(candidate)

    assert {:ok, %{rows: [[persisted_bytes]]}} =
             SQL.query(Repo, "SELECT octet_length($1::jsonb::text)", [candidate])

    assert persisted_bytes > BotConfigApproval.max_config_bytes()

    hub =
      %Hub{}
      |> Hub.changeset(scene, %{name: "Oversized legacy candidate"})
      |> Hub.add_account_to_changeset(admin)
      |> Repo.insert!()

    hub =
      hub
      |> Ecto.Changeset.change(user_data: %{"bots" => Map.put(candidate, "enabled", false)})
      |> Repo.update!()

    Repo.insert!(%BotConfigApproval{
      hub_id: hub.hub_id,
      state: "quarantined",
      candidate_bots: candidate,
      runtime_revision: 1,
      last_quarantined_at: DateTime.utc_now(),
      last_quarantine_reason: "legacy_migration"
    })

    entry =
      BotConfigApproval.inventory(%{"limit" => "100"}).approvals
      |> Enum.find(&(&1.hub_sid == hub.hub_sid))

    assert entry.candidate_config_fingerprint == nil
    assert entry.candidate_summary.enabled
    refute entry.runtime_approved
  end

  test "an unrelated user_data change preserves exact approval but a mismatch fails closed", %{
    account: account,
    admin: admin,
    scene: scene
  } do
    {:ok, hub} = insert_active_hub(scene, admin, "Preserved approval")
    original_approval = Repo.get!(BotConfigApproval, hub.hub_id)

    assert {:ok, preserved_hub} =
             hub
             |> Hub.add_attrs_to_changeset(%{
               user_data: Map.put(hub.user_data, "theme", "aurora")
             })
             |> BotConfigAdmission.update(account)

    assert Repo.get!(BotConfigApproval, hub.hub_id) == original_approval
    assert BotConfigApproval.runtime_enabled?(preserved_hub)

    mismatched_hub =
      preserved_hub
      |> Ecto.Changeset.change(%{
        user_data: put_in(preserved_hub.user_data, ["bots", "count"], 2)
      })
      |> Repo.update!()

    refute BotConfigApproval.runtime_enabled?(mismatched_hub)
  end

  test "integer-to-float drift invalidates runtime approval, capacity and lease registration", %{
    admin: admin,
    scene: scene
  } do
    {:ok, hub} = insert_active_hub(scene, admin, "Type-sensitive approval")
    bots = Map.put(hub.user_data["bots"], "future_number", 1)

    assert {:ok, hub} =
             hub
             |> Ecto.Changeset.change()
             |> Ecto.Changeset.force_change(:user_data, %{"bots" => bots})
             |> BotConfigAdmission.update(admin)

    approval = Repo.get!(BotConfigApproval, hub.hub_id)

    assert BotConfigApproval.runtime_approved?(hub, approval)
    assert BotConfigApproval.runtime_enabled?(hub)
    assert {:ok, %{bots: ^bots}} = BotConfigApproval.runtime_decision(hub)
    assert BotConfigApproval.configured_active_room_count() == 1

    drifted_user_data = put_in(hub.user_data, ["bots", "future_number"], 1.0)

    drifted_hub =
      hub
      |> Ecto.Changeset.change()
      |> Ecto.Changeset.force_change(:user_data, drifted_user_data)
      |> Repo.update!()

    assert drifted_hub.user_data["bots"]["future_number"] === 1.0
    refute BotConfigApproval.runtime_approved?(drifted_hub, approval)
    refute BotConfigApproval.runtime_enabled?(drifted_hub)
    assert {:error, :bot_config_unapproved} = BotConfigApproval.runtime_decision(drifted_hub)
    assert BotConfigApproval.configured_active_room_count() == 0

    assert {:error, :bot_config_unapproved} =
             BotConfigApproval.register_runtime_lease(
               hub.hub_sid,
               Ecto.UUID.generate(),
               generation_claims(hub.hub_sid)
             )

    assert BotRunnerLease.snapshot(hub.hub_sid) == nil
  end

  test "an oversized admin auto-approval rolls back the hub and approval together", %{
    admin: admin,
    scene: scene
  } do
    {:ok, hub} = insert_active_hub(scene, admin, "Bounded mutation")
    original_hub = Repo.get!(Hub, hub.hub_id)
    original_approval = Repo.get!(BotConfigApproval, hub.hub_id)

    oversized_bots =
      active_bots()
      |> Map.put("prompt", String.duplicate("p", BotConfigApproval.max_config_bytes()))

    assert {:error, changeset} =
             hub
             |> Ecto.Changeset.change(user_data: %{"bots" => oversized_bots})
             |> BotConfigAdmission.update(admin)

    assert "bot configuration exceeds the 16384 byte approval limit" in errors_on(changeset).user_data
    assert Repo.get!(Hub, hub.hub_id).user_data == original_hub.user_data
    assert Repo.get!(BotConfigApproval, hub.hub_id) == original_approval

    jsonb_text_heavy_bots =
      active_bots()
      |> Map.put("future_values", List.duplicate(0, 6_000))

    assert {:ok, _fingerprint} = BotConfigApproval.fingerprint(jsonb_text_heavy_bots)

    assert {:error, jsonb_changeset} =
             hub
             |> Ecto.Changeset.change(user_data: %{"bots" => jsonb_text_heavy_bots})
             |> BotConfigAdmission.update(admin)

    assert "bot configuration exceeds the 16384 byte approval limit" in errors_on(jsonb_changeset).user_data

    assert Repo.get!(Hub, hub.hub_id).user_data == original_hub.user_data
    assert Repo.get!(BotConfigApproval, hub.hub_id) == original_approval
  end

  test "closing quarantines the prior active candidate even when entry_mode is the only change",
       %{
         account: account,
         admin: admin,
         scene: scene
       } do
    {:ok, hub} = insert_active_hub(scene, admin, "Close quarantine")
    active_bots = hub.user_data["bots"]

    assert {:ok, closed} =
             hub
             |> Ecto.Changeset.change(entry_mode: :deny)
             |> BotConfigAdmission.update(account)

    approval = Repo.get!(BotConfigApproval, hub.hub_id)
    assert closed.entry_mode == :deny
    assert closed.user_data["bots"] == active_bots
    assert approval.state == "quarantined"
    assert approval.candidate_bots == active_bots
    assert approval.last_quarantine_reason == "room_closed"
    refute approval.approved_bots
    refute BotConfigApproval.runtime_enabled?(closed)

    assert {:ok, reopened} =
             closed
             |> Ecto.Changeset.change(entry_mode: :allow)
             |> BotConfigAdmission.update(admin)

    assert reopened.entry_mode == :allow
    assert Repo.get!(BotConfigApproval, hub.hub_id).state == "quarantined"
    refute BotConfigApproval.runtime_enabled?(reopened)
  end

  test "explicit approval rejects a closed room without changing revision or durable events", %{
    admin: admin,
    scene: scene
  } do
    {:ok, hub} = insert_active_hub(scene, admin, "Closed explicit approval")

    assert {:ok, closed} =
             hub
             |> Ecto.Changeset.change(entry_mode: :deny)
             |> BotConfigAdmission.update(admin)

    before_hub = Repo.get!(Hub, hub.hub_id)
    before_approval = Repo.get!(BotConfigApproval, hub.hub_id)

    before_events =
      BotRuntimeOutbox
      |> where([o], o.hub_id == ^hub.hub_id)
      |> order_by([o], asc: o.runtime_revision)
      |> Repo.all()

    assert closed.entry_mode == :deny
    assert before_approval.state == "quarantined"
    assert before_approval.runtime_revision == 2

    assert Enum.map(before_events, &{&1.runtime_revision, &1.event_kind}) == [
             {1, "config"},
             {2, "stop"}
           ]

    assert {:ok, fingerprint} =
             BotConfigApproval.fingerprint(before_approval.candidate_bots)

    assert {:error, :room_closed} =
             BotConfigApproval.approve_candidate(closed.hub_sid, fingerprint, admin)

    assert Repo.get!(Hub, hub.hub_id) == before_hub
    assert Repo.get!(BotConfigApproval, hub.hub_id) == before_approval

    assert BotRuntimeOutbox
           |> where([o], o.hub_id == ^hub.hub_id)
           |> order_by([o], asc: o.runtime_revision)
           |> Repo.all() == before_events
  end

  test "disabling quarantines and does not retain an approved runtime match", %{
    account: account,
    admin: admin,
    scene: scene
  } do
    {:ok, hub} = insert_active_hub(scene, admin, "Disable quarantine")
    disabled_user_data = put_in(hub.user_data, ["bots", "enabled"], false)

    assert {:ok, disabled_hub} =
             hub
             |> Hub.add_attrs_to_changeset(%{user_data: disabled_user_data})
             |> BotConfigAdmission.update(account)

    approval = Repo.get!(BotConfigApproval, hub.hub_id)
    assert approval.state == "quarantined"
    assert approval.candidate_bots["enabled"] == false
    assert approval.last_quarantine_reason == "bots_disabled"
    refute approval.approved_bots
    refute BotConfigApproval.runtime_enabled?(disabled_hub)
  end

  test "quarantine plus expected fingerprint approval is fail-closed and inventory is redacted",
       %{
         admin: admin,
         scene: scene
       } do
    secret_prompt = "never-return-this-private-prompt"

    {:ok, hub} =
      insert_active_hub(
        scene,
        admin,
        "Explicit approval",
        put_in(active_user_data(), ["bots", "prompt"], secret_prompt)
      )

    assert {:ok, quarantined_hub} = BotConfigApproval.quarantine(hub.hub_sid, admin)
    refute quarantined_hub.user_data["bots"]["enabled"]

    quarantined = Repo.get!(BotConfigApproval, hub.hub_id)
    assert quarantined.state == "quarantined"
    assert quarantined.candidate_bots["prompt"] == secret_prompt
    assert {:ok, fingerprint} = BotConfigApproval.fingerprint(quarantined.candidate_bots)

    assert {:error, :fingerprint_mismatch} =
             BotConfigApproval.approve_candidate(
               hub.hub_sid,
               "v1:" <> String.duplicate("0", 64),
               admin
             )

    refute Repo.get!(Hub, hub.hub_id).user_data["bots"]["enabled"]

    inventory = BotConfigApproval.inventory(%{"limit" => "1"})
    assert inventory.protocol == 1
    assert [%{hub_sid: hub_sid} = entry] = inventory.approvals
    assert hub_sid == hub.hub_sid
    assert entry.state == "quarantined"
    assert entry.candidate_config_fingerprint == fingerprint
    assert entry.candidate_summary.prompt_present
    assert entry.candidate_summary.prompt_bytes == byte_size(secret_prompt)
    assert entry.candidate_summary.prompt_codepoints == String.length(secret_prompt)
    assert entry.current_summary.enabled == false
    assert entry.created_by_account_id == admin.account_id
    assert entry.entry_mode != :deny
    refute Map.has_key?(entry, :candidate_bots)
    refute Poison.encode!(inventory) =~ secret_prompt

    assert {:ok, approved_hub} =
             BotConfigApproval.approve_candidate(hub.hub_sid, fingerprint, admin)

    assert approved_hub.user_data["bots"]["enabled"]
    assert approved_hub.user_data["bots"]["prompt"] == secret_prompt
    assert Repo.get!(BotConfigApproval, hub.hub_id).state == "approved"
    assert BotConfigApproval.runtime_enabled?(approved_hub)
  end

  test "quarantine rolls back hub, approval and revoke epoch if stop enqueue cannot commit", %{
    admin: admin,
    scene: scene
  } do
    {:ok, hub} = insert_active_hub(scene, admin, "Atomic stop enqueue")
    bots = hub.user_data["bots"]

    assert {:ok, %BotRuntimeOutbox{}} =
             Ret.BotRuntimeOutbox.Store.enqueue_config(%{
               hub_id: hub.hub_id,
               hub_sid: hub.hub_sid,
               runtime_revision: 2,
               bots: bots
             })

    assert {:error, :approval_unavailable} = BotConfigApproval.quarantine(hub.hub_sid, admin)

    persisted_hub = Repo.get!(Hub, hub.hub_id)
    persisted_approval = Repo.get!(BotConfigApproval, hub.hub_id)
    assert persisted_hub.user_data["bots"]["enabled"]
    assert persisted_approval.state == "approved"
    assert persisted_approval.runtime_revision == 1

    assert %{rows: [[0]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM ret0.bot_runner_leases WHERE hub_id = $1",
               [hub.hub_id]
             )

    assert ["config", "config"] ==
             BotRuntimeOutbox
             |> where([o], o.hub_id == ^hub.hub_id)
             |> order_by([o], asc: o.runtime_revision)
             |> select([o], o.event_kind)
             |> Repo.all()
  end

  test "approval rolls back the enabled Hub and approval row if config enqueue cannot commit", %{
    admin: admin,
    scene: scene
  } do
    {:ok, hub} = insert_active_hub(scene, admin, "Atomic config enqueue")
    assert {:ok, _quarantined_hub} = BotConfigApproval.quarantine(hub.hub_sid, admin)
    approval = Repo.get!(BotConfigApproval, hub.hub_id)
    assert approval.runtime_revision == 2
    assert {:ok, fingerprint} = BotConfigApproval.fingerprint(approval.candidate_bots)

    assert {:ok, %BotRuntimeOutbox{}} =
             Ret.BotRuntimeOutbox.Store.enqueue_config(%{
               hub_id: hub.hub_id,
               hub_sid: hub.hub_sid,
               runtime_revision: 3,
               bots: approval.candidate_bots
             })

    assert {:error, :approval_unavailable} =
             BotConfigApproval.approve_candidate(hub.hub_sid, fingerprint, admin)

    persisted_hub = Repo.get!(Hub, hub.hub_id)
    persisted_approval = Repo.get!(BotConfigApproval, hub.hub_id)
    refute persisted_hub.user_data["bots"]["enabled"]
    assert persisted_approval.state == "quarantined"
    assert persisted_approval.runtime_revision == 2

    assert ["config", "stop", "config"] ==
             BotRuntimeOutbox
             |> where([o], o.hub_id == ^hub.hub_id)
             |> order_by([o], asc: o.runtime_revision)
             |> select([o], o.event_kind)
             |> Repo.all()
  end

  test "runtime producers reject a hub sid outside the orchestrator contract", %{
    admin: admin,
    scene: scene
  } do
    bots = active_bots()

    # Seed a synthetic pre-outbox approval so this test can corrupt the Hub SID
    # deliberately. Once an outbox event exists, the composite identity FK must
    # (and does) reject changing the Hub side of that durable event identity.
    hub =
      %Hub{}
      |> Hub.changeset(scene, %{
        name: "Invalid runtime sid",
        user_data: %{"bots" => bots}
      })
      |> Hub.add_account_to_changeset(admin)
      |> Repo.insert!()

    Repo.insert!(%BotConfigApproval{
      hub_id: hub.hub_id,
      state: "approved",
      candidate_bots: bots,
      approved_bots: bots,
      approved_by_account_id: admin.account_id,
      approved_at: DateTime.utc_now(),
      runtime_revision: 1
    })

    invalid_sid = String.duplicate("a", 65)

    hub = hub |> Ecto.Changeset.change(hub_sid: invalid_sid) |> Repo.update!()

    assert {:error, :approval_unavailable} = BotConfigApproval.quarantine(invalid_sid, admin)
    assert Repo.get!(Hub, hub.hub_id).user_data["bots"]["enabled"]
    assert Repo.get!(BotConfigApproval, hub.hub_id).runtime_revision == 1

    assert Repo.aggregate(
             from(o in BotRuntimeOutbox, where: o.hub_id == ^hub.hub_id),
             :count
           ) == 0
  end

  test "the approved fingerprint identifies the exact JSONB that becomes active", %{
    admin: admin,
    scene: scene
  } do
    hub =
      %Hub{}
      |> Hub.changeset(scene, %{name: "Exact legacy candidate"})
      |> Hub.add_account_to_changeset(admin)
      |> Repo.insert!()

    candidate = %{
      "enabled" => true,
      "count" => "1",
      "mobility" => "static",
      "chat_enabled" => 1,
      "prompt" => "",
      "future_field" => %{"typed" => [1, "1", true]}
    }

    disabled = Map.put(candidate, "enabled", false)

    hub =
      hub
      |> Ecto.Changeset.change(user_data: %{"bots" => disabled})
      |> Repo.update!()

    Repo.insert!(%BotConfigApproval{
      hub_id: hub.hub_id,
      state: "quarantined",
      candidate_bots: candidate,
      runtime_revision: 1,
      last_quarantined_at: DateTime.utc_now(),
      last_quarantine_reason: "legacy_migration"
    })

    assert {:ok, expected_fingerprint} = BotConfigApproval.fingerprint(candidate)

    assert {:ok, approved_hub} =
             BotConfigApproval.approve_candidate(hub.hub_sid, expected_fingerprint, admin)

    approval = Repo.get!(BotConfigApproval, hub.hub_id)
    assert approved_hub.user_data["bots"] == candidate
    assert approval.candidate_bots == candidate
    assert approval.approved_bots == candidate

    assert BotConfigApproval.fingerprint(approved_hub.user_data["bots"]) ==
             {:ok, expected_fingerprint}

    assert BotConfigApproval.runtime_enabled?(approved_hub)
  end

  test "approval and quarantine revalidate the administrator from the database", %{
    admin: stale_admin,
    scene: scene
  } do
    {:ok, hub} = insert_active_hub(scene, stale_admin, "Stale admin")

    stale_admin
    |> Ecto.Changeset.change(state: :disabled)
    |> Repo.update!()

    assert {:error, :forbidden} = BotConfigApproval.quarantine(hub.hub_sid, stale_admin)
    assert BotConfigApproval.runtime_enabled?(Repo.get!(Hub, hub.hub_id))
  end

  test "a competing replica rejection does not revoke the active database lease", %{
    admin: admin,
    scene: scene
  } do
    {:ok, hub} = insert_active_hub(scene, admin, "Replica overlap")
    incumbent_lease_id = Ecto.UUID.generate()
    incumbent_holder_id = Ecto.UUID.generate()
    incumbent_session_id = Ecto.UUID.generate()

    assert {:ok, %{epoch: incumbent_epoch}} =
             PostgresStore.acquire(
               Repo,
               hub.hub_sid,
               incumbent_lease_id,
               incumbent_holder_id,
               incumbent_session_id,
               generation_claims(hub.hub_sid),
               15
             )

    assert {:error, :lease_unavailable} =
             BotConfigApproval.register_runtime_lease(
               hub.hub_sid,
               Ecto.UUID.generate(),
               generation_claims(hub.hub_sid)
             )

    assert {:ok, :incumbent_still_authoritative} =
             PostgresStore.with_authority(
               Repo,
               hub.hub_sid,
               incumbent_lease_id,
               incumbent_holder_id,
               incumbent_session_id,
               incumbent_epoch,
               fn -> :incumbent_still_authoritative end
             )

    assert {:ok, _fenced_epoch} =
             PostgresStore.release(
               Repo,
               hub.hub_sid,
               incumbent_lease_id,
               incumbent_holder_id,
               incumbent_session_id,
               incumbent_epoch
             )
  end

  @tag timeout: 15_000
  test "runtime registration and final delivery queued behind quarantine reject stale approval" do
    prefix = "bot-register-quarantine-race-#{System.unique_integer([:positive])}"
    parent = self()

    Sandbox.unboxed_run(Repo, fn ->
      admin = Repo.insert!(%Ret.Account{is_admin: true})

      {:ok, hub} =
        %Hub{}
        |> Hub.changeset(nil, %{name: prefix, user_data: active_user_data()})
        |> Hub.add_account_to_changeset(admin)
        |> BotConfigAdmission.insert(admin)

      assert {:ok, approval_decision} = BotConfigApproval.runtime_decision(hub)

      row_gate =
        Task.async(fn ->
          receive do
            :start -> :ok
          end

          with_unboxed_connection(fn ->
            Repo.transaction(fn ->
              Repo.one!(
                from a in Ret.Account,
                  where: a.account_id == ^admin.account_id,
                  lock: "FOR UPDATE"
              )

              send(parent, {:admin_row_locked, self()})

              receive do
                :release -> :ok
              end
            end)
          end)
        end)

      quarantine =
        Task.async(fn ->
          receive do
            :start -> :ok
          end

          with_unboxed_connection(fn ->
            database_pid = postgres_backend_pid()
            send(parent, {:quarantine_started, self(), database_pid})
            BotConfigApproval.quarantine(hub.hub_sid, admin)
          end)
        end)

      registration =
        Task.async(fn ->
          receive do
            :start -> :ok
          end

          with_unboxed_connection(fn ->
            database_pid = postgres_backend_pid()
            send(parent, {:registration_started, self(), database_pid})

            BotConfigApproval.register_runtime_lease(
              hub.hub_sid,
              generation_claims(hub.hub_sid)
            )
          end)
        end)

      delivery =
        Task.async(fn ->
          receive do
            :start -> :ok
          end

          with_unboxed_connection(fn ->
            database_pid = postgres_backend_pid()
            send(parent, {:delivery_started, self(), database_pid})

            BotConfigApproval.with_current_runtime_decision(
              hub.hub_id,
              approval_decision,
              fn ->
                send(parent, :stale_delivery_executed)
                :delivered
              end
            )
          end)
        end)

      row_gate_pid = row_gate.pid
      quarantine_pid = quarantine.pid
      registration_pid = registration.pid
      delivery_pid = delivery.pid

      try do
        send(row_gate_pid, :start)
        assert_receive {:admin_row_locked, ^row_gate_pid}, 2_000

        send(quarantine_pid, :start)
        assert_receive {:quarantine_started, ^quarantine_pid, quarantine_db_pid}, 2_000
        assert_eventually(fn -> advisory_holder_blocked?(quarantine_db_pid) end)

        send(registration_pid, :start)
        assert_receive {:registration_started, ^registration_pid, registration_db_pid}, 2_000

        assert_eventually(fn ->
          waiting_on_same_advisory?(quarantine_db_pid, registration_db_pid)
        end)

        send(delivery_pid, :start)
        assert_receive {:delivery_started, ^delivery_pid, delivery_db_pid}, 2_000

        assert_eventually(fn ->
          waiting_on_same_advisory?(quarantine_db_pid, delivery_db_pid)
        end)

        send(row_gate_pid, :release)
        assert {:ok, :ok} = Task.await(row_gate, 5_000)
        assert {:ok, %Hub{}} = Task.await(quarantine, 5_000)
        assert {:error, :bot_config_unapproved} = Task.await(registration, 5_000)
        assert {:error, :bot_config_unapproved} = Task.await(delivery, 5_000)
        refute_receive :stale_delivery_executed

        assert Repo.get!(BotConfigApproval, hub.hub_id).state == "quarantined"
        refute Repo.get!(Hub, hub.hub_id).user_data["bots"]["enabled"]
        assert BotRunnerLease.snapshot(hub.hub_sid) == nil
      after
        send(row_gate_pid, :release)
        Enum.each([delivery, registration, quarantine, row_gate], &shutdown_task/1)
        _ = BotRunnerLease.revoke(hub.hub_sid)
        matching_outbox = from o in BotRuntimeOutbox, where: o.hub_id == ^hub.hub_id

        Repo.update_all(matching_outbox,
          set: [
            delivered_at: DateTime.utc_now(),
            claim_owner: nil,
            claim_token: nil,
            claim_expires_at: nil
          ]
        )

        Repo.delete_all(matching_outbox)
        Repo.delete_all(from h in Hub, where: h.hub_id == ^hub.hub_id)
        Repo.delete_all(from a in Ret.Account, where: a.account_id == ^admin.account_id)
      end
    end)
  end

  defp insert_active_hub(scene, admin, name, user_data \\ active_user_data()) do
    %Hub{}
    |> Hub.changeset(scene, %{name: name, user_data: user_data})
    |> Hub.add_account_to_changeset(admin)
    |> BotConfigAdmission.insert(admin)
  end

  defp active_user_data, do: %{"bots" => active_bots()}

  defp generation_claims(hub_sid) do
    %{
      "v" => 1,
      "aud" => "yenhubs-bot-runner",
      "hub_sid" => hub_sid,
      "process_generation" => Ecto.UUID.generate(),
      "holder_id" => Ecto.UUID.generate(),
      "recovery_epoch" => "44444444-4444-4444-8444-444444444444",
      "exp" => System.system_time(:second) + 300
    }
  end

  defp active_bots do
    %{
      "enabled" => true,
      "count" => 1,
      "mobility" => "static",
      "chat_enabled" => true,
      "prompt" => ""
    }
  end

  defp with_unboxed_connection(fun) do
    :ok = Sandbox.checkout(Repo, sandbox: false)

    try do
      fun.()
    after
      Sandbox.checkin(Repo)
    end
  end

  defp postgres_backend_pid do
    %{rows: [[database_pid]]} = SQL.query!(Repo, "SELECT pg_backend_pid()", [])
    database_pid
  end

  defp advisory_holder_blocked?(database_pid) do
    %{rows: [[true, true]]} =
      SQL.query!(
        Repo,
        """
        SELECT
          EXISTS (
            SELECT 1 FROM pg_locks
            WHERE pid = $1 AND locktype = 'advisory' AND granted
          ),
          EXISTS (
            SELECT 1 FROM pg_locks
            WHERE pid = $1 AND NOT granted
          )
        """,
        [database_pid]
      )

    true
  rescue
    MatchError -> false
  end

  defp waiting_on_same_advisory?(holder_pid, waiter_pid) do
    %{rows: [[matched]]} =
      SQL.query!(
        Repo,
        """
        SELECT EXISTS (
          SELECT 1
          FROM pg_locks holder
          JOIN pg_locks waiter
            ON waiter.locktype = holder.locktype
           AND waiter.database IS NOT DISTINCT FROM holder.database
           AND waiter.classid IS NOT DISTINCT FROM holder.classid
           AND waiter.objid IS NOT DISTINCT FROM holder.objid
           AND waiter.objsubid IS NOT DISTINCT FROM holder.objsubid
          WHERE holder.pid = $1
            AND waiter.pid = $2
            AND holder.locktype = 'advisory'
            AND holder.granted
            AND NOT waiter.granted
        )
        """,
        [holder_pid, waiter_pid]
      )

    matched
  end

  defp assert_eventually(fun, attempts \\ 200)

  defp assert_eventually(_fun, 0),
    do: flunk("expected PostgreSQL lock state was not observed")

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp shutdown_task(task) do
    _ = Task.shutdown(task, :brutal_kill)
    :ok
  catch
    :exit, _reason -> :ok
  end
end
