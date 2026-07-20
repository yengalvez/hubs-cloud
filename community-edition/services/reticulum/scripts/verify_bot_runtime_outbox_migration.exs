{:ok, _apps} = Application.ensure_all_started(:ecto_sql)
{:ok, _apps} = Application.ensure_all_started(:postgrex)

defmodule Ret.BotRuntimeOutboxMigrationVerifier.Repo do
  use Ecto.Repo,
    otp_app: :ret,
    adapter: Ecto.Adapters.Postgres
end

migrations_path = Path.expand("../priv/repo/migrations", __DIR__)

for migration <- [
      "20260718120000_create_bot_config_approvals.exs",
      "20260718130000_create_bot_runner_leases.exs",
      "20260718140000_create_bot_runner_generations.exs",
      "20260719150000_create_bot_runtime_outbox.exs"
    ] do
  Code.require_file(Path.join(migrations_path, migration))
end

defmodule Ret.BotRuntimeOutboxMigrationVerifier do
  @moduledoc false

  alias Ecto.Adapters.SQL
  alias Ecto.Migrator
  alias Ret.BotRuntimeOutboxMigrationVerifier.Repo, as: VerificationRepo
  alias Ret.Repo.Migrations.CreateBotConfigApprovals
  alias Ret.Repo.Migrations.CreateBotRunnerGenerations
  alias Ret.Repo.Migrations.CreateBotRunnerLeases
  alias Ret.Repo.Migrations.CreateBotRuntimeOutbox

  @approval_version 20_260_718_120_000
  @lease_version 20_260_718_130_000
  @generation_version 20_260_718_140_000
  @outbox_version 20_260_719_150_000
  @max_javascript_integer 9_007_199_254_740_991
  @max_config_bytes 16_384

  @base_source [
    {@approval_version, CreateBotConfigApprovals},
    {@lease_version, CreateBotRunnerLeases},
    {@generation_version, CreateBotRunnerGenerations}
  ]
  @outbox_source [{@outbox_version, CreateBotRuntimeOutbox}]

  def run do
    database =
      "ret_aud078_#{System.system_time(:millisecond)}_#{System.unique_integer([:positive])}"

    config =
      Ret.Repo.config()
      |> Keyword.delete(:url)
      |> Keyword.put(:database, database)
      |> Keyword.put(:pool, DBConnection.ConnectionPool)
      |> Keyword.put(:pool_size, 4)
      |> Keyword.put(:log, false)
      |> Keyword.put(:migration_default_prefix, "ret0")

    assert!(
      Ecto.Adapters.Postgres.storage_up(config) == :ok,
      "could not create isolated database"
    )

    try do
      {:ok, repo_pid} = VerificationRepo.start_link(config)

      try do
        verify_migration!()
      after
        Supervisor.stop(repo_pid)
      end
    after
      assert!(
        Ecto.Adapters.Postgres.storage_down(config) == :ok,
        "could not remove isolated database"
      )
    end
  end

  defp verify_migration! do
    SQL.query!(VerificationRepo, "CREATE SCHEMA ret0", [])

    SQL.query!(
      VerificationRepo,
      "CREATE TABLE ret0.accounts (account_id bigserial PRIMARY KEY)",
      []
    )

    SQL.query!(
      VerificationRepo,
      "CREATE TYPE ret0.hub_entry_mode AS ENUM ('allow', 'deny', 'invite')",
      []
    )

    SQL.query!(
      VerificationRepo,
      """
      CREATE TABLE ret0.hubs (
        hub_id bigserial PRIMARY KEY,
        hub_sid varchar(128) NOT NULL UNIQUE,
        created_by_account_id bigint REFERENCES ret0.accounts(account_id) ON DELETE CASCADE,
        entry_mode ret0.hub_entry_mode NOT NULL DEFAULT 'allow',
        user_data jsonb NOT NULL DEFAULT '{}'::jsonb,
        updated_at timestamp(6) without time zone NOT NULL DEFAULT timezone('UTC', now())
      )
      """,
      []
    )

    quarantined_hub_id =
      insert_hub!("audit-quarantined", %{
        "bots" => %{"enabled" => true, "count" => 2, "future" => %{"typed" => [1, true]}},
        "theme" => "aurora"
      })

    assert!(
      run_base_up() == [@approval_version, @lease_version, @generation_version],
      "base migrations failed"
    )

    approved_bots = %{
      "enabled" => true,
      "count" => 1,
      "chat_enabled" => 1.0,
      "mobility" => "static",
      "future" => %{"value" => "preserve-exactly"}
    }

    approved_hub_id = insert_hub!("audit-approved", %{"bots" => approved_bots})
    insert_approved!(approved_hub_id, approved_bots)

    stale_approved_bots = %{
      "enabled" => true,
      "count" => 1,
      "mobility" => "static",
      "prompt" => "historical approval"
    }

    stale_current_bots = %{
      "enabled" => true,
      "count" => 2,
      "mobility" => "static",
      "prompt" => "current Hub configuration"
    }

    stale_candidate_bots = %{
      "enabled" => true,
      "count" => 3,
      "mobility" => "static",
      "prompt" => "preserve stale candidate literally"
    }

    stale_approved_hub_id =
      insert_hub!("audit-approved-stale", %{"bots" => stale_current_bots})

    insert_approved!(stale_approved_hub_id, stale_approved_bots, stale_candidate_bots)

    numeric_approved_bots = %{
      "enabled" => 1,
      "count" => 1,
      "mobility" => "static",
      "prompt" => "integer approval"
    }

    numeric_current_bots = %{
      "enabled" => 1.0,
      "count" => 1,
      "mobility" => "static",
      "prompt" => "integer approval"
    }

    numeric_candidate_bots = %{
      "enabled" => true,
      "count" => 2,
      "mobility" => "static",
      "prompt" => "preserve numeric-scale candidate"
    }

    numeric_stale_hub_id =
      insert_hub!("audit-approved-numeric-stale", %{"bots" => numeric_current_bots})

    insert_approved!(numeric_stale_hub_id, numeric_approved_bots, numeric_candidate_bots)

    runtime_oversize_bots = %{
      "enabled" => true,
      "count" => 1,
      "padding" => String.duplicate("x", 16_325)
    }

    runtime_oversize_hub_id =
      insert_hub!("audit-approved-runtime-oversize", %{"bots" => runtime_oversize_bots})

    insert_approved!(runtime_oversize_hub_id, runtime_oversize_bots)
    assert_runtime_projection_fixture_boundary!(runtime_oversize_bots)

    closed_approved_bots = %{
      "enabled" => true,
      "count" => 3,
      "mobility" => "static",
      "historical" => %{"approved" => true}
    }

    closed_candidate_bots = %{
      "enabled" => true,
      "count" => 4,
      "mobility" => "static",
      "historical" => %{"candidate" => "preserve-literally"}
    }

    closed_approved_hub_id =
      insert_hub!(
        "audit-approved-closed",
        %{"bots" => closed_approved_bots},
        nil,
        "deny"
      )

    insert_approved!(closed_approved_hub_id, closed_approved_bots, closed_candidate_bots)

    approved_updated_at = approval_updated_at(approved_hub_id)
    quarantined_updated_at = approval_updated_at(quarantined_hub_id)
    stale_approval_audit = approval_audit(stale_approved_hub_id)
    numeric_stale_audit = approval_audit(numeric_stale_hub_id)
    runtime_oversize_audit = approval_audit(runtime_oversize_hub_id)
    closed_approval_audit = approval_audit(closed_approved_hub_id)
    incumbent_epoch = insert_active_lease!(quarantined_hub_id)

    assert!(run_outbox(:up) == [@outbox_version], "runtime outbox migration did not run")

    assert_schema!()
    assert_approved_backfill!(approved_hub_id, approved_bots, approved_updated_at)

    stop_epoch =
      assert_quarantined_backfill!(
        quarantined_hub_id,
        quarantined_updated_at,
        incumbent_epoch
      )

    stale_migration_evidence =
      assert_stale_approved_backfill!(
        stale_approved_hub_id,
        stale_candidate_bots,
        stale_approved_bots,
        stale_current_bots,
        stale_approval_audit,
        stop_epoch
      )

    numeric_migration_evidence =
      assert_stale_approved_backfill!(
        numeric_stale_hub_id,
        numeric_candidate_bots,
        numeric_approved_bots,
        numeric_current_bots,
        numeric_stale_audit,
        stale_migration_evidence.stop_epoch
      )

    runtime_oversize_migration_evidence =
      assert_runtime_projection_oversize_backfill!(
        runtime_oversize_hub_id,
        runtime_oversize_bots,
        runtime_oversize_audit,
        numeric_migration_evidence.stop_epoch
      )

    closed_migration_evidence =
      assert_closed_approved_backfill!(
        closed_approved_hub_id,
        closed_candidate_bots,
        closed_approved_bots,
        closed_approval_audit,
        runtime_oversize_migration_evidence.stop_epoch
      )

    assert_hub_delete_lifecycle!()
    assert_constraints!(approved_hub_id)

    assert_down_guards_and_safe_rollback!(
      approved_hub_id,
      closed_approved_hub_id,
      closed_candidate_bots,
      closed_migration_evidence,
      stale_approved_hub_id,
      stale_candidate_bots,
      stale_migration_evidence,
      runtime_oversize_hub_id,
      runtime_oversize_bots,
      runtime_oversize_migration_evidence
    )

    assert_corrupt_approval_fails_closed!()

    IO.puts("bot runtime outbox migration verification passed")
  end

  defp insert_hub!(hub_sid, user_data, account_id \\ nil, entry_mode \\ "allow") do
    %{rows: [[hub_id]]} =
      SQL.query!(
        VerificationRepo,
        """
        INSERT INTO ret0.hubs (hub_sid, user_data, created_by_account_id, entry_mode)
        VALUES ($1, $2::jsonb, $3, $4::ret0.hub_entry_mode)
        RETURNING hub_id
        """,
        [hub_sid, user_data, account_id, entry_mode]
      )

    hub_id
  end

  defp insert_account! do
    %{rows: [[account_id]]} =
      SQL.query!(
        VerificationRepo,
        "INSERT INTO ret0.accounts DEFAULT VALUES RETURNING account_id",
        []
      )

    account_id
  end

  defp insert_approved!(hub_id, approved_bots, candidate_bots \\ nil) do
    candidate_bots = candidate_bots || approved_bots

    SQL.query!(
      VerificationRepo,
      """
      INSERT INTO ret0.bot_config_approvals (
        hub_id,
        state,
        candidate_bots,
        approved_bots,
        approved_by_account_id,
        approved_at,
        inserted_at,
        updated_at
      )
      VALUES (
        $1,
        'approved',
        $2::jsonb,
        $3::jsonb,
        7,
        timezone('UTC', clock_timestamp()) - interval '2 hours',
        timezone('UTC', clock_timestamp()) - interval '2 hours',
        timezone('UTC', clock_timestamp()) - interval '2 hours'
      )
      """,
      [hub_id, candidate_bots, approved_bots]
    )
  end

  defp assert_runtime_projection_fixture_boundary!(bots) do
    %{rows: [[raw_bytes, runtime_bytes]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT
          octet_length($1::jsonb::text),
          octet_length(
            ($1::jsonb || jsonb_build_object('chat_enabled', false))::text
          )
        """,
        [bots]
      )

    assert!(
      raw_bytes <= @max_config_bytes,
      "runtime oversize fixture no longer fits the historical raw config bound"
    )

    assert!(
      runtime_bytes > @max_config_bytes,
      "runtime oversize fixture no longer crosses the normalized payload bound"
    )
  end

  defp insert_active_lease!(hub_id) do
    %{rows: [[epoch]]} =
      SQL.query!(
        VerificationRepo,
        """
        INSERT INTO ret0.bot_runner_leases (
          hub_id,
          lease_id,
          holder_instance_id,
          session_id,
          authority_epoch,
          expires_at,
          inserted_at,
          updated_at
        )
        VALUES (
          $1,
          '11111111-1111-4111-8111-111111111111',
          '22222222-2222-4222-8222-222222222222',
          '33333333-3333-4333-8333-333333333333',
          nextval('ret0.bot_runner_authority_epoch_seq'),
          timezone('UTC', clock_timestamp()) + interval '1 hour',
          timezone('UTC', clock_timestamp()),
          timezone('UTC', clock_timestamp())
        )
        RETURNING authority_epoch
        """,
        [hub_id]
      )

    epoch
  end

  defp assert_schema! do
    assert!(table_exists?(), "runtime outbox table is missing")
    assert!(runtime_revision_column?(), "approval runtime_revision column is missing")
    assert!(runtime_chat_enabled_column?(), "durable runtime chat decision column is missing")
    assert!(hub_delete_trigger_exists?(), "Hub delete lifecycle trigger is missing")
    assert!(hub_delete_function_exists?(), "Hub delete lifecycle function is missing")

    assert!(
      outbox_trigger_exists?("bot_runtime_outbox_immutable_event"),
      "immutability trigger is missing"
    )

    assert!(
      outbox_trigger_exists?("bot_runtime_outbox_pending_delete"),
      "pending delete trigger is missing"
    )

    assert!(json_validation_functions_exist?(), "recursive JSON validation functions are missing")

    for index <- [
          "hubs_hub_id_hub_sid_idx",
          "bot_runtime_outbox_operation_id_idx",
          "bot_runtime_outbox_hub_revision_idx",
          "bot_runtime_outbox_pending_room_order_idx",
          "bot_runtime_outbox_due_idx",
          "bot_runtime_outbox_claim_expiry_idx"
        ] do
      assert!(index_exists?(index), "missing runtime outbox index #{index}")
    end

    %{rows: [[delete_action]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT constraint_record.confdeltype::text
        FROM pg_constraint AS constraint_record
        WHERE constraint_record.conname = 'bot_runtime_outbox_hub_identity_fkey'
        """,
        []
      )

    assert!(delete_action == "a", "outbox composite Hub identity foreign key is not NO ACTION")
  end

  defp assert_hub_delete_lifecycle! do
    no_history_hub_id = insert_hub!("delete-no-history", %{})
    delete_hub!(no_history_hub_id)
    refute!(hub_exists?(no_history_hub_id), "Hub without runtime history was not deleted")

    delivered_config_hub_id = insert_hub!("delete-after-config", %{})
    insert_runtime_config!(delivered_config_hub_id, "delete-after-config", 1, true)

    assert_query_refused!(
      "DELETE FROM ret0.hubs WHERE hub_id = $1",
      [delivered_config_hub_id],
      "bot_runtime_outbox_hub_identity_fkey"
    )

    assert!(
      hub_exists?(delivered_config_hub_id),
      "latest delivered config did not keep the Hub fail-closed"
    )

    assert!(
      outbox_count(delivered_config_hub_id) == 1,
      "latest delivered config was deleted before a terminal stop"
    )

    pending_stop_account_id = insert_account!()

    pending_stop_hub_id =
      insert_hub!("delete-pending-stop", %{}, pending_stop_account_id)

    insert_runtime_config!(pending_stop_hub_id, "delete-pending-stop", 1, true)
    insert_runtime_stop!(pending_stop_hub_id, "delete-pending-stop", 2, 101, false)
    insert_quarantined_approval!(pending_stop_hub_id, 2)
    insert_tombstone_lease!(pending_stop_hub_id, 101)

    assert_query_refused!(
      "DELETE FROM ret0.accounts WHERE account_id = $1",
      [pending_stop_account_id],
      "bot_runtime_outbox_hub_identity_fkey"
    )

    assert!(account_exists?(pending_stop_account_id), "pending stop partially deleted account")
    assert!(hub_exists?(pending_stop_hub_id), "pending stop did not block Hub deletion")

    assert!(
      outbox_count(pending_stop_hub_id) == 2,
      "Hub deletion removed pending or earlier delivered runtime events"
    )

    delivered_stop_account_id = insert_account!()

    delivered_stop_hub_id =
      insert_hub!("delete-after-stop", %{}, delivered_stop_account_id)

    insert_runtime_config!(delivered_stop_hub_id, "delete-after-stop", 1, true)
    insert_runtime_stop!(delivered_stop_hub_id, "delete-after-stop", 2, 201, true)
    insert_quarantined_approval!(delivered_stop_hub_id, 2)
    insert_tombstone_lease!(delivered_stop_hub_id, 201)
    delete_account!(delivered_stop_account_id)

    refute!(account_exists?(delivered_stop_account_id), "terminal stop retained owning account")
    refute!(hub_exists?(delivered_stop_hub_id), "terminal stop did not permit Hub deletion")

    assert!(
      outbox_count(delivered_stop_hub_id) == 0,
      "terminal delivered runtime history was not cleaned before Hub deletion"
    )

    mismatched_stop_hub_id = insert_hub!("delete-mismatched-stop", %{})
    insert_runtime_config!(mismatched_stop_hub_id, "delete-mismatched-stop", 1, true)
    insert_runtime_stop!(mismatched_stop_hub_id, "delete-mismatched-stop", 2, 301, true)
    insert_quarantined_approval!(mismatched_stop_hub_id, 3)
    insert_tombstone_lease!(mismatched_stop_hub_id, 302)

    assert_query_refused!(
      "DELETE FROM ret0.hubs WHERE hub_id = $1",
      [mismatched_stop_hub_id],
      "bot_runtime_outbox_hub_identity_fkey"
    )

    assert!(
      outbox_count(mismatched_stop_hub_id) == 2,
      "mismatched approval revision or fencing epoch did not block cleanup"
    )

    active_lease_hub_id = insert_hub!("delete-active-lease", %{})
    insert_runtime_config!(active_lease_hub_id, "delete-active-lease", 1, true)
    insert_runtime_stop!(active_lease_hub_id, "delete-active-lease", 2, 401, true)
    insert_quarantined_approval!(active_lease_hub_id, 2)
    insert_runtime_active_lease!(active_lease_hub_id, 401)

    assert_query_refused!(
      "DELETE FROM ret0.hubs WHERE hub_id = $1",
      [active_lease_hub_id],
      "bot_runtime_outbox_hub_identity_fkey"
    )

    assert!(
      outbox_count(active_lease_hub_id) == 2,
      "active runner lease did not block delivered history cleanup"
    )
  end

  defp assert_approved_backfill!(hub_id, approved_bots, original_updated_at) do
    %{rows: [[revision, current_updated_at]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT runtime_revision, updated_at
        FROM ret0.bot_config_approvals
        WHERE hub_id = $1
        """,
        [hub_id]
      )

    assert!(revision == 1, "approved row did not receive runtime revision 1")
    assert!(current_updated_at == original_updated_at, "approval audit timestamp was rewritten")

    %{
      rows: [
        [operation_id, event_kind, bots, runtime_chat_enabled, revoke_epoch, next_attempt_at]
      ]
    } =
      SQL.query!(
        VerificationRepo,
        """
        SELECT operation_id::text, event_kind, bots, runtime_chat_enabled,
               revoke_epoch, next_attempt_at
        FROM ret0.bot_runtime_outbox
        WHERE hub_id = $1 AND runtime_revision = 1
        """,
        [hub_id]
      )

    assert_uuid_v4!(operation_id)
    assert!(event_kind == "config", "approved row did not enqueue config")
    assert!(bots == approved_bots, "approved config payload changed during backfill")
    assert!(bots["chat_enabled"] === 1.0, "approved raw chat JSON type was not preserved")
    refute!(runtime_chat_enabled, "float chat flag became enabled during backfill")
    assert!(revoke_epoch == nil, "config backfill unexpectedly contains a revoke epoch")
    assert!(match?(%NaiveDateTime{}, next_attempt_at), "config backfill is not immediately due")
  end

  defp assert_quarantined_backfill!(hub_id, original_updated_at, incumbent_epoch) do
    %{rows: [[revision, current_updated_at]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT runtime_revision, updated_at
        FROM ret0.bot_config_approvals
        WHERE hub_id = $1
        """,
        [hub_id]
      )

    assert!(revision == 1, "quarantined row did not receive runtime revision 1")
    assert!(current_updated_at == original_updated_at, "quarantine audit timestamp was rewritten")

    %{rows: [[operation_id, event_kind, bots, runtime_chat_enabled, stop_epoch]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT operation_id::text, event_kind, bots, runtime_chat_enabled, revoke_epoch
        FROM ret0.bot_runtime_outbox
        WHERE hub_id = $1 AND runtime_revision = 1
        """,
        [hub_id]
      )

    assert_uuid_v4!(operation_id)
    assert!(event_kind == "stop", "quarantined row did not enqueue stop")
    assert!(bots == nil, "stop backfill unexpectedly contains config")
    assert!(runtime_chat_enabled == nil, "stop backfill retained a chat decision")
    assert!(stop_epoch > incumbent_epoch, "quarantine did not advance the authority epoch")

    %{rows: [[lease_id, holder_id, session_id, lease_epoch, expires_at]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT lease_id, holder_instance_id, session_id, authority_epoch, expires_at
        FROM ret0.bot_runner_leases
        WHERE hub_id = $1
        """,
        [hub_id]
      )

    assert!(
      Enum.all?([lease_id, holder_id, session_id, expires_at], &is_nil/1),
      "lease was not fenced"
    )

    assert!(lease_epoch == stop_epoch, "stop event and lease tombstone epochs differ")

    stop_epoch
  end

  defp assert_stale_approved_backfill!(
         hub_id,
         original_candidate_bots,
         approved_bots,
         current_bots,
         original_audit,
         previous_stop_epoch
       ) do
    %{
      inserted_at: original_inserted_at,
      updated_at: original_updated_at,
      approved_at: original_approved_at
    } = original_audit

    %{
      state: "quarantined",
      candidate_bots: ^original_candidate_bots,
      approved_bots: nil,
      approved_by_account_id: nil,
      approved_at: nil,
      last_quarantined_by_account_id: nil,
      last_quarantined_at: quarantined_at,
      last_quarantine_reason: "stale_approval_runtime_migration",
      runtime_revision: 1,
      inserted_at: ^original_inserted_at,
      updated_at: updated_at
    } = approval_evidence(hub_id)

    assert!(
      NaiveDateTime.compare(quarantined_at, original_approved_at) == :gt,
      "stale approval migration did not record a new quarantine timestamp"
    )

    assert!(
      NaiveDateTime.compare(updated_at, original_updated_at) == :gt and
        updated_at == quarantined_at,
      "stale approval migration did not preserve one atomic audit timestamp"
    )

    %{rows: [[persisted_bots, expected_current_bots, historical_approved_bots]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT
          (user_data->'bots')::text,
          ($2::jsonb)::text,
          ($3::jsonb)::text
        FROM ret0.hubs
        WHERE hub_id = $1
        """,
        [hub_id, current_bots, approved_bots]
      )

    assert!(
      persisted_bots == expected_current_bots,
      "stale approval migration rewrote current Hub bots"
    )

    refute!(
      persisted_bots == historical_approved_bots,
      "stale approval fixture no longer proves a type-sensitive approval mismatch"
    )

    %{rows: [[operation_id, event_kind, bots, stop_epoch]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT operation_id::text, event_kind, bots, revoke_epoch
        FROM ret0.bot_runtime_outbox
        WHERE hub_id = $1 AND runtime_revision = 1
        """,
        [hub_id]
      )

    assert_uuid_v4!(operation_id)
    assert!(event_kind == "stop", "stale approval was reactivated as a config event")
    assert!(bots == nil, "stale approval leaked obsolete config into its stop event")
    assert!(stop_epoch > previous_stop_epoch, "stale approval did not advance authority epoch")

    %{rows: [[lease_id, holder_id, session_id, lease_epoch, expires_at]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT lease_id, holder_instance_id, session_id, authority_epoch, expires_at
        FROM ret0.bot_runner_leases
        WHERE hub_id = $1
        """,
        [hub_id]
      )

    assert!(
      Enum.all?([lease_id, holder_id, session_id, expires_at], &is_nil/1),
      "stale approval did not receive a lease tombstone"
    )

    assert!(lease_epoch == stop_epoch, "stale approval stop and lease epochs differ")

    %{
      candidate_bots: original_candidate_bots,
      inserted_at: original_inserted_at,
      quarantined_at: quarantined_at,
      updated_at: updated_at,
      stop_epoch: stop_epoch
    }
  end

  defp assert_runtime_projection_oversize_backfill!(
         hub_id,
         original_bots,
         original_audit,
         previous_stop_epoch
       ) do
    %{
      inserted_at: original_inserted_at,
      updated_at: original_updated_at,
      approved_at: original_approved_at
    } = original_audit

    %{
      state: "quarantined",
      candidate_bots: ^original_bots,
      approved_bots: nil,
      approved_by_account_id: nil,
      approved_at: nil,
      last_quarantined_by_account_id: nil,
      last_quarantined_at: quarantined_at,
      last_quarantine_reason: "runtime_payload_too_large_migration",
      runtime_revision: 1,
      inserted_at: ^original_inserted_at,
      updated_at: updated_at
    } = approval_evidence(hub_id)

    assert!(
      NaiveDateTime.compare(quarantined_at, original_approved_at) == :gt and
        NaiveDateTime.compare(updated_at, original_updated_at) == :gt and
        updated_at == quarantined_at,
      "runtime-oversize migration did not preserve one atomic quarantine timestamp"
    )

    %{rows: [[persisted_bots]]} =
      SQL.query!(
        VerificationRepo,
        "SELECT user_data->'bots' FROM ret0.hubs WHERE hub_id = $1",
        [hub_id]
      )

    assert!(
      persisted_bots == original_bots,
      "runtime-oversize migration rewrote the original Hub bot payload"
    )

    %{rows: [[operation_id, event_kind, event_bots, stop_epoch]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT operation_id::text, event_kind, bots, revoke_epoch
        FROM ret0.bot_runtime_outbox
        WHERE hub_id = $1 AND runtime_revision = 1
        """,
        [hub_id]
      )

    assert_uuid_v4!(operation_id)
    assert!(event_kind == "stop", "runtime-oversize approval was emitted as CONFIG")
    assert!(event_bots == nil, "runtime-oversize STOP leaked the rejected config")

    assert!(
      stop_epoch > previous_stop_epoch,
      "runtime-oversize quarantine did not advance the authority epoch"
    )

    %{rows: [[lease_id, holder_id, session_id, lease_epoch, expires_at]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT lease_id, holder_instance_id, session_id, authority_epoch, expires_at
        FROM ret0.bot_runner_leases
        WHERE hub_id = $1
        """,
        [hub_id]
      )

    assert!(
      Enum.all?([lease_id, holder_id, session_id, expires_at], &is_nil/1) and
        lease_epoch == stop_epoch,
      "runtime-oversize quarantine did not persist the exact lease tombstone"
    )

    %{
      candidate_bots: original_bots,
      inserted_at: original_inserted_at,
      quarantined_at: quarantined_at,
      updated_at: updated_at,
      stop_epoch: stop_epoch
    }
  end

  defp assert_closed_approved_backfill!(
         hub_id,
         original_candidate_bots,
         approved_bots,
         original_audit,
         previous_stop_epoch
       ) do
    %{
      inserted_at: original_inserted_at,
      updated_at: original_updated_at,
      approved_at: original_approved_at
    } = original_audit

    %{
      state: "quarantined",
      candidate_bots: ^original_candidate_bots,
      approved_bots: nil,
      approved_by_account_id: nil,
      approved_at: nil,
      last_quarantined_by_account_id: nil,
      last_quarantined_at: quarantined_at,
      last_quarantine_reason: "room_closed_runtime_migration",
      runtime_revision: 1,
      inserted_at: ^original_inserted_at,
      updated_at: updated_at
    } = approval_evidence(hub_id)

    assert!(
      NaiveDateTime.compare(quarantined_at, original_approved_at) == :gt,
      "closed approved migration did not record a new quarantine timestamp"
    )

    assert!(
      NaiveDateTime.compare(updated_at, original_updated_at) == :gt,
      "closed approved migration did not advance the approval audit timestamp"
    )

    assert!(
      updated_at == quarantined_at,
      "closed approved migration timestamps do not identify the same atomic change"
    )

    %{rows: [[entry_mode, persisted_bots]]} =
      SQL.query!(
        VerificationRepo,
        "SELECT entry_mode, user_data->'bots' FROM ret0.hubs WHERE hub_id = $1",
        [hub_id]
      )

    assert!(entry_mode == "deny", "closed approved fixture no longer has deny entry mode")

    assert!(
      persisted_bots == approved_bots,
      "closed approved migration rewrote the Hub bot payload"
    )

    %{rows: [[operation_id, event_kind, bots, stop_epoch]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT operation_id::text, event_kind, bots, revoke_epoch
        FROM ret0.bot_runtime_outbox
        WHERE hub_id = $1 AND runtime_revision = 1
        """,
        [hub_id]
      )

    assert_uuid_v4!(operation_id)
    assert!(event_kind == "stop", "closed approved room did not enqueue a stop")
    assert!(bots == nil, "closed approved room leaked a config payload into its stop")

    assert!(
      stop_epoch > previous_stop_epoch,
      "closed approved room did not advance the authority epoch"
    )

    %{rows: [[lease_id, holder_id, session_id, lease_epoch, expires_at]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT lease_id, holder_instance_id, session_id, authority_epoch, expires_at
        FROM ret0.bot_runner_leases
        WHERE hub_id = $1
        """,
        [hub_id]
      )

    assert!(
      Enum.all?([lease_id, holder_id, session_id, expires_at], &is_nil/1),
      "closed approved room did not receive a lease tombstone"
    )

    assert!(
      lease_epoch == stop_epoch,
      "closed approved stop and lease tombstone epochs differ"
    )

    assert!(
      sequence_last_value() == stop_epoch,
      "closed approved backfill consumed an unexpected authority epoch"
    )

    %{
      candidate_bots: original_candidate_bots,
      inserted_at: original_inserted_at,
      quarantined_at: quarantined_at,
      updated_at: updated_at,
      stop_epoch: stop_epoch
    }
  end

  defp assert_constraints!(hub_id) do
    hub_sid = hub_sid(hub_id)
    invalid_sid_hub_id = insert_hub!("invalid.sid", %{})
    max_event_id = insert_outbox!(hub_id, hub_sid, @max_javascript_integer)

    assert_query_refused!(
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, bots,
        runtime_chat_enabled, inserted_at, updated_at
      ) VALUES (
        '44444444-4444-4444-8444-444444444444', $1, $2, $3,
        'config', '{}'::jsonb, false, timezone('UTC', now()), timezone('UTC', now())
      )
      """,
      [hub_id, hub_sid, @max_javascript_integer + 1],
      "bot_runtime_outbox_runtime_revision_safe"
    )

    assert_query_refused!(
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind,
        runtime_chat_enabled, inserted_at, updated_at
      ) VALUES (
        '55555555-5555-4555-8555-555555555555', $1, $2, 2,
        'config', false, timezone('UTC', now()), timezone('UTC', now())
      )
      """,
      [hub_id, hub_sid],
      "bot_runtime_outbox_event_complete"
    )

    assert_query_refused!(
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, bots,
        inserted_at, updated_at
      ) VALUES (
        '56565656-5656-4656-8656-565656565656', $1, $2, 13,
        'config', '{}'::jsonb, timezone('UTC', now()), timezone('UTC', now())
      )
      """,
      [hub_id, hub_sid],
      "bot_runtime_outbox_event_complete"
    )

    assert_query_refused!(
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, runtime_chat_enabled,
        revoke_epoch, inserted_at, updated_at
      ) VALUES (
        '57575757-5757-4757-8757-575757575757', $1, $2, 14,
        'stop', false, 6, timezone('UTC', now()), timezone('UTC', now())
      )
      """,
      [hub_id, hub_sid],
      "bot_runtime_outbox_event_complete"
    )

    assert_query_refused!(
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, revoke_epoch,
        claim_owner, inserted_at, updated_at
      ) VALUES (
        '66666666-6666-4666-8666-666666666666', $1, $2, 3,
        'stop', 5, 'orphan-owner', timezone('UTC', now()), timezone('UTC', now())
      )
      """,
      [hub_id, hub_sid],
      "bot_runtime_outbox_claim_complete"
    )

    assert_query_refused!(
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, bots,
        runtime_chat_enabled, inserted_at, updated_at
      ) VALUES (
        '77777777-7777-4777-8777-777777777777', 999999999, 'alien-hub', 1,
        'config', '{}'::jsonb, false, timezone('UTC', now()), timezone('UTC', now())
      )
      """,
      [],
      "bot_runtime_outbox_hub_identity_fkey"
    )

    assert_query_refused!(
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, bots,
        runtime_chat_enabled, inserted_at, updated_at
      ) VALUES (
        '12121212-1212-4212-8212-121212121212', $1, 'valid_but_wrong_sid', 4,
        'config', '{}'::jsonb, false, timezone('UTC', now()), timezone('UTC', now())
      )
      """,
      [hub_id],
      "bot_runtime_outbox_hub_identity_fkey"
    )

    assert_query_refused!(
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, bots,
        runtime_chat_enabled, inserted_at, updated_at
      ) VALUES (
        '13131313-1313-1313-8131-131313131313', $1, $2, 5,
        'config', '{}'::jsonb, false, timezone('UTC', now()), timezone('UTC', now())
      )
      """,
      [hub_id, hub_sid],
      "bot_runtime_outbox_operation_id_v4"
    )

    assert_query_refused!(
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, bots,
        runtime_chat_enabled, inserted_at, updated_at
      ) VALUES (
        '14141414-1414-4414-8414-141414141414', $1, 'invalid.sid', 6,
        'config', '{}'::jsonb, false, timezone('UTC', now()), timezone('UTC', now())
      )
      """,
      [invalid_sid_hub_id],
      "bot_runtime_outbox_hub_sid_valid"
    )

    assert_query_refused!(
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, bots,
        runtime_chat_enabled, inserted_at, updated_at
      ) VALUES (
        '15151515-1515-4515-8515-151515151515', $1, $2, 7,
        'config', jsonb_build_object('prompt', repeat('x', 16384)), false,
        timezone('UTC', now()), timezone('UTC', now())
      )
      """,
      [hub_id, hub_sid],
      "bot_runtime_outbox_bots_size"
    )

    assert_query_refused!(
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, bots,
        runtime_chat_enabled, inserted_at, updated_at
      ) VALUES (
        '21212121-2121-4121-8121-212121212121', $1, $2, 15,
        'config',
        jsonb_build_object('enabled', true, 'count', 1, 'padding', repeat('x', 16325)),
        false, timezone('UTC', now()), timezone('UTC', now())
      )
      """,
      [hub_id, hub_sid],
      "bot_runtime_outbox_runtime_bots_size"
    )

    assert_query_refused!(
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, bots,
        runtime_chat_enabled, inserted_at, updated_at
      ) VALUES (
        '16161616-1616-4616-8616-161616161616', $1, $2, 8,
        'config', '{"number":9007199254740992}'::jsonb, false,
        timezone('UTC', now()), timezone('UTC', now())
      )
      """,
      [hub_id, hub_sid],
      "bot_runtime_outbox_bots_js_interoperable"
    )

    assert_query_refused!(
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, bots,
        runtime_chat_enabled, inserted_at, updated_at
      ) VALUES (
        '20202020-2020-4020-8020-202020202020', $1, $2, 12,
        'config', '{"number":9007199254740992.0}'::jsonb, false,
        timezone('UTC', now()), timezone('UTC', now())
      )
      """,
      [hub_id, hub_sid],
      "bot_runtime_outbox_bots_js_interoperable"
    )

    assert_query_refused!(
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, bots,
        runtime_chat_enabled, inserted_at, updated_at
      ) VALUES (
        '17171717-1717-4717-8717-171717171717', $1, $2, 9,
        'config', '{"number":0.10000000000000001}'::jsonb, false,
        timezone('UTC', now()), timezone('UTC', now())
      )
      """,
      [hub_id, hub_sid],
      "bot_runtime_outbox_bots_js_interoperable"
    )

    assert_query_refused!(
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, bots,
        runtime_chat_enabled, inserted_at, updated_at
      ) VALUES (
        '18181818-1818-4818-8818-181818181818', $1, $2, 10,
        'config', $3::jsonb, false, timezone('UTC', now()), timezone('UTC', now())
      )
      """,
      [hub_id, hub_sid, %{"depth" => nested_lists(64)}],
      "bot_runtime_outbox_bots_js_interoperable"
    )

    SQL.query!(
      VerificationRepo,
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, bots,
        runtime_chat_enabled, inserted_at, updated_at
      ) VALUES (
        '19191919-1919-4919-8919-191919191919', $1, $2, 11,
        'config', $3::jsonb, false, timezone('UTC', now()), timezone('UTC', now())
      )
      """,
      [
        hub_id,
        hub_sid,
        %{
          "depth" => nested_lists(63),
          "float" => 1.2345678901234567,
          "subnormal" => 5.0e-324,
          "safe_integer" => @max_javascript_integer
        }
      ]
    )

    assert_query_refused!(
      "UPDATE ret0.bot_runtime_outbox SET bots = $2::jsonb WHERE id = $1",
      [max_event_id, %{"changed" => true}],
      "bot_runtime_outbox_event_immutable"
    )

    assert_query_refused!(
      "UPDATE ret0.bot_runtime_outbox SET runtime_chat_enabled = true WHERE id = $1",
      [max_event_id],
      "bot_runtime_outbox_event_immutable"
    )

    %{rows: [[integer_payload]]} =
      SQL.query!(
        VerificationRepo,
        "SELECT bots::text FROM ret0.bot_runtime_outbox WHERE id = $1",
        [max_event_id]
      )

    assert!(
      integer_payload == ~s({"typed": 1}),
      "numeric immutability fixture is not integer JSON"
    )

    assert_query_refused!(
      "UPDATE ret0.bot_runtime_outbox SET bots = '{\"typed\":1.0}'::jsonb WHERE id = $1",
      [max_event_id],
      "bot_runtime_outbox_event_immutable"
    )

    %{rows: [[persisted_payload]]} =
      SQL.query!(
        VerificationRepo,
        "SELECT bots::text FROM ret0.bot_runtime_outbox WHERE id = $1",
        [max_event_id]
      )

    assert!(
      persisted_payload == integer_payload,
      "rejected numeric-scale update changed the original outbox payload"
    )

    assert_query_refused!(
      "UPDATE ret0.bot_runtime_outbox SET inserted_at = inserted_at + interval '1 second' WHERE id = $1",
      [max_event_id],
      "bot_runtime_outbox_event_immutable"
    )

    SQL.query!(
      VerificationRepo,
      """
      UPDATE ret0.bot_runtime_outbox
      SET attempt_count = attempt_count + 1,
          last_failure_code = 'http_timeout',
          updated_at = timezone('UTC', clock_timestamp())
      WHERE id = $1
      """,
      [max_event_id]
    )

    assert_query_refused!(
      "DELETE FROM ret0.bot_runtime_outbox WHERE id = $1",
      [max_event_id],
      "bot_runtime_outbox_pending_delete"
    )

    SQL.query!(
      VerificationRepo,
      """
      UPDATE ret0.bot_runtime_outbox
      SET delivered_at = timezone('UTC', clock_timestamp())
      WHERE id = $1
      """,
      [max_event_id]
    )

    %{num_rows: 1} =
      SQL.query!(
        VerificationRepo,
        "DELETE FROM ret0.bot_runtime_outbox WHERE id = $1",
        [max_event_id]
      )

    assert_query_refused!(
      "DELETE FROM ret0.hubs WHERE hub_id = $1",
      [hub_id],
      "bot_runtime_outbox_hub_identity_fkey"
    )
  end

  defp assert_down_guards_and_safe_rollback!(
         approved_hub_id,
         closed_approved_hub_id,
         closed_candidate_bots,
         closed_migration_evidence,
         stale_approved_hub_id,
         stale_candidate_bots,
         stale_migration_evidence,
         runtime_oversize_hub_id,
         runtime_oversize_bots,
         runtime_oversize_migration_evidence
       ) do
    %{stop_epoch: sequence_value} = closed_migration_evidence

    SQL.query!(
      VerificationRepo,
      """
      UPDATE ret0.hubs
      SET user_data = jsonb_set(user_data, '{bots,enabled}', '1'::jsonb, true)
      WHERE hub_id = $1
      """,
      [approved_hub_id]
    )

    assert_down_refused!("stored bot configuration is enabled")
    assert!(table_exists?(), "refused down removed outbox table")
    assert!(runtime_revision_column?(), "refused down removed runtime revision")
    assert!(migration_version_present?(), "refused down removed migration version")

    SQL.query!(
      VerificationRepo,
      """
      UPDATE ret0.hubs
      SET user_data = jsonb_set(user_data, '{bots,enabled}', 'false'::jsonb, true)
      WHERE jsonb_typeof(user_data->'bots') = 'object'
      """,
      []
    )

    SQL.query!(
      VerificationRepo,
      """
      UPDATE ret0.bot_config_approvals
      SET state = 'quarantined',
          approved_bots = NULL,
          approved_by_account_id = NULL,
          approved_at = NULL,
          last_quarantined_at = timezone('UTC', clock_timestamp()),
          last_quarantine_reason = 'rollback_verification'
      WHERE hub_id <> $1
        AND state <> 'quarantined'
      """,
      [closed_approved_hub_id]
    )

    assert_down_refused!("delivery is pending")

    SQL.query!(
      VerificationRepo,
      """
      UPDATE ret0.bot_runtime_outbox
      SET delivered_at = timezone('UTC', clock_timestamp()),
          claim_owner = NULL,
          claim_token = NULL,
          claim_expires_at = NULL
      """,
      []
    )

    SQL.query!(
      VerificationRepo,
      """
      INSERT INTO ret0.bot_runner_leases (
        hub_id, lease_id, holder_instance_id, session_id, authority_epoch,
        expires_at, inserted_at, updated_at
      ) VALUES (
        $1,
        '88888888-8888-4888-8888-888888888888',
        '99999999-9999-4999-8999-999999999999',
        'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
        $2,
        timezone('UTC', clock_timestamp()) + interval '1 hour',
        timezone('UTC', clock_timestamp()),
        timezone('UTC', clock_timestamp())
      )
      ON CONFLICT (hub_id) DO UPDATE
      SET lease_id = EXCLUDED.lease_id,
          holder_instance_id = EXCLUDED.holder_instance_id,
          session_id = EXCLUDED.session_id,
          authority_epoch = EXCLUDED.authority_epoch,
          expires_at = EXCLUDED.expires_at,
          updated_at = EXCLUDED.updated_at
      """,
      [approved_hub_id, sequence_value]
    )

    assert_down_refused!("runner lease remains")

    SQL.query!(
      VerificationRepo,
      """
      UPDATE ret0.bot_runner_leases
      SET lease_id = NULL,
          holder_instance_id = NULL,
          session_id = NULL,
          expires_at = NULL
      """,
      []
    )

    assert!(run_outbox(:down) == [@outbox_version], "safe runtime outbox down failed")
    refute!(table_exists?(), "safe down retained runtime outbox table")
    refute!(runtime_revision_column?(), "safe down retained runtime revision")
    refute!(migration_version_present?(), "safe down retained migration version")
    refute!(hub_delete_trigger_exists?(), "safe down retained Hub delete trigger")
    refute!(hub_delete_function_exists?(), "safe down retained Hub delete function")
    refute!(index_exists?("hubs_hub_id_hub_sid_idx"), "safe down retained Hub pair index")
    refute!(json_validation_functions_exist?(), "safe down retained JSON validation functions")

    assert!(
      sequence_last_value() == sequence_value,
      "runtime outbox down rewound the authority epoch"
    )

    assert_migration_quarantine_survives_down!(
      closed_approved_hub_id,
      closed_candidate_bots,
      closed_migration_evidence,
      "room_closed_runtime_migration",
      "closed room"
    )

    assert_migration_quarantine_survives_down!(
      stale_approved_hub_id,
      stale_candidate_bots,
      stale_migration_evidence,
      "stale_approval_runtime_migration",
      "stale approval"
    )

    assert_migration_quarantine_survives_down!(
      runtime_oversize_hub_id,
      runtime_oversize_bots,
      runtime_oversize_migration_evidence,
      "runtime_payload_too_large_migration",
      "runtime-oversize approval"
    )
  end

  defp assert_migration_quarantine_survives_down!(
         hub_id,
         candidate_bots,
         evidence,
         expected_reason,
         label
       ) do
    %{
      inserted_at: inserted_at,
      quarantined_at: quarantined_at,
      updated_at: updated_at,
      stop_epoch: stop_epoch
    } = evidence

    %{
      rows: [
        [
          state,
          persisted_candidate,
          approved_bots,
          approved_by,
          approved_at,
          reason,
          persisted_quarantined_at,
          persisted_inserted_at,
          persisted_updated_at
        ]
      ]
    } =
      SQL.query!(
        VerificationRepo,
        """
        SELECT state, candidate_bots, approved_bots, approved_by_account_id, approved_at,
               last_quarantine_reason, last_quarantined_at, inserted_at, updated_at
        FROM ret0.bot_config_approvals
        WHERE hub_id = $1
        """,
        [hub_id]
      )

    assert!(state == "quarantined", "safe down reversed the #{label} quarantine")

    assert!(
      persisted_candidate == candidate_bots,
      "safe down discarded the #{label} exact candidate evidence"
    )

    assert!(
      Enum.all?([approved_bots, approved_by, approved_at], &is_nil/1),
      "safe down restored approval authority for the #{label}"
    )

    assert!(
      reason == expected_reason and
        persisted_quarantined_at == quarantined_at and
        persisted_inserted_at == inserted_at and persisted_updated_at == updated_at,
      "safe down rewrote the #{label} migration evidence"
    )

    %{rows: [[lease_id, holder_id, session_id, lease_epoch, expires_at]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT lease_id, holder_instance_id, session_id, authority_epoch, expires_at
        FROM ret0.bot_runner_leases
        WHERE hub_id = $1
        """,
        [hub_id]
      )

    assert!(
      Enum.all?([lease_id, holder_id, session_id, expires_at], &is_nil/1) and
        lease_epoch == stop_epoch,
      "safe down removed the #{label} durable authority fence"
    )
  end

  defp assert_corrupt_approval_fails_closed! do
    rollback_approved_bots = %{
      "enabled" => true,
      "count" => 5,
      "historical" => %{"approved" => "rollback-proof"}
    }

    rollback_candidate_bots = %{
      "enabled" => true,
      "count" => 6,
      "historical" => %{"candidate" => "rollback-proof"}
    }

    rollback_closed_hub_id =
      insert_hub!(
        "audit-approved-closed-rollback",
        %{"bots" => rollback_approved_bots},
        nil,
        "deny"
      )

    insert_approved!(
      rollback_closed_hub_id,
      rollback_approved_bots,
      rollback_candidate_bots
    )

    rollback_evidence = persistent_approval_evidence(rollback_closed_hub_id)

    corrupt_bots = %{"enabled" => true, "count" => 7}
    corrupt_hub_id = insert_hub!("audit-corrupt-approved", %{"bots" => corrupt_bots})
    insert_approved!(corrupt_hub_id, corrupt_bots)

    SQL.query!(
      VerificationRepo,
      "ALTER TABLE ret0.bot_config_approvals DROP CONSTRAINT bot_config_approvals_state_is_complete",
      []
    )

    SQL.query!(
      VerificationRepo,
      """
      UPDATE ret0.bot_config_approvals
      SET state = 'approved',
          approved_bots = NULL,
          approved_by_account_id = 7,
          approved_at = timezone('UTC', clock_timestamp())
      WHERE hub_id = $1
      """,
      [corrupt_hub_id]
    )

    failure = capture_migration_failure(fn -> run_outbox(:up) end)

    assert!(
      String.contains?(failure, "approved bot configuration is incomplete"),
      "corrupt approved_bots did not fail closed"
    )

    refute!(table_exists?(), "failed corrupt migration left an outbox table")
    refute!(runtime_revision_column?(), "failed corrupt migration left a revision column")
    refute!(migration_version_present?(), "failed corrupt migration recorded a version")
    refute!(hub_delete_trigger_exists?(), "failed migration left a Hub delete trigger")
    refute!(hub_delete_function_exists?(), "failed migration left a Hub delete function")
    refute!(index_exists?("hubs_hub_id_hub_sid_idx"), "failed migration left a Hub pair index")
    refute!(json_validation_functions_exist?(), "failed migration left JSON validation functions")

    assert!(
      persistent_approval_evidence(rollback_closed_hub_id) == rollback_evidence,
      "failed migration did not roll back the closed approved conversion atomically"
    )

    refute!(
      runner_lease_exists?(rollback_closed_hub_id),
      "failed migration retained a closed-room lease tombstone"
    )

    SQL.query!(
      VerificationRepo,
      "DELETE FROM ret0.hubs WHERE hub_id = $1",
      [corrupt_hub_id]
    )

    invalid_bots = %{"enabled" => false, "count" => 0, "historical" => "inactive"}
    invalid_hub_id = insert_hub!("audit-invalid-approved", %{"bots" => invalid_bots})
    insert_approved!(invalid_hub_id, invalid_bots)

    invalid_failure = capture_migration_failure(fn -> run_outbox(:up) end)

    assert!(
      String.contains?(invalid_failure, "approved bot configuration is invalid"),
      "inactive exact approval did not fail closed"
    )

    refute!(table_exists?(), "failed invalid migration left an outbox table")
    refute!(runtime_revision_column?(), "failed invalid migration left a revision column")
    refute!(migration_version_present?(), "failed invalid migration recorded a version")
    refute!(hub_delete_trigger_exists?(), "failed invalid migration left a Hub delete trigger")
    refute!(hub_delete_function_exists?(), "failed invalid migration left a Hub delete function")

    refute!(
      index_exists?("hubs_hub_id_hub_sid_idx"),
      "failed invalid migration left a Hub pair index"
    )

    refute!(
      json_validation_functions_exist?(),
      "failed invalid migration left JSON validation functions"
    )

    assert!(
      persistent_approval_evidence(rollback_closed_hub_id) == rollback_evidence,
      "failed invalid migration did not roll back quarantine conversion atomically"
    )

    refute!(
      runner_lease_exists?(rollback_closed_hub_id),
      "failed invalid migration retained a closed-room lease tombstone"
    )
  end

  defp insert_runtime_config!(hub_id, hub_sid, runtime_revision, delivered?) do
    SQL.query!(
      VerificationRepo,
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, bots,
        runtime_chat_enabled, delivered_at, inserted_at, updated_at
      ) VALUES (
        $1::text::uuid, $2, $3, $4, 'config', '{"enabled":true,"count":1}'::jsonb,
        false, CASE WHEN $5 THEN timezone('UTC', clock_timestamp()) ELSE NULL END,
        timezone('UTC', clock_timestamp()), timezone('UTC', clock_timestamp())
      )
      """,
      [Ecto.UUID.generate(), hub_id, hub_sid, runtime_revision, delivered?]
    )
  end

  defp insert_runtime_stop!(hub_id, hub_sid, runtime_revision, revoke_epoch, delivered?) do
    SQL.query!(
      VerificationRepo,
      """
      INSERT INTO ret0.bot_runtime_outbox (
        operation_id, hub_id, hub_sid, runtime_revision, event_kind, revoke_epoch,
        delivered_at, inserted_at, updated_at
      ) VALUES (
        $1::text::uuid, $2, $3, $4, 'stop', $5,
        CASE WHEN $6 THEN timezone('UTC', clock_timestamp()) ELSE NULL END,
        timezone('UTC', clock_timestamp()), timezone('UTC', clock_timestamp())
      )
      """,
      [Ecto.UUID.generate(), hub_id, hub_sid, runtime_revision, revoke_epoch, delivered?]
    )
  end

  defp insert_quarantined_approval!(hub_id, runtime_revision) do
    SQL.query!(
      VerificationRepo,
      """
      INSERT INTO ret0.bot_config_approvals (
        hub_id, state, candidate_bots, runtime_revision,
        last_quarantined_at, last_quarantine_reason, inserted_at, updated_at
      ) VALUES (
        $1, 'quarantined', '{"enabled":false}'::jsonb, $2,
        timezone('UTC', clock_timestamp()), 'delete_lifecycle_verification',
        timezone('UTC', clock_timestamp()), timezone('UTC', clock_timestamp())
      )
      """,
      [hub_id, runtime_revision]
    )
  end

  defp insert_tombstone_lease!(hub_id, authority_epoch) do
    SQL.query!(
      VerificationRepo,
      """
      INSERT INTO ret0.bot_runner_leases (
        hub_id, authority_epoch, inserted_at, updated_at
      ) VALUES (
        $1, $2, timezone('UTC', clock_timestamp()), timezone('UTC', clock_timestamp())
      )
      """,
      [hub_id, authority_epoch]
    )
  end

  defp insert_runtime_active_lease!(hub_id, authority_epoch) do
    SQL.query!(
      VerificationRepo,
      """
      INSERT INTO ret0.bot_runner_leases (
        hub_id, lease_id, holder_instance_id, session_id, authority_epoch,
        expires_at, inserted_at, updated_at
      ) VALUES (
        $1, $2::text::uuid, $3::text::uuid, $4::text::uuid, $5,
        timezone('UTC', clock_timestamp()) + interval '1 hour',
        timezone('UTC', clock_timestamp()), timezone('UTC', clock_timestamp())
      )
      """,
      [
        hub_id,
        Ecto.UUID.generate(),
        Ecto.UUID.generate(),
        Ecto.UUID.generate(),
        authority_epoch
      ]
    )
  end

  defp delete_hub!(hub_id) do
    %{num_rows: 1} =
      SQL.query!(VerificationRepo, "DELETE FROM ret0.hubs WHERE hub_id = $1", [hub_id])
  end

  defp delete_account!(account_id) do
    %{num_rows: 1} =
      SQL.query!(VerificationRepo, "DELETE FROM ret0.accounts WHERE account_id = $1", [account_id])
  end

  defp account_exists?(account_id) do
    %{rows: [[exists]]} =
      SQL.query!(
        VerificationRepo,
        "SELECT EXISTS (SELECT 1 FROM ret0.accounts WHERE account_id = $1)",
        [account_id]
      )

    exists
  end

  defp hub_exists?(hub_id) do
    %{rows: [[exists]]} =
      SQL.query!(
        VerificationRepo,
        "SELECT EXISTS (SELECT 1 FROM ret0.hubs WHERE hub_id = $1)",
        [hub_id]
      )

    exists
  end

  defp outbox_count(hub_id) do
    %{rows: [[count]]} =
      SQL.query!(
        VerificationRepo,
        "SELECT count(*) FROM ret0.bot_runtime_outbox WHERE hub_id = $1",
        [hub_id]
      )

    count
  end

  defp insert_outbox!(hub_id, hub_sid, runtime_revision) do
    %{rows: [[id]]} =
      SQL.query!(
        VerificationRepo,
        """
        INSERT INTO ret0.bot_runtime_outbox (
          operation_id, hub_id, hub_sid, runtime_revision, event_kind, bots,
          runtime_chat_enabled, inserted_at, updated_at
        ) VALUES (
          'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', $1, $2, $3,
          'config', '{"typed":1}'::jsonb, false,
          timezone('UTC', now()), timezone('UTC', now())
        )
        RETURNING id
        """,
        [hub_id, hub_sid, runtime_revision]
      )

    id
  end

  defp hub_sid(hub_id) do
    %{rows: [[hub_sid]]} =
      SQL.query!(VerificationRepo, "SELECT hub_sid FROM ret0.hubs WHERE hub_id = $1", [hub_id])

    hub_sid
  end

  defp nested_lists(count) do
    Enum.reduce(1..count, 0, fn _index, nested -> [nested] end)
  end

  defp approval_updated_at(hub_id) do
    %{rows: [[updated_at]]} =
      SQL.query!(
        VerificationRepo,
        "SELECT updated_at FROM ret0.bot_config_approvals WHERE hub_id = $1",
        [hub_id]
      )

    updated_at
  end

  defp approval_audit(hub_id) do
    %{rows: [[inserted_at, updated_at, approved_at]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT inserted_at, updated_at, approved_at
        FROM ret0.bot_config_approvals
        WHERE hub_id = $1
        """,
        [hub_id]
      )

    %{inserted_at: inserted_at, updated_at: updated_at, approved_at: approved_at}
  end

  defp persistent_approval_evidence(hub_id) do
    %{rows: [row]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT state, candidate_bots, approved_bots, approved_by_account_id, approved_at,
               last_quarantined_by_account_id, last_quarantined_at,
               last_quarantine_reason, inserted_at, updated_at
        FROM ret0.bot_config_approvals
        WHERE hub_id = $1
        """,
        [hub_id]
      )

    row
  end

  defp runner_lease_exists?(hub_id) do
    %{rows: [[exists]]} =
      SQL.query!(
        VerificationRepo,
        "SELECT EXISTS (SELECT 1 FROM ret0.bot_runner_leases WHERE hub_id = $1)",
        [hub_id]
      )

    exists
  end

  defp approval_evidence(hub_id) do
    %{
      rows: [
        [
          state,
          candidate_bots,
          approved_bots,
          approved_by_account_id,
          approved_at,
          last_quarantined_by_account_id,
          last_quarantined_at,
          last_quarantine_reason,
          runtime_revision,
          inserted_at,
          updated_at
        ]
      ]
    } =
      SQL.query!(
        VerificationRepo,
        """
        SELECT state, candidate_bots, approved_bots, approved_by_account_id, approved_at,
               last_quarantined_by_account_id, last_quarantined_at,
               last_quarantine_reason, runtime_revision, inserted_at, updated_at
        FROM ret0.bot_config_approvals
        WHERE hub_id = $1
        """,
        [hub_id]
      )

    %{
      state: state,
      candidate_bots: candidate_bots,
      approved_bots: approved_bots,
      approved_by_account_id: approved_by_account_id,
      approved_at: approved_at,
      last_quarantined_by_account_id: last_quarantined_by_account_id,
      last_quarantined_at: last_quarantined_at,
      last_quarantine_reason: last_quarantine_reason,
      runtime_revision: runtime_revision,
      inserted_at: inserted_at,
      updated_at: updated_at
    }
  end

  defp assert_down_refused!(expected_message) do
    failure = capture_migration_failure(fn -> run_outbox(:down) end)

    assert!(
      String.contains?(failure, expected_message),
      "migration down did not refuse unsafe state: #{expected_message}"
    )
  end

  defp assert_query_refused!(statement, params, constraint_name) do
    failure =
      try do
        SQL.query!(VerificationRepo, statement, params)
        "query unexpectedly succeeded"
      rescue
        error -> Exception.message(error)
      catch
        kind, reason -> "#{kind}: #{inspect(reason)}"
      end

    assert!(
      String.contains?(failure, constraint_name),
      "database did not enforce #{constraint_name}"
    )
  end

  defp capture_migration_failure(fun) do
    try do
      fun.()
      "migration unexpectedly succeeded"
    rescue
      error -> Exception.message(error)
    catch
      kind, reason -> "#{kind}: #{inspect(reason)}"
    end
  end

  defp assert_uuid_v4!(uuid) do
    assert!(
      Regex.match?(
        ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/,
        uuid
      ),
      "backfill operation id is not a deterministic UUID v4"
    )
  end

  defp run_base_up do
    Migrator.run(VerificationRepo, @base_source, :up,
      all: true,
      prefix: "ret0",
      log: false
    )
  end

  defp run_outbox(direction) do
    Migrator.run(VerificationRepo, @outbox_source, direction,
      step: 1,
      prefix: "ret0",
      log: false
    )
  end

  defp table_exists? do
    %{rows: [[exists]]} =
      SQL.query!(
        VerificationRepo,
        "SELECT to_regclass('ret0.bot_runtime_outbox') IS NOT NULL",
        []
      )

    exists
  end

  defp hub_delete_trigger_exists? do
    %{rows: [[exists]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT EXISTS (
          SELECT 1
          FROM pg_trigger AS trigger_record
          JOIN pg_class AS table_record ON table_record.oid = trigger_record.tgrelid
          JOIN pg_namespace AS namespace_record ON namespace_record.oid = table_record.relnamespace
          WHERE namespace_record.nspname = 'ret0'
            AND table_record.relname = 'hubs'
            AND trigger_record.tgname = 'bot_runtime_outbox_cleanup_before_hub_delete'
            AND NOT trigger_record.tgisinternal
        )
        """,
        []
      )

    exists
  end

  defp hub_delete_function_exists? do
    %{rows: [[exists]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT to_regprocedure(
          'ret0.cleanup_delivered_bot_runtime_outbox_before_hub_delete()'
        ) IS NOT NULL
        """,
        []
      )

    exists
  end

  defp outbox_trigger_exists?(trigger_name) do
    %{rows: [[exists]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT EXISTS (
          SELECT 1
          FROM pg_trigger AS trigger_record
          JOIN pg_class AS table_record ON table_record.oid = trigger_record.tgrelid
          JOIN pg_namespace AS namespace_record ON namespace_record.oid = table_record.relnamespace
          WHERE namespace_record.nspname = 'ret0'
            AND table_record.relname = 'bot_runtime_outbox'
            AND trigger_record.tgname = $1
            AND NOT trigger_record.tgisinternal
        )
        """,
        [trigger_name]
      )

    exists
  end

  defp json_validation_functions_exist? do
    %{rows: [[public_exists, depth_exists]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT
          to_regprocedure('ret0.bot_runtime_json_is_js_interoperable(jsonb)') IS NOT NULL,
          to_regprocedure(
            'ret0.bot_runtime_json_is_js_interoperable_at_depth(jsonb,integer)'
          ) IS NOT NULL
        """,
        []
      )

    public_exists and depth_exists
  end

  defp runtime_revision_column? do
    %{rows: [[exists]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = 'ret0'
            AND table_name = 'bot_config_approvals'
            AND column_name = 'runtime_revision'
            AND is_nullable = 'NO'
        )
        """,
        []
      )

    exists
  end

  defp runtime_chat_enabled_column? do
    %{rows: [[exists]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = 'ret0'
            AND table_name = 'bot_runtime_outbox'
            AND column_name = 'runtime_chat_enabled'
            AND data_type = 'boolean'
            AND is_nullable = 'YES'
        )
        """,
        []
      )

    exists
  end

  defp index_exists?(index_name) do
    %{rows: [[exists]]} =
      SQL.query!(VerificationRepo, "SELECT to_regclass('ret0.' || $1) IS NOT NULL", [index_name])

    exists
  end

  defp migration_version_present? do
    %{rows: [[present]]} =
      SQL.query!(
        VerificationRepo,
        "SELECT EXISTS (SELECT 1 FROM ret0.schema_migrations WHERE version = $1)",
        [@outbox_version]
      )

    present
  end

  defp sequence_last_value do
    %{rows: [[last_value]]} =
      SQL.query!(
        VerificationRepo,
        "SELECT last_value FROM ret0.bot_runner_authority_epoch_seq",
        []
      )

    last_value
  end

  defp assert!(true, _message), do: :ok
  defp assert!(false, message), do: Mix.raise(message)
  defp refute!(false, _message), do: :ok
  defp refute!(true, message), do: Mix.raise(message)
end

Ret.BotRuntimeOutboxMigrationVerifier.run()
