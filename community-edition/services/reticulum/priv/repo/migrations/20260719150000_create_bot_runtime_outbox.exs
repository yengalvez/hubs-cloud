defmodule Ret.Repo.Migrations.CreateBotRuntimeOutbox do
  use Ecto.Migration

  @max_javascript_integer 9_007_199_254_740_991
  @max_config_bytes 16_384

  def up do
    alter table(:bot_config_approvals) do
      add :runtime_revision, :bigint
    end

    create constraint(:bot_config_approvals, :bot_config_approvals_runtime_revision_safe,
             check:
               "runtime_revision IS NULL OR runtime_revision BETWEEN 1 AND #{@max_javascript_integer}"
           )

    create unique_index(:hubs, [:hub_id, :hub_sid], name: :hubs_hub_id_hub_sid_idx)

    create table(:bot_runtime_outbox, primary_key: false) do
      add :id, :bigserial, primary_key: true
      add :operation_id, :uuid, null: false
      add :hub_id, :bigint, null: false
      add :hub_sid, :string, null: false
      add :runtime_revision, :bigint, null: false
      add :event_kind, :string, null: false
      add :bots, :map
      add :runtime_chat_enabled, :boolean
      add :revoke_epoch, :bigint
      add :claim_owner, :string
      add :claim_token, :uuid
      add :claim_expires_at, :utc_datetime_usec
      add :attempt_count, :bigint, null: false, default: 0

      add :next_attempt_at, :utc_datetime_usec,
        null: false,
        default: fragment("timezone('UTC', clock_timestamp())")

      add :delivered_at, :utc_datetime_usec
      add :last_failure_code, :string

      timestamps(type: :utc_datetime_usec)
    end

    execute("""
    ALTER TABLE ret0.bot_runtime_outbox
    ADD CONSTRAINT bot_runtime_outbox_hub_identity_fkey
    FOREIGN KEY (hub_id, hub_sid)
    REFERENCES ret0.hubs (hub_id, hub_sid)
    ON DELETE NO ACTION
    """)

    create unique_index(:bot_runtime_outbox, [:operation_id],
             name: :bot_runtime_outbox_operation_id_idx
           )

    create unique_index(:bot_runtime_outbox, [:hub_id, :runtime_revision],
             name: :bot_runtime_outbox_hub_revision_idx
           )

    create index(:bot_runtime_outbox, [:hub_id, :runtime_revision],
             name: :bot_runtime_outbox_pending_room_order_idx,
             where: "delivered_at IS NULL"
           )

    create index(:bot_runtime_outbox, [:next_attempt_at, :id],
             name: :bot_runtime_outbox_due_idx,
             where: "delivered_at IS NULL"
           )

    create index(:bot_runtime_outbox, [:claim_expires_at, :id],
             name: :bot_runtime_outbox_claim_expiry_idx,
             where: "delivered_at IS NULL AND claim_token IS NOT NULL"
           )

    execute("""
    CREATE FUNCTION ret0.bot_runtime_json_is_js_interoperable_at_depth(
      payload jsonb,
      current_depth integer
    )
    RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    AS $$
    DECLARE
      payload_kind text;
      child jsonb;
      numeric_value numeric;
      float_value double precision;
      float_text text;
    BEGIN
      IF current_depth > 64 THEN
        RETURN FALSE;
      END IF;

      payload_kind := jsonb_typeof(payload);

      IF payload_kind IN ('null', 'boolean', 'string') THEN
        RETURN TRUE;
      ELSIF payload_kind = 'array' THEN
        FOR child IN SELECT value FROM jsonb_array_elements(payload)
        LOOP
          IF NOT ret0.bot_runtime_json_is_js_interoperable_at_depth(
            child,
            current_depth + CASE WHEN jsonb_typeof(child) IN ('array', 'object') THEN 1 ELSE 0 END
          ) THEN
            RETURN FALSE;
          END IF;
        END LOOP;

        RETURN TRUE;
      ELSIF payload_kind = 'object' THEN
        FOR child IN SELECT value FROM jsonb_each(payload)
        LOOP
          IF NOT ret0.bot_runtime_json_is_js_interoperable_at_depth(
            child,
            current_depth + CASE WHEN jsonb_typeof(child) IN ('array', 'object') THEN 1 ELSE 0 END
          ) THEN
            RETURN FALSE;
          END IF;
        END LOOP;

        RETURN TRUE;
      ELSIF payload_kind = 'number' THEN
        BEGIN
          numeric_value := (payload #>> '{}')::numeric;

          IF trunc(numeric_value) = numeric_value THEN
            RETURN numeric_value BETWEEN -#{@max_javascript_integer} AND #{@max_javascript_integer};
          END IF;

          float_value := numeric_value::double precision;
          float_text := float_value::text;

          IF float_text IN ('Infinity', '-Infinity', 'NaN') THEN
            RETURN FALSE;
          END IF;

          RETURN float_text::numeric = numeric_value;
        EXCEPTION
          WHEN numeric_value_out_of_range OR invalid_text_representation THEN
            RETURN FALSE;
        END;
      END IF;

      RETURN FALSE;
    END
    $$
    """)

    execute("""
    CREATE FUNCTION ret0.bot_runtime_json_is_js_interoperable(payload jsonb)
    RETURNS boolean
    LANGUAGE sql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    AS $$
      SELECT ret0.bot_runtime_json_is_js_interoperable_at_depth(payload, 1)
    $$
    """)

    create constraint(:bot_runtime_outbox, :bot_runtime_outbox_hub_sid_valid,
             check: "hub_sid ~ '^[A-Za-z0-9_-]{1,64}$'"
           )

    create constraint(:bot_runtime_outbox, :bot_runtime_outbox_operation_id_v4,
             check:
               "operation_id::text ~ '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'"
           )

    create constraint(:bot_runtime_outbox, :bot_runtime_outbox_runtime_revision_safe,
             check: "runtime_revision BETWEEN 1 AND #{@max_javascript_integer}"
           )

    create constraint(:bot_runtime_outbox, :bot_runtime_outbox_event_complete,
             check: """
             (event_kind = 'config'
              AND bots IS NOT NULL
              AND jsonb_typeof(bots) = 'object'
              AND runtime_chat_enabled IS NOT NULL
              AND revoke_epoch IS NULL)
             OR
             (event_kind = 'stop'
              AND bots IS NULL
              AND runtime_chat_enabled IS NULL
              AND revoke_epoch BETWEEN 1 AND #{@max_javascript_integer})
             """
           )

    create constraint(:bot_runtime_outbox, :bot_runtime_outbox_bots_size,
             check: "bots IS NULL OR octet_length(bots::text) <= #{@max_config_bytes}"
           )

    create constraint(:bot_runtime_outbox, :bot_runtime_outbox_runtime_bots_size,
             check: """
             bots IS NULL
             OR runtime_chat_enabled IS NULL
             OR octet_length(
               (bots || jsonb_build_object('chat_enabled', runtime_chat_enabled))::text
             ) <= #{@max_config_bytes}
             """
           )

    create constraint(:bot_runtime_outbox, :bot_runtime_outbox_bots_js_interoperable,
             check: "bots IS NULL OR ret0.bot_runtime_json_is_js_interoperable(bots)"
           )

    create constraint(:bot_runtime_outbox, :bot_runtime_outbox_claim_complete,
             check: """
             (claim_owner IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL)
             OR
             (claim_owner IS NOT NULL
              AND octet_length(claim_owner) BETWEEN 1 AND 128
              AND claim_token IS NOT NULL
              AND claim_expires_at IS NOT NULL)
             """
           )

    create constraint(:bot_runtime_outbox, :bot_runtime_outbox_delivery_releases_claim,
             check:
               "delivered_at IS NULL OR (claim_owner IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL)"
           )

    create constraint(:bot_runtime_outbox, :bot_runtime_outbox_attempt_count_valid,
             check: "attempt_count >= 0"
           )

    create constraint(:bot_runtime_outbox, :bot_runtime_outbox_failure_code_bounded,
             check: """
             last_failure_code IS NULL
             OR (
               octet_length(last_failure_code) BETWEEN 1 AND 64
               AND last_failure_code ~ '^[a-z0-9][a-z0-9_.:-]{0,63}$'
             )
             """
           )

    execute("""
    CREATE FUNCTION ret0.guard_bot_runtime_outbox_immutable_event()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
      IF NEW.operation_id IS DISTINCT FROM OLD.operation_id
         OR NEW.hub_id IS DISTINCT FROM OLD.hub_id
         OR NEW.hub_sid IS DISTINCT FROM OLD.hub_sid
         OR NEW.runtime_revision IS DISTINCT FROM OLD.runtime_revision
         OR NEW.event_kind IS DISTINCT FROM OLD.event_kind
         OR NEW.bots::text IS DISTINCT FROM OLD.bots::text
         OR NEW.runtime_chat_enabled IS DISTINCT FROM OLD.runtime_chat_enabled
         OR NEW.revoke_epoch IS DISTINCT FROM OLD.revoke_epoch
         OR NEW.inserted_at IS DISTINCT FROM OLD.inserted_at THEN
        RAISE EXCEPTION 'bot runtime outbox event identity and payload are immutable'
          USING ERRCODE = '23514',
                CONSTRAINT = 'bot_runtime_outbox_event_immutable';
      END IF;

      RETURN NEW;
    END
    $$
    """)

    execute("""
    CREATE TRIGGER bot_runtime_outbox_immutable_event
    BEFORE UPDATE ON ret0.bot_runtime_outbox
    FOR EACH ROW
    EXECUTE FUNCTION ret0.guard_bot_runtime_outbox_immutable_event()
    """)

    execute("""
    CREATE FUNCTION ret0.guard_pending_bot_runtime_outbox_delete()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
      IF OLD.delivered_at IS NULL THEN
        RAISE EXCEPTION 'pending bot runtime outbox events cannot be deleted'
          USING ERRCODE = '23514',
                CONSTRAINT = 'bot_runtime_outbox_pending_delete';
      END IF;

      RETURN OLD;
    END
    $$
    """)

    execute("""
    CREATE TRIGGER bot_runtime_outbox_pending_delete
    BEFORE DELETE ON ret0.bot_runtime_outbox
    FOR EACH ROW
    EXECUTE FUNCTION ret0.guard_pending_bot_runtime_outbox_delete()
    """)

    execute("""
    CREATE FUNCTION ret0.cleanup_delivered_bot_runtime_outbox_before_hub_delete()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM ret0.bot_runtime_outbox AS pending
        WHERE pending.hub_id = OLD.hub_id
          AND pending.delivered_at IS NULL
      ) AND EXISTS (
        SELECT 1
        FROM ret0.bot_runtime_outbox AS terminal_stop
        WHERE terminal_stop.hub_id = OLD.hub_id
          AND terminal_stop.event_kind = 'stop'
          AND terminal_stop.delivered_at IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM ret0.bot_config_approvals AS approval
            JOIN ret0.bot_runner_leases AS lease
              ON lease.hub_id = approval.hub_id
            WHERE approval.hub_id = OLD.hub_id
              AND approval.state = 'quarantined'
              AND approval.runtime_revision = terminal_stop.runtime_revision
              AND lease.lease_id IS NULL
              AND lease.holder_instance_id IS NULL
              AND lease.session_id IS NULL
              AND lease.expires_at IS NULL
              AND lease.authority_epoch = terminal_stop.revoke_epoch
          )
          AND NOT EXISTS (
            SELECT 1
            FROM ret0.bot_runtime_outbox AS later
            WHERE later.hub_id = OLD.hub_id
              AND later.runtime_revision > terminal_stop.runtime_revision
          )
      ) THEN
        DELETE FROM ret0.bot_runtime_outbox
        WHERE hub_id = OLD.hub_id
          AND delivered_at IS NOT NULL;
      END IF;

      RETURN OLD;
    END
    $$
    """)

    execute("""
    CREATE TRIGGER bot_runtime_outbox_cleanup_before_hub_delete
    BEFORE DELETE ON ret0.hubs
    FOR EACH ROW
    EXECUTE FUNCTION ret0.cleanup_delivered_bot_runtime_outbox_before_hub_delete()
    """)

    execute("""
    DO $$
    DECLARE
      approval record;
      fenced_epoch bigint;
      operation_uuid uuid;
      database_now timestamp(6) without time zone;
      operation_hash text;
      migration_requires_stop boolean;
      migration_quarantine_reason text;
      normalized_runtime_chat_enabled boolean;
    BEGIN
      FOR approval IN
        SELECT
          approvals.hub_id,
          approvals.state,
          approvals.approved_bots,
          hubs.hub_sid,
          hubs.entry_mode,
          hubs.user_data->'bots' AS current_bots
        FROM ret0.bot_config_approvals AS approvals
        JOIN ret0.hubs AS hubs ON hubs.hub_id = approvals.hub_id
        ORDER BY approvals.hub_id
      LOOP
        database_now := timezone('UTC', clock_timestamp());
        operation_hash := md5('yenhubs-bot-runtime:v2:' || approval.hub_id::text || ':1');
        operation_uuid := overlay(overlay(operation_hash placing '4' from 13 for 1) placing '8' from 17 for 1)::uuid;
        migration_requires_stop := FALSE;
        migration_quarantine_reason := NULL;

        IF approval.state = 'approved' THEN
          IF approval.approved_bots IS NULL OR jsonb_typeof(approval.approved_bots) <> 'object' THEN
            RAISE EXCEPTION 'approved bot configuration is incomplete during runtime outbox migration';
          END IF;

          normalized_runtime_chat_enabled := COALESCE(
            (approval.approved_bots->'chat_enabled')::text IN ('true', '1', '"true"'),
            FALSE
          );

          IF approval.entry_mode = 'deny' THEN
            migration_requires_stop := TRUE;
            migration_quarantine_reason := 'room_closed_runtime_migration';
          ELSIF approval.current_bots::text IS DISTINCT FROM approval.approved_bots::text THEN
            -- A historical approval is authority only for the exact current
            -- Hub payload. Migrating a stale snapshot as CONFIG would briefly
            -- resurrect obsolete desired state before the runtime admission
            -- layer rejected it. Preserve the candidate as audit evidence and
            -- convert the row to the same durable STOP fence as quarantine.
            migration_requires_stop := TRUE;
            migration_quarantine_reason := 'stale_approval_runtime_migration';
          ELSIF NOT (
            octet_length(approval.approved_bots::text) <= #{@max_config_bytes}
            AND ret0.bot_runtime_json_is_js_interoperable(approval.approved_bots)
            AND COALESCE(
              (approval.approved_bots->'enabled')::text IN ('true', '1', '"true"'),
              FALSE
            )
            AND CASE jsonb_typeof(approval.approved_bots->'count')
              WHEN 'number' THEN
                ((approval.approved_bots->>'count') ~ '^[0-9]+$' AND
                 (approval.approved_bots->>'count') ~ '[1-9]')
              WHEN 'string' THEN
                CASE WHEN octet_length(approval.approved_bots->>'count') <= 64
                  THEN ((approval.approved_bots->>'count') ~ '^([0-9]+|[+][0-9]+)$' AND
                        (approval.approved_bots->>'count') ~ '[1-9]')
                  ELSE FALSE
                END
              ELSE FALSE
            END
          ) THEN
            RAISE EXCEPTION 'approved bot configuration is invalid during runtime outbox migration';
          ELSIF octet_length(
            (
              approval.approved_bots ||
              jsonb_build_object('chat_enabled', normalized_runtime_chat_enabled)
            )::text
          ) > #{@max_config_bytes} THEN
            migration_requires_stop := TRUE;
            migration_quarantine_reason := 'runtime_payload_too_large_migration';
          END IF;

          IF migration_requires_stop THEN
            UPDATE ret0.bot_config_approvals
            SET state = 'quarantined',
                approved_bots = NULL,
                approved_by_account_id = NULL,
                approved_at = NULL,
                last_quarantined_by_account_id = NULL,
                last_quarantined_at = database_now,
                last_quarantine_reason = migration_quarantine_reason,
                runtime_revision = 1,
                updated_at = database_now
            WHERE hub_id = approval.hub_id;
          ELSE
            UPDATE ret0.bot_config_approvals
            SET runtime_revision = 1
            WHERE hub_id = approval.hub_id;

            INSERT INTO ret0.bot_runtime_outbox (
              operation_id,
              hub_id,
              hub_sid,
              runtime_revision,
              event_kind,
              bots,
              runtime_chat_enabled,
              next_attempt_at,
              inserted_at,
              updated_at
            )
            VALUES (
              operation_uuid,
              approval.hub_id,
              approval.hub_sid,
              1,
              'config',
              approval.approved_bots,
              normalized_runtime_chat_enabled,
              database_now,
              database_now,
              database_now
            );
          END IF;
        ELSIF approval.state = 'quarantined' THEN
          UPDATE ret0.bot_config_approvals
          SET runtime_revision = 1
          WHERE hub_id = approval.hub_id;
        ELSE
          RAISE EXCEPTION 'unsupported bot config approval state during runtime outbox migration';
        END IF;

        IF approval.state = 'quarantined'
           OR (approval.state = 'approved' AND migration_requires_stop) THEN
          fenced_epoch := nextval('ret0.bot_runner_authority_epoch_seq');

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
            approval.hub_id,
            NULL,
            NULL,
            NULL,
            fenced_epoch,
            NULL,
            database_now,
            database_now
          )
          ON CONFLICT (hub_id) DO UPDATE
          SET lease_id = NULL,
              holder_instance_id = NULL,
              session_id = NULL,
              authority_epoch = EXCLUDED.authority_epoch,
              expires_at = NULL,
              updated_at = EXCLUDED.updated_at;

          INSERT INTO ret0.bot_runtime_outbox (
            operation_id,
            hub_id,
            hub_sid,
            runtime_revision,
            event_kind,
            revoke_epoch,
            next_attempt_at,
            inserted_at,
            updated_at
          )
          VALUES (
            operation_uuid,
            approval.hub_id,
            approval.hub_sid,
            1,
            'stop',
            fenced_epoch,
            database_now,
            database_now,
            database_now
          );
        END IF;
      END LOOP;
    END
    $$
    """)

    alter table(:bot_config_approvals) do
      modify :runtime_revision, :bigint, null: false
    end
  end

  def down do
    execute("""
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1
        FROM ret0.hubs
        WHERE jsonb_typeof(user_data->'bots') = 'object'
          AND (user_data->'bots'->'enabled') IS DISTINCT FROM 'false'::jsonb
      ) THEN
        RAISE EXCEPTION 'refusing to remove bot runtime outbox while a stored bot configuration is enabled';
      END IF;

      IF EXISTS (
        SELECT 1
        FROM ret0.bot_config_approvals
        WHERE state <> 'quarantined'
      ) THEN
        RAISE EXCEPTION 'refusing to remove bot runtime outbox while an approval is not quarantined';
      END IF;

      IF EXISTS (
        SELECT 1
        FROM ret0.bot_runtime_outbox
        WHERE delivered_at IS NULL
      ) THEN
        RAISE EXCEPTION 'refusing to remove bot runtime outbox while delivery is pending';
      END IF;

      IF EXISTS (
        SELECT 1
        FROM ret0.bot_runner_leases
        WHERE lease_id IS NOT NULL
      ) THEN
        RAISE EXCEPTION 'refusing to remove bot runtime outbox while a runner lease remains';
      END IF;
    END
    $$
    """)

    execute("""
    DROP TRIGGER bot_runtime_outbox_cleanup_before_hub_delete ON ret0.hubs
    """)

    execute("""
    DROP FUNCTION ret0.cleanup_delivered_bot_runtime_outbox_before_hub_delete()
    """)

    execute("""
    DROP TRIGGER bot_runtime_outbox_immutable_event ON ret0.bot_runtime_outbox
    """)

    execute("""
    DROP FUNCTION ret0.guard_bot_runtime_outbox_immutable_event()
    """)

    execute("""
    DROP TRIGGER bot_runtime_outbox_pending_delete ON ret0.bot_runtime_outbox
    """)

    execute("""
    DROP FUNCTION ret0.guard_pending_bot_runtime_outbox_delete()
    """)

    drop table(:bot_runtime_outbox)

    execute("DROP FUNCTION ret0.bot_runtime_json_is_js_interoperable(jsonb)")

    execute("DROP FUNCTION ret0.bot_runtime_json_is_js_interoperable_at_depth(jsonb, integer)")

    drop index(:hubs, [:hub_id, :hub_sid], name: :hubs_hub_id_hub_sid_idx)

    drop constraint(:bot_config_approvals, :bot_config_approvals_runtime_revision_safe)

    alter table(:bot_config_approvals) do
      remove :runtime_revision
    end
  end
end
