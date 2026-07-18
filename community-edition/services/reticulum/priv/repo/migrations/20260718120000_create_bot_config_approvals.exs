defmodule Ret.Repo.Migrations.CreateBotConfigApprovals do
  use Ecto.Migration

  def up do
    create table(:bot_config_approvals, primary_key: false) do
      add :hub_id, references(:hubs, column: :hub_id, on_delete: :delete_all), primary_key: true

      add :state, :string, null: false
      add :candidate_bots, :map, null: false
      add :approved_bots, :map
      add :approved_by_account_id, :bigint
      add :approved_at, :utc_datetime_usec
      add :last_quarantined_by_account_id, :bigint
      add :last_quarantined_at, :utc_datetime_usec
      add :last_quarantine_reason, :string

      timestamps(type: :utc_datetime_usec)
    end

    create constraint(:bot_config_approvals, :bot_config_approvals_state_valid,
             check: "state IN ('quarantined', 'approved')"
           )

    create constraint(:bot_config_approvals, :bot_config_approvals_candidate_is_object,
             check: "jsonb_typeof(candidate_bots) = 'object'"
           )

    create constraint(:bot_config_approvals, :bot_config_approvals_approved_is_object,
             check: "approved_bots IS NULL OR jsonb_typeof(approved_bots) = 'object'"
           )

    create constraint(:bot_config_approvals, :bot_config_approvals_state_is_complete,
             check: """
             (state = 'approved'
              AND approved_bots IS NOT NULL
              AND approved_by_account_id IS NOT NULL
              AND approved_at IS NOT NULL)
             OR
             (state = 'quarantined'
              AND approved_bots IS NULL
              AND approved_by_account_id IS NULL
              AND approved_at IS NULL
              AND last_quarantined_at IS NOT NULL
              AND octet_length(last_quarantine_reason) BETWEEN 1 AND 128)
             """
           )

    create index(:bot_config_approvals, [:state])

    execute("""
    INSERT INTO ret0.bot_config_approvals (
      hub_id,
      state,
      candidate_bots,
      approved_bots,
      approved_by_account_id,
      approved_at,
      last_quarantined_by_account_id,
      last_quarantined_at,
      last_quarantine_reason,
      inserted_at,
      updated_at
    )
    SELECT
      hub_id,
      'quarantined',
      user_data->'bots',
      NULL,
      NULL,
      NULL,
      NULL,
      timezone('UTC', now()),
      'legacy_migration',
      timezone('UTC', now()),
      timezone('UTC', now())
    FROM ret0.hubs
    WHERE jsonb_typeof(user_data->'bots') = 'object'
    """)

    execute("""
    UPDATE ret0.hubs
    SET user_data = jsonb_set(user_data, '{bots,enabled}', 'false'::jsonb, true),
        updated_at = timezone('UTC', now())
    WHERE jsonb_typeof(user_data->'bots') = 'object'
    """)
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
        RAISE EXCEPTION
          'refusing to drop bot_config_approvals while a stored bot configuration is not explicitly disabled';
      END IF;
    END
    $$
    """)

    drop table(:bot_config_approvals)
  end
end
