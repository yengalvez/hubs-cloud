defmodule Ret.Repo.Migrations.CreateBotRunnerLeases do
  use Ecto.Migration

  @max_javascript_integer 9_007_199_254_740_991

  def up do
    execute("""
    CREATE SEQUENCE ret0.bot_runner_authority_epoch_seq
      AS bigint
      MINVALUE 1
      MAXVALUE #{@max_javascript_integer}
      START WITH 1
      INCREMENT BY 1
      NO CYCLE
    """)

    create table(:bot_runner_leases, primary_key: false) do
      add :hub_id, references(:hubs, column: :hub_id, on_delete: :delete_all), primary_key: true
      add :lease_id, :uuid
      add :holder_instance_id, :uuid
      add :session_id, :uuid
      add :authority_epoch, :bigint, null: false
      add :expires_at, :utc_datetime_usec

      timestamps(type: :utc_datetime_usec)
    end

    create index(:bot_runner_leases, [:expires_at],
             name: :bot_runner_leases_expiry_idx,
             where: "lease_id IS NOT NULL"
           )

    create constraint(:bot_runner_leases, :bot_runner_leases_active_state_complete,
             check: """
             (lease_id IS NULL AND holder_instance_id IS NULL AND session_id IS NULL AND expires_at IS NULL)
             OR
             (lease_id IS NOT NULL AND holder_instance_id IS NOT NULL AND session_id IS NOT NULL
              AND expires_at IS NOT NULL AND authority_epoch > 0)
             """
           )

    create constraint(:bot_runner_leases, :bot_runner_leases_epoch_javascript_safe,
             check: "authority_epoch BETWEEN 1 AND #{@max_javascript_integer}"
           )
  end

  def down do
    drop table(:bot_runner_leases)
    execute("DROP SEQUENCE ret0.bot_runner_authority_epoch_seq")
  end
end
