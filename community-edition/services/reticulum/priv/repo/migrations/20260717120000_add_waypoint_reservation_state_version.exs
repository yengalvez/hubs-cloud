defmodule Ret.Repo.Migrations.AddWaypointReservationStateVersion do
  use Ecto.Migration

  @sequence "ret0.waypoint_reservation_state_version_seq"
  @max_safe_integer 9_007_199_254_740_991

  def up do
    execute("""
    CREATE SEQUENCE #{@sequence}
    AS bigint
    MINVALUE 1
    MAXVALUE #{@max_safe_integer}
    START WITH 1
    NO CYCLE
    """)

    alter table(:waypoint_reservations) do
      add :state_version, :bigint
    end

    execute("""
    UPDATE ret0.waypoint_reservations
    SET state_version = nextval('#{@sequence}')
    """)

    alter table(:waypoint_reservations) do
      modify :state_version, :bigint, null: false
    end

    create constraint(
             :waypoint_reservations,
             :waypoint_reservations_state_version_safe_positive,
             check: "state_version BETWEEN 1 AND #{@max_safe_integer}"
           )
  end

  def down do
    alter table(:waypoint_reservations) do
      remove :state_version
    end

    execute("DROP SEQUENCE #{@sequence}")
  end
end
