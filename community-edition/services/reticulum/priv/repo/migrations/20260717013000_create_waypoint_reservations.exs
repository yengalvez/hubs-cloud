defmodule Ret.Repo.Migrations.CreateWaypointReservations do
  use Ecto.Migration

  def change do
    create table(:waypoint_reservations, primary_key: false) do
      add :waypoint_reservation_id, :bigint,
        default: fragment("ret0.next_id()"),
        primary_key: true

      add :hub_id, references(:hubs, column: :hub_id, on_delete: :delete_all), null: false
      add :session_id, :uuid, null: false
      add :client_instance_id, :uuid, null: false
      add :channel_id, :uuid, null: false

      # The active lease. A row with these four fields cleared is the session tombstone.
      add :waypoint_id, :string, size: 512
      add :operation_id, :uuid
      add :reservation_id, :uuid
      add :expires_at, :utc_datetime_usec

      # Exact request fingerprint and public response for monotonic idempotency, including errors.
      add :last_request_seq, :bigint, null: false, default: 0
      add :last_request_action, :string, size: 16
      add :last_request_waypoint_id, :string, size: 512
      add :last_request_operation_id, :uuid
      add :last_request_reservation_id, :uuid
      add :last_response, :map

      add :rate_window_started_at, :utc_datetime_usec, null: false
      add :rate_request_count, :integer, null: false, default: 0

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:waypoint_reservations, [:hub_id, :session_id],
             name: :waypoint_reservations_hub_session_uidx
           )

    create unique_index(:waypoint_reservations, [:hub_id, :waypoint_id],
             name: :waypoint_reservations_hub_waypoint_uidx,
             where: "waypoint_id IS NOT NULL"
           )

    create index(:waypoint_reservations, [:hub_id, :expires_at],
             name: :waypoint_reservations_expiry_idx,
             where: "waypoint_id IS NOT NULL"
           )

    create constraint(:waypoint_reservations, :waypoint_reservations_active_lease_complete,
             check: """
             (waypoint_id IS NULL AND operation_id IS NULL AND reservation_id IS NULL AND expires_at IS NULL)
             OR
             (waypoint_id IS NOT NULL AND operation_id IS NOT NULL AND reservation_id IS NOT NULL AND expires_at IS NOT NULL)
             """
           )

    create constraint(:waypoint_reservations, :waypoint_reservations_request_cache_complete,
             check: """
             (last_request_seq = 0 AND last_request_action IS NULL AND last_request_waypoint_id IS NULL
              AND last_request_operation_id IS NULL AND last_request_reservation_id IS NULL AND last_response IS NULL)
             OR
             (last_request_seq > 0 AND last_request_action IS NOT NULL AND last_request_waypoint_id IS NOT NULL
              AND last_request_operation_id IS NOT NULL AND last_request_reservation_id IS NOT NULL AND last_response IS NOT NULL)
             """
           )

    create constraint(:waypoint_reservations, :waypoint_reservations_request_seq_nonnegative,
             check: "last_request_seq >= 0"
           )

    create constraint(:waypoint_reservations, :waypoint_reservations_rate_count_nonnegative,
             check: "rate_request_count >= 0"
           )

    create constraint(:waypoint_reservations, :waypoint_reservations_waypoint_id_valid,
             check: "waypoint_id IS NULL OR (octet_length(waypoint_id) BETWEEN 1 AND 512)"
           )
  end
end
