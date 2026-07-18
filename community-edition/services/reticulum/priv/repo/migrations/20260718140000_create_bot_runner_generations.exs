defmodule Ret.Repo.Migrations.CreateBotRunnerGenerations do
  use Ecto.Migration

  def change do
    create table(:bot_runner_generations, primary_key: false) do
      add :hub_id, references(:hubs, column: :hub_id, on_delete: :delete_all), primary_key: true

      add :process_generation, :uuid, primary_key: true
      add :holder_id, :string, null: false
      add :token_expires_at, :utc_datetime_usec, null: false
      add :consumed_at, :utc_datetime_usec, null: false
    end

    create constraint(:bot_runner_generations, :bot_runner_generations_holder_id_bounded,
             check: "octet_length(holder_id) BETWEEN 1 AND 128"
           )

    create index(:bot_runner_generations, [:token_expires_at],
             name: :bot_runner_generations_expiry_idx
           )
  end
end
