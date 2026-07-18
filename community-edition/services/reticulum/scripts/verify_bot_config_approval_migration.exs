{:ok, _apps} = Application.ensure_all_started(:ecto_sql)
{:ok, _apps} = Application.ensure_all_started(:postgrex)

defmodule Ret.BotConfigApprovalMigrationVerifier.Repo do
  use Ecto.Repo,
    otp_app: :ret,
    adapter: Ecto.Adapters.Postgres
end

migration_path =
  Path.expand("../priv/repo/migrations/20260718120000_create_bot_config_approvals.exs", __DIR__)

Code.require_file(migration_path)

defmodule Ret.BotConfigApprovalMigrationVerifier do
  @moduledoc false

  alias Ecto.Adapters.SQL
  alias Ecto.Migrator
  alias Ret.BotConfigApprovalMigrationVerifier.Repo, as: VerificationRepo
  alias Ret.Repo.Migrations.CreateBotConfigApprovals, as: Migration

  @version 20_260_718_120_000
  @source [{@version, Migration}]

  def run do
    database =
      "ret_aud077_#{System.system_time(:millisecond)}_#{System.unique_integer([:positive])}"

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
      """
      CREATE TABLE ret0.hubs (
        hub_id bigserial PRIMARY KEY,
        user_data jsonb,
        updated_at timestamp(6) without time zone NOT NULL DEFAULT timezone('UTC', now())
      )
      """,
      []
    )

    legacy_user_data = %{
      "bots" => %{
        "enabled" => true,
        "count" => "3",
        "mobility" => "low",
        "chat_enabled" => 1,
        "prompt" => "migration-private-prompt",
        "future_contract" => %{"typed" => [1, "1", true]}
      },
      "theme" => %{"name" => "aurora"}
    }

    ignored_user_data = %{"bots" => "not-an-object", "theme" => %{"name" => "ember"}}
    absent_user_data = %{"theme" => %{"name" => "plain"}}

    object_hub_id = insert_hub!(legacy_user_data)
    ignored_hub_id = insert_hub!(ignored_user_data)
    absent_hub_id = insert_hub!(absent_user_data)

    assert!(run_migration(:up) == [@version], "bot approval migration did not run")

    assert!(
      approval_count() == 1,
      "migration created approvals for absent or non-object bot configurations"
    )

    assert_exact_backfill!(object_hub_id, legacy_user_data)
    assert!(hub_user_data(ignored_hub_id) == ignored_user_data, "non-object bots were modified")
    assert!(hub_user_data(absent_hub_id) == absent_user_data, "bot-free hub was modified")

    set_enabled!(object_hub_id, true)
    assert_down_refused!()
    assert!(approval_table_exists?(), "failed down removed the approval table")
    assert!(migration_version_present?(), "failed down removed the migration version")

    set_enabled!(object_hub_id, false)
    assert!(run_migration(:down) == [@version], "safe migration down did not run")
    refute!(approval_table_exists?(), "safe down retained the approval table")
    refute!(migration_version_present?(), "safe down retained the migration version")

    IO.puts("bot config approval migration verification passed")
  end

  defp insert_hub!(user_data) do
    %{rows: [[hub_id]]} =
      SQL.query!(
        VerificationRepo,
        "INSERT INTO ret0.hubs (user_data) VALUES ($1::jsonb) RETURNING hub_id",
        [user_data]
      )

    hub_id
  end

  defp assert_exact_backfill!(hub_id, legacy_user_data) do
    %{rows: [[state, candidate, approved, reason, quarantined_at, inserted_at, updated_at]]} =
      SQL.query!(
        VerificationRepo,
        """
        SELECT state, candidate_bots, approved_bots, last_quarantine_reason,
               last_quarantined_at, inserted_at, updated_at
        FROM ret0.bot_config_approvals
        WHERE hub_id = $1
        """,
        [hub_id]
      )

    assert!(state == "quarantined", "legacy bot config was not quarantined")
    assert!(candidate == legacy_user_data["bots"], "legacy candidate JSON changed")
    assert!(approved == nil, "legacy bot config was silently approved")
    assert!(reason == "legacy_migration", "legacy quarantine reason is incorrect")

    assert!(
      Enum.all?([quarantined_at, inserted_at, updated_at], &match?(%NaiveDateTime{}, &1)),
      "migration audit timestamps are incomplete"
    )

    expected_user_data = put_in(legacy_user_data, ["bots", "enabled"], false)

    assert!(
      hub_user_data(hub_id) == expected_user_data,
      "migration changed fields beyond bots.enabled"
    )
  end

  defp set_enabled!(hub_id, enabled) do
    SQL.query!(
      VerificationRepo,
      """
      UPDATE ret0.hubs
      SET user_data = jsonb_set(user_data, '{bots,enabled}', $2::jsonb, true),
          updated_at = timezone('UTC', now())
      WHERE hub_id = $1
      """,
      [hub_id, enabled]
    )
  end

  defp hub_user_data(hub_id) do
    %{rows: [[user_data]]} =
      SQL.query!(VerificationRepo, "SELECT user_data FROM ret0.hubs WHERE hub_id = $1", [hub_id])

    user_data
  end

  defp approval_count do
    %{rows: [[count]]} =
      SQL.query!(VerificationRepo, "SELECT count(*) FROM ret0.bot_config_approvals", [])

    count
  end

  defp assert_down_refused! do
    failure =
      try do
        run_migration(:down)
        :not_refused
      rescue
        error -> {:raised, Exception.message(error)}
      catch
        kind, reason -> {kind, inspect(reason)}
      end

    message =
      case failure do
        {_kind, value} -> value
        :not_refused -> ""
      end

    assert!(
      String.contains?(
        message,
        "refusing to drop bot_config_approvals while a stored bot configuration is not explicitly disabled"
      ),
      "migration down did not refuse an active stored bot configuration"
    )
  end

  defp run_migration(direction) do
    Migrator.run(VerificationRepo, @source, direction,
      step: 1,
      prefix: "ret0",
      log: false
    )
  end

  defp approval_table_exists? do
    %{rows: [[exists]]} =
      SQL.query!(
        VerificationRepo,
        "SELECT to_regclass('ret0.bot_config_approvals') IS NOT NULL",
        []
      )

    exists
  end

  defp migration_version_present? do
    %{rows: [[present]]} =
      SQL.query!(
        VerificationRepo,
        "SELECT EXISTS (SELECT 1 FROM ret0.schema_migrations WHERE version = $1)",
        [@version]
      )

    present
  end

  defp assert!(true, _message), do: :ok
  defp assert!(false, message), do: Mix.raise(message)
  defp refute!(false, _message), do: :ok
  defp refute!(true, message), do: Mix.raise(message)
end

Ret.BotConfigApprovalMigrationVerifier.run()
