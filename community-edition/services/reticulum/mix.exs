defmodule Ret.Mixfile do
  use Mix.Project

  def project do
    [
      aliases: aliases(),
      app: :ret,
      compilers: [:phoenix] ++ Mix.compilers(),
      deps: deps(),
      elixir: "~> 1.18.4",
      elixirc_paths: elixirc_paths(Mix.env()),
      # cowlib 2.18.0 is the latest release. Revisit these two upstream advisories
      # as soon as a patched cowlib version is published; every new advisory
      # remains fatal in CI.
      hex: [ignore_advisories: ["CVE-2026-43966", "CVE-2026-43969"]],
      releases: releases(),
      start_permanent: Mix.env() == :prod,
      version: System.get_env("RELEASE_VERSION", "1.0.0")
    ]
  end

  # Configuration for the OTP application.
  #
  # Type `mix help compile.app` for more information.
  def application do
    [
      mod: {Ret.Application, []},
      extra_applications: [:runtime_tools, :canada]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Specifies your project dependencies.
  #
  # Type `mix help deps` for examples and options.
  defp deps do
    [
      {:ecto_boot_migration, "~> 0.2.0"},
      {:phoenix, "~> 1.6.17"},
      {:phoenix_pubsub, "~> 2.1"},
      {:phoenix_ecto, "~> 4.7.0"},
      {:plug, "~> 1.16.6"},
      {:ecto, "~> 3.14.1"},
      {:ecto_sql, "~> 3.14.0"},
      {:absinthe, "~> 1.10.2"},
      {:dataloader, "~> 1.0.0"},
      {:absinthe_plug, "~> 1.5.10"},
      {:absinthe_phoenix, "~> 2.0.0"},
      {:postgrex, "~> 0.22.3"},
      {:phoenix_html, "~> 3.0.4"},
      {:phoenix_live_reload, "~> 1.2", only: :dev},
      {:gettext, "~> 0.26.2"},
      {:cowboy, "~> 2.15.0"},
      {:plug_cowboy, "~> 2.8.1"},
      {:peerage, "~> 1.0"},
      {:httpoison, "~> 3.0.0", override: true},
      {:hackney, "~> 4.5.2", override: true},
      {:poison, "~> 3.1"},
      {:ecto_autoslug_field, "~> 2.0"},
      {:cors_plug, "~> 2.0"},
      {:quantum, "~> 2.2.7"},
      {:credo, "~> 1.1", only: [:dev, :test], runtime: false},
      {:mox, "~> 1.0.1", only: [:dev, :test]},
      {:plug_attack, "~> 0.4"},
      {:ecto_enum, "~> 1.3"},
      {:cachex, "~> 3.2"},
      {:retry, "~> 0.15.0"},
      {:open_graph, "~> 0.0.6"},
      {:secure_random, "~> 0.5"},
      {:bamboo, "~> 2.2.0"},
      {:bamboo_phoenix, "~> 1.0.0"},
      {:bamboo_smtp, "~> 4.2.2"},
      {:guardian, "~> 2.4.0"},
      {:guardian_phoenix, "~> 2.0"},
      {:canary, "~> 1.1.1"},
      {:temp, "~> 0.4"},
      {:timex, "~> 3.7.13"},
      # 0.2.2 breaks FCM without an auth token, not sure what's up with that.
      {:web_push_encryption, "~> 0.3.1"},
      {:sentry, "~> 13.3.0"},
      {:toml, "~> 0.5"},
      {:scrivener_ecto, "~> 2.0"},
      {:ua_parser, "~> 1.5"},
      {:download, git: "https://github.com/gfodor/download.git", branch: "reticulum/master"},
      {:reverse_proxy_plug, "~> 3.0.4"},
      {:inet_cidr, "~> 1.0"},
      {:dns, "~> 2.2.0"},
      {:oauther, "~> 1.1"},
      {:jason, "~> 1.1"},
      {:ex_rated, "~> 2.1.0"},
      {:mix_test_watch, "~> 1.0", only: :dev, runtime: false},
      {:ex_json_schema, "~> 0.7.3"},
      {:observer_cli, "~> 1.5"},
      {:stream_data, "~> 1.4", only: [:dev, :test]}
    ]
  end

  # Aliases are shortcuts or tasks specific to the current project.
  # For example, to create, migrate and run the seeds file at once:
  #
  #     $ mix ecto.setup
  #
  # See the documentation for `Mix` for more info on aliases.
  defp aliases do
    [
      "ecto.setup": ["ecto.create", "ecto.migrate", "run priv/repo/seeds.exs"],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      test: ["ecto.create --quiet", "ecto.migrate", "test"]
    ]
  end

  defp releases,
    do: [
      ret: [
        config_providers: [
          {Toml.Provider, [path: {:system, "RELEASE_CONFIG_DIR", "/config.toml"}]}
        ]
      ]
    ]
end
