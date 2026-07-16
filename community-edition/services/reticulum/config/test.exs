import Config

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :ret, RetWeb.Endpoint,
  http: [port: 4001],
  allowed_origins: "*",
  secret_key_base: String.duplicate("test-only-", 8),
  cors_proxy_url: [scheme: "https", host: "hubs-proxy.local", port: 4000],
  server: false

# Print only warnings and errors during test
config :logger, level: :warning

config :ret, Ret.AppConfig, caching?: false

config :ret, Ret.Repo,
  adapter: Ecto.Adapters.Postgres,
  database: "ret_test",
  template: "template0",
  pool_size: 10,
  pool: Ecto.Adapters.SQL.Sandbox

config :ret, Ret.SessionLockRepo,
  adapter: Ecto.Adapters.Postgres,
  database: "ret_test",
  template: "template0",
  pool_size: 10,
  pool: Ecto.Adapters.SQL.Sandbox

config :ret, Ret.Locking, lock_timeout_ms: 1000 * 60 * 15

config :ret, Ret.Guardian,
  issuer: "ret",
  secret_key: String.duplicate("test-only-", 8)

config :ret, Ret.Storage,
  host: "https://hubs.local:4000",
  storage_path: "storage/test",
  ttl: 60 * 60 * 24

config :sentry,
  environment_name: :test,
  json_library: Poison,
  tags: %{
    env: "test"
  }

config :ret, Ret.Repo.Migrations.AdminSchemaInit, postgrest_password: "password"
config :ret, Ret.Locking, lock_timeout_ms: 1000 * 60 * 15
config :ret, Ret.Account, admin_email: "admin@mozilla.com"

config :ret, RetWeb.HubChannel, enable_terminate_actions: false

config :ret, Ret.PermsToken,
  perms_key: File.read!(Path.expand("../container/dev-perms.pem", __DIR__))

config :ret, Ret.MediaResolver,
  giphy_api_key: nil,
  deviantart_client_id: nil,
  deviantart_client_secret: nil,
  imgur_mashape_api_key: nil,
  imgur_client_id: nil,
  youtube_api_key: nil,
  sketchfab_api_key: nil,
  ytdl_host: nil,
  photomnemonic_endpoint: "https://uvnsm9nzhe.execute-api.us-west-1.amazonaws.com/public"

config :ret, :ex_unit_configuration, exclude: [dev_only: true]

config :ret, RetWeb.Plugs.RateLimit, throttle?: false
