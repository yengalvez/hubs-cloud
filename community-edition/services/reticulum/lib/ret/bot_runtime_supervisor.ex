defmodule Ret.BotRuntimeSupervisor do
  @moduledoc """
  Keeps local lease-to-channel bindings and their observable state atomic.

  PostgreSQL retains the shared fencing authority, but none of these local
  processes can reconstruct a surviving channel owner independently. A lease
  coordinator restart cannot recover its process binding, while an Endpoint or
  Presence restart cannot republish the state held by surviving peers.
  Restarting the complete group closes every old channel and removes stale
  authority advertisements before clients reconnect and register again.
  """

  use Supervisor

  def start_link(_opts) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    Supervisor.init(
      [
        Ret.BotRunnerLease,
        RetWeb.Endpoint,
        RetWeb.Presence
      ],
      strategy: :one_for_all
    )
  end
end
