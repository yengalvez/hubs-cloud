defmodule Ret.BotRuntimeSupervisor do
  @moduledoc """
  Keeps process-local bot authority and its externally observable state atomic.

  None of these processes can be reconstructed independently. A lease restart
  cannot recover its process-bound channel owners, while an Endpoint or
  Presence restart cannot republish the state held by the surviving peers.
  Restarting the complete group closes every old channel and removes every
  stale authority advertisement before clients reconnect and register again.
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
