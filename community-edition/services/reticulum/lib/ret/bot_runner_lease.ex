defmodule Ret.BotRunnerLease do
  @moduledoc """
  Serializes authenticated bot-runner authority independently of Presence.

  Presence is eventually consistent and is therefore only an observable mirror
  of this state. A channel must hold the exact, process-bound lease token and
  current authority epoch before Reticulum accepts any bot mutation.
  """

  use GenServer

  @name __MODULE__

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: @name)
  end

  def register(hub_sid) when is_binary(hub_sid) and byte_size(hub_sid) > 0 do
    GenServer.call(@name, {:register, hub_sid})
  end

  def authorized?(hub_sid, lease_id, authority_epoch)
      when is_binary(hub_sid) and is_binary(lease_id) and is_integer(authority_epoch) and
             authority_epoch > 0 do
    GenServer.call(@name, {:authorized?, hub_sid, lease_id, authority_epoch})
  end

  def authorized?(_hub_sid, _lease_id, _authority_epoch), do: false

  def unregister(hub_sid, lease_id) when is_binary(hub_sid) and is_binary(lease_id) do
    GenServer.call(@name, {:unregister, hub_sid, lease_id})
  end

  @doc false
  def snapshot(hub_sid) when is_binary(hub_sid) do
    GenServer.call(@name, {:snapshot, hub_sid})
  end

  @impl true
  def init(:ok) do
    {:ok,
     %{
       hubs: %{},
       monitor_refs: %{}
     }}
  end

  @impl true
  def handle_call({:register, hub_sid}, {owner_pid, _tag}, state) do
    join_order = next_monotonic_id()
    lease_id = issue_unique_token(join_order)
    monitor_ref = Process.monitor(owner_pid)

    lease = %{
      lease_id: lease_id,
      join_order: join_order,
      owner_pid: owner_pid,
      monitor_ref: monitor_ref
    }

    hub =
      case Map.get(state.hubs, hub_sid) do
        nil ->
          epoch = next_monotonic_id()

          %{authority_lease_id: lease_id, authority_epoch: epoch, leases: %{lease_id => lease}}

        existing ->
          %{existing | leases: Map.put(existing.leases, lease_id, lease)}
      end

    state = %{
      state
      | hubs: Map.put(state.hubs, hub_sid, hub),
        monitor_refs: Map.put(state.monitor_refs, monitor_ref, {hub_sid, lease_id})
    }

    notify_hub(state, hub_sid)

    {:reply,
     {:ok,
      %{
        lease_id: lease_id,
        join_order: join_order,
        authority_epoch: hub.authority_epoch,
        authoritative: hub.authority_lease_id == lease_id
      }}, state}
  end

  def handle_call(
        {:authorized?, hub_sid, lease_id, authority_epoch},
        {owner_pid, _tag},
        state
      ) do
    authorized =
      case Map.get(state.hubs, hub_sid) do
        %{
          authority_lease_id: ^lease_id,
          authority_epoch: ^authority_epoch,
          leases: %{^lease_id => %{owner_pid: ^owner_pid}}
        } ->
          true

        _ ->
          false
      end

    {:reply, authorized, state}
  end

  def handle_call({:unregister, hub_sid, lease_id}, {owner_pid, _tag}, state) do
    case get_in(state, [:hubs, hub_sid, :leases, lease_id]) do
      %{owner_pid: ^owner_pid} = lease ->
        Process.demonitor(lease.monitor_ref, [:flush])
        state = remove_lease(state, hub_sid, lease_id, lease.monitor_ref)
        {:reply, :ok, state}

      nil ->
        {:reply, :ok, state}

      _other_owner ->
        {:reply, {:error, :not_owner}, state}
    end
  end

  def handle_call({:snapshot, hub_sid}, _from, state) do
    snapshot =
      case Map.get(state.hubs, hub_sid) do
        nil ->
          nil

        hub ->
          %{
            authority_lease_id: hub.authority_lease_id,
            authority_epoch: hub.authority_epoch,
            leases:
              hub.leases
              |> Map.values()
              |> Enum.map(&Map.take(&1, [:lease_id, :join_order, :owner_pid]))
              |> Enum.sort_by(& &1.join_order)
          }
      end

    {:reply, snapshot, state}
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, _owner_pid, _reason}, state) do
    case Map.get(state.monitor_refs, monitor_ref) do
      {hub_sid, lease_id} ->
        {:noreply, remove_lease(state, hub_sid, lease_id, monitor_ref)}

      nil ->
        {:noreply, state}
    end
  end

  defp remove_lease(state, hub_sid, lease_id, monitor_ref) do
    hub = Map.fetch!(state.hubs, hub_sid)
    remaining_leases = Map.delete(hub.leases, lease_id)
    monitor_refs = Map.delete(state.monitor_refs, monitor_ref)

    cond do
      map_size(remaining_leases) == 0 ->
        %{state | hubs: Map.delete(state.hubs, hub_sid), monitor_refs: monitor_refs}

      hub.authority_lease_id == lease_id ->
        successor = remaining_leases |> Map.values() |> Enum.min_by(& &1.join_order)
        authority_epoch = next_monotonic_id()

        promoted_hub = %{
          hub
          | leases: remaining_leases,
            authority_lease_id: successor.lease_id,
            authority_epoch: authority_epoch
        }

        state = %{
          state
          | hubs: Map.put(state.hubs, hub_sid, promoted_hub),
            monitor_refs: monitor_refs
        }

        notify_hub(state, hub_sid)
        state

      true ->
        state = %{
          state
          | hubs: Map.put(state.hubs, hub_sid, %{hub | leases: remaining_leases}),
            monitor_refs: monitor_refs
        }

        notify_hub(state, hub_sid)
        state
    end
  end

  defp notify_hub(state, hub_sid) do
    case Map.get(state.hubs, hub_sid) do
      nil ->
        :ok

      hub ->
        Enum.each(hub.leases, fn {lease_id, lease} ->
          send(
            lease.owner_pid,
            {:bot_runner_lease_authority, lease_id, hub.authority_epoch,
             lease_id == hub.authority_lease_id}
          )
        end)
    end
  end

  defp issue_unique_token(join_order) do
    # The cryptographic component is unguessable while the VM-monotonic suffix
    # makes reuse impossible even if a UUID collision were to occur.
    "#{SecureRandom.uuid()}:#{join_order}"
  end

  defp next_monotonic_id do
    System.unique_integer([:monotonic, :positive])
  end
end
