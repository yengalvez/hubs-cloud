defmodule Ret.BotChatPresence do
  @moduledoc """
  Process-bound, server-only proof that an authenticated account has entered a hub.

  The tracker intentionally keeps account identifiers out of Phoenix Presence
  metadata. A channel process is removed immediately when it terminates, and
  callers also check process liveness so an unprocessed `:DOWN` cannot authorize
  a chat action.
  """

  use GenServer

  def start_link(_options), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def track(channel_pid, hub_sid, account_id, capability)
      when is_pid(channel_pid) and is_binary(hub_sid) and not is_nil(account_id) and
             is_binary(capability) do
    GenServer.call(
      __MODULE__,
      {:track, channel_pid, hub_sid, to_string(account_id), capability}
    )
  end

  def present?(hub_sid, account_id, capability)
      when is_binary(hub_sid) and not is_nil(account_id) and is_binary(capability) and
             byte_size(capability) <= 64 do
    GenServer.call(__MODULE__, {:present?, hub_sid, to_string(account_id), capability})
  end

  def present?(_hub_sid, _account_id, _capability), do: false

  def untrack(channel_pid) when is_pid(channel_pid) do
    GenServer.call(__MODULE__, {:untrack, channel_pid})
  end

  @impl true
  def init(_initial), do: {:ok, %{by_pid: %{}, by_key: %{}}}

  @impl true
  def handle_call({:track, channel_pid, hub_sid, account_id, capability}, _from, state) do
    state = remove_entry(state, channel_pid)
    key = {hub_sid, account_id, capability}

    state =
      case Map.get(state.by_key, key) do
        existing_pid when is_pid(existing_pid) -> remove_entry(state, existing_pid)
        _missing -> state
      end

    ref = Process.monitor(channel_pid)

    entry = %{
      hub_sid: hub_sid,
      account_id: account_id,
      capability: capability,
      key: key,
      monitor_ref: ref
    }

    state = %{
      state
      | by_pid: Map.put(state.by_pid, channel_pid, entry),
        by_key: Map.put(state.by_key, key, channel_pid)
    }

    {:reply, :ok, state}
  end

  def handle_call({:present?, hub_sid, account_id, capability}, _from, state) do
    key = {hub_sid, account_id, capability}

    {present, state} =
      case Map.get(state.by_key, key) do
        channel_pid when is_pid(channel_pid) ->
          case Map.get(state.by_pid, channel_pid) do
            %{key: ^key} ->
              if Process.alive?(channel_pid) do
                {true, state}
              else
                {false, remove_entry(state, channel_pid)}
              end

            _entry ->
              {false, %{state | by_key: Map.delete(state.by_key, key)}}
          end

        _missing ->
          {false, state}
      end

    {:reply, present, state}
  end

  def handle_call({:untrack, channel_pid}, _from, state) do
    {:reply, :ok, remove_entry(state, channel_pid)}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, channel_pid, _reason}, state) do
    state =
      case Map.get(state.by_pid, channel_pid) do
        %{monitor_ref: ^ref} -> remove_entry(state, channel_pid)
        _entry -> state
      end

    {:noreply, state}
  end

  defp remove_entry(state, channel_pid) do
    case Map.pop(state.by_pid, channel_pid) do
      {nil, _by_pid} ->
        state

      {%{monitor_ref: ref, key: key}, by_pid} ->
        Process.demonitor(ref, [:flush])

        by_key =
          if Map.get(state.by_key, key) == channel_pid do
            Map.delete(state.by_key, key)
          else
            state.by_key
          end

        %{state | by_pid: by_pid, by_key: by_key}
    end
  end
end
