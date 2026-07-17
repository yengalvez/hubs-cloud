defmodule Ret.BotRunnerLeaseTest do
  use ExUnit.Case, async: false

  alias Ret.BotRunnerLease
  alias RetWeb.HubChannel

  test "a paused older registration remains authoritative before Presence publication" do
    hub_sid = "lease-probe-#{System.unique_integer([:positive])}"
    parent = self()

    older_pid =
      spawn(fn ->
        {:ok, lease} = BotRunnerLease.register(hub_sid)
        send(parent, {:older_registered_before_presence, self(), lease})

        receive do
          :publish_presence ->
            send(parent, {:older_presence_published, lease})

            receive do
              :stop -> :ok
            end
        end
      end)

    assert_receive {:older_registered_before_presence, ^older_pid, older_lease}
    {:ok, newer_lease} = BotRunnerLease.register(hub_sid)

    assert older_lease.authoritative
    refute newer_lease.authoritative
    assert newer_lease.authority_epoch == older_lease.authority_epoch

    assert %{
             authority_lease_id: authority_lease_id,
             authority_epoch: authority_epoch,
             leases: leases
           } = BotRunnerLease.snapshot(hub_sid)

    assert authority_lease_id == older_lease.lease_id
    assert authority_epoch == older_lease.authority_epoch
    assert Enum.map(leases, & &1.lease_id) == [older_lease.lease_id, newer_lease.lease_id]

    refute BotRunnerLease.authorized?(
             hub_sid,
             newer_lease.lease_id,
             newer_lease.authority_epoch
           )

    # This mirrors the exact async gap: the newer runner has published
    # Presence while the older registered channel is deliberately paused.
    newer_only_presence = %{
      "newer" => %{
        metas: [
          %{
            context: %{"bot_runner" => true},
            bot_runner_lease_id: newer_lease.lease_id,
            bot_runner_join_order: newer_lease.join_order,
            bot_runner_authority_epoch: newer_lease.authority_epoch,
            bot_runner_authoritative: false
          }
        ]
      }
    }

    assert HubChannel.authoritative_bot_runner_lease_id(newer_only_presence) == nil

    send(older_pid, :publish_presence)
    assert_receive {:older_presence_published, ^older_lease}
    send(older_pid, :stop)

    promoted = wait_for_authority(hub_sid, newer_lease.lease_id)
    assert promoted.authority_epoch > older_lease.authority_epoch

    refute BotRunnerLease.authorized?(
             hub_sid,
             newer_lease.lease_id,
             older_lease.authority_epoch
           )

    assert BotRunnerLease.authorized?(
             hub_sid,
             newer_lease.lease_id,
             promoted.authority_epoch
           )

    :ok = BotRunnerLease.unregister(hub_sid, newer_lease.lease_id)
  end

  test "simultaneous registrations yield one process-bound authority and unique tokens" do
    hub_sid = "lease-race-#{System.unique_integer([:positive])}"
    parent = self()

    owners =
      for index <- 1..12 do
        spawn(fn ->
          receive do
            :register ->
              {:ok, lease} = BotRunnerLease.register(hub_sid)
              send(parent, {:registered, index, self(), lease})

              receive do
                {:check_authority, epoch} ->
                  send(
                    parent,
                    {:authority_result, index, self(), lease,
                     BotRunnerLease.authorized?(hub_sid, lease.lease_id, epoch)}
                  )

                  receive do
                    :stop -> :ok
                  end
              end
          end
        end)
      end

    Enum.each(owners, &send(&1, :register))

    registrations =
      for _ <- owners do
        assert_receive {:registered, index, owner_pid, lease}
        {index, owner_pid, lease}
      end

    tokens = Enum.map(registrations, fn {_index, _pid, lease} -> lease.lease_id end)
    assert MapSet.size(MapSet.new(tokens)) == length(tokens)

    snapshot = BotRunnerLease.snapshot(hub_sid)
    assert length(snapshot.leases) == length(owners)

    Enum.each(registrations, fn {_index, owner_pid, _lease} ->
      send(owner_pid, {:check_authority, snapshot.authority_epoch})
    end)

    authority_results =
      for _ <- registrations do
        assert_receive {:authority_result, index, owner_pid, lease, authorized}, 1_000
        {index, owner_pid, lease, authorized}
      end

    authorized =
      for {index, owner_pid, lease, true} <- authority_results,
          do: {index, owner_pid, lease}

    assert [{_index, leader_pid, leader_lease}] = authorized

    refute BotRunnerLease.authorized?(hub_sid, leader_lease.lease_id, snapshot.authority_epoch)

    send(leader_pid, :stop)
    successor = wait_for_different_authority(hub_sid, leader_lease.lease_id)
    assert successor.authority_epoch > snapshot.authority_epoch

    Enum.each(owners -- [leader_pid], &send(&1, :stop))
  end

  test "a lease crash atomically replaces Lease, Endpoint and Presence" do
    assert_atomic_runtime_restart(BotRunnerLease)
  end

  test "an Endpoint crash atomically replaces Lease, Endpoint and Presence" do
    assert_atomic_runtime_restart(RetWeb.Endpoint)
  end

  test "a Presence crash atomically replaces Lease, Endpoint and Presence" do
    assert_atomic_runtime_restart(RetWeb.Presence)
  end

  defp wait_for_authority(hub_sid, lease_id, attempts \\ 100)

  defp wait_for_authority(_hub_sid, _lease_id, 0), do: flunk("lease was not promoted")

  defp wait_for_authority(hub_sid, lease_id, attempts) do
    case BotRunnerLease.snapshot(hub_sid) do
      %{authority_lease_id: ^lease_id} = snapshot ->
        snapshot

      _ ->
        Process.sleep(10)
        wait_for_authority(hub_sid, lease_id, attempts - 1)
    end
  end

  defp wait_for_different_authority(hub_sid, previous_lease_id, attempts \\ 100)

  defp wait_for_different_authority(_hub_sid, _previous_lease_id, 0),
    do: flunk("successor was not promoted")

  defp wait_for_different_authority(hub_sid, previous_lease_id, attempts) do
    case BotRunnerLease.snapshot(hub_sid) do
      %{authority_lease_id: lease_id} = snapshot when lease_id != previous_lease_id ->
        snapshot

      _ ->
        Process.sleep(10)
        wait_for_different_authority(hub_sid, previous_lease_id, attempts - 1)
    end
  end

  defp assert_atomic_runtime_restart(crashed_name) do
    hub_sid = "runtime-supervisor-recovery-#{System.unique_integer([:positive])}"
    presence_key = "normal-presence-#{System.unique_integer([:positive])}"
    repo_pid = Process.whereis(Ret.Repo)
    previous_pids = runtime_pids()

    assert is_pid(repo_pid)
    assert Enum.all?(Map.values(previous_pids), &is_pid/1)

    tracker_pid =
      spawn(fn ->
        Process.flag(:trap_exit, true)

        receive do
          :stop -> :ok
        end
      end)

    {:ok, old_lease} = BotRunnerLease.register(hub_sid)
    assert old_lease.authoritative
    assert BotRunnerLease.authorized?(hub_sid, old_lease.lease_id, old_lease.authority_epoch)

    {:ok, _presence_ref} =
      RetWeb.Presence.track(tracker_pid, "ret", presence_key, %{hub_id: hub_sid})

    assert wait_for_presence(presence_key, true)

    monitors =
      Map.new(previous_pids, fn {name, pid} ->
        {name, Process.monitor(pid)}
      end)

    crashed_child_pid = runtime_child_pid(crashed_name)
    assert is_pid(crashed_child_pid)
    GenServer.stop(crashed_child_pid, :shutdown)

    Enum.each(previous_pids, fn {name, pid} ->
      monitor_ref = Map.fetch!(monitors, name)
      assert_receive {:DOWN, ^monitor_ref, :process, ^pid, _reason}, 5_000
    end)

    replacement_pids =
      Map.new(previous_pids, fn {name, previous_pid} ->
        {name, wait_for_replacement(name, previous_pid)}
      end)

    assert Enum.all?(replacement_pids, fn {name, pid} ->
             is_pid(pid) and pid != Map.fetch!(previous_pids, name) and Process.alive?(pid)
           end)

    assert Process.whereis(Ret.Repo) == repo_pid
    refute BotRunnerLease.authorized?(hub_sid, old_lease.lease_id, old_lease.authority_epoch)
    assert BotRunnerLease.snapshot(hub_sid) == nil

    # Presence cannot reconstruct a surviving channel's state. Only a fresh
    # client/channel track (the reconnect boundary) republishes the session.
    refute wait_for_presence(presence_key, false)

    {:ok, _presence_ref} =
      RetWeb.Presence.track(tracker_pid, "ret", presence_key, %{hub_id: hub_sid})

    assert wait_for_presence(presence_key, true)

    {:ok, recovered_lease} = BotRunnerLease.register(hub_sid)
    assert recovered_lease.authoritative
    assert recovered_lease.authority_epoch > old_lease.authority_epoch

    assert BotRunnerLease.authorized?(
             hub_sid,
             recovered_lease.lease_id,
             recovered_lease.authority_epoch
           )

    :ok = BotRunnerLease.unregister(hub_sid, recovered_lease.lease_id)
    :ok = RetWeb.Presence.untrack(tracker_pid, "ret", presence_key)
    send(tracker_pid, :stop)
  end

  defp runtime_pids do
    %{
      BotRunnerLease => Process.whereis(BotRunnerLease),
      RetWeb.Endpoint => Process.whereis(RetWeb.Endpoint),
      RetWeb.Presence => Process.whereis(RetWeb.Presence)
    }
  end

  defp runtime_child_pid(name) do
    Ret.BotRuntimeSupervisor
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {^name, pid, _type, _modules} -> pid
      _other -> nil
    end)
  end

  defp wait_for_presence(key, expected, attempts \\ 100)

  defp wait_for_presence(_key, expected, 0), do: !expected

  defp wait_for_presence(key, expected, attempts) do
    present? = Map.has_key?(RetWeb.Presence.list("ret"), key)

    if present? == expected do
      present?
    else
      Process.sleep(10)
      wait_for_presence(key, expected, attempts - 1)
    end
  end

  defp wait_for_replacement(name, previous_pid, attempts \\ 200)

  defp wait_for_replacement(_name, _previous_pid, 0),
    do: flunk("supervised process was not replaced")

  defp wait_for_replacement(name, previous_pid, attempts) do
    case Process.whereis(name) do
      pid when is_pid(pid) and pid != previous_pid ->
        pid

      _ ->
        Process.sleep(10)
        wait_for_replacement(name, previous_pid, attempts - 1)
    end
  end
end
