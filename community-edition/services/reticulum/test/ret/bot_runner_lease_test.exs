defmodule Ret.BotRunnerLeaseTest.ControlledStore do
  @behaviour Ret.BotRunnerLease.Store

  def acquire(
        agent,
        hub_sid,
        lease_id,
        holder_id,
        session_id,
        _generation_claims,
        _ttl_seconds
      ) do
    Agent.get_and_update(agent, fn state ->
      cond do
        state.mode == :database_unavailable ->
          {{:error, :database_unavailable}, state}

        state.active ->
          {{:error, :unavailable}, state}

        true ->
          epoch = state.epoch + 1

          active = %{
            epoch: epoch,
            holder_instance_id: holder_id,
            hub_sid: hub_sid,
            lease_id: lease_id,
            session_id: session_id
          }

          {{:ok, %{epoch: epoch}}, %{state | active: active, epoch: epoch}}
      end
    end)
  end

  def renew(agent, hub_sid, lease_id, holder_id, session_id, epoch, _ttl_seconds) do
    state = Agent.get(agent, & &1)
    gate = state.renew_gate

    cond do
      state.mode == :database_unavailable ->
        {:error, :database_unavailable}

      gate ->
        {test_pid, reference} = gate
        send(test_pid, {:renew_started, reference, self()})

        receive do
          {:finish_stale_renewal, ^reference} -> :ok
        end

        {:ok, %{epoch: epoch}}

      true ->
        if exact_active?(agent, hub_sid, lease_id, holder_id, session_id, epoch),
          do: {:ok, %{epoch: epoch}},
          else: {:error, :lost}
    end
  end

  def with_authority(agent, hub_sid, lease_id, holder_id, session_id, epoch, fun) do
    if exact_active?(agent, hub_sid, lease_id, holder_id, session_id, epoch),
      do: {:ok, fun.()},
      else: {:error, :not_authoritative}
  end

  def with_current_authority(agent, hub_sid, fun) do
    case Agent.get(agent, & &1.active) do
      %{hub_sid: ^hub_sid} = active -> {:ok, fun.(active)}
      _ -> {:error, :not_authoritative}
    end
  end

  def release(agent, hub_sid, lease_id, holder_id, session_id, epoch) do
    Agent.get_and_update(agent, fn state ->
      if exact?(state.active, hub_sid, lease_id, holder_id, session_id, epoch) do
        fenced_epoch = state.epoch + 1
        {{:ok, fenced_epoch}, %{state | active: nil, epoch: fenced_epoch}}
      else
        {{:ok, nil}, state}
      end
    end)
  end

  def revoke(agent, _hub_sid) do
    Agent.get_and_update(agent, fn state ->
      fenced_epoch = state.epoch + 1
      {{:ok, fenced_epoch}, %{state | active: nil, epoch: fenced_epoch}}
    end)
  end

  def current(agent, hub_sid) do
    case Agent.get(agent, & &1.active) do
      %{hub_sid: ^hub_sid} = active -> {:ok, active}
      _ -> {:ok, nil}
    end
  end

  defp exact_active?(agent, hub_sid, lease_id, holder_id, session_id, epoch) do
    agent
    |> Agent.get(& &1.active)
    |> exact?(hub_sid, lease_id, holder_id, session_id, epoch)
  end

  defp exact?(active, hub_sid, lease_id, holder_id, session_id, epoch) do
    active && active.hub_sid == hub_sid && active.lease_id == lease_id &&
      active.holder_instance_id == holder_id && active.session_id == session_id &&
      active.epoch == epoch
  end
end

defmodule Ret.BotRunnerLeaseTest do
  use Ret.DataCase

  import Ret.TestHelpers

  alias Ecto.Adapters.SQL
  alias Ret.BotRunnerLease
  alias Ret.BotRunnerLease.PostgresStore

  setup [:create_account, :create_owned_file, :create_scene, :create_hub]

  test "revocation consumes a global epoch and rejects the old exact fence", %{hub: hub} do
    session_id = Ecto.UUID.generate()

    {:ok, lease} =
      BotRunnerLease.register_for_session(hub.hub_sid, session_id, generation_claims(hub.hub_sid))

    assert {:ok, :executed} =
             BotRunnerLease.with_authority(
               hub.hub_sid,
               lease.lease_id,
               lease.authority_epoch,
               fn -> :executed end
             )

    assert :ok = BotRunnerLease.revoke(hub.hub_sid)

    assert_receive {:bot_runner_lease_authority, lease_id, revoked_epoch, false}
    assert lease_id == lease.lease_id
    assert revoked_epoch > lease.authority_epoch

    assert {:error, :not_authoritative} =
             BotRunnerLease.with_authority(
               hub.hub_sid,
               lease.lease_id,
               lease.authority_epoch,
               fn -> flunk("a revoked fence executed") end
             )

    assert BotRunnerLease.snapshot(hub.hub_sid) == nil
  end

  test "two coordinators elect one authority and expiry takeover fences stale renew and release",
       %{
         hub: hub
       } do
    first_holder = Ecto.UUID.generate()
    second_holder = Ecto.UUID.generate()
    first = start_coordinator(holder_id: first_holder)
    second = start_coordinator(holder_id: second_holder)
    first_session = Ecto.UUID.generate()
    second_session = Ecto.UUID.generate()

    {:ok, first_lease} =
      BotRunnerLease.register_for_session(
        first,
        hub.hub_sid,
        first_session,
        generation_claims(hub.hub_sid)
      )

    assert {:error, :lease_unavailable} =
             BotRunnerLease.register_for_session(
               second,
               hub.hub_sid,
               second_session,
               generation_claims(hub.hub_sid)
             )

    force_expiry!(hub.hub_id)

    {:ok, second_lease} =
      BotRunnerLease.register_for_session(
        second,
        hub.hub_sid,
        second_session,
        generation_claims(hub.hub_sid)
      )

    assert second_lease.authority_epoch > first_lease.authority_epoch

    assert {:error, :not_authoritative} =
             BotRunnerLease.with_authority(
               first,
               hub.hub_sid,
               first_lease.lease_id,
               first_lease.authority_epoch,
               fn -> flunk("expired authority executed after takeover") end
             )

    assert {:error, :lost} =
             PostgresStore.renew(
               Repo,
               hub.hub_sid,
               first_lease.lease_id,
               first_holder,
               first_session,
               first_lease.authority_epoch,
               15
             )

    assert {:ok, nil} =
             PostgresStore.release(
               Repo,
               hub.hub_sid,
               first_lease.lease_id,
               first_holder,
               first_session,
               first_lease.authority_epoch
             )

    assert {:ok, :current} =
             BotRunnerLease.with_authority(
               second,
               hub.hub_sid,
               second_lease.lease_id,
               second_lease.authority_epoch,
               fn -> :current end
             )

    assert :ok = BotRunnerLease.unregister(second, hub.hub_sid, second_lease.lease_id)
  end

  test "twelve simultaneous replica coordinators yield exactly one lease", %{hub: hub} do
    parent = self()

    coordinators =
      for _index <- 1..12 do
        start_coordinator(holder_id: Ecto.UUID.generate())
      end

    tasks =
      Enum.map(coordinators, fn coordinator ->
        Task.async(fn ->
          receive do
            :acquire -> :ok
          end

          result =
            BotRunnerLease.register_for_session(
              coordinator,
              hub.hub_sid,
              Ecto.UUID.generate(),
              generation_claims(hub.hub_sid)
            )

          send(parent, {:acquisition_result, self(), coordinator, result})

          receive do
            :release ->
              case result do
                {:ok, lease} ->
                  BotRunnerLease.unregister(coordinator, hub.hub_sid, lease.lease_id)

                _ ->
                  :ok
              end
          end
        end)
      end)

    Enum.each(tasks, &send(&1.pid, :acquire))

    results =
      for _task <- tasks do
        assert_receive {:acquisition_result, task_pid, coordinator, result}, 2_000
        {task_pid, coordinator, result}
      end

    assert 1 ==
             Enum.count(results, fn {_pid, _coordinator, result} -> match?({:ok, _}, result) end)

    assert 11 ==
             Enum.count(results, fn {_pid, _coordinator, result} ->
               result == {:error, :lease_unavailable}
             end)

    Enum.each(tasks, &send(&1.pid, :release))
    Enum.each(tasks, &Task.await(&1, 2_000))
  end

  test "database unavailability fails registration closed without a local fallback", %{hub: hub} do
    {:ok, agent} =
      Agent.start_link(fn ->
        %{active: nil, epoch: 0, mode: :database_unavailable, renew_gate: nil}
      end)

    coordinator =
      start_coordinator(
        holder_id: Ecto.UUID.generate(),
        repo: agent,
        store: Ret.BotRunnerLeaseTest.ControlledStore
      )

    assert {:error, :lease_database_unavailable} =
             BotRunnerLease.register_for_session(
               coordinator,
               hub.hub_sid,
               Ecto.UUID.generate(),
               generation_claims(hub.hub_sid)
             )

    assert BotRunnerLease.snapshot(coordinator, hub.hub_sid) == nil
  end

  test "database loss during renewal fences locally and tells the owner to disconnect", %{
    hub: hub
  } do
    {:ok, agent} =
      Agent.start_link(fn ->
        %{active: nil, epoch: 0, mode: :ok, renew_gate: nil}
      end)

    coordinator =
      start_coordinator(
        holder_id: Ecto.UUID.generate(),
        repo: agent,
        store: Ret.BotRunnerLeaseTest.ControlledStore
      )

    {:ok, lease} =
      BotRunnerLease.register_for_session(
        coordinator,
        hub.hub_sid,
        Ecto.UUID.generate(),
        generation_claims(hub.hub_sid)
      )

    Agent.update(agent, &%{&1 | mode: :database_unavailable})
    assert :ok = BotRunnerLease.renew_now(coordinator)

    assert_receive {:bot_runner_lease_database_unavailable, lease_id}
    assert lease_id == lease.lease_id
    assert_receive {:bot_runner_lease_authority, ^lease_id, _epoch, false}
    assert BotRunnerLease.snapshot(coordinator, hub.hub_sid) == nil
  end

  test "a delayed successful renewal response cannot revive a replaced fence", %{hub: hub} do
    {:ok, agent} =
      Agent.start_link(fn ->
        %{active: nil, epoch: 0, mode: :ok, renew_gate: nil}
      end)

    coordinator =
      start_coordinator(
        holder_id: Ecto.UUID.generate(),
        repo: agent,
        store: Ret.BotRunnerLeaseTest.ControlledStore
      )

    {:ok, lease} =
      BotRunnerLease.register_for_session(
        coordinator,
        hub.hub_sid,
        Ecto.UUID.generate(),
        generation_claims(hub.hub_sid)
      )

    reference = make_ref()
    test_pid = self()
    Agent.update(agent, &%{&1 | renew_gate: {test_pid, reference}})
    renewal = Task.async(fn -> BotRunnerLease.renew_now(coordinator) end)

    assert_receive {:renew_started, ^reference, renewal_pid}

    Agent.update(agent, fn state ->
      %{state | active: nil, epoch: state.epoch + 1, renew_gate: nil}
    end)

    send(renewal_pid, {:finish_stale_renewal, reference})
    assert :ok = Task.await(renewal)

    assert {:error, :not_authoritative} =
             BotRunnerLease.with_authority(
               coordinator,
               hub.hub_sid,
               lease.lease_id,
               lease.authority_epoch,
               fn -> flunk("stale renewal response revived authority") end
             )
  end

  test "revoke waits for an in-flight fenced callback and no stale callback starts after it", %{
    hub: hub
  } do
    parent = self()

    owner =
      Task.async(fn ->
        {:ok, lease} =
          BotRunnerLease.register_for_session(
            hub.hub_sid,
            Ecto.UUID.generate(),
            generation_claims(hub.hub_sid)
          )

        send(parent, {:callback_lease, self(), lease})

        receive do
          :run_callback -> :ok
        end

        result =
          BotRunnerLease.with_authority(
            hub.hub_sid,
            lease.lease_id,
            lease.authority_epoch,
            fn ->
              send(parent, :fenced_callback_entered)

              receive do
                :finish_callback -> :ok
              end

              send(parent, :fenced_side_effect)
              :delivered
            end
          )

        send(parent, {:fenced_callback_result, result})
        :ok
      end)

    assert_receive {:callback_lease, owner_pid, lease}
    send(owner_pid, :run_callback)
    assert_receive :fenced_callback_entered

    revoker =
      Task.async(fn ->
        send(parent, :revoke_started)
        result = BotRunnerLease.revoke(hub.hub_sid)
        send(parent, {:revoke_finished, result})
        :ok
      end)

    assert_receive :revoke_started
    refute_receive {:revoke_finished, _result}, 0

    send(owner_pid, :finish_callback)
    assert_receive :fenced_side_effect
    assert_receive {:fenced_callback_result, {:ok, :delivered}}
    assert_receive {:revoke_finished, :ok}

    assert :ok = Task.await(owner)
    assert :ok = Task.await(revoker)

    assert {:error, :not_authoritative} =
             BotRunnerLease.with_authority(
               hub.hub_sid,
               lease.lease_id,
               lease.authority_epoch,
               fn -> flunk("callback started after revoke") end
             )
  end

  test "a lease crash replaces the runtime group and waits for durable expiry", %{hub: hub} do
    assert_atomic_runtime_restart(BotRunnerLease, hub)
  end

  test "an Endpoint crash replaces the runtime group and waits for durable expiry", %{hub: hub} do
    assert_atomic_runtime_restart(RetWeb.Endpoint, hub)
  end

  test "a Presence crash replaces the runtime group and waits for durable expiry", %{hub: hub} do
    assert_atomic_runtime_restart(RetWeb.Presence, hub)
  end

  test "release and A-B-A replacement cannot replay a consumed generation or advance epoch", %{
    hub: hub
  } do
    generation_a =
      generation_claims(hub.hub_sid, %{
        "process_generation" => "11111111-1111-4111-8111-111111111111",
        "holder_id" => "holder-a"
      })

    {lease_a, holder_a, session_a, epoch_a} = acquire_direct!(hub, generation_a)

    assert {:ok, release_epoch_a} =
             PostgresStore.release(
               Repo,
               hub.hub_sid,
               lease_a,
               holder_a,
               session_a,
               epoch_a
             )

    epoch_before_replay = authority_sequence_last()

    assert {:error, :generation_replayed} =
             acquire_direct(hub, generation_a) |> elem(0)

    assert authority_sequence_last() == epoch_before_replay
    assert release_epoch_a == epoch_before_replay

    generation_b =
      generation_claims(hub.hub_sid, %{
        "process_generation" => "22222222-2222-4222-8222-222222222222",
        "holder_id" => "holder-b"
      })

    {lease_b, holder_b, session_b, epoch_b} = acquire_direct!(hub, generation_b)

    assert {:ok, _release_epoch_b} =
             PostgresStore.release(
               Repo,
               hub.hub_sid,
               lease_b,
               holder_b,
               session_b,
               epoch_b
             )

    epoch_before_a_b_a = authority_sequence_last()
    assert {:error, :generation_replayed} = acquire_direct(hub, generation_a) |> elem(0)
    assert authority_sequence_last() == epoch_before_a_b_a
    assert generation_count(hub.hub_id) == 2
  end

  test "revoke preserves the consumed generation and replay does not advance epoch", %{hub: hub} do
    claims = generation_claims(hub.hub_sid)
    {_lease_id, _holder_id, _session_id, _epoch} = acquire_direct!(hub, claims)

    assert {:ok, revoke_epoch} = PostgresStore.revoke(Repo, hub.hub_sid)
    epoch_before_replay = authority_sequence_last()
    assert revoke_epoch == epoch_before_replay

    assert {:error, :generation_replayed} = acquire_direct(hub, claims) |> elem(0)
    assert authority_sequence_last() == epoch_before_replay
    assert generation_count(hub.hub_id) == 1
  end

  test "lease expiry and coordinator restart cannot make a consumed generation reusable", %{
    hub: hub
  } do
    coordinator = start_coordinator(holder_id: Ecto.UUID.generate())
    claims = generation_claims(hub.hub_sid)

    assert {:ok, _lease} =
             BotRunnerLease.register_for_session(
               coordinator,
               hub.hub_sid,
               Ecto.UUID.generate(),
               claims
             )

    GenServer.stop(coordinator, :normal)
    force_expiry!(hub.hub_id)
    replacement = start_coordinator(holder_id: Ecto.UUID.generate())
    epoch_before_replay = authority_sequence_last()

    assert {:error, :runner_generation_replayed} =
             BotRunnerLease.register_for_session(
               replacement,
               hub.hub_sid,
               Ecto.UUID.generate(),
               claims
             )

    assert authority_sequence_last() == epoch_before_replay
    assert generation_count(hub.hub_id) == 1
  end

  test "an active lease blocks but does not consume the next exact generation", %{hub: hub} do
    incumbent = start_coordinator(holder_id: Ecto.UUID.generate())
    replacement = start_coordinator(holder_id: Ecto.UUID.generate())
    incumbent_claims = generation_claims(hub.hub_sid)

    replacement_claims =
      generation_claims(hub.hub_sid, %{
        "process_generation" => "33333333-3333-4333-8333-333333333333",
        "holder_id" => "replacement-holder",
        "exp" => System.system_time(:second) + 240
      })

    assert {:ok, incumbent_lease} =
             BotRunnerLease.register_for_session(
               incumbent,
               hub.hub_sid,
               Ecto.UUID.generate(),
               incumbent_claims
             )

    epoch_before_block = authority_sequence_last()

    assert {:error, :lease_unavailable} =
             BotRunnerLease.register_for_session(
               replacement,
               hub.hub_sid,
               Ecto.UUID.generate(),
               replacement_claims
             )

    assert authority_sequence_last() == epoch_before_block
    refute generation_consumed?(hub.hub_id, replacement_claims["process_generation"])
    assert :ok = BotRunnerLease.unregister(incumbent, hub.hub_sid, incumbent_lease.lease_id)

    assert {:ok, replacement_lease} =
             BotRunnerLease.register_for_session(
               replacement,
               hub.hub_sid,
               Ecto.UUID.generate(),
               replacement_claims
             )

    assert generation_consumed?(hub.hub_id, replacement_claims["process_generation"])

    assert %{rows: [["replacement-holder", token_expiry]]} =
             SQL.query!(
               Repo,
               """
               SELECT holder_id, extract(epoch FROM token_expires_at)::bigint
               FROM ret0.bot_runner_generations
               WHERE hub_id = $1 AND process_generation = $2::text::uuid
               """,
               [hub.hub_id, replacement_claims["process_generation"]]
             )

    assert token_expiry == replacement_claims["exp"]
    assert :ok = BotRunnerLease.unregister(replacement, hub.hub_sid, replacement_lease.lease_id)
  end

  defp assert_atomic_runtime_restart(crashed_name, hub) do
    old_holder = BotRunnerLease.holder_id()
    previous_pids = runtime_pids()

    {:ok, old_lease} =
      BotRunnerLease.register_for_session(
        hub.hub_sid,
        Ecto.UUID.generate(),
        generation_claims(hub.hub_sid)
      )

    monitors =
      Map.new(previous_pids, fn {name, pid} ->
        {name, Process.monitor(pid)}
      end)

    GenServer.stop(runtime_child_pid(crashed_name), :shutdown)

    Enum.each(previous_pids, fn {name, pid} ->
      monitor_ref = Map.fetch!(monitors, name)
      assert_receive {:DOWN, ^monitor_ref, :process, ^pid, _reason}, 5_000
    end)

    Enum.each(previous_pids, fn {name, previous_pid} ->
      replacement = wait_for_replacement(name, previous_pid)
      assert Process.alive?(replacement)
    end)

    refute BotRunnerLease.holder_id() == old_holder

    refute BotRunnerLease.authorized?(
             hub.hub_sid,
             old_lease.lease_id,
             old_lease.authority_epoch
           )

    blocked_generation = generation_claims(hub.hub_sid)

    assert {:error, :lease_unavailable} =
             BotRunnerLease.register_for_session(
               hub.hub_sid,
               Ecto.UUID.generate(),
               blocked_generation
             )

    force_expiry!(hub.hub_id)

    {:ok, recovered} =
      BotRunnerLease.register_for_session(
        hub.hub_sid,
        Ecto.UUID.generate(),
        blocked_generation
      )

    assert recovered.authority_epoch > old_lease.authority_epoch
    assert :ok = BotRunnerLease.unregister(hub.hub_sid, recovered.lease_id)
  end

  test "authority sequence is globally JavaScript-safe and cannot cycle" do
    assert %{rows: [[9_007_199_254_740_991, false]]} =
             SQL.query!(
               Repo,
               """
               SELECT seqmax, seqcycle
               FROM pg_sequence
               WHERE seqrelid = 'ret0.bot_runner_authority_epoch_seq'::regclass
               """,
               []
             )
  end

  defp start_coordinator(opts) do
    {:ok, pid} =
      BotRunnerLease.start_link(Keyword.merge([name: nil, renew_interval_ms: 60_000], opts))

    pid
  end

  defp acquire_direct(hub, claims) do
    lease_id = Ecto.UUID.generate()
    holder_instance_id = Ecto.UUID.generate()
    session_id = Ecto.UUID.generate()

    result =
      PostgresStore.acquire(
        Repo,
        hub.hub_sid,
        lease_id,
        holder_instance_id,
        session_id,
        claims,
        15
      )

    {result, lease_id, holder_instance_id, session_id}
  end

  defp acquire_direct!(hub, claims) do
    {{:ok, %{epoch: epoch}}, lease_id, holder_instance_id, session_id} =
      acquire_direct(hub, claims)

    {lease_id, holder_instance_id, session_id, epoch}
  end

  defp authority_sequence_last do
    %{rows: [[epoch]]} =
      SQL.query!(Repo, "SELECT last_value FROM ret0.bot_runner_authority_epoch_seq", [])

    epoch
  end

  defp generation_count(hub_id) do
    %{rows: [[count]]} =
      SQL.query!(
        Repo,
        "SELECT count(*) FROM ret0.bot_runner_generations WHERE hub_id = $1",
        [hub_id]
      )

    count
  end

  defp generation_consumed?(hub_id, process_generation) do
    %{rows: [[consumed]]} =
      SQL.query!(
        Repo,
        "SELECT EXISTS (SELECT 1 FROM ret0.bot_runner_generations WHERE hub_id = $1 AND process_generation = $2::text::uuid)",
        [hub_id, process_generation]
      )

    consumed
  end

  defp generation_claims(hub_sid, overrides \\ %{}) do
    Map.merge(
      %{
        "v" => 1,
        "aud" => "yenhubs-bot-runner",
        "hub_sid" => hub_sid,
        "process_generation" => Ecto.UUID.generate(),
        "holder_id" => Ecto.UUID.generate(),
        "exp" => System.system_time(:second) + 300
      },
      overrides
    )
  end

  defp force_expiry!(hub_id) do
    %{num_rows: 1} =
      SQL.query!(
        Repo,
        """
        UPDATE ret0.bot_runner_leases
        SET expires_at = timezone('UTC', clock_timestamp()) - INTERVAL '1 second'
        WHERE hub_id = $1
        """,
        [hub_id]
      )
  end

  defp runtime_child_pid(name) do
    Ret.BotRuntimeSupervisor
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {^name, pid, _type, _modules} -> pid
      _other -> nil
    end)
  end

  defp runtime_pids do
    %{
      BotRunnerLease => Process.whereis(BotRunnerLease),
      RetWeb.Endpoint => Process.whereis(RetWeb.Endpoint),
      RetWeb.Presence => Process.whereis(RetWeb.Presence)
    }
  end

  defp wait_for_replacement(name, previous_pid, attempts \\ 200)

  defp wait_for_replacement(_name, _previous_pid, 0),
    do: flunk("supervised process was not replaced")

  defp wait_for_replacement(name, previous_pid, attempts) do
    case Process.whereis(name) do
      pid when is_pid(pid) and pid != previous_pid ->
        pid

      _ ->
        receive do
        after
          10 -> wait_for_replacement(name, previous_pid, attempts - 1)
        end
    end
  end
end
