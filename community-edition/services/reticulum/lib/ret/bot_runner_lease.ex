defmodule Ret.BotRunnerLease do
  @moduledoc """
  Process-bound bot-runner authority backed by a PostgreSQL fencing lease.

  PostgreSQL is the only authority shared by Reticulum replicas. The GenServer
  passes the exact verified generation claims into the durable one-time
  acquisition, binds the resulting lease to its channel process, applies a
  conservative local monotonic deadline and mirrors loss of authority into
  Presence. Database work runs in callers or renewal tasks, never while
  blocking this coordinator; this is important when registration already holds
  the bot-config admission advisory lock.

  Runtime side effects must use `with_authority/5`. It locks and validates the
  exact database row, then executes the side effect before releasing that lock,
  so revoke and takeover have one linear order with the broadcast or ACK.
  """

  use GenServer

  @name __MODULE__
  @lease_ttl_seconds 15
  @renew_interval_ms 5_000

  def start_link(opts) when is_list(opts) do
    name = Keyword.get(opts, :name, @name)

    if name do
      GenServer.start_link(__MODULE__, opts, name: name)
    else
      GenServer.start_link(__MODULE__, opts)
    end
  end

  def register(hub_sid, generation_claims), do: register(@name, hub_sid, generation_claims)

  @doc false
  def register(server, hub_sid, generation_claims)
      when is_binary(hub_sid) and byte_size(hub_sid) > 0 and is_map(generation_claims) do
    register_for_session(server, hub_sid, SecureRandom.uuid(), generation_claims)
  end

  def register(_server, _hub_sid, _generation_claims), do: {:error, :lease_unavailable}

  def register_for_session(hub_sid, session_id, generation_claims),
    do: register_for_session(@name, hub_sid, session_id, generation_claims)

  @doc false
  def register_for_session(server, hub_sid, session_id, generation_claims)
      when is_binary(hub_sid) and byte_size(hub_sid) > 0 and is_binary(session_id) and
             is_map(generation_claims) do
    case GenServer.call(server, {:prepare_registration, hub_sid, session_id, generation_claims}) do
      {:ok, context} ->
        result =
          context.store.acquire(
            context.repo,
            hub_sid,
            context.lease_id,
            context.holder_id,
            context.session_id,
            context.generation_claims,
            context.ttl_seconds
          )

        finalized =
          GenServer.call(
            server,
            {:finalize_registration, context.registration_id, result}
          )

        if match?({:ok, %{epoch: _epoch}}, result) and not match?({:ok, _lease}, finalized) do
          %{epoch: epoch} = elem(result, 1)

          _ =
            context.store.release(
              context.repo,
              hub_sid,
              context.lease_id,
              context.holder_id,
              context.session_id,
              epoch
            )
        end

        finalized

      {:error, :lease_unavailable} = error ->
        error
    end
  catch
    :exit, _reason -> {:error, :lease_database_unavailable}
  end

  def register_for_session(_server, _hub_sid, _session_id, _generation_claims),
    do: {:error, :lease_unavailable}

  def authorized?(hub_sid, lease_id, authority_epoch),
    do: authorized?(@name, hub_sid, lease_id, authority_epoch)

  @doc false
  def authorized?(server, hub_sid, lease_id, authority_epoch) do
    match?(
      {:ok, true},
      with_authority(server, hub_sid, lease_id, authority_epoch, fn -> true end)
    )
  end

  @doc """
  Executes `fun` while holding the exact live PostgreSQL fencing row.

  The caller must be the process that acquired the local lease. A stale epoch,
  a different holder, expiry, takeover or any database error fails closed and
  the side effect is not executed.
  """
  def with_authority(hub_sid, lease_id, authority_epoch, fun),
    do: with_authority(@name, hub_sid, lease_id, authority_epoch, fun)

  @doc false
  def with_authority(server, hub_sid, lease_id, authority_epoch, fun)
      when is_binary(hub_sid) and is_binary(lease_id) and is_integer(authority_epoch) and
             authority_epoch > 0 and is_function(fun, 0) do
    case GenServer.call(server, {:authority_context, hub_sid, lease_id, authority_epoch}) do
      {:ok, context} ->
        result =
          context.store.with_authority(
            context.repo,
            hub_sid,
            lease_id,
            context.holder_id,
            context.session_id,
            authority_epoch,
            fun
          )

        case result do
          {:ok, _value} ->
            result

          {:error, reason} when reason in [:not_authoritative, :database_unavailable] ->
            GenServer.cast(
              server,
              {:fence_if_current, hub_sid, lease_id, authority_epoch, reason}
            )

            result
        end

      {:error, :not_authoritative} = error ->
        error
    end
  catch
    :exit, _reason -> {:error, :database_unavailable}
  end

  def with_authority(_server, _hub_sid, _lease_id, _authority_epoch, _fun),
    do: {:error, :not_authoritative}

  @doc """
  Executes `fun` under the currently active room fence.

  This form is for trusted server-side delivery such as a validated bot chat
  command, where the HTTP process is not the runner channel owner. The callback
  receives the exact lease and epoch that must travel with the command.
  """
  def with_current_authority(hub_sid, fun),
    do: with_current_authority(@name, hub_sid, fun)

  @doc false
  def with_current_authority(server, hub_sid, fun)
      when is_binary(hub_sid) and byte_size(hub_sid) > 0 and is_function(fun, 1) do
    context = GenServer.call(server, :store_context)
    result = context.store.with_current_authority(context.repo, hub_sid, fun)

    case result do
      {:ok, _value} ->
        result

      {:error, reason} when reason in [:not_authoritative, :database_unavailable] ->
        GenServer.cast(server, {:fence_hub, hub_sid, reason})
        result
    end
  catch
    :exit, _reason -> {:error, :database_unavailable}
  end

  def with_current_authority(_server, _hub_sid, _fun),
    do: {:error, :not_authoritative}

  def unregister(hub_sid, lease_id), do: unregister(@name, hub_sid, lease_id)

  @doc false
  def unregister(server, hub_sid, lease_id)
      when is_binary(hub_sid) and is_binary(lease_id) do
    case GenServer.call(server, {:take_owned_lease, hub_sid, lease_id}) do
      {:ok, context} ->
        case context.store.release(
               context.repo,
               hub_sid,
               lease_id,
               context.holder_id,
               context.session_id,
               context.authority_epoch
             ) do
          {:ok, _fenced_epoch} -> :ok
          {:error, :database_unavailable} -> {:error, :lease_database_unavailable}
        end

      :ok ->
        :ok

      {:error, :not_owner} = error ->
        error
    end
  catch
    :exit, _reason -> {:error, :lease_database_unavailable}
  end

  def unregister(_server, _hub_sid, _lease_id), do: :ok

  def revoke(hub_sid), do: revoke(@name, hub_sid)

  @doc false
  def revoke(server, hub_sid) when is_binary(hub_sid) and byte_size(hub_sid) > 0 do
    context = GenServer.call(server, :store_context)
    result = context.store.revoke(context.repo, hub_sid)

    durable_epoch =
      case result do
        {:ok, epoch} -> epoch
        {:error, :database_unavailable} -> nil
      end

    GenServer.call(
      server,
      {:apply_revoke, hub_sid, durable_epoch, result == {:error, :database_unavailable}}
    )

    case result do
      {:ok, _epoch} -> :ok
      {:error, :database_unavailable} -> {:error, :lease_database_unavailable}
    end
  catch
    :exit, _reason -> {:error, :lease_database_unavailable}
  end

  def revoke(_server, _hub_sid), do: :ok

  @doc false
  def snapshot(hub_sid), do: snapshot(@name, hub_sid)

  @doc false
  def snapshot(server, hub_sid) when is_binary(hub_sid) do
    case GenServer.call(server, {:snapshot_context, hub_sid}) do
      {:ok, context} ->
        case context.store.current(context.repo, hub_sid) do
          {:ok,
           %{
             lease_id: lease_id,
             holder_instance_id: holder_instance_id,
             session_id: session_id,
             epoch: epoch
           }}
          when lease_id == context.lease_id and
                 holder_instance_id == context.holder_id and session_id == context.session_id and
                 epoch == context.authority_epoch ->
            %{
              authority_lease_id: lease_id,
              authority_epoch: epoch,
              leases: [
                %{
                  lease_id: lease_id,
                  join_order: epoch,
                  owner_pid: context.owner_pid
                }
              ]
            }

          _unavailable_or_stale ->
            GenServer.cast(
              server,
              {:fence_if_current, hub_sid, context.lease_id, context.authority_epoch,
               :not_authoritative}
            )

            nil
        end

      :none ->
        nil
    end
  catch
    :exit, _reason -> nil
  end

  @doc false
  def renew_now(server \\ @name) do
    contexts = GenServer.call(server, :renewal_contexts)
    results = renew_contexts(contexts)
    GenServer.call(server, {:apply_renewal_results, results})
  end

  @doc false
  def holder_id(server \\ @name), do: GenServer.call(server, :holder_id)

  @impl true
  def init(opts) do
    state = %{
      holder_id: Keyword.get_lazy(opts, :holder_id, &SecureRandom.uuid/0),
      leases: %{},
      monitor_refs: %{},
      pending: %{},
      pending_hubs: MapSet.new(),
      renewal_in_flight: false,
      renew_interval_ms: Keyword.get(opts, :renew_interval_ms, @renew_interval_ms),
      repo: Keyword.get(opts, :repo, Ret.Repo),
      store: Keyword.get(opts, :store, Ret.BotRunnerLease.PostgresStore),
      ttl_seconds: Keyword.get(opts, :ttl_seconds, @lease_ttl_seconds)
    }

    schedule_renewal(state.renew_interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call(
        {:prepare_registration, hub_sid, session_id, generation_claims},
        {owner_pid, _tag},
        state
      ) do
    if not Ret.BotRunnerGenerationToken.valid_claims?(generation_claims, hub_sid) do
      {:reply, {:error, :lease_unavailable}, state}
    else
      prepare_registration(hub_sid, session_id, generation_claims, owner_pid, state)
    end
  end

  def handle_call(
        {:finalize_registration, registration_id, result},
        {owner_pid, _tag},
        state
      ) do
    case Map.get(state.pending, registration_id) do
      %{owner_pid: ^owner_pid} = pending ->
        local_deadline_ms = pending.local_started_at_ms + state.ttl_seconds * 1_000

        case result do
          {:ok, %{epoch: epoch}} ->
            if local_deadline_ms > monotonic_ms() do
              lease = %{
                authority_epoch: epoch,
                lease_id: pending.lease_id,
                local_deadline_ms: local_deadline_ms,
                monitor_ref: pending.monitor_ref,
                owner_pid: owner_pid,
                session_id: pending.session_id
              }

              next_state =
                state
                |> remove_pending(registration_id, pending, false)
                |> put_lease(pending.hub_sid, lease)

              {:reply,
               {:ok,
                %{
                  lease_id: lease.lease_id,
                  session_id: lease.session_id,
                  join_order: epoch,
                  authority_epoch: epoch,
                  authoritative: true
                }}, next_state}
            else
              {:reply, {:error, :lease_unavailable},
               remove_pending(state, registration_id, pending)}
            end

          {:error, :unavailable} ->
            {:reply, {:error, :lease_unavailable},
             remove_pending(state, registration_id, pending)}

          {:error, :database_unavailable} ->
            {:reply, {:error, :lease_database_unavailable},
             remove_pending(state, registration_id, pending)}

          {:error, :generation_replayed} ->
            {:reply, {:error, :runner_generation_replayed},
             remove_pending(state, registration_id, pending)}

          {:error, :invalid_generation_claims} ->
            {:reply, {:error, :lease_unavailable},
             remove_pending(state, registration_id, pending)}
        end

      _missing_or_other_owner ->
        {:reply, {:error, :lease_unavailable}, state}
    end
  end

  def handle_call(
        {:authority_context, hub_sid, lease_id, authority_epoch},
        {owner_pid, _tag},
        state
      ) do
    case Map.get(state.leases, hub_sid) do
      %{
        authority_epoch: ^authority_epoch,
        lease_id: ^lease_id,
        local_deadline_ms: deadline_ms,
        owner_pid: ^owner_pid
      } = lease ->
        if deadline_ms > monotonic_ms() do
          {:reply,
           {:ok,
            %{
              holder_id: state.holder_id,
              repo: state.repo,
              session_id: lease.session_id,
              store: state.store
            }}, state}
        else
          {:reply, {:error, :not_authoritative},
           remove_local_lease(state, hub_sid, lease, authority_epoch)}
        end

      _ ->
        {:reply, {:error, :not_authoritative}, state}
    end
  end

  def handle_call({:take_owned_lease, hub_sid, lease_id}, {owner_pid, _tag}, state) do
    case Map.get(state.leases, hub_sid) do
      %{lease_id: ^lease_id, owner_pid: ^owner_pid} = lease ->
        context = %{
          authority_epoch: lease.authority_epoch,
          holder_id: state.holder_id,
          repo: state.repo,
          session_id: lease.session_id,
          store: state.store
        }

        {:reply, {:ok, context}, remove_local_lease(state, hub_sid, lease, lease.authority_epoch)}

      nil ->
        {:reply, :ok, state}

      _other_owner ->
        {:reply, {:error, :not_owner}, state}
    end
  end

  def handle_call(:store_context, _from, state) do
    {:reply, %{repo: state.repo, store: state.store}, state}
  end

  def handle_call({:apply_revoke, hub_sid, durable_epoch, database_unavailable}, _from, state) do
    next_state =
      case Map.get(state.leases, hub_sid) do
        nil ->
          state

        lease ->
          if database_unavailable, do: notify_database_unavailable(lease)
          remove_local_lease(state, hub_sid, lease, durable_epoch || lease.authority_epoch)
      end

    {:reply, :ok, next_state}
  end

  def handle_call({:snapshot_context, hub_sid}, _from, state) do
    case Map.get(state.leases, hub_sid) do
      %{local_deadline_ms: deadline_ms} = lease ->
        if deadline_ms > monotonic_ms() do
          {:reply,
           {:ok,
            %{
              authority_epoch: lease.authority_epoch,
              holder_id: state.holder_id,
              lease_id: lease.lease_id,
              owner_pid: lease.owner_pid,
              repo: state.repo,
              session_id: lease.session_id,
              store: state.store
            }}, state}
        else
          {:reply, :none, remove_local_lease(state, hub_sid, lease, lease.authority_epoch)}
        end

      _ ->
        {:reply, :none, state}
    end
  end

  def handle_call(:renewal_contexts, _from, state) do
    {:reply, renewal_contexts(state), state}
  end

  def handle_call({:apply_renewal_results, results}, _from, state) do
    {:reply, :ok, apply_renewal_results(state, results)}
  end

  def handle_call(:holder_id, _from, state), do: {:reply, state.holder_id, state}

  defp prepare_registration(hub_sid, session_id, generation_claims, owner_pid, state) do
    if Map.has_key?(state.leases, hub_sid) or MapSet.member?(state.pending_hubs, hub_sid) do
      {:reply, {:error, :lease_unavailable}, state}
    else
      registration_id = SecureRandom.uuid()
      monitor_ref = Process.monitor(owner_pid)

      pending = %{
        hub_sid: hub_sid,
        lease_id: SecureRandom.uuid(),
        local_started_at_ms: monotonic_ms(),
        monitor_ref: monitor_ref,
        owner_pid: owner_pid,
        session_id: session_id
      }

      context = %{
        generation_claims: generation_claims,
        holder_id: state.holder_id,
        lease_id: pending.lease_id,
        registration_id: registration_id,
        repo: state.repo,
        session_id: session_id,
        store: state.store,
        ttl_seconds: state.ttl_seconds
      }

      next_state = %{
        state
        | pending: Map.put(state.pending, registration_id, pending),
          pending_hubs: MapSet.put(state.pending_hubs, hub_sid),
          monitor_refs: Map.put(state.monitor_refs, monitor_ref, {:pending, registration_id})
      }

      {:reply, {:ok, context}, next_state}
    end
  end

  @impl true
  def handle_cast(
        {:fence_if_current, hub_sid, lease_id, authority_epoch, reason},
        state
      ) do
    next_state =
      case Map.get(state.leases, hub_sid) do
        %{lease_id: ^lease_id, authority_epoch: ^authority_epoch} = lease ->
          if reason == :database_unavailable, do: notify_database_unavailable(lease)
          remove_local_lease(state, hub_sid, lease, authority_epoch)

        _ ->
          state
      end

    {:noreply, next_state}
  end

  def handle_cast({:fence_hub, hub_sid, reason}, state) do
    next_state =
      case Map.get(state.leases, hub_sid) do
        nil ->
          state

        lease ->
          if reason == :database_unavailable, do: notify_database_unavailable(lease)
          remove_local_lease(state, hub_sid, lease, lease.authority_epoch)
      end

    {:noreply, next_state}
  end

  @impl true
  def handle_info(:renew_leases, %{renewal_in_flight: false} = state) do
    schedule_renewal(state.renew_interval_ms)
    contexts = renewal_contexts(state)

    if contexts == [] do
      {:noreply, state}
    else
      coordinator = self()

      {:ok, _task_pid} =
        Task.start(fn ->
          send(coordinator, {:renewal_results, renew_contexts(contexts)})
        end)

      {:noreply, %{state | renewal_in_flight: true}}
    end
  end

  def handle_info(:renew_leases, state) do
    schedule_renewal(state.renew_interval_ms)
    {:noreply, state}
  end

  def handle_info({:renewal_results, results}, state) do
    {:noreply, %{apply_renewal_results(state, results) | renewal_in_flight: false}}
  end

  def handle_info({:DOWN, monitor_ref, :process, _owner_pid, _reason}, state) do
    case Map.get(state.monitor_refs, monitor_ref) do
      {:pending, registration_id} ->
        case Map.get(state.pending, registration_id) do
          nil -> {:noreply, %{state | monitor_refs: Map.delete(state.monitor_refs, monitor_ref)}}
          pending -> {:noreply, remove_pending(state, registration_id, pending, false)}
        end

      {:lease, hub_sid, lease_id} ->
        case Map.get(state.leases, hub_sid) do
          %{lease_id: ^lease_id} = lease ->
            # A graceful channel terminate explicitly releases. On an abrupt
            # owner crash, retain the durable row until its short expiry; this
            # is the same fail-closed boundary used for VM loss and partitions.
            {:noreply, remove_local_lease(state, hub_sid, lease, lease.authority_epoch, false)}

          _ ->
            {:noreply, %{state | monitor_refs: Map.delete(state.monitor_refs, monitor_ref)}}
        end

      nil ->
        {:noreply, state}
    end
  end

  defp renewal_contexts(state) do
    Enum.map(state.leases, fn {hub_sid, lease} -> renewal_context(state, hub_sid, lease) end)
  end

  defp renewal_context(state, hub_sid, lease) do
    %{
      authority_epoch: lease.authority_epoch,
      holder_id: state.holder_id,
      hub_sid: hub_sid,
      lease_id: lease.lease_id,
      local_started_at_ms: monotonic_ms(),
      repo: state.repo,
      session_id: lease.session_id,
      store: state.store,
      ttl_seconds: state.ttl_seconds
    }
  end

  defp renew_contexts(contexts) do
    Enum.map(contexts, fn context ->
      result =
        context.store.renew(
          context.repo,
          context.hub_sid,
          context.lease_id,
          context.holder_id,
          context.session_id,
          context.authority_epoch,
          context.ttl_seconds
        )

      {context, result, monotonic_ms()}
    end)
  end

  defp apply_renewal_results(state, results) do
    Enum.reduce(results, state, fn {context, result, completed_at_ms}, current_state ->
      case Map.get(current_state.leases, context.hub_sid) do
        %{
          authority_epoch: authority_epoch,
          lease_id: lease_id
        } = lease
        when authority_epoch == context.authority_epoch and lease_id == context.lease_id ->
          local_deadline_ms =
            context.local_started_at_ms + context.ttl_seconds * 1_000

          if result == {:ok, %{epoch: authority_epoch}} and
               local_deadline_ms > completed_at_ms do
            put_in(
              current_state,
              [:leases, context.hub_sid, :local_deadline_ms],
              local_deadline_ms
            )
          else
            if result == {:error, :database_unavailable},
              do: notify_database_unavailable(lease)

            remove_local_lease(
              current_state,
              context.hub_sid,
              lease,
              lease.authority_epoch
            )
          end

        _stale_result ->
          current_state
      end
    end)
  end

  defp put_lease(state, hub_sid, lease) do
    %{
      state
      | leases: Map.put(state.leases, hub_sid, lease),
        monitor_refs:
          Map.put(state.monitor_refs, lease.monitor_ref, {:lease, hub_sid, lease.lease_id})
    }
  end

  defp remove_pending(state, registration_id, pending, demonitor \\ true) do
    if demonitor, do: Process.demonitor(pending.monitor_ref, [:flush])

    %{
      state
      | pending: Map.delete(state.pending, registration_id),
        pending_hubs: MapSet.delete(state.pending_hubs, pending.hub_sid),
        monitor_refs: Map.delete(state.monitor_refs, pending.monitor_ref)
    }
  end

  defp remove_local_lease(state, hub_sid, lease, fenced_epoch, demonitor \\ true) do
    if demonitor, do: Process.demonitor(lease.monitor_ref, [:flush])

    send(
      lease.owner_pid,
      {:bot_runner_lease_authority, lease.lease_id, fenced_epoch, false}
    )

    %{
      state
      | leases: Map.delete(state.leases, hub_sid),
        monitor_refs: Map.delete(state.monitor_refs, lease.monitor_ref)
    }
  end

  defp notify_database_unavailable(lease) do
    send(lease.owner_pid, {:bot_runner_lease_database_unavailable, lease.lease_id})
  end

  defp schedule_renewal(interval_ms), do: Process.send_after(self(), :renew_leases, interval_ms)
  defp monotonic_ms, do: System.monotonic_time(:millisecond)
end
