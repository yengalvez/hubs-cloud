defmodule Ret.BotRuntimeOutboxDispatcherTest do
  use ExUnit.Case, async: false

  alias Ret.BotRuntimeOutboxDispatcher
  alias Ret.BotRuntimeOutboxDispatcherTestDelivery, as: Delivery
  alias Ret.BotRuntimeOutboxDispatcherTestStore, as: Store

  @operation_id "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @claim_token "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

  setup do
    previous_store = Application.get_env(:ret, Store)
    previous_delivery = Application.get_env(:ret, Delivery)

    on_exit(fn ->
      restore_env(Store, previous_store)
      restore_env(Delivery, previous_delivery)
    end)

    :ok
  end

  test "a pending delivery releases the exact claim with bounded backoff" do
    parent = self()
    event = event(claim_owner: "dispatcher-a", attempt_count: 3)

    Application.put_env(:ret, Store, fn
      {:claim_next, "dispatcher-a", claim_ttl_seconds: 30} ->
        {:ok, event}

      {:retry_claim, 17, 4, "dispatcher-a", @claim_token, "accepted_pending", 4} ->
        send(parent, :retried_exact_claim)
        :ok
    end)

    Application.put_env(:ret, Delivery, fn delivered ->
      assert delivered.operation_id == @operation_id
      {:pending, :accepted_pending}
    end)

    assert {:ok, :retry_scheduled} = dispatch_once(claim_owner: "dispatcher-a")
    assert_receive :retried_exact_claim
  end

  test "only a terminal delivery completes the exact revision and claim" do
    parent = self()
    event = event(claim_owner: "dispatcher-a")

    Application.put_env(:ret, Store, fn
      {:claim_next, "dispatcher-a", _opts} ->
        {:ok, event}

      {:complete_claim, 17, 4, "dispatcher-a", @claim_token} ->
        send(parent, :completed_exact_claim)
        :ok
    end)

    Application.put_env(:ret, Delivery, fn _event -> :ok end)

    assert {:ok, :completed} = dispatch_once(claim_owner: "dispatcher-a")
    assert_receive :completed_exact_claim
  end

  test "terminal HTTP ACK followed by completion loss retries the same operation after reclaim" do
    {:ok, state} = Agent.start_link(fn -> 0 end)
    parent = self()
    second_token = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"

    Application.put_env(:ret, Store, fn
      {:claim_next, "dispatcher-a", _opts} ->
        {:ok, event(claim_owner: "dispatcher-a")}

      {:claim_next, "dispatcher-b", _opts} ->
        {:ok, event(claim_owner: "dispatcher-b", claim_token: second_token, attempt_count: 2)}

      {:complete_claim, 17, 4, "dispatcher-a", @claim_token} ->
        {:error, :claim_lost}

      {:complete_claim, 17, 4, "dispatcher-b", ^second_token} ->
        :ok
    end)

    Application.put_env(:ret, Delivery, fn delivered ->
      Agent.update(state, &(&1 + 1))
      send(parent, {:delivered_operation, delivered.operation_id})
      :ok
    end)

    assert {:error, :claim_lost_after_terminal_ack} =
             dispatch_once(claim_owner: "dispatcher-a")

    assert {:ok, :completed} = dispatch_once(claim_owner: "dispatcher-b")
    assert Agent.get(state, & &1) == 2
    assert_receive {:delivered_operation, @operation_id}
    assert_receive {:delivered_operation, @operation_id}
  end

  test "unsafe dispatcher timing configuration falls back inside Store bounds" do
    parent = self()

    invalid_owner = String.duplicate("a", 129)

    Application.put_env(:ret, Store, fn
      {:claim_next, claim_owner, claim_ttl_seconds: ttl} ->
        send(parent, {:bounded_config, claim_owner, ttl})
        :empty
    end)

    Application.put_env(:ret, Delivery, fn _event -> flunk("empty store must not deliver") end)

    assert :empty =
             dispatch_once(
               claim_owner: invalid_owner,
               claim_ttl_seconds: 3_601,
               retry_base_seconds: 86_401,
               retry_max_seconds: 86_401
             )

    assert_receive {:bounded_config, claim_owner, 30}
    assert is_binary(claim_owner)
    assert byte_size(claim_owner) in 1..128
    refute claim_owner == invalid_owner
  end

  defp dispatch_once(extra) do
    BotRuntimeOutboxDispatcher.dispatch_once(
      [store_module: Store, delivery_module: Delivery] ++ extra
    )
  end

  defp event(overrides) do
    defaults = %{
      id: 17,
      operation_id: @operation_id,
      hub_id: 99,
      hub_sid: "room-1",
      runtime_revision: 4,
      event_kind: "config",
      bots: %{"enabled" => true, "count" => 1},
      runtime_chat_enabled: false,
      revoke_epoch: nil,
      claim_owner: "dispatcher-a",
      claim_token: @claim_token,
      attempt_count: 1
    }

    Enum.into(overrides, defaults)
  end

  defp restore_env(key, nil), do: Application.delete_env(:ret, key)
  defp restore_env(key, value), do: Application.put_env(:ret, key, value)
end
