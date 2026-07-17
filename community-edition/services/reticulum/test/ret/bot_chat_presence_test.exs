defmodule Ret.BotChatPresenceTest do
  use ExUnit.Case, async: false

  alias Ret.BotChatPresence

  test "presence is exact to hub/account and disappears with the channel process" do
    channel_pid = spawn(fn -> receive do: (:stop -> :ok) end)
    monitor_ref = Process.monitor(channel_pid)

    assert :ok = BotChatPresence.track(channel_pid, "room-a", "account-a", "capability-a")
    assert BotChatPresence.present?("room-a", "account-a", "capability-a")
    refute BotChatPresence.present?("room-b", "account-a", "capability-a")
    refute BotChatPresence.present?("room-a", "account-b", "capability-a")
    refute BotChatPresence.present?("room-a", "account-a", "capability-b")

    send(channel_pid, :stop)
    assert_receive {:DOWN, ^monitor_ref, :process, ^channel_pid, :normal}
    refute BotChatPresence.present?("room-a", "account-a", "capability-a")
  end

  test "retracking a channel replaces its previous authorization exactly" do
    assert :ok = BotChatPresence.track(self(), "room-a", "account-a", "capability-a")
    assert :ok = BotChatPresence.track(self(), "room-b", "account-b", "capability-b")

    refute BotChatPresence.present?("room-a", "account-a", "capability-a")
    assert BotChatPresence.present?("room-b", "account-b", "capability-b")
    assert :ok = BotChatPresence.untrack(self())
    refute BotChatPresence.present?("room-b", "account-b", "capability-b")
  end
end
