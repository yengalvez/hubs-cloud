defmodule Ret.StatixTest do
  use ExUnit.Case, async: true

  test "formats StatsD metrics with the legacy prefix and options" do
    assert Ret.Statix.metric_packet(
             :counter,
             "ret.channels.hub.joins.ok",
             1,
             [sample_rate: 0.5, tags: ["source:channel"]],
             prefix: "ret",
             tags: ["env:test"]
           ) ==
             "ret.ret.channels.hub.joins.ok:1|c|@0.5|#source:channel,env:test"
  end

  test "formats gauges without optional suffixes" do
    assert Ret.Statix.metric_packet(:gauge, "ret.nodes", 2, [], prefix: "ret") ==
             "ret.ret.nodes:2|g"
  end
end
