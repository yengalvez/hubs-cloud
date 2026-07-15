defmodule Ret.Statix do
  @moduledoc """
  Minimal asynchronous StatsD client used by Reticulum.

  The former Statix dependency writes directly to an Erlang port and waits for
  an internal `:inet_reply`. That implementation blocks indefinitely on modern
  OTP releases. Keeping the existing Ret.Statix API avoids changing callers,
  while a supervised GenServer owns the UDP socket and keeps metric emission
  off request and channel processes.
  """

  use GenServer

  require Logger

  @memory_stats ~w(atom binary ets processes system processes_used atom_used ets total)a
  @metric_types %{counter: "c", gauge: "g", histogram: "h", timing: "ms", set: "s"}

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def increment(key, value \\ 1, options \\ []),
    do: emit(:counter, key, value, options)

  def decrement(key, value \\ 1, options \\ []),
    do: emit(:counter, key, -value, options)

  def gauge(key, value, options \\ []),
    do: emit(:gauge, key, value, options)

  def histogram(key, value, options \\ []),
    do: emit(:histogram, key, value, options)

  def timing(key, value, options \\ []),
    do: emit(:timing, key, value, options)

  def set(key, value, options \\ []),
    do: emit(:set, key, value, options)

  def send_gauges do
    for stat <- @memory_stats, do: gauge("erl.memory.#{stat}", :erlang.memory(stat))
    gauge("ret.present_sessions", RetWeb.Presence.present_session_count())
    gauge("ret.present_rooms", RetWeb.Presence.present_room_count())
    gauge("ret.nodes", Enum.count(Node.list()) + 1)
  end

  @doc false
  def metric_packet(type, key, value, options \\ [], config \\ []) do
    prefix = Keyword.get(config, :prefix)
    tags = Keyword.get(options, :tags, []) ++ Keyword.get(config, :tags, [])
    sample_rate = Keyword.get(options, :sample_rate)

    metric_name =
      [prefix, key]
      |> Enum.reject(&(&1 in [nil, ""]))
      |> Enum.join(".")

    [metric_name, ":", to_string(value), "|", Map.fetch!(@metric_types, type)]
    |> append_sample_rate(sample_rate)
    |> append_tags(tags)
    |> IO.iodata_to_binary()
  end

  @impl true
  def init(opts) do
    config = statix_config(Keyword.get(opts, :config, []))

    state =
      with {:ok, address} <- resolve_address(config[:host]),
           {:ok, socket} <- :gen_udp.open(0, [:binary, active: false]) do
        %{address: address, config: config, socket: socket}
      else
        {:error, reason} ->
          Logger.warning(
            "StatsD disabled because its UDP socket could not be initialized: #{inspect(reason)}"
          )

          %{address: nil, config: config, socket: nil}
      end

    {:ok, state}
  end

  @impl true
  def handle_cast({:metric, type, key, value, options}, state) do
    packet = metric_packet(type, key, value, options, state.config)

    if state.socket && state.address do
      case :gen_udp.send(state.socket, state.address, state.config[:port], packet) do
        :ok -> :ok
        {:error, reason} -> Logger.debug("StatsD metric dropped: #{inspect(reason)}")
      end
    end

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{socket: socket}) when not is_nil(socket) do
    :gen_udp.close(socket)
  end

  def terminate(_reason, _state), do: :ok

  defp emit(type, key, value, options) when is_list(options) do
    sample_rate = Keyword.get(options, :sample_rate)

    if should_sample?(sample_rate) && Process.whereis(__MODULE__) do
      GenServer.cast(__MODULE__, {:metric, type, key, value, options})
    end

    :ok
  end

  defp should_sample?(nil), do: true
  defp should_sample?(rate) when is_number(rate), do: rate >= :rand.uniform()

  defp statix_config(overrides) do
    :ret
    |> Application.get_env(__MODULE__, [])
    |> Keyword.merge(overrides)
    |> Keyword.put_new(:host, "127.0.0.1")
    |> Keyword.put_new(:port, 8125)
    |> Keyword.put_new(:tags, [])
  end

  defp resolve_address(host) when is_tuple(host), do: {:ok, host}

  defp resolve_address(host) when is_binary(host),
    do: :inet.getaddr(String.to_charlist(host), :inet)

  defp resolve_address(host) when is_list(host), do: :inet.getaddr(host, :inet)

  defp append_sample_rate(packet, nil), do: packet

  defp append_sample_rate(packet, sample_rate) when is_number(sample_rate) do
    [packet, "|@", :erlang.float_to_binary(sample_rate / 1, [:compact, decimals: 4])]
  end

  defp append_tags(packet, []), do: packet
  defp append_tags(packet, tags), do: [packet, "|#", Enum.join(tags, ",")]
end
