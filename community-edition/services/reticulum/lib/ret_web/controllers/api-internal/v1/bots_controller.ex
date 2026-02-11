defmodule RetWeb.ApiInternal.V1.BotsController do
  use RetWeb, :controller

  import Ecto.Query

  alias Ret.{Hub, Repo}

  @active_window_secs 900

  def active_with_bots(conn, _params) do
    cutoff = DateTime.utc_now() |> DateTime.add(-@active_window_secs, :second)

    hubs =
      Hub
      |> where([h], h.last_active_at > ^cutoff)
      |> select([h], %{hub_sid: h.hub_sid, user_data: h.user_data, last_active_at: h.last_active_at})
      |> Repo.all()
      |> Enum.map(&to_bot_hub_entry/1)
      |> Enum.filter(& &1)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Poison.encode!(%{hubs: hubs}))
  end

  defp to_bot_hub_entry(%{hub_sid: hub_sid, user_data: user_data, last_active_at: last_active_at}) do
    bots = map_get(user_data || %{}, "bots") || %{}
    enabled = normalize_bool(map_get(bots, "enabled"))
    count = normalize_integer(map_get(bots, "count"), 0)

    if enabled && count > 0 do
      mobility = normalize_mobility(map_get(bots, "mobility"))
      chat_enabled = normalize_bool(map_get(bots, "chat_enabled"))

      %{
        hub_sid: hub_sid,
        bots: %{
          enabled: enabled,
          count: count,
          mobility: mobility,
          chat_enabled: chat_enabled
        },
        last_active_at: NaiveDateTime.to_iso8601(last_active_at)
      }
    else
      nil
    end
  end

  defp normalize_integer(value, _default) when is_integer(value), do: value

  defp normalize_integer(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} -> parsed
      _ -> default
    end
  end

  defp normalize_integer(_, default), do: default

  defp normalize_bool(true), do: true
  defp normalize_bool("true"), do: true
  defp normalize_bool(1), do: true
  defp normalize_bool(_), do: false

  defp normalize_mobility("low"), do: "low"
  defp normalize_mobility("high"), do: "high"
  defp normalize_mobility(_), do: "medium"

  defp map_get(map, key) do
    atom_key =
      if is_binary(key) do
        try do
          String.to_existing_atom(key)
        rescue
          ArgumentError -> nil
        end
      else
        key
      end

    cond do
      is_map(map) and Map.has_key?(map, key) -> Map.get(map, key)
      is_map(map) and atom_key != nil and Map.has_key?(map, atom_key) -> Map.get(map, atom_key)
      true -> nil
    end
  end
end
