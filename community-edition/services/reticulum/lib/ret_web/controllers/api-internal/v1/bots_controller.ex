defmodule RetWeb.ApiInternal.V1.BotsController do
  use RetWeb, :controller

  import Ecto.Query

  alias Ret.{BotConfig, BotConfigApproval, Hub, Repo}

  @max_configured_bot_hubs 10
  @max_bot_config_bytes 16_384

  def configured_with_bots(conn, _params) do
    # Used by the bot-orchestrator to rehydrate desired runner state after restarts.
    # This lists hubs by config (user_data.bots) rather than Presence.
    Hub
    |> where([h], h.hub_sid != "admin")
    |> where([h], is_nil(h.entry_mode) or h.entry_mode != :deny)
    |> BotConfig.with_active_bot_config()
    |> BotConfigApproval.with_runtime_approval()
    |> send_bot_snapshot(conn)
  end

  def active_with_bots(conn, _params) do
    # Use Presence as the source of truth for "active rooms".
    #
    # Relying on `hubs.last_active_at` is brittle because that column is not
    # consistently updated by all room activity paths in our deployment.
    present_hub_sids =
      RetWeb.Presence.present_hub_sids()
      |> Enum.filter(& &1)
      |> Enum.reject(&(&1 == "admin"))
      |> Enum.uniq()

    Hub
    |> where([h], h.hub_sid in ^present_hub_sids)
    |> where([h], h.hub_sid != "admin")
    |> where([h], is_nil(h.entry_mode) or h.entry_mode != :deny)
    |> BotConfig.with_active_bot_config()
    |> BotConfigApproval.with_runtime_approval()
    |> send_bot_snapshot(conn)
  end

  defp send_bot_snapshot(query, conn) do
    candidates =
      query
      |> order_by([h], asc: h.hub_sid)
      |> limit(^(@max_configured_bot_hubs + 1))
      |> select([h], %{
        hub_sid: h.hub_sid,
        bot_config_bytes: fragment("octet_length((?->'bots')::text)", h.user_data),
        bots:
          fragment(
            "CASE WHEN octet_length((?->'bots')::text) <= ? THEN ?->'bots' ELSE '{}'::jsonb END",
            h.user_data,
            ^@max_bot_config_bytes,
            h.user_data
          ),
        last_active_at: h.last_active_at
      })
      |> Repo.all()

    cond do
      Enum.any?(candidates, &(&1.bot_config_bytes > @max_bot_config_bytes)) ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          409,
          Poison.encode!(%{
            error: "bot_config_too_large",
            max_bot_config_bytes: @max_bot_config_bytes
          })
        )

      length(candidates) > @max_configured_bot_hubs ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          409,
          Poison.encode!(%{
            error: "configured_room_limit_exceeded",
            max_configured_rooms: @max_configured_bot_hubs
          })
        )

      true ->
        hubs = candidates |> Enum.map(&to_bot_hub_entry/1) |> Enum.filter(& &1)

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Poison.encode!(%{hubs: hubs}))
    end
  end

  defp to_bot_hub_entry(%{hub_sid: hub_sid, bots: raw_bots, last_active_at: last_active_at}) do
    bots = BotConfig.normalize(%{"bots" => raw_bots})
    enabled = bots["enabled"]
    count = bots["count"]

    if enabled && count > 0 do
      %{
        hub_sid: hub_sid,
        bots: %{
          enabled: enabled,
          count: count,
          mobility: bots["mobility"],
          chat_enabled: bots["chat_enabled"],
          prompt: bots["prompt"]
        },
        last_active_at: serialize_datetime(last_active_at)
      }
    else
      nil
    end
  end

  defp serialize_datetime(nil), do: nil
  defp serialize_datetime(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp serialize_datetime(%DateTime{} = value), do: DateTime.to_iso8601(value)
end
