defmodule RetWeb.Api.V1.BotController do
  use RetWeb, :controller

  import Canada, only: [can?: 2]

  alias Ret.{AppConfig, BotOrchestrator, Hub, Repo}

  @max_message_length 800

  def chat(conn, %{"hub_sid" => hub_sid, "bot_id" => bot_id, "message" => message} = params)
      when is_binary(message) do
    if !AppConfig.get_cached_config_value("features|enable_bot_chat") do
      conn |> send_resp(404, "not found")
    else
      account = Guardian.Plug.current_resource(conn)

      case Hub
           |> Repo.get_by(hub_sid: hub_sid)
           |> Repo.preload([:created_by_account, :hub_bindings, :hub_role_memberships]) do
        %Hub{} = hub ->
          if account |> can?(join_hub(hub)) do
            chat_with_hub(conn, hub, bot_id, message, params["context"])
          else
            conn |> send_resp(401, "unauthorized")
          end

        _ ->
          conn |> send_resp(404, "not found")
      end
    end
  end

  def chat(conn, _params), do: conn |> send_resp(400, "message is required")

  defp chat_with_hub(conn, hub, bot_id, message, context) do
    with {:ok, bots} <- validate_bots_config(hub.user_data),
         :ok <- validate_bot_id(bot_id, bots["count"]),
         :ok <- validate_message(message),
         {:ok, response} <-
           BotOrchestrator.chat(%{
             hub_sid: hub.hub_sid,
             bot_id: bot_id,
             message: String.trim(message),
             context: normalize_context(context)
           }) do
      reply = map_get(response, "reply") || "No reply from bot."
      action = normalize_action(map_get(response, "action"))

      conn
      |> put_resp_content_type("application/json")
      |> send_resp(200, Poison.encode!(%{reply: reply, action: action}))
    else
      {:error, :bots_disabled} -> conn |> send_resp(403, "bots are disabled for this room")
      {:error, :chat_disabled} -> conn |> send_resp(403, "bot chat is disabled for this room")
      {:error, :invalid_bot_id} -> conn |> send_resp(400, "invalid bot id")
      {:error, :invalid_message} -> conn |> send_resp(400, "message must be non-empty")
      {:error, :message_too_long} -> conn |> send_resp(400, "message too long")
      {:error, _reason} -> conn |> send_resp(502, "bot service unavailable")
    end
  end

  defp validate_message(message) do
    trimmed = String.trim(message)

    cond do
      trimmed == "" -> {:error, :invalid_message}
      String.length(trimmed) > @max_message_length -> {:error, :message_too_long}
      true -> :ok
    end
  end

  defp validate_bots_config(user_data) do
    bots = map_get(user_data || %{}, "bots") || %{}
    enabled = normalize_bool(map_get(bots, "enabled"))
    chat_enabled = normalize_bool(map_get(bots, "chat_enabled"))
    count = normalize_integer(map_get(bots, "count"), 0)
    mobility = normalize_mobility(map_get(bots, "mobility"))

    cond do
      !enabled or count <= 0 -> {:error, :bots_disabled}
      !chat_enabled -> {:error, :chat_disabled}
      true -> {:ok, %{"enabled" => enabled, "chat_enabled" => chat_enabled, "count" => count, "mobility" => mobility}}
    end
  end

  defp validate_bot_id("bot-" <> suffix, count) do
    case Integer.parse(suffix) do
      {parsed, ""} when parsed >= 1 and parsed <= count -> :ok
      _ -> {:error, :invalid_bot_id}
    end
  end

  defp validate_bot_id(_bot_id, _count), do: {:error, :invalid_bot_id}

  defp normalize_action(%{} = action) do
    type = map_get(action, "type")

    case type do
      "go_to_waypoint" ->
        waypoint = map_get(action, "waypoint")
        if is_binary(waypoint) and waypoint != "" do
          %{"type" => "go_to_waypoint", "waypoint" => waypoint}
        else
          nil
        end

      _ ->
        nil
    end
  end

  defp normalize_action(_), do: nil

  defp normalize_context(%{} = context), do: context
  defp normalize_context(_), do: %{}

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
