defmodule Ret.BotOrchestrator do
  @moduledoc false

  require Logger

  @request_timeout_ms 5_000

  def chat(%{} = payload), do: post_json("/internal/bots/chat", payload)
  def room_config(%{} = payload), do: post_json("/internal/bots/room-config", payload)
  def room_stop(%{} = payload), do: post_json("/internal/bots/room-stop", payload)

  defp post_json(path, payload) do
    url = "#{endpoint()}#{path}"
    headers = [{"Content-Type", "application/json"}] ++ auth_header()

    case HTTPoison.post(url, Poison.encode!(payload), headers,
           timeout: @request_timeout_ms,
           recv_timeout: @request_timeout_ms
         ) do
      {:ok, %HTTPoison.Response{status_code: status_code, body: body}}
      when status_code >= 200 and status_code < 300 ->
        {:ok, decode_json(body)}

      {:ok, %HTTPoison.Response{status_code: status_code, body: body}} ->
        Logger.warning(
          "Bot orchestrator request failed with status=#{status_code} path=#{path} body=#{String.slice(body || "", 0, 300)}"
        )

        {:error, {:http_error, status_code}}

      {:error, %HTTPoison.Error{reason: reason}} ->
        Logger.warning("Bot orchestrator request failed path=#{path} reason=#{inspect(reason)}")
        {:error, {:request_error, reason}}
    end
  end

  defp decode_json(nil), do: %{}
  defp decode_json(""), do: %{}

  defp decode_json(body) when is_binary(body) do
    case Poison.decode(body) do
      {:ok, decoded} when is_map(decoded) -> decoded
      _ -> %{}
    end
  end

  defp auth_header do
    case access_key() do
      "" -> []
      nil -> []
      key -> [{"x-ret-bot-access-key", key}]
    end
  end

  defp endpoint do
    (Application.get_env(:ret, __MODULE__)[:endpoint] || "http://bot-orchestrator:5001")
    |> String.trim_trailing("/")
  end

  defp access_key do
    Application.get_env(:ret, __MODULE__)[:access_key] || Application.get_env(:ret, :bot_access_key) || ""
  end
end
