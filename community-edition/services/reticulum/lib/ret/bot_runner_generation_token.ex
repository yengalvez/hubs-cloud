defmodule Ret.BotRunnerGenerationToken do
  @moduledoc false

  @version "v1"
  @audience "yenhubs-bot-runner"
  @max_token_bytes 2_048
  @clock_skew_seconds 30
  @max_token_ttl_seconds 3_600
  @uuid_v4 ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
  @hub_sid ~r/^[A-Za-z0-9_-]{1,64}$/
  @holder_id ~r/^[A-Za-z0-9_.:-]{1,128}$/
  @required_keys ~w(aud exp holder_id hub_sid process_generation recovery_epoch v)

  def verify(token, hub_sid, now_seconds \\ System.system_time(:second))

  def verify(token, hub_sid, now_seconds)
      when is_binary(token) and is_binary(hub_sid) and is_integer(now_seconds) and
             byte_size(token) <= @max_token_bytes do
    key = Application.get_env(:ret, :bot_orchestrator_access_key)

    with true <- is_binary(key) and byte_size(key) >= 32,
         [@version, encoded_payload, provided_signature] <- String.split(token, "."),
         true <- encoded_payload != "" and provided_signature != "",
         expected_signature <- sign(encoded_payload, key),
         true <- secure_match?(provided_signature, expected_signature),
         {:ok, payload_json} <- Base.url_decode64(encoded_payload, padding: false),
         true <- Base.url_encode64(payload_json, padding: false) == encoded_payload,
         {:ok, payload} <- Jason.decode(payload_json),
         true <- valid_payload?(payload, hub_sid, now_seconds) do
      {:ok, payload}
    else
      _ -> :error
    end
  end

  def verify(_, _, _), do: :error

  @doc false
  def valid_claims?(claims, hub_sid, now_seconds \\ System.system_time(:second))

  def valid_claims?(claims, hub_sid, now_seconds)
      when is_map(claims) and is_binary(hub_sid) and is_integer(now_seconds),
      do: valid_payload?(claims, hub_sid, now_seconds)

  def valid_claims?(_, _, _), do: false

  @doc false
  def valid_claims_after_lock?(claims, hub_sid, database_now_seconds)

  def valid_claims_after_lock?(claims, hub_sid, database_now_seconds)
      when is_map(claims) and is_binary(hub_sid) and is_integer(database_now_seconds) do
    valid_payload?(claims, hub_sid, database_now_seconds) and
      claims["exp"] > database_now_seconds and
      claims["exp"] <= database_now_seconds + @max_token_ttl_seconds
  end

  def valid_claims_after_lock?(_, _, _), do: false

  defp sign(encoded_payload, key) do
    :crypto.mac(:hmac, :sha256, key, "#{@version}.#{encoded_payload}")
    |> Base.url_encode64(padding: false)
  end

  defp secure_match?(provided, expected)
       when is_binary(provided) and is_binary(expected) and
              byte_size(provided) == byte_size(expected),
       do: Plug.Crypto.secure_compare(provided, expected)

  defp secure_match?(_, _), do: false

  defp valid_payload?(payload, hub_sid, now_seconds) when is_map(payload) do
    keys = Map.keys(payload)
    recovery_epoch = Application.get_env(:ret, :bot_runner_recovery_epoch)

    Enum.all?(@required_keys, &(&1 in keys)) and
      length(keys) == length(@required_keys) and
      Enum.all?(keys, &(&1 in @required_keys)) and
      payload["v"] === 1 and
      payload["aud"] === @audience and
      payload["hub_sid"] === hub_sid and
      Regex.match?(@hub_sid, payload["hub_sid"]) and
      is_binary(payload["process_generation"]) and
      Regex.match?(@uuid_v4, payload["process_generation"]) and
      is_binary(payload["holder_id"]) and
      Regex.match?(@holder_id, payload["holder_id"]) and
      is_binary(recovery_epoch) and
      Regex.match?(@uuid_v4, recovery_epoch) and
      payload["recovery_epoch"] === recovery_epoch and
      is_integer(payload["exp"]) and
      payload["exp"] > now_seconds - @clock_skew_seconds and
      payload["exp"] <= now_seconds + @max_token_ttl_seconds
  end

  defp valid_payload?(_, _, _), do: false
end
