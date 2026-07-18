defmodule Ret.BotRunnerGenerationTokenTest do
  use ExUnit.Case, async: false

  alias Ret.BotRunnerGenerationToken

  @key "test-orchestrator-generation-key-at-least-32"
  @hub_sid "Room_123"
  @generation "11111111-1111-4111-8111-111111111111"
  @holder_id "22222222-2222-4222-8222-222222222222"
  @now 2_000_000_000

  setup do
    original = Application.get_env(:ret, :bot_orchestrator_access_key)
    Application.put_env(:ret, :bot_orchestrator_access_key, @key)

    on_exit(fn ->
      if is_nil(original),
        do: Application.delete_env(:ret, :bot_orchestrator_access_key),
        else: Application.put_env(:ret, :bot_orchestrator_access_key, original)
    end)
  end

  test "accepts only the signed room and generation scope before expiry" do
    token = token(%{"exp" => @now + 300})

    assert {:ok, claims} = BotRunnerGenerationToken.verify(token, @hub_sid, @now)
    assert claims["process_generation"] == @generation
    assert claims["holder_id"] == @holder_id

    assert :error = BotRunnerGenerationToken.verify(token, "another-room", @now)
    assert :error = BotRunnerGenerationToken.verify(token, @hub_sid, @now + 331)

    assert :error =
             BotRunnerGenerationToken.verify(token(%{"exp" => @now + 86_431}), @hub_sid, @now)
  end

  test "rejects tampering, unknown authority claims and weak signing keys" do
    token = token(%{"exp" => @now + 300})
    assert :error = BotRunnerGenerationToken.verify(token <> "x", @hub_sid, @now)

    assert :error =
             BotRunnerGenerationToken.verify(
               token(%{"exp" => @now + 300, "extra" => true}),
               @hub_sid,
               @now
             )

    for claim <- [%{"fence_epoch" => 17}, %{"lease_id" => SecureRandom.uuid()}] do
      assert :error =
               BotRunnerGenerationToken.verify(
                 token(Map.merge(%{"exp" => @now + 300}, claim)),
                 @hub_sid,
                 @now
               )
    end

    Application.put_env(:ret, :bot_orchestrator_access_key, "short")
    assert :error = BotRunnerGenerationToken.verify(token, @hub_sid, @now)
  end

  defp token(overrides) do
    payload =
      Map.merge(
        %{
          "v" => 1,
          "aud" => "yenhubs-bot-runner",
          "hub_sid" => @hub_sid,
          "process_generation" => @generation,
          "holder_id" => @holder_id
        },
        overrides
      )

    encoded_payload = payload |> Jason.encode!() |> Base.url_encode64(padding: false)

    signature =
      :crypto.mac(:hmac, :sha256, @key, "v1.#{encoded_payload}")
      |> Base.url_encode64(padding: false)

    "v1.#{encoded_payload}.#{signature}"
  end
end
