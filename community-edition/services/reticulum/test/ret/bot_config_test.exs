defmodule Ret.BotConfigTest do
  use ExUnit.Case, async: true

  alias Ret.BotConfig

  test "uses explicit cross-runtime boundary whitespace including NEL and BOM" do
    boundary = "\u0009\u0020\u0085\u00A0\u2009\u3000\uFEFF"

    assert BotConfig.normalize_prompt(boundary <> "contenido" <> boundary) == "contenido"
    assert BotConfig.normalize_prompt("\u200Bcontenido\u200B") == "\u200Bcontenido\u200B"
  end

  test "caps prompt by Unicode codepoints and UTF-8 bytes" do
    prompt = "\u0085\uFEFF" <> String.duplicate("😀", 2_000) <> "\uFEFF\u0085"
    normalized = BotConfig.normalize_prompt(prompt)

    assert normalized == String.duplicate("😀", 1_500)
    assert normalized |> String.codepoints() |> length() == 1_500
    assert byte_size(normalized) == 6_000
  end

  test "normalizes legacy atom and string bot fields through one helper" do
    assert BotConfig.normalize(%{
             bots: %{
               enabled: 1,
               count: "99",
               mobility: "static",
               chat_enabled: "true",
               prompt: "\u0085 hola \uFEFF"
             }
           }) == %{
             "enabled" => true,
             "count" => 10,
             "mobility" => "static",
             "chat_enabled" => true,
             "prompt" => "hola"
           }
  end

  test "rejects oversized textual counts before integer parsing" do
    assert BotConfig.normalize(%{
             "bots" => %{
               "enabled" => true,
               "count" => String.duplicate("9", 65)
             }
           })["count"] == 0
  end

  test "rejects an oversized prompt before materializing its codepoints" do
    oversized = String.duplicate("x", 1_000_000)

    task = Task.async(fn -> BotConfig.normalize_prompt(oversized) end)
    assert Task.await(task, 1_000) == ""
  end
end
