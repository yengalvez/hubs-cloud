defmodule Ret.BotConfig do
  @moduledoc false

  import Ecto.Query

  @max_count 10
  @max_count_text_bytes 64
  @max_prompt_codepoints 1_500
  @max_prompt_bytes 6_000
  @max_prompt_input_bytes 16_384

  # Explicitly shared with bot-orchestrator/app.js. In particular, include
  # both U+0085 (trimmed by Elixir but not ECMAScript) and U+FEFF (trimmed by
  # ECMAScript but not Elixir) so the two runtimes cannot normalize differently.
  @boundary_whitespace [
    "\u0009",
    "\u000A",
    "\u000B",
    "\u000C",
    "\u000D",
    "\u0020",
    "\u0085",
    "\u00A0",
    "\u1680",
    "\u2000",
    "\u2001",
    "\u2002",
    "\u2003",
    "\u2004",
    "\u2005",
    "\u2006",
    "\u2007",
    "\u2008",
    "\u2009",
    "\u200A",
    "\u2028",
    "\u2029",
    "\u202F",
    "\u205F",
    "\u3000",
    "\uFEFF"
  ]

  def normalize(user_data) do
    bots = map_get(user_data || %{}, "bots") || %{}

    %{
      "enabled" => normalize_bool(map_get(bots, "enabled")),
      "count" => normalize_integer(map_get(bots, "count")),
      "mobility" => normalize_mobility(map_get(bots, "mobility")),
      "chat_enabled" => normalize_bool(map_get(bots, "chat_enabled")),
      "prompt" => normalize_prompt(map_get(bots, "prompt"))
    }
  end

  def active?(user_data) do
    bots = normalize(user_data)
    bots["enabled"] && bots["count"] > 0
  end

  # This query is the database-side mirror of normalize/1 for the two fields
  # that consume runner capacity. Keep the predicates here so admission and
  # snapshot discovery cannot drift apart.
  def with_active_bot_config(query) do
    query
    |> where(
      [h],
      fragment(
        "(?->'bots'->'enabled')::text IN ('true', '1', '\"true\"')",
        h.user_data
      )
    )
    |> where(
      [h],
      fragment(
        """
        CASE jsonb_typeof(?->'bots'->'count')
          WHEN 'number' THEN
            ((?->'bots'->>'count') ~ '^[0-9]+$' AND
             (?->'bots'->>'count') ~ '[1-9]')
          WHEN 'string' THEN
            CASE WHEN octet_length(?->'bots'->>'count') <= 64
              THEN ((?->'bots'->>'count') ~ '^([0-9]+|[+][0-9]+)$' AND
                    (?->'bots'->>'count') ~ '[1-9]')
              ELSE false
            END
          ELSE false
        END
        """,
        h.user_data,
        h.user_data,
        h.user_data,
        h.user_data,
        h.user_data,
        h.user_data
      )
    )
  end

  def normalize_prompt(value)
      when is_binary(value) and byte_size(value) > @max_prompt_input_bytes,
      do: ""

  def normalize_prompt(value) when is_binary(value) do
    value
    |> String.codepoints()
    |> trim_boundary_whitespace()
    |> Enum.take(@max_prompt_codepoints)
    |> take_codepoints_within_bytes(@max_prompt_bytes)
  end

  def normalize_prompt(_), do: ""

  defp trim_boundary_whitespace(codepoints) do
    codepoints
    |> Enum.drop_while(&(&1 in @boundary_whitespace))
    |> Enum.reverse()
    |> Enum.drop_while(&(&1 in @boundary_whitespace))
    |> Enum.reverse()
  end

  defp take_codepoints_within_bytes(codepoints, max_bytes) do
    codepoints
    |> Enum.reduce_while({[], 0}, fn codepoint, {accepted, bytes} ->
      next_bytes = bytes + byte_size(codepoint)

      if next_bytes <= max_bytes do
        {:cont, {[codepoint | accepted], next_bytes}}
      else
        {:halt, {accepted, bytes}}
      end
    end)
    |> elem(0)
    |> Enum.reverse()
    |> IO.iodata_to_binary()
  end

  defp normalize_integer(value) when is_integer(value),
    do: value |> max(0) |> min(@max_count)

  defp normalize_integer(value)
       when is_binary(value) and byte_size(value) <= @max_count_text_bytes do
    case Integer.parse(value) do
      {parsed, ""} -> parsed
      _ -> 0
    end
    |> max(0)
    |> min(@max_count)
  end

  defp normalize_integer(value) when is_binary(value), do: 0

  defp normalize_integer(_), do: 0

  defp normalize_bool(true), do: true
  defp normalize_bool("true"), do: true
  defp normalize_bool(1), do: true
  defp normalize_bool(_), do: false

  defp normalize_mobility("low"), do: "low"
  defp normalize_mobility("high"), do: "high"
  defp normalize_mobility("static"), do: "static"
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
      (is_map(map) and atom_key) && Map.has_key?(map, atom_key) -> Map.get(map, atom_key)
      true -> nil
    end
  end
end
