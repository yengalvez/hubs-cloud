defmodule Ret.BotRuntimeOutbox do
  @moduledoc """
  Immutable desired-runtime event awaiting exact acknowledgement by the bot
  orchestrator.

  Delivery metadata is mutable, but a room, revision, operation identifier and
  config/stop payload are fixed at insertion. Database constraints mirror the
  changeset so every producer, including migration backfill, has the same
  fail-closed contract.
  """

  use Ecto.Schema

  import Ecto.Changeset

  @schema_prefix "ret0"
  @primary_key {:id, :id, autogenerate: true}

  @max_javascript_integer 9_007_199_254_740_991
  @max_config_bytes 16_384
  @max_json_depth 64
  @event_kinds ["config", "stop"]
  @failure_code ~r/\A[a-z0-9][a-z0-9_.:-]{0,63}\z/
  @hub_sid ~r/\A[A-Za-z0-9_-]{1,64}\z/
  @canonical_uuid_v4 ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/

  @immutable_event_fields [
    :operation_id,
    :hub_id,
    :hub_sid,
    :runtime_revision,
    :event_kind,
    :bots,
    :runtime_chat_enabled,
    :revoke_epoch
  ]

  schema "bot_runtime_outbox" do
    field :operation_id, Ecto.UUID
    field :hub_id, :integer
    field :hub_sid, :string
    field :runtime_revision, :integer
    field :event_kind, :string
    field :bots, :map
    field :runtime_chat_enabled, :boolean
    field :revoke_epoch, :integer
    field :claim_owner, :string
    field :claim_token, Ecto.UUID
    field :claim_expires_at, :utc_datetime_usec
    field :attempt_count, :integer, default: 0
    field :next_attempt_at, :utc_datetime_usec, read_after_writes: true
    field :delivered_at, :utc_datetime_usec
    field :last_failure_code, :string

    timestamps(type: :utc_datetime_usec)
  end

  @type t :: %__MODULE__{}

  def max_javascript_integer, do: @max_javascript_integer
  def max_config_bytes, do: @max_config_bytes
  def max_json_depth, do: @max_json_depth
  def event_kinds, do: @event_kinds

  def changeset(outbox, attrs) do
    outbox
    |> cast(attrs, @immutable_event_fields)
    |> force_exact_json_change(:bots, attrs)
    |> validate_immutable_event(outbox)
    |> validate_required([
      :operation_id,
      :hub_id,
      :hub_sid,
      :runtime_revision,
      :event_kind
    ])
    |> validate_format(:operation_id, @canonical_uuid_v4, message: "must be a canonical UUID v4")
    |> validate_format(:hub_sid, @hub_sid, message: "must match ^[A-Za-z0-9_-]{1,64}$")
    |> validate_number(:hub_id, greater_than: 0)
    |> validate_number(:runtime_revision,
      greater_than: 0,
      less_than_or_equal_to: @max_javascript_integer
    )
    |> validate_inclusion(:event_kind, @event_kinds)
    |> validate_event()
    |> unique_constraint(:operation_id, name: :bot_runtime_outbox_operation_id_idx)
    |> unique_constraint(:runtime_revision, name: :bot_runtime_outbox_hub_revision_idx)
    |> foreign_key_constraint(:hub_id, name: :bot_runtime_outbox_hub_identity_fkey)
    |> check_constraint(:operation_id, name: :bot_runtime_outbox_operation_id_v4)
    |> check_constraint(:hub_sid, name: :bot_runtime_outbox_hub_sid_valid)
    |> check_constraint(:runtime_revision, name: :bot_runtime_outbox_runtime_revision_safe)
    |> check_constraint(:event_kind, name: :bot_runtime_outbox_event_complete)
    |> check_constraint(:bots, name: :bot_runtime_outbox_bots_size)
    |> check_constraint(:bots, name: :bot_runtime_outbox_runtime_bots_size)
    |> check_constraint(:bots, name: :bot_runtime_outbox_bots_js_interoperable)
  end

  def canonical_uuid_v4?(uuid) when is_binary(uuid), do: Regex.match?(@canonical_uuid_v4, uuid)
  def canonical_uuid_v4?(_uuid), do: false

  def valid_failure_code?(code) when is_binary(code), do: Regex.match?(@failure_code, code)
  def valid_failure_code?(_code), do: false

  @doc false
  def runtime_bots_projection(%{} = bots, runtime_chat_enabled)
      when is_boolean(runtime_chat_enabled) do
    bots
    |> Map.delete(:chat_enabled)
    |> Map.put("chat_enabled", runtime_chat_enabled)
  end

  @doc false
  def runtime_bots_projection_valid?(%{} = bots, runtime_chat_enabled)
      when is_boolean(runtime_chat_enabled) do
    bots
    |> runtime_bots_projection(runtime_chat_enabled)
    |> bots_errors()
    |> Enum.empty?()
  end

  def runtime_bots_projection_valid?(_bots, _runtime_chat_enabled), do: false

  defp validate_event(changeset) do
    case get_field(changeset, :event_kind) do
      "config" ->
        changeset
        |> validate_required([:bots, :runtime_chat_enabled])
        |> validate_inclusion(:runtime_chat_enabled, [true, false])
        |> validate_bots()
        |> validate_runtime_bots_projection()
        |> reject_present(:revoke_epoch)

      "stop" ->
        changeset
        |> validate_required([:revoke_epoch])
        |> validate_number(:revoke_epoch,
          greater_than: 0,
          less_than_or_equal_to: @max_javascript_integer
        )
        |> reject_present(:bots)
        |> reject_present(:runtime_chat_enabled)

      _other ->
        changeset
    end
  end

  defp reject_present(changeset, field) do
    if is_nil(get_field(changeset, field)) do
      changeset
    else
      add_error(changeset, field, "must be absent for this event kind")
    end
  end

  defp validate_immutable_event(changeset, %__MODULE__{__meta__: %{state: :loaded}}) do
    Enum.reduce(@immutable_event_fields, changeset, fn field, validated ->
      if Map.has_key?(validated.changes, field) do
        add_error(validated, field, "is immutable once inserted")
      else
        validated
      end
    end)
  end

  defp validate_immutable_event(changeset, _outbox), do: changeset

  # Ecto's ordinary change comparison follows Elixir's loose numeric equality,
  # so nested JSON 1 and 1.0 would otherwise appear unchanged. Preserve the
  # exact JSON term change so loaded immutable events reject that mutation.
  defp force_exact_json_change(changeset, field, attrs) do
    case fetch_attr(attrs, field) do
      {:ok, value} ->
        if Map.get(changeset.data, field) === value do
          changeset
        else
          force_change(changeset, field, value)
        end

      :error ->
        changeset
    end
  end

  defp fetch_attr(attrs, field) do
    case Map.fetch(attrs, field) do
      :error -> Map.fetch(attrs, Atom.to_string(field))
      result -> result
    end
  end

  defp validate_bots(changeset) do
    validate_change(changeset, :bots, fn
      :bots, bots when is_map(bots) -> bots_errors(bots)
      :bots, _value -> [bots: "must be a JSON object"]
    end)
  end

  defp validate_runtime_bots_projection(changeset) do
    bots = get_field(changeset, :bots)
    runtime_chat_enabled = get_field(changeset, :runtime_chat_enabled)

    if runtime_bots_projection_valid?(bots, runtime_chat_enabled) do
      changeset
    else
      add_error(
        changeset,
        :bots,
        "runtime projection with normalized chat_enabled must be interoperable JSON within #{@max_config_bytes} encoded bytes"
      )
    end
  end

  defp bots_errors(bots) do
    with :ok <- validate_json_value(bots, 1),
         {:ok, jason} <- Jason.encode(bots),
         {:ok, poison} <- Poison.encode(bots),
         true <- max(byte_size(jason), byte_size(poison)) <= @max_config_bytes do
      []
    else
      {:error, :unsafe_number} ->
        [bots: "contains a number that is not interoperable with JavaScript"]

      {:error, :invalid_json} ->
        [bots: "must contain only JSON values with UTF-8 string keys"]

      {:error, :too_deep} ->
        [bots: "must not exceed #{@max_json_depth} nested JSON containers"]

      false ->
        [bots: "must not exceed #{@max_config_bytes} encoded bytes"]

      _encoding_error ->
        [bots: "must encode as interoperable JSON"]
    end
  end

  defp validate_json_value(value, _depth) when is_binary(value) do
    if String.valid?(value), do: :ok, else: {:error, :invalid_json}
  end

  defp validate_json_value(value, _depth) when is_integer(value) do
    if value >= -@max_javascript_integer and value <= @max_javascript_integer do
      :ok
    else
      {:error, :unsafe_number}
    end
  end

  defp validate_json_value(value, _depth) when is_float(value) do
    if javascript_float_roundtrips?(value), do: :ok, else: {:error, :unsafe_number}
  end

  defp validate_json_value(value, _depth) when is_boolean(value) or is_nil(value), do: :ok

  defp validate_json_value(values, depth) when is_list(values) do
    if depth <= @max_json_depth do
      validate_json_values(values, depth)
    else
      {:error, :too_deep}
    end
  end

  defp validate_json_value(%{} = map, depth) do
    if depth > @max_json_depth do
      {:error, :too_deep}
    else
      validate_json_map(map, depth)
    end
  end

  defp validate_json_value(_value, _depth), do: {:error, :invalid_json}

  defp validate_json_map(map, depth) do
    Enum.reduce_while(map, :ok, fn
      {key, value}, :ok when is_binary(key) ->
        case validate_json_value(key, depth) do
          :ok -> continue_json_validation(value, depth + container_increment(value))
          error -> {:halt, error}
        end

      _entry, :ok ->
        {:halt, {:error, :invalid_json}}
    end)
  end

  defp validate_json_values(values, depth) do
    Enum.reduce_while(values, :ok, fn value, :ok ->
      continue_json_validation(value, depth + container_increment(value))
    end)
  end

  defp continue_json_validation(value, depth) do
    case validate_json_value(value, depth) do
      :ok -> {:cont, :ok}
      error -> {:halt, error}
    end
  end

  defp container_increment(value) when is_list(value) or is_map(value), do: 1
  defp container_increment(_value), do: 0

  defp javascript_float_roundtrips?(value) do
    finite_float?(value) and safe_integral_float?(value) and
      Enum.all?([Jason, Poison], fn codec ->
        with {:ok, encoded} <- codec.encode(value),
             {:ok, decoded} when is_float(decoded) <- codec.decode(encoded) do
          same_float_bits?(value, decoded)
        else
          _error -> false
        end
      end)
  end

  defp safe_integral_float?(value) do
    if trunc(value) == value do
      value >= -@max_javascript_integer and value <= @max_javascript_integer
    else
      true
    end
  end

  defp finite_float?(value) do
    <<_sign::1, exponent::11, _fraction::52>> = <<value::float-64>>
    exponent != 0x7FF
  end

  defp same_float_bits?(left, right), do: <<left::float-64>> == <<right::float-64>>
end
