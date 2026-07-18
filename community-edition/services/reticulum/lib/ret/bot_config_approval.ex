defmodule Ret.BotConfigApproval do
  @moduledoc """
  Durable, fail-closed approval state for room bot configuration.

  A room is runnable only while its approval row is `approved` and the exact
  approved JSON object still matches `hubs.user_data.bots`. Fingerprints are
  deterministic hashes of the exact canonical JSON candidate, including
  unknown fields and JSON types. They are redacted references for
  administrator decisions; they never replace the exact database comparison
  used at runtime.
  """

  use Ecto.Schema

  import Ecto.Changeset
  import Ecto.Query

  alias Ret.{Account, BotConfig, BotConfigApproval, BotRunnerLease, Hub, Locking, Repo}

  @schema_prefix "ret0"
  @primary_key {:hub_id, :id, autogenerate: false}

  @protocol 1
  @admission_lock "bot-config-admission:v1"
  @max_config_bytes 16_384
  @default_page_size 50
  @max_page_size 100

  schema "bot_config_approvals" do
    field :state, :string
    field :candidate_bots, :map
    field :approved_bots, :map
    field :approved_by_account_id, :integer
    field :approved_at, :utc_datetime_usec
    field :last_quarantined_by_account_id, :integer
    field :last_quarantined_at, :utc_datetime_usec
    field :last_quarantine_reason, :string

    timestamps(type: :utc_datetime_usec)
  end

  def capability_contract do
    %{
      protocol: @protocol,
      legacy_default: "quarantined",
      runtime_match: "exact_jsonb"
    }
  end

  def admission_lock, do: @admission_lock
  def max_config_bytes, do: @max_config_bytes

  def raw_bots(user_data) when is_map(user_data) do
    case Map.get(user_data, "bots") || Map.get(user_data, :bots) do
      %{} = bots -> stringify_keys(bots)
      _ -> nil
    end
  end

  def raw_bots(_user_data), do: nil

  def fingerprint(%{} = bots) do
    canonical = canonical_json(bots)

    with :ok <- validate_canonical_size(canonical) do
      digest =
        :crypto.hash(:sha256, "yenhubs-bot-config:v#{@protocol}\n" <> canonical)
        |> Base.encode16(case: :lower)

      {:ok, "v#{@protocol}:#{digest}"}
    end
  rescue
    _ -> {:error, :invalid_config}
  end

  def fingerprint(_bots), do: {:error, :invalid_config}

  def runtime_approved?(%Hub{} = hub) do
    match?({:ok, _decision}, approved_decision(hub))
  end

  def runtime_enabled?(%Hub{} = hub) do
    match?({:ok, _decision}, runtime_decision(hub))
  end

  def runtime_decision(%Hub{entry_mode: entry_mode} = hub) when entry_mode != :deny,
    do: approved_decision(hub)

  def runtime_decision(%Hub{}), do: {:error, :bot_config_unapproved}

  @doc """
  Registers a process-bound runner lease with the durable lock order used by
  approval and quarantine changes.

  The order is the global admission advisory lock, the Hub and approval rows,
  then the room-specific runner authority advisory/row lock. The PostgreSQL
  lease acquisition executes in this caller and therefore on the existing
  admission transaction connection; the local coordinator never opens a
  second database wait while the caller holds the global lock.
  """
  def register_runtime_lease(hub_sid),
    do: register_runtime_lease(hub_sid, SecureRandom.uuid())

  def register_runtime_lease(hub_sid, session_id)
      when is_binary(hub_sid) and byte_size(hub_sid) > 0 and is_binary(session_id) do
    result =
      Locking.exec_after_lock(@admission_lock, fn ->
        with %Hub{} = hub <- hub_for_update(hub_sid),
             %BotConfigApproval{} = approval <- approval_for_update(hub.hub_id),
             true <- hub.entry_mode != :deny and runtime_approved?(hub, approval) do
          BotRunnerLease.register_for_session(hub_sid, session_id)
        else
          _ -> {:error, :bot_config_unapproved}
        end
      end)

    case result do
      {:ok, registration_result} -> registration_result
      {:error, _reason} -> fail_registration_closed(hub_sid)
    end
  rescue
    _ -> fail_registration_closed(hub_sid)
  catch
    :exit, _reason -> fail_registration_closed(hub_sid)
  end

  def register_runtime_lease(_hub_sid, _session_id), do: {:error, :bot_config_unapproved}

  @doc """
  Executes a bounded runtime side effect only if the exact approval decision is
  still current while holding the same durable lock as quarantine.

  This makes post-provider bot chat delivery linearizable with approval
  changes: delivery wins the lock and completes first, or quarantine wins and
  the stale delivery is rejected.
  """
  def with_current_runtime_decision(hub_id, expected_decision, fun)
      when is_integer(hub_id) and is_map(expected_decision) and is_function(fun, 0) do
    result =
      Locking.exec_after_lock(@admission_lock, fn ->
        with %Hub{} = current_hub <- hub_for_update_by_id(hub_id),
             %BotConfigApproval{} = approval <- approval_for_update(current_hub.hub_id),
             {:ok, ^expected_decision} <- locked_runtime_decision(current_hub, approval) do
          case fun.() do
            {:ok, _value} = ok -> ok
            {:error, _reason} = error -> error
            value -> {:ok, value}
          end
        else
          _ -> {:error, :bot_config_unapproved}
        end
      end)

    case result do
      {:ok, decision_result} -> decision_result
      {:error, _reason} -> {:error, :bot_config_unavailable}
    end
  rescue
    _error in DBConnection.ConnectionError -> {:error, :bot_config_unavailable}
  catch
    :exit, _reason -> {:error, :bot_config_unavailable}
  end

  def with_current_runtime_decision(_hub_id, _expected_decision, _fun),
    do: {:error, :bot_config_unapproved}

  defp fail_registration_closed(hub_sid) do
    _ = BotRunnerLease.revoke(hub_sid)
    {:error, :bot_config_unavailable}
  catch
    :exit, _reason -> {:error, :bot_config_unavailable}
  end

  def runtime_approved?(%Hub{} = hub, %BotConfigApproval{} = approval) do
    bots = raw_bots(hub.user_data)

    is_map(bots) and approval.state == "approved" and approval.approved_bots == bots and
      valid_runtime_config?(bots)
  end

  def runtime_approved?(_hub, _approval), do: false

  def with_runtime_approval(query) do
    where(
      query,
      [h],
      fragment(
        """
        EXISTS (
          SELECT 1
          FROM ret0.bot_config_approvals AS bca
          WHERE bca.hub_id = ?
            AND bca.state = 'approved'
            AND bca.approved_bots = ?->'bots'
        )
        """,
        h.hub_id,
        h.user_data
      )
    )
  end

  def configured_active_room_count do
    Hub
    |> where([h], h.hub_sid != "admin")
    |> where([h], is_nil(h.entry_mode) or h.entry_mode != :deny)
    |> BotConfig.with_active_bot_config()
    |> with_runtime_approval()
    |> select([h], count(h.hub_id))
    |> Repo.one()
  end

  def validate_admitted_change(current_hub, intended_user_data, actor) do
    current_bots = current_hub && raw_bots(current_hub.user_data)
    intended_bots = raw_bots(intended_user_data)

    if current_bots != intended_bots and is_map(intended_bots) and
         BotConfig.active?(%{"bots" => intended_bots}) and fresh_admin?(actor) do
      case validate_persisted_size(intended_bots) do
        :ok ->
          :ok

        {:error, :config_too_large} ->
          {:error, :bot_config_too_large,
           "bot configuration exceeds the 16384 byte approval limit"}

        {:error, :invalid_config} ->
          {:error, :bot_config_invalid, "bot configuration cannot be fingerprinted"}
      end
    else
      :ok
    end
  end

  def record_admitted_change!(current_hub, %Hub{} = persisted_hub, actor) do
    current_bots = current_hub && raw_bots(current_hub.user_data)
    persisted_bots = raw_bots(persisted_hub.user_data)

    cond do
      room_closed?(current_hub, persisted_hub) and
          (is_map(current_bots) or is_map(persisted_bots)) ->
        quarantine_exact!(
          persisted_hub,
          current_bots || persisted_bots || %{},
          actor,
          "room_closed"
        )

      current_bots != persisted_bots ->
        cond do
          is_map(persisted_bots) and BotConfig.active?(%{"bots" => persisted_bots}) and
              fresh_admin?(actor) ->
            approve_exact!(persisted_hub, persisted_bots, actor)

          true ->
            quarantine_exact!(
              persisted_hub,
              persisted_bots || %{},
              actor,
              quarantine_reason(current_hub, persisted_hub, persisted_bots)
            )
        end

      true ->
        :ok
    end

    persisted_hub
  end

  def approve_candidate(hub_sid, expected_fingerprint, %Account{} = actor)
      when is_binary(hub_sid) and is_binary(expected_fingerprint) do
    run_locked(fn ->
      with {:ok, current_actor} <- current_admin(actor),
           %Hub{} = hub <- hub_for_update(hub_sid),
           %BotConfigApproval{} = approval <- approval_for_update(hub.hub_id),
           {:ok, actual_fingerprint} <- fingerprint(approval.candidate_bots),
           :ok <- verify_fingerprint(expected_fingerprint, actual_fingerprint),
           :ok <- validate_active_candidate(approval.candidate_bots),
           :ok <- admit_candidate_capacity(hub, approval.candidate_bots),
           {:ok, persisted_hub} <- persist_candidate(hub, approval.candidate_bots) do
        bots = raw_bots(persisted_hub.user_data)
        approve_exact!(persisted_hub, bots, current_actor)
        {:ok, persisted_hub}
      else
        nil -> {:error, :not_found}
        {:error, %Ecto.Changeset{}} -> {:error, :invalid_candidate}
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  def approve_candidate(_hub_sid, _expected_fingerprint, _actor),
    do: {:error, :invalid_request}

  def quarantine(hub_sid, %Account{} = actor) when is_binary(hub_sid) do
    run_locked(fn ->
      with {:ok, current_actor} <- current_admin(actor),
           %Hub{} = hub <- hub_for_update(hub_sid) do
        approval = approval_for_update(hub.hub_id)

        candidate =
          case approval do
            %BotConfigApproval{state: "quarantined", candidate_bots: %{} = bots} -> bots
            _ -> raw_bots(hub.user_data) || %{}
          end

        with {:ok, persisted_hub} <- disable_current_bots(hub) do
          quarantine_exact!(
            persisted_hub,
            candidate,
            current_actor,
            "admin_quarantine"
          )

          {:ok, persisted_hub}
        else
          {:error, %Ecto.Changeset{}} -> {:error, :invalid_config}
        end
      else
        nil -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  def quarantine(_hub_sid, _actor), do: {:error, :invalid_request}

  def inventory(params \\ %{}) do
    limit = parse_limit(Map.get(params, "limit") || Map.get(params, :limit))
    cursor = parse_cursor(Map.get(params, "cursor") || Map.get(params, :cursor))

    query =
      from a in BotConfigApproval,
        join: h in Hub,
        on: h.hub_id == a.hub_id,
        order_by: [asc: a.hub_id],
        limit: ^(limit + 1),
        select: {a, h, fragment("octet_length(?::text)", a.candidate_bots)}

    query = if cursor, do: where(query, [a, _h], a.hub_id > ^cursor), else: query
    rows = Repo.all(query)
    page = Enum.take(rows, limit)

    next_cursor =
      if length(rows) > limit do
        page |> List.last() |> elem(0) |> Map.fetch!(:hub_id) |> Integer.to_string()
      end

    %{
      protocol: @protocol,
      approvals:
        Enum.map(page, fn {approval, hub, candidate_bytes} ->
          inventory_entry(approval, hub, candidate_bytes)
        end),
      next_cursor: next_cursor
    }
  end

  def inventory_entry(%BotConfigApproval{} = approval, %Hub{} = hub) do
    inventory_entry(approval, hub, persisted_jsonb_bytes(approval.candidate_bots))
  end

  defp inventory_entry(%BotConfigApproval{} = approval, %Hub{} = hub, candidate_bytes) do
    current_bots = raw_bots(hub.user_data)

    %{
      hub_sid: hub.hub_sid,
      state: approval.state,
      candidate_config_fingerprint:
        approvable_fingerprint_or_nil(approval.candidate_bots, candidate_bytes),
      approved_config_fingerprint: fingerprint_or_nil(approval.approved_bots),
      current_config_fingerprint: fingerprint_or_nil(current_bots),
      candidate_summary: redacted_summary(approval.candidate_bots),
      current_summary: redacted_summary(current_bots),
      runtime_approved: runtime_approved?(hub, approval) and hub.entry_mode != :deny,
      created_by_account_id: hub.created_by_account_id,
      entry_mode: hub.entry_mode,
      approved_by_account_id: approval.approved_by_account_id,
      approved_at: approval.approved_at,
      last_quarantined_by_account_id: approval.last_quarantined_by_account_id,
      last_quarantined_at: approval.last_quarantined_at,
      last_quarantine_reason: approval.last_quarantine_reason,
      updated_at: approval.updated_at
    }
  end

  defp approve_exact!(hub, bots, %Account{} = actor) do
    now = DateTime.utc_now()

    hub.hub_id
    |> approval_changeset(%{
      state: "approved",
      candidate_bots: bots,
      approved_bots: bots,
      approved_by_account_id: actor.account_id,
      approved_at: now
    })
    |> Repo.insert_or_update!()
  end

  defp quarantine_exact!(hub, candidate_bots, actor, reason) do
    now = DateTime.utc_now()

    approval =
      hub.hub_id
      |> approval_changeset(%{
        state: "quarantined",
        candidate_bots: stringify_keys(candidate_bots),
        approved_bots: nil,
        approved_by_account_id: nil,
        approved_at: nil,
        last_quarantined_by_account_id: actor_account_id(actor),
        last_quarantined_at: now,
        last_quarantine_reason: reason
      })
      |> Repo.insert_or_update!()

    :ok = BotRunnerLease.revoke(hub.hub_sid)
    approval
  end

  defp approval_changeset(hub_id, attrs) do
    approval = Repo.get(BotConfigApproval, hub_id) || %BotConfigApproval{hub_id: hub_id}

    approval
    |> cast(attrs, [
      :state,
      :candidate_bots,
      :approved_bots,
      :approved_by_account_id,
      :approved_at,
      :last_quarantined_by_account_id,
      :last_quarantined_at,
      :last_quarantine_reason
    ])
    |> validate_required([:state, :candidate_bots])
    |> validate_inclusion(:state, ["quarantined", "approved"])
    |> validate_length(:last_quarantine_reason, max: 128)
  end

  defp current_admin(%Account{account_id: account_id}) do
    case Repo.one(from a in Account, where: a.account_id == ^account_id, lock: "FOR SHARE") do
      %Account{is_admin: true, state: state} = actor when state != :disabled -> {:ok, actor}
      _ -> {:error, :forbidden}
    end
  end

  defp hub_for_update(hub_sid) do
    Repo.one(from h in Hub, where: h.hub_sid == ^hub_sid, lock: "FOR UPDATE")
  end

  defp hub_for_update_by_id(hub_id) do
    Repo.one(from h in Hub, where: h.hub_id == ^hub_id, lock: "FOR UPDATE")
  end

  defp approval_for_update(hub_id) do
    Repo.one(from a in BotConfigApproval, where: a.hub_id == ^hub_id, lock: "FOR UPDATE")
  end

  defp locked_runtime_decision(%Hub{} = hub, %BotConfigApproval{} = approval) do
    if hub.entry_mode != :deny and runtime_approved?(hub, approval) do
      {:ok, %{bots: raw_bots(hub.user_data), approval_updated_at: approval.updated_at}}
    else
      {:error, :bot_config_unapproved}
    end
  end

  defp persist_candidate(hub, candidate_bots) do
    user_data = (hub.user_data || %{}) |> Map.delete(:bots) |> Map.put("bots", candidate_bots)

    hub
    |> Ecto.Changeset.change(user_data: user_data)
    |> Repo.update()
  end

  defp disable_current_bots(%Hub{} = hub) do
    case raw_bots(hub.user_data) do
      %{} = bots ->
        disabled = Map.put(bots, "enabled", false)
        user_data = (hub.user_data || %{}) |> Map.delete(:bots) |> Map.put("bots", disabled)

        hub
        |> Ecto.Changeset.change(user_data: user_data)
        |> Repo.update()

      nil ->
        {:ok, hub}
    end
  end

  defp admit_candidate_capacity(hub, candidate_bots) do
    candidate_active = hub.entry_mode != :deny and BotConfig.active?(%{"bots" => candidate_bots})

    if candidate_active and not runtime_enabled?(hub) and
         configured_active_room_count() >= Ret.BotConfigAdmission.max_active_rooms() do
      {:error, :room_limit}
    else
      :ok
    end
  end

  defp validate_active_candidate(candidate_bots) do
    with :ok <- validate_persisted_size(candidate_bots),
         true <- BotConfig.active?(%{"bots" => candidate_bots}) do
      :ok
    else
      false -> {:error, :inactive_candidate}
      {:error, reason} -> {:error, reason}
    end
  end

  defp verify_fingerprint(expected, actual) do
    if expected == actual, do: :ok, else: {:error, :fingerprint_mismatch}
  end

  defp run_locked(fun) do
    case Locking.exec_after_lock(@admission_lock, fun) do
      {:ok, result} -> result
      {:error, _reason} -> {:error, :approval_unavailable}
    end
  rescue
    _error in DBConnection.ConnectionError -> {:error, :approval_unavailable}
  catch
    :exit, _reason -> {:error, :approval_unavailable}
  end

  defp valid_runtime_config?(bots) do
    validate_persisted_size(bots) == :ok and BotConfig.active?(%{"bots" => bots})
  end

  defp valid_runtime_shape?(bots) do
    validate_encoded_size(bots) == :ok and BotConfig.active?(%{"bots" => bots})
  end

  defp approved_decision(%Hub{} = hub) do
    bots = raw_bots(hub.user_data)

    if is_map(bots) and valid_runtime_shape?(bots) do
      query =
        from a in BotConfigApproval,
          where:
            a.hub_id == ^hub.hub_id and a.state == "approved" and
              fragment("? = ?::jsonb", a.approved_bots, ^bots) and
              fragment("octet_length(?::text) <= ?", a.approved_bots, ^@max_config_bytes),
          select: a.updated_at

      case Repo.one(query) do
        %DateTime{} = updated_at ->
          {:ok, %{bots: bots, approval_updated_at: updated_at}}

        _ ->
          {:error, :bot_config_unapproved}
      end
    else
      {:error, :bot_config_unapproved}
    end
  end

  defp validate_encoded_size(bots) do
    bots
    |> canonical_json()
    |> validate_canonical_size()
  rescue
    _ -> {:error, :invalid_config}
  end

  # PostgreSQL's canonical JSONB text includes separator whitespace, while the
  # fingerprint uses compact canonical JSON. Enforce the database-side size as
  # well so an approved row can never pass admission but fail snapshot delivery.
  defp validate_persisted_size(bots) do
    with :ok <- validate_encoded_size(bots),
         {:ok, %{rows: [[bytes]]}} <-
           Ecto.Adapters.SQL.query(
             Repo,
             "SELECT octet_length($1::jsonb::text)",
             [bots]
           ),
         true <- bytes <= @max_config_bytes do
      :ok
    else
      false -> {:error, :config_too_large}
      {:error, :config_too_large} = error -> error
      _ -> {:error, :invalid_config}
    end
  rescue
    _ -> {:error, :invalid_config}
  end

  defp validate_canonical_size(canonical) when byte_size(canonical) <= @max_config_bytes, do: :ok
  defp validate_canonical_size(_canonical), do: {:error, :config_too_large}

  defp fresh_admin?(%Account{is_admin: true, state: state}) when state != :disabled, do: true
  defp fresh_admin?(_actor), do: false

  defp actor_account_id(%Account{account_id: account_id}), do: account_id
  defp actor_account_id(_actor), do: nil

  defp quarantine_reason(current_hub, persisted_hub, persisted_bots) do
    cond do
      is_nil(persisted_bots) ->
        "bots_removed"

      current_hub && current_hub.entry_mode != :deny && persisted_hub.entry_mode == :deny ->
        "room_closed"

      not BotConfig.active?(%{"bots" => persisted_bots}) ->
        "bots_disabled"

      true ->
        "unapproved_bot_config_change"
    end
  end

  defp room_closed?(%Hub{entry_mode: current}, %Hub{entry_mode: :deny}),
    do: current != :deny

  defp room_closed?(_current_hub, _persisted_hub), do: false

  defp fingerprint_or_nil(%{} = bots) do
    case fingerprint(bots) do
      {:ok, value} -> value
      {:error, _reason} -> nil
    end
  end

  defp fingerprint_or_nil(_bots), do: nil

  defp approvable_fingerprint_or_nil(%{} = bots, candidate_bytes)
       when is_integer(candidate_bytes) and candidate_bytes <= @max_config_bytes,
       do: fingerprint_or_nil(bots)

  defp approvable_fingerprint_or_nil(_bots, _candidate_bytes), do: nil

  defp persisted_jsonb_bytes(%{} = bots) do
    case Ecto.Adapters.SQL.query(Repo, "SELECT octet_length($1::jsonb::text)", [bots]) do
      {:ok, %{rows: [[bytes]]}} when is_integer(bytes) -> bytes
      _ -> nil
    end
  rescue
    _error in DBConnection.ConnectionError -> nil
  end

  defp persisted_jsonb_bytes(_bots), do: nil

  defp redacted_summary(%{} = bots) do
    normalized = BotConfig.normalize(%{"bots" => bots})
    prompt = Map.get(bots, "prompt") || Map.get(bots, :prompt)
    prompt = if is_binary(prompt), do: prompt, else: ""

    %{
      enabled: normalized["enabled"],
      count: normalized["count"],
      mobility: normalized["mobility"],
      chat_enabled: normalized["chat_enabled"],
      prompt_present: prompt != "",
      prompt_bytes: byte_size(prompt),
      prompt_codepoints: prompt |> String.codepoints() |> length()
    }
  end

  defp redacted_summary(_bots), do: nil

  defp parse_limit(value) when is_integer(value), do: value |> max(1) |> min(@max_page_size)

  defp parse_limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} -> parse_limit(parsed)
      _ -> @default_page_size
    end
  end

  defp parse_limit(_value), do: @default_page_size

  defp parse_cursor(value) when is_integer(value) and value >= 0, do: value

  defp parse_cursor(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed >= 0 -> parsed
      _ -> nil
    end
  end

  defp parse_cursor(_value), do: nil

  defp stringify_keys(map) do
    Map.new(map, fn {key, value} -> {to_string(key), value} end)
  end

  defp canonical_json(%{} = map) do
    pairs = Enum.map(map, fn {key, value} -> {to_string(key), value} end)

    if pairs |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> length() != map_size(map) do
      raise ArgumentError, "duplicate JSON object key"
    end

    encoded_pairs =
      pairs
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn {key, value} -> [Jason.encode!(key), ?:, canonical_json(value)] end)
      |> Enum.intersperse(?,)

    IO.iodata_to_binary([?{, encoded_pairs, ?}])
  end

  defp canonical_json(list) when is_list(list) do
    encoded_values = list |> Enum.map(&canonical_json/1) |> Enum.intersperse(?,)
    IO.iodata_to_binary([?[, encoded_values, ?]])
  end

  defp canonical_json(value)
       when is_binary(value) or is_boolean(value) or is_number(value) or is_nil(value),
       do: Jason.encode!(value)

  defp canonical_json(_value), do: raise(ArgumentError, "unsupported JSON value")
end
