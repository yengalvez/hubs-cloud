defmodule Ret.BotConfigAdmission do
  @moduledoc """
  Serializes admission of rooms that consume bot-runner capacity.

  Reticulum is the durable source of desired bot state, so admission happens
  before a hub `user_data` change is persisted. A global PostgreSQL advisory
  transaction lock makes the configured room ceiling authoritative across
  Reticulum replicas.
  """

  import Ecto.Changeset
  import Ecto.Query

  alias Ret.{Account, BotConfig, Hub, Locking, Repo}

  @admission_lock "bot-config-admission:v1"
  @default_max_active_rooms 5
  @hard_max_active_rooms 10

  def insert(%Ecto.Changeset{} = changeset, actor) do
    persist(changeset, actor, :insert)
  end

  def update(%Ecto.Changeset{} = changeset, actor) do
    persist(changeset, actor, :update)
  end

  def max_active_rooms do
    :ret
    |> Application.get_env(:max_active_bot_rooms, @default_max_active_rooms)
    |> normalize_max_active_rooms()
  end

  defp persist(%Ecto.Changeset{valid?: false} = changeset, _actor, operation) do
    apply_repo(operation, changeset)
  end

  defp persist(changeset, actor, operation) do
    if admission_required?(changeset, operation) do
      persist_with_admission(changeset, actor, operation)
    else
      apply_repo(operation, changeset)
    end
  end

  defp persist_with_admission(changeset, actor, operation) do
    result =
      Locking.exec_after_lock(@admission_lock, fn ->
        current_actor = current_actor(actor)
        current_hub = current_hub_for_update(changeset, operation)
        intended_user_data = intended_user_data(changeset, current_hub)
        intended_entry_mode = intended_entry_mode(changeset, current_hub)

        with :ok <- validate_presented_actor(actor, current_actor),
             :ok <-
               authorize_change(
                 current_actor,
                 current_hub,
                 intended_user_data,
                 intended_entry_mode
               ),
             :ok <- admit_capacity(current_hub, intended_user_data, intended_entry_mode) do
          apply_repo(operation, changeset)
        else
          {:error, code, message} ->
            {:error, add_error(changeset, :user_data, message, validation: code)}
        end
      end)

    case result do
      {:ok, repo_result} ->
        repo_result

      {:error, _reason} ->
        {:error,
         add_error(changeset, :user_data, "bot configuration admission is unavailable",
           validation: :bot_admission_unavailable
         )}
    end
  end

  defp validate_presented_actor(%Account{}, %Account{state: state}) when state != :disabled,
    do: :ok

  defp validate_presented_actor(%Account{}, _current_actor) do
    {:error, :bot_actor_inactive,
     "the authenticated account is no longer active for bot configuration changes"}
  end

  defp validate_presented_actor(_system_actor, _current_actor), do: :ok

  defp authorize_change(
         %Account{is_admin: true, state: state},
         _current_hub,
         _intended_user_data,
         _intended_entry_mode
       )
       when state != :disabled,
       do: :ok

  defp authorize_change(_actor, current_hub, intended_user_data, intended_entry_mode) do
    current_state = bot_config_state(current_hub && current_hub.user_data)
    intended_state = bot_config_state(intended_user_data)

    current_consumes =
      !!(current_hub && effective_active?(current_hub.user_data, current_hub.entry_mode))

    intended_consumes = effective_active?(intended_user_data, intended_entry_mode)

    cond do
      not current_consumes && intended_consumes ->
        {:error, :bot_admin_required,
         "only a global administrator may create or modify room bot configuration"}

      current_state == intended_state or exact_disable?(current_state, intended_state) ->
        :ok

      true ->
        {:error, :bot_admin_required,
         "only a global administrator may create or modify room bot configuration"}
    end
  end

  defp admit_capacity(current_hub, intended_user_data, intended_entry_mode) do
    if consumes_new_slot?(current_hub, intended_user_data, intended_entry_mode) &&
         configured_active_room_count() >= max_active_rooms() do
      {:error, :bot_room_limit, "the configured active bot room limit has been reached"}
    else
      :ok
    end
  end

  defp consumes_new_slot?(current_hub, intended_user_data, intended_entry_mode) do
    current_consumes =
      !!(current_hub && effective_active?(current_hub.user_data, current_hub.entry_mode))

    not current_consumes && effective_active?(intended_user_data, intended_entry_mode)
  end

  defp configured_active_room_count do
    Hub
    |> where([h], h.hub_sid != "admin")
    |> where([h], is_nil(h.entry_mode) or h.entry_mode != :deny)
    |> BotConfig.with_active_bot_config()
    |> select([h], count(h.hub_id))
    |> Repo.one()
  end

  defp admission_required?(changeset, :insert) do
    bot_config_state(get_field(changeset, :user_data)) != :absent
  end

  defp admission_required?(changeset, :update) do
    bot_related =
      bot_config_state(changeset.data.user_data) != :absent or
        bot_config_state(get_field(changeset, :user_data)) != :absent

    Map.has_key?(changeset.changes, :entry_mode) or
      (bot_related && Map.has_key?(changeset.changes, :user_data))
  end

  defp current_hub_for_update(_changeset, :insert), do: nil

  defp current_hub_for_update(%Ecto.Changeset{data: %Hub{hub_id: hub_id}}, :update) do
    Repo.one(from h in Hub, where: h.hub_id == ^hub_id, lock: "FOR UPDATE")
  end

  defp current_actor(%Account{account_id: account_id}) do
    Repo.one(from a in Account, where: a.account_id == ^account_id, lock: "FOR SHARE")
  end

  defp current_actor(_actor), do: nil

  defp intended_user_data(changeset, nil), do: get_field(changeset, :user_data)

  defp intended_user_data(changeset, %Hub{} = current_hub) do
    case fetch_change(changeset, :user_data) do
      {:ok, user_data} -> user_data
      :error -> current_hub.user_data
    end
  end

  defp intended_entry_mode(changeset, nil), do: get_field(changeset, :entry_mode)

  defp intended_entry_mode(changeset, %Hub{} = current_hub) do
    case fetch_change(changeset, :entry_mode) do
      {:ok, entry_mode} -> entry_mode
      :error -> current_hub.entry_mode
    end
  end

  defp apply_repo(:insert, changeset), do: Repo.insert(changeset)
  defp apply_repo(:update, changeset), do: Repo.update(changeset)

  defp bot_config_state(user_data) when is_map(user_data) do
    bots = Map.get(user_data, "bots") || Map.get(user_data, :bots)

    if is_map(bots) do
      {:present, BotConfig.normalize(%{"bots" => bots})}
    else
      :absent
    end
  end

  defp bot_config_state(_user_data), do: :absent

  defp exact_disable?({:present, current}, {:present, intended}) do
    current["enabled"] && intended == Map.put(current, "enabled", false)
  end

  defp exact_disable?(_current, _intended), do: false

  defp effective_active?(user_data, entry_mode) do
    entry_mode != :deny && BotConfig.active?(user_data)
  end

  defp normalize_max_active_rooms(value) when is_integer(value) and value > 0 do
    min(value, @hard_max_active_rooms)
  end

  defp normalize_max_active_rooms(_value), do: @default_max_active_rooms
end
