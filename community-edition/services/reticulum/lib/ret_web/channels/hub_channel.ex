defmodule RetWeb.HubChannel do
  @moduledoc "Ret Web Channel for Hubs"

  use RetWeb, :channel

  import Canada, only: [can?: 2]

  alias Ret.{
    AppConfig,
    Hub,
    HubInvite,
    Account,
    AccountFavorite,
    BotConfig,
    BotConfigAdmission,
    BotConfigApproval,
    BotChatPresence,
    BotOrchestrator,
    BotRunnerLease,
    Identity,
    Repo,
    RoomObject,
    OwnedFile,
    Scene,
    Storage,
    SessionStat,
    Statix,
    WaypointReservation,
    WebPushSubscription
  }

  alias RetWeb.{Presence, EntityView}
  alias RetWeb.Api.V1.{HubView}

  intercept [
    "hub_refresh",
    "mute",
    "add_owner",
    "remove_owner",
    "message",
    "block",
    "unblock",
    # See internal_naf_event_for/2
    "maybe-naf",
    "maybe-nafr"
  ]

  def join("hub:" <> hub_sid, %{"profile" => profile, "context" => context} = params, socket) do
    hub =
      Hub
      |> Repo.get_by(hub_sid: hub_sid)
      |> Repo.preload(Hub.hub_preloads())

    socket
    |> assign(:profile, profile)
    |> assign(:block_naf, false)
    |> assign(:blocked_session_ids, %{})
    |> assign(:blocked_by_session_ids, %{})
    |> assign(:has_blocks, false)
    |> assign(:has_embeds, false)
    |> perform_join(
      hub,
      context,
      params
      |> Map.take([
        "push_subscription_endpoint",
        "auth_token",
        "perms_token",
        "bot_access_key",
        "hub_invite_id",
        "waypoint_reservation"
      ])
    )
  end

  defp perform_join(_socket, nil, _context, _params) do
    Statix.increment("ret.channels.hub.joins.not_found")
    {:error, %{message: "No such Hub", reason: "not_found"}}
  end

  defp perform_join(socket, hub, context, params) do
    bot_access_key = Application.get_env(:ret, :bot_runner_access_key)
    has_valid_bot_access_key = secure_compare(params["bot_access_key"], bot_access_key)

    with {:ok, context, authenticated_bot_runner} <-
           authenticate_bot_runner_context(context, has_valid_bot_access_key) do
      account =
        case Ret.Guardian.resource_from_token(params["auth_token"]) do
          {:ok, %Account{} = account, _claims} -> account
          _ -> nil
        end

      hub_requires_oauth = hub.hub_bindings |> Enum.empty?() |> Kernel.not()

      socket = assign(socket, :context, context)

      account_has_provider_for_hub =
        account |> Ret.Account.matching_oauth_providers(hub) |> Enum.empty?() |> Kernel.not()

      account_can_join = account |> can?(join_hub(hub))
      account_can_update = account |> can?(update_hub(hub))

      perms_token = params["perms_token"]

      has_perms_token = perms_token != nil

      decoded_perms = perms_token |> Ret.PermsToken.decode_and_verify()

      perms_token_can_join =
        case decoded_perms do
          {:ok, %{"join_hub" => true}} -> true
          _ -> false
        end

      {oauth_account_id, oauth_source} =
        case decoded_perms do
          {:ok, %{"oauth_account_id" => oauth_account_id, "oauth_source" => oauth_source}} ->
            {oauth_account_id, oauth_source |> String.to_atom()}

          _ ->
            {nil, nil}
        end

      has_active_invite = Ret.HubInvite.active?(hub, params["hub_invite_id"])

      params =
        params
        |> Map.merge(%{
          has_active_invite: has_active_invite,
          hub_requires_oauth: hub_requires_oauth,
          has_valid_bot_access_key: authenticated_bot_runner,
          account_has_provider_for_hub: account_has_provider_for_hub,
          account_can_join: account_can_join,
          account_can_update: account_can_update,
          has_perms_token: has_perms_token,
          oauth_account_id: oauth_account_id,
          oauth_source: oauth_source,
          perms_token_can_join: perms_token_can_join
        })

      hub |> join_with_hub(account, socket, context, params)
    end
  end

  # Optimization: "raw" NAF event, with the underlying NAF payload as a string.
  # By going through this event, the server can avoid parsing the NAF messages.
  def handle_in("nafr" = event, %{"naf" => naf_payload} = payload, socket) do
    # Decode before using the raw fast path so an escaped dedicated bot
    # template or protected bot network id can never bypass authorization.
    case Jason.decode(naf_payload) do
      {:ok, decoded_payload} ->
        template = naf_payload_template(decoded_payload)

        cond do
          not valid_naf_network_ids?(decoded_payload) ->
            {:reply, {:error, %{reason: "invalid_network_id"}}, socket}

          contains_first_sync?(decoded_payload) or
              bot_sensitive_payload?(decoded_payload, socket) ->
            handle_in("naf", decoded_payload, socket)

          is_binary(template) and spawn_permitted?(template, socket) ->
            broadcast_from!(
              socket,
              event |> internal_naf_event_for(socket),
              payload |> payload_with_from(socket)
            )

            {:noreply, socket}

          is_binary(template) ->
            {:noreply, socket}

          true ->
            broadcast_from!(
              socket,
              event |> internal_naf_event_for(socket),
              payload |> payload_with_from(socket)
            )

            {:noreply, socket}
        end

      {:error, _reason} ->
        {:noreply, socket}
    end
  end

  # Captures all inbound NAF messages that result in spawned objects.
  def handle_in(
        "naf" = event,
        %{"data" => %{"isFirstSync" => true, "persistent" => false, "template" => template}} =
          payload,
        socket
      ) do
    data = payload["data"]
    network_id = data["networkId"]

    cond do
      not valid_network_id?(network_id) ->
        {:reply, {:error, %{reason: "invalid_network_id"}}, socket}

      template == "#remote-bot-avatar" and not bot_network_id?(network_id, socket) ->
        {:reply, {:error, %{reason: "invalid_network_id"}}, socket}

      template != "#remote-bot-avatar" and bot_network_id?(network_id, socket) ->
        {:reply, {:error, %{reason: "reserved_bot_network_id"}}, socket}

      not spawn_permitted?(template, socket) ->
        if template == "#remote-bot-avatar" do
          {:reply, {:error, %{reason: bot_runner_rejection_reason(socket)}}, socket}
        else
          {:noreply, socket}
        end

      true ->
        data =
          data
          |> Map.put("creator", socket.assigns.session_id)
          |> Map.put("owner", socket.assigns.session_id)

        payload = payload |> Map.put("data", data)

        broadcast_from!(
          socket,
          event |> internal_naf_event_for(socket),
          payload |> payload_with_from(socket)
        )

        if template == "#remote-bot-avatar" do
          {:reply,
           {:ok,
            %{
              bot_spawn_accepted: true,
              network_id: data["networkId"],
              bot_runner_authority_epoch: socket.assigns.bot_runner_authority_epoch
            }}, socket}
        else
          {:noreply, socket}
        end
    end
  end

  # Every update targeting the reserved bot namespace must retain the exact
  # dedicated shape and remain authenticated after the acknowledged first sync.
  def handle_in(
        "naf" = event,
        %{
          "dataType" => "u",
          "data" => %{"networkId" => network_id} = data
        } = payload,
        socket
      ) do
    protected_network_id = bot_network_id?(network_id, socket)
    dedicated_shape = data["template"] == "#remote-bot-avatar" and data["persistent"] === false

    cond do
      not valid_network_id?(network_id) ->
        {:reply, {:error, %{reason: "invalid_network_id"}}, socket}

      protected_network_id and not dedicated_shape ->
        {:reply, {:error, %{reason: "invalid_bot_spawn_payload"}}, socket}

      protected_network_id and not bot_runner?(socket) ->
        {:reply, {:error, %{reason: bot_runner_rejection_reason(socket)}}, socket}

      not protected_network_id and data["template"] == "#remote-bot-avatar" ->
        {:reply, {:error, %{reason: "invalid_network_id"}}, socket}

      true ->
        broadcast_from!(
          socket,
          event |> internal_naf_event_for(socket),
          payload |> payload_with_from(socket)
        )

        {:noreply, socket}
    end
  end

  def handle_in(
        "naf" = event,
        %{"dataType" => "r", "data" => %{"networkId" => network_id}} = payload,
        socket
      ) do
    cond do
      not valid_network_id?(network_id) ->
        {:reply, {:error, %{reason: "invalid_network_id"}}, socket}

      bot_network_id?(network_id, socket) ->
        if bot_runner?(socket) do
          broadcast_from!(
            socket,
            event |> internal_naf_event_for(socket),
            payload |> payload_with_from(socket)
          )

          {:noreply, socket}
        else
          {:reply, {:error, %{reason: bot_runner_rejection_reason(socket)}}, socket}
        end

      true ->
        broadcast_from!(
          socket,
          event |> internal_naf_event_for(socket),
          payload |> payload_with_from(socket)
        )

        {:noreply, socket}
    end
  end

  def handle_in("naf", %{"dataType" => data_type}, socket) when data_type in ["u", "r"] do
    {:reply, {:error, %{reason: "invalid_network_id"}}, socket}
  end

  def handle_in("naf" = event, %{"data" => %{"networkId" => network_id}} = payload, socket)
      when is_binary(network_id) and byte_size(network_id) > 0 do
    cond do
      payload["dataType"] == "um" ->
        {:reply, {:error, %{reason: "invalid_network_id"}}, socket}

      bot_network_id?(network_id, socket) ->
        {:reply, {:error, %{reason: "invalid_bot_spawn_payload"}}, socket}

      payload["data"]["template"] == "#remote-bot-avatar" ->
        {:reply, {:error, %{reason: "invalid_network_id"}}, socket}

      true ->
        broadcast_from!(
          socket,
          event |> internal_naf_event_for(socket),
          payload |> payload_with_from(socket)
        )

        {:noreply, socket}
    end
  end

  def handle_in("naf", %{"data" => %{"template" => "#remote-bot-avatar"}}, socket) do
    {:reply, {:error, %{reason: "invalid_bot_spawn_payload"}}, socket}
  end

  # Captures all inbound NAF Update Multi messages
  def handle_in(
        "naf" = event,
        %{"dataType" => "um", "data" => %{"d" => updates}} = payload,
        socket
      ) do
    cond do
      not is_list(updates) ->
        {:reply, {:error, %{reason: "invalid_network_id"}}, socket}

      not Enum.all?(updates, &valid_naf_update?/1) ->
        {:reply, {:error, %{reason: "invalid_network_id"}}, socket}

      Enum.any?(updates, &(is_map(&1) and Map.has_key?(&1, "isFirstSync"))) ->
        # NAF should never send first syncs in a multi update.
        {:noreply, socket}

      Enum.any?(updates, &malformed_bot_avatar_update?(&1, socket)) ->
        {:reply, {:error, %{reason: "invalid_bot_spawn_payload"}}, socket}

      Enum.any?(updates, &bot_network_update?(&1, socket)) and not bot_runner?(socket) ->
        {:reply, {:error, %{reason: bot_runner_rejection_reason(socket)}}, socket}

      Enum.all?(updates, &multi_update_permitted?(&1, socket)) ->
        broadcast_from!(
          socket,
          event |> internal_naf_event_for(socket),
          payload |> payload_with_from(socket)
        )

        {:noreply, socket}

      Enum.any?(updates, &bot_sensitive_update?(&1, socket)) ->
        {:reply, {:error, %{reason: bot_runner_rejection_reason(socket)}}, socket}

      true ->
        {:noreply, socket}
    end
  end

  def handle_in("naf", %{"dataType" => "um"}, socket) do
    {:reply, {:error, %{reason: "invalid_network_id"}}, socket}
  end

  # Fallthrough for all other NAF dataTypes
  def handle_in("naf" = event, payload, socket) do
    broadcast_from!(
      socket,
      event |> internal_naf_event_for(socket),
      payload |> payload_with_from(socket)
    )

    {:noreply, socket}
  end

  def handle_in("events:entering", _payload, socket) do
    context = socket.assigns.context || %{}

    socket =
      socket
      |> assign(:context, context |> Map.put("entering", true))
      |> broadcast_presence_update

    {:noreply, socket}
  end

  def handle_in("events:entering_cancelled", _payload, socket) do
    context = socket.assigns.context || %{}

    socket =
      socket |> assign(:context, context |> Map.delete("entering")) |> broadcast_presence_update

    {:noreply, socket}
  end

  def handle_in("events:entered", %{"initialOccupantCount" => occupant_count} = payload, socket) do
    socket =
      socket
      |> handle_max_occupant_update(occupant_count)
      |> handle_entered_event(payload)

    Statix.increment("ret.channels.hub.event_entered", 1)

    {:noreply, socket}
  end

  def handle_in("events:entered", payload, socket) do
    socket = socket |> handle_entered_event(payload)

    Statix.increment("ret.channels.hub.event_entered", 1)

    {:noreply, socket}
  end

  def handle_in("waypoint_reservation:request", payload, socket) do
    reservation = socket.assigns.waypoint_reservation

    if reservation.supported do
      context = socket.assigns.context || %{}

      result =
        WaypointReservation.request(
          reservation.hub_id,
          socket.assigns.session_id,
          reservation.client_instance_id,
          reservation.channel_id,
          payload,
          %{
            allowed: socket.assigns.presence == :room || context["entering"] == true,
            bot_runner: reservation.bot_runner
          }
        )

      broadcast_waypoint_states(socket, result.states)
      reply_status = if result.response["status"] == "ok", do: :ok, else: :error
      {:reply, {reply_status, result.response}, socket}
    else
      response = WaypointReservation.unsupported_response(payload)
      {:reply, {:error, response}, socket}
    end
  end

  def handle_in("events:object_spawned", %{"object_type" => object_type}, socket) do
    socket = socket |> handle_object_spawned(object_type)

    Statix.increment("ret.channels.hub.objects_spawned", 1)

    {:noreply, socket}
  end

  def handle_in("events:request_support", _payload, socket) do
    hub = socket |> hub_for_socket
    Task.start_link(fn -> hub |> Ret.Support.request_support_for_hub() end)

    {:noreply, socket}
  end

  def handle_in("events:profile_updated", %{"profile" => profile}, socket) do
    socket = socket |> assign(:profile, profile) |> broadcast_presence_update
    {:noreply, socket}
  end

  def handle_in("events:begin_recording", _payload, socket),
    do: socket |> set_presence_flag(:recording, true)

  def handle_in("events:end_recording", _payload, socket),
    do: socket |> set_presence_flag(:recording, false)

  def handle_in("events:raise_hand", _payload, socket),
    do: socket |> set_presence_flag(:hand_raised, true)

  def handle_in("events:lower_hand", _payload, socket),
    do: socket |> set_presence_flag(:hand_raised, false)

  def handle_in("events:begin_streaming", _payload, socket),
    do: socket |> set_presence_flag(:streaming, true)

  def handle_in("events:end_streaming", _payload, socket),
    do: socket |> set_presence_flag(:streaming, false)

  def handle_in("events:begin_typing", _payload, socket),
    do: socket |> set_presence_flag(:typing, true)

  def handle_in("events:end_typing", _payload, socket),
    do: socket |> set_presence_flag(:typing, false)

  # Only Reticulum emits bot commands, after the authenticated bot API has
  # validated the room, bot id and model-proposed action.
  def handle_in("message", %{"type" => "bot_command"}, socket), do: {:noreply, socket}

  def handle_in("message" = event, %{"type" => type} = payload, socket) do
    account = Guardian.Phoenix.Socket.current_resource(socket)
    hub = socket |> hub_for_socket

    if (type != "photo" and type != "video") or account |> can?(spawn_camera(hub)) do
      broadcast!(
        socket,
        event,
        payload
        |> Map.delete("session_id")
        |> Map.put(:session_id, socket.assigns.session_id)
        |> payload_with_from(socket)
      )
    end

    {:noreply, socket}
  end

  def handle_in("mute" = event, payload, socket) do
    hub = socket |> hub_for_socket
    account = Guardian.Phoenix.Socket.current_resource(socket)

    if account |> can?(mute_users(hub)) do
      broadcast_from!(socket, event, payload)
    end

    {:noreply, socket}
  end

  def handle_in("subscribe", %{"subscription" => subscription}, socket) do
    socket
    |> hub_for_socket
    |> WebPushSubscription.subscribe_to_hub(subscription)

    {:noreply, socket}
  end

  def handle_in("favorite", _params, socket) do
    account = Guardian.Phoenix.Socket.current_resource(socket)
    socket |> hub_for_socket |> AccountFavorite.ensure_favorited(account)
    {:noreply, socket}
  end

  def handle_in("unfavorite", _params, socket) do
    account = Guardian.Phoenix.Socket.current_resource(socket)
    socket |> hub_for_socket |> AccountFavorite.ensure_not_favorited(account)
    {:noreply, socket}
  end

  def handle_in("unsubscribe", %{"subscription" => subscription}, socket) do
    socket
    |> hub_for_socket
    |> WebPushSubscription.unsubscribe_from_hub(subscription)

    has_remaining_subscriptions =
      WebPushSubscription.endpoint_has_subscriptions?(subscription["endpoint"])

    {:reply, {:ok, %{has_remaining_subscriptions: has_remaining_subscriptions}}, socket}
  end

  def handle_in("sign_in", %{"token" => token} = payload, socket) do
    creator_assignment_token = payload["creator_assignment_token"]

    case Ret.Guardian.resource_from_token(token) do
      {:ok, %Account{} = account, _claims} ->
        socket =
          socket
          |> Guardian.Phoenix.Socket.put_current_resource(account)
          |> rotate_bot_chat_capability(account)

        hub = socket |> hub_for_socket |> Repo.preload(Hub.hub_preloads())

        hub =
          if creator_assignment_token do
            hub
            |> Hub.changeset_for_creator_assignment(account, creator_assignment_token)
            |> Repo.update!()
          else
            hub
          end

        perms_token = get_perms_token(hub, account)
        broadcast_presence_update(socket)

        {:reply,
         {:ok,
          %{
            perms_token: perms_token,
            bot_chat_capability: socket.assigns.bot_chat_capability
          }}, socket}

      {:error, reason} ->
        {:reply, {:error, %{message: "Sign in failed", reason: reason}}, socket}
    end
  end

  def handle_in("sign_out", _payload, socket) do
    :ok = BotChatPresence.untrack(self())

    socket =
      socket
      |> Guardian.Phoenix.Socket.put_current_resource(nil)
      |> assign(:bot_chat_capability, nil)

    broadcast_presence_update(socket)

    # Disconnect if signing out and account is required
    if AppConfig.get_cached_config_value("features|require_account_for_join") do
      Process.send_after(self(), :close_channel, 5000)
    end

    {:reply, {:ok, %{bot_chat_capability: nil}}, socket}
  end

  def handle_in(
        "pin",
        %{
          "id" => object_id,
          "gltf_node" => gltf_node,
          "file_id" => file_id,
          "file_access_token" => file_access_token,
          "promotion_token" => promotion_token
        },
        socket
      ) do
    with_account(socket, fn account ->
      hub = socket |> hub_for_socket

      if account |> can?(pin_objects(hub)) do
        perform_pin!(object_id, gltf_node, account, socket)
        Storage.promote(file_id, file_access_token, promotion_token, account)
        OwnedFile.set_active(file_id, account.account_id)
      end
    end)
  end

  def handle_in("pin", %{"id" => object_id, "gltf_node" => gltf_node}, socket) do
    with_account(socket, fn account ->
      hub = socket |> hub_for_socket

      if account |> can?(pin_objects(hub)) do
        perform_pin!(object_id, gltf_node, account, socket)
      end
    end)
  end

  def handle_in("unpin", %{"id" => object_id, "file_id" => file_id}, socket) do
    hub = socket |> hub_for_socket

    case Guardian.Phoenix.Socket.current_resource(socket) do
      %Account{} = account ->
        if account |> can?(pin_objects(hub)) do
          RoomObject.perform_unpin(hub, object_id)
          OwnedFile.set_inactive(file_id, account.account_id)
        end

      _ ->
        nil
    end

    {:noreply, socket}
  end

  def handle_in("unpin", %{"id" => object_id}, socket) do
    hub = socket |> hub_for_socket

    case Guardian.Phoenix.Socket.current_resource(socket) do
      %Account{} = account ->
        if account |> can?(pin_objects(hub)) do
          RoomObject.perform_unpin(hub, object_id)
        end

      _ ->
        nil
    end

    {:noreply, socket}
  end

  def handle_in("list_entities", _, socket) do
    hub = socket |> hub_for_socket
    entities = Ret.list_entities(hub.hub_id)
    {:reply, {:ok, EntityView.render("index.json", %{entities: entities})}, socket}
  end

  def handle_in("save_entity_state", params, socket) do
    params = parse(params)

    with {:ok, hub, account} <- authorize(socket, :write_entity_state),
         {:ok, %{entity: entity}} <- Ret.create_entity(hub, params),
         :ok <- maybe_promote_file(params, account, socket) do
      entity = Repo.preload(entity, [:sub_entities])

      broadcast!(
        socket,
        "entity_state_saved",
        EntityView.render("show.json", %{entity: entity})
      )

      {:reply, :ok, socket}
    else
      {:error, reason} ->
        reply_error(socket, reason)
    end
  end

  def handle_in("update_entity_state", %{"update_message" => update_message} = params, socket) do
    params = parse(params)

    with {:ok, hub, _account} <- authorize(socket, :write_entity_state),
         {:ok, _} <- Ret.insert_or_update_sub_entity(hub, params) do
      broadcast!(socket, "entity_state_updated", update_message)
      {:reply, :ok, socket}
    else
      {:error, reason} ->
        reply_error(socket, reason)
    end
  end

  def handle_in("delete_entity_state", %{"nid" => nid} = payload, socket) do
    with {:ok, hub, account} <- authorize(socket, :write_entity_state),
         {:ok, _} <- Ret.delete_entity(hub.hub_id, nid),
         {:ok, _} <- maybe_set_owned_file_inactive(payload, account) do
      RoomObject.perform_unpin(hub, nid)

      broadcast!(socket, "entity_state_deleted", %{
        "nid" => nid,
        "creator" => socket.assigns.session_id
      })

      {:reply, :ok, socket}
    else
      {:error, reason} ->
        reply_error(socket, reason)
    end
  end

  def handle_in("get_host", _args, socket) do
    hub = socket |> hub_for_socket |> Hub.ensure_host()

    {:reply, {:ok, %{host: hub.host, port: Hub.janus_port(), turn: Hub.generate_turn_info()}},
     socket}
  end

  def handle_in("update_hub", payload, socket) do
    hub = socket |> hub_for_socket
    account = Guardian.Phoenix.Socket.current_resource(socket)

    if account |> can?(update_hub(hub)) do
      name_changed = hub.name != payload["name"]
      description_changed = hub.description != payload["description"]

      member_permissions_changed =
        hub.member_permissions != payload |> Hub.member_permissions_from_attrs()

      room_size_changed = hub.room_size != payload["room_size"]
      user_data_changed = hub.user_data != payload["user_data"]
      can_change_promotion = account |> can?(update_hub_promotion(hub))

      promotion_changed =
        can_change_promotion and hub.allow_promotion != payload["allow_promotion"]

      # Older clients may not send an entry_mode in the payload.
      entry_mode_changed =
        payload["entry_mode"] !== nil and hub.entry_mode != payload["entry_mode"]

      stale_fields = []
      stale_fields = if name_changed, do: ["name" | stale_fields], else: stale_fields

      stale_fields =
        if description_changed, do: ["description" | stale_fields], else: stale_fields

      stale_fields =
        if member_permissions_changed,
          do: ["member_permissions" | stale_fields],
          else: stale_fields

      stale_fields = if room_size_changed, do: ["room_size" | stale_fields], else: stale_fields
      stale_fields = if user_data_changed, do: ["user_data" | stale_fields], else: stale_fields

      stale_fields =
        if promotion_changed, do: ["allow_promotion" | stale_fields], else: stale_fields

      stale_fields = if entry_mode_changed, do: ["entry_mode" | stale_fields], else: stale_fields

      update_changeset =
        hub
        |> Hub.add_attrs_to_changeset(payload)
        |> Hub.add_member_permissions_to_changeset(payload)
        |> Hub.maybe_add_promotion_to_changeset(account, hub, payload)
        |> Hub.maybe_add_entry_mode_to_changeset(payload)

      case BotConfigAdmission.update(update_changeset, account) do
        {:ok, updated_hub} ->
          updated_hub = Repo.preload(updated_hub, Hub.hub_preloads())
          sync_bot_orchestrator(updated_hub)
          broadcast_hub_refresh!(updated_hub, socket, stale_fields)
          {:noreply, socket}

        {:error, _changeset} ->
          reply_error(socket, "bot_config_rejected")
      end
    else
      {:noreply, socket}
    end
  end

  def handle_in("fetch_invite", _payload, socket) do
    hub = hub_for_socket(socket)
    account = Guardian.Phoenix.Socket.current_resource(socket)

    if account |> can?(update_hub(hub)) do
      hub_invite = hub |> HubInvite.find_or_create_invite_for_hub()
      {:reply, {:ok, %{hub_invite_id: hub_invite && hub_invite.hub_invite_sid}}, socket}
    else
      {:reply, {:ok, %{}}, socket}
    end
  end

  def handle_in("revoke_invite", payload, socket) do
    hub = hub_for_socket(socket)
    account = Guardian.Phoenix.Socket.current_resource(socket)

    if account |> can?(update_hub(hub)) and HubInvite.active?(hub, payload["hub_invite_id"]) do
      HubInvite.revoke_invite(hub, payload["hub_invite_id"])

      # Hubs can only have one invite for now, so we create a new one when the old one was revoked.
      hub_invite = hub |> HubInvite.find_or_create_invite_for_hub()
      {:reply, {:ok, %{hub_invite_id: hub_invite.hub_invite_sid}}, socket}
    else
      {:reply, {:ok, %{}}, socket}
    end
  end

  def handle_in("close_hub", _payload, socket) do
    socket |> handle_entry_mode_change(:deny)
  end

  def handle_in("update_scene", %{"url" => url}, socket) do
    hub = socket |> hub_for_socket |> Repo.preload([:scene, :scene_listing])
    account = Guardian.Phoenix.Socket.current_resource(socket)

    if account |> can?(update_hub(hub)) do
      endpoint_host = RetWeb.Endpoint.host()

      case url |> URI.parse() do
        %URI{host: ^endpoint_host, path: "/scenes/" <> scene_path} ->
          scene_or_listing =
            scene_path |> String.split("/") |> Enum.at(0) |> Scene.scene_or_scene_listing_by_sid()

          hub |> Hub.changeset_for_new_scene(scene_or_listing)

        _ ->
          hub |> Hub.changeset_for_new_environment_url(url)
      end
      |> Repo.update!()
      |> Repo.preload(Hub.hub_preloads(), force: true)
      |> broadcast_hub_refresh!(socket, ["scene"])
    end

    {:noreply, socket}
  end

  def handle_in(
        "refresh_perms_token",
        _args,
        %{assigns: %{oauth_account_id: oauth_account_id, oauth_source: oauth_source}} = socket
      )
      when oauth_account_id != nil do
    perms_token =
      socket
      |> hub_for_socket
      |> get_perms_token(%Ret.OAuthProvider{
        provider_account_id: oauth_account_id,
        source: oauth_source
      })

    {:reply, {:ok, %{perms_token: perms_token}}, socket}
  end

  def handle_in("refresh_perms_token", _args, socket) do
    account = Guardian.Phoenix.Socket.current_resource(socket)
    perms_token = socket |> hub_for_socket |> get_perms_token(account)
    {:reply, {:ok, %{perms_token: perms_token}}, socket}
  end

  def handle_in("block" = event, %{"session_id" => session_id} = payload, socket) do
    socket =
      socket
      |> assign(
        :blocked_session_ids,
        socket.assigns.blocked_session_ids |> Map.put(session_id, true)
      )
      |> assign_has_blocks

    broadcast_from!(socket, event, payload |> payload_with_from(socket))
    {:noreply, socket}
  end

  def handle_in("unblock" = event, %{"session_id" => session_id} = payload, socket) do
    socket =
      socket
      |> assign(
        :blocked_session_ids,
        socket.assigns.blocked_session_ids |> Map.delete(session_id)
      )
      |> assign_has_blocks

    broadcast_from!(socket, event, payload |> payload_with_from(socket))
    {:noreply, socket}
  end

  def handle_in("kick", %{"session_id" => session_id}, socket) do
    account = Guardian.Phoenix.Socket.current_resource(socket)
    hub = socket |> hub_for_socket

    if account |> can?(kick_users(hub)) do
      RetWeb.Endpoint.broadcast("session:#{session_id}", "disconnect", %{})
    end

    {:noreply, socket}
  end

  # NOTE: block_naf will only work if the hub is embedded. We *only* enable packet filtering
  # (and therefore, only respect block_naf) when a hub is embedded (or if there are blocks on the socket.)
  def handle_in("block_naf", _payload, socket), do: {:noreply, socket |> assign(:block_naf, true)}

  def handle_in("unblock_naf", _payload, socket),
    do: {:noreply, socket |> assign(:block_naf, false)}

  def handle_in(event, %{"session_id" => _session_id} = payload, socket)
      when event in ["add_owner", "remove_owner"] do
    account = Guardian.Phoenix.Socket.current_resource(socket)
    hub = socket |> hub_for_socket

    if account |> can?(update_roles(hub)) do
      broadcast_from!(socket, event, payload)
    end

    {:noreply, socket}
  end

  def handle_in("oauth", %{"type" => "twitter"}, socket) do
    hub = socket |> hub_for_socket

    case Guardian.Phoenix.Socket.current_resource(socket) do
      %Account{} = account ->
        case Ret.TwitterClient.get_oauth_url(hub.hub_sid, account.account_id) do
          {:error, reason} -> {:reply, {:error, %{reason: reason}}, socket}
          url -> {:reply, {:ok, %{oauth_url: url}}, socket}
        end

      _ ->
        {:reply, :error, socket}
    end
  end

  def handle_in(_message, _payload, socket) do
    {:noreply, socket}
  end

  # If the maybe- variant of the naf/nafr messages are seen, we are performing packet filtering due to blocks
  # or iframe embeds opting out of NAF traffic. Handle them appropriately. (This is expensive, and should be rare!)
  def handle_out(event, payload, socket) when event in ["maybe-nafr"] do
    %{
      block_naf: block_naf,
      blocked_session_ids: blocked_session_ids,
      blocked_by_session_ids: blocked_by_session_ids
    } = socket.assigns

    socket
    |> maybe_push_naf("nafr", payload, block_naf, blocked_session_ids, blocked_by_session_ids)
  end

  def handle_out(event, payload, socket) when event in ["maybe-naf"] do
    %{
      block_naf: block_naf,
      blocked_session_ids: blocked_session_ids,
      blocked_by_session_ids: blocked_by_session_ids
    } = socket.assigns

    socket
    |> maybe_push_naf("naf", payload, block_naf, blocked_session_ids, blocked_by_session_ids)
  end

  def handle_out("mute" = event, %{"session_id" => session_id} = payload, socket) do
    if socket.assigns.session_id == session_id do
      push(socket, event, payload)
    end

    {:noreply, socket}
  end

  def handle_out("hub_refresh" = event, %{stale_fields: stale_fields} = payload, socket) do
    push(socket, event, payload)

    if stale_fields |> Enum.member?("member_permissions") do
      # If hub member permissions change, everyone should flush their new permissions into presence so that other
      # clients can correctly authorized their actions.
      broadcast_presence_update(socket)
    end

    {:noreply, socket}
  end

  def handle_out(
        "block",
        %{"session_id" => session_id, :from_session_id => from_session_id},
        socket
      ) do
    socket =
      if socket.assigns.session_id === session_id do
        socket
        |> assign(
          :blocked_by_session_ids,
          socket.assigns.blocked_by_session_ids |> Map.put(from_session_id, true)
        )
        |> assign_has_blocks
      else
        socket
      end

    {:noreply, socket}
  end

  def handle_out(
        "unblock",
        %{"session_id" => session_id, :from_session_id => from_session_id},
        socket
      ) do
    socket =
      if socket.assigns.session_id === session_id do
        socket
        |> assign(
          :blocked_by_session_ids,
          socket.assigns.blocked_by_session_ids |> Map.delete(from_session_id)
        )
        |> assign_has_blocks
      else
        socket
      end

    {:noreply, socket}
  end

  def handle_out(event, %{"session_id" => session_id}, socket)
      when event in ["add_owner", "remove_owner"] do
    account = Guardian.Phoenix.Socket.current_resource(socket)

    if account && socket.assigns.session_id == session_id do
      # Outgoing message has already had a permission check on the sender side, so perform the action
      action =
        if event == "add_owner" do
          &Hub.add_owner!/2
        else
          &Hub.remove_owner!/2
        end

      socket |> hub_for_socket |> action.(account)

      broadcast_presence_update(socket)
      push(socket, "permissions_updated", %{})
    end

    {:noreply, socket}
  end

  def handle_out("message" = event, %{from_session_id: from_session_id} = payload, socket) do
    blocked_session_ids = socket.assigns.blocked_session_ids
    blocked_by_session_ids = socket.assigns.blocked_by_session_ids

    if !Map.has_key?(blocked_session_ids, from_session_id) and
         !Map.has_key?(blocked_by_session_ids, from_session_id) do
      push(socket, event, payload |> payload_without_from)
    end

    {:noreply, socket}
  end

  def handle_out("host_changed" = event, payload, socket) do
    push(socket, event, payload)
    {:noreply, socket}
  end

  defp maybe_push_naf(
         socket,
         event,
         payload,
         false = _block_naf,
         blocked_session_ids,
         blocked_by_session_ids
       )
       when blocked_session_ids === %{} and blocked_by_session_ids === %{} do
    push(socket, event, payload)
    {:noreply, socket}
  end

  defp maybe_push_naf(
         socket,
         event,
         %{from_session_id: from_session_id} = payload,
         false = _block_naf,
         blocked_session_ids,
         blocked_by_session_ids
       ) do
    if !Map.has_key?(blocked_session_ids, from_session_id) and
         !Map.has_key?(blocked_by_session_ids, from_session_id) do
      push(socket, event, payload)
    end

    {:noreply, socket}
  end

  # Sockets can block NAF as an optimization, eg iframe embeds do not need NAF messages until user clicks load
  defp maybe_push_naf(
         socket,
         _event,
         _payload,
         true = _block_naf,
         _blocked_session_ids,
         _blocked_by_session_ids
       ) do
    {:noreply, socket}
  end

  defp spawn_permitted?(template, _socket) when not is_binary(template), do: false

  defp spawn_permitted?(template, socket) do
    account = Guardian.Phoenix.Socket.current_resource(socket)
    hub = socket |> hub_for_socket

    cond do
      template == "#remote-bot-avatar" -> bot_runner?(socket)
      generic_avatar_template?(template) -> true
      template |> String.ends_with?("-media") -> account |> can?(spawn_and_move_media(hub))
      template |> String.ends_with?("-camera") -> account |> can?(spawn_camera(hub))
      template |> String.ends_with?("-drawing") -> account |> can?(spawn_drawing(hub))
      template |> String.ends_with?("-pen") -> account |> can?(spawn_drawing(hub))
      template |> String.ends_with?("-emoji") -> account |> can?(spawn_emoji(hub))
      # We want to forbid messages if they fall through the above list of template suffixes
      true -> false
    end
  end

  defp generic_avatar_template?(template) do
    Regex.match?(~r/\A#[A-Za-z0-9_-]+-avatar\z/, template)
  end

  defp bot_network_id?(network_id, socket) when is_binary(network_id) do
    hub_sid = socket.assigns[:hub_sid]
    prefix = if is_binary(hub_sid), do: "room-bot-#{hub_sid}-bot-", else: nil

    with true <- is_binary(prefix),
         true <- String.starts_with?(network_id, prefix),
         suffix <- String.replace_prefix(network_id, prefix, ""),
         {bot_number, ""} <- Integer.parse(suffix),
         true <- bot_number in 1..10 do
      true
    else
      _ -> false
    end
  end

  defp bot_network_id?(_network_id, _socket), do: false

  defp valid_network_id?(network_id),
    do: is_binary(network_id) and byte_size(network_id) > 0

  defp valid_naf_update?(%{"networkId" => network_id}), do: valid_network_id?(network_id)
  defp valid_naf_update?(_update), do: false

  defp valid_naf_network_ids?(%{
         "dataType" => data_type,
         "data" => %{"networkId" => network_id}
       })
       when data_type in ["u", "r"],
       do: valid_network_id?(network_id)

  defp valid_naf_network_ids?(%{"dataType" => data_type}) when data_type in ["u", "r"],
    do: false

  defp valid_naf_network_ids?(%{"dataType" => "um", "data" => %{"d" => updates}})
       when is_list(updates),
       do: Enum.all?(updates, &valid_naf_update?/1)

  defp valid_naf_network_ids?(%{"dataType" => "um"}), do: false
  defp valid_naf_network_ids?(_payload), do: true

  defp naf_payload_template(%{"data" => %{"template" => template}}), do: template
  defp naf_payload_template(_payload), do: nil

  defp bot_sensitive_payload?(%{"dataType" => "um", "data" => %{"d" => updates}}, socket)
       when is_list(updates),
       do: Enum.any?(updates, &bot_sensitive_update?(&1, socket))

  defp bot_sensitive_payload?(%{"data" => data}, socket) when is_map(data),
    do: bot_sensitive_update?(data, socket)

  defp bot_sensitive_payload?(_payload, _socket), do: false

  defp contains_first_sync?(value) when is_list(value),
    do: Enum.any?(value, &contains_first_sync?/1)

  defp contains_first_sync?(value) when is_map(value) do
    Map.has_key?(value, "isFirstSync") or Enum.any?(Map.values(value), &contains_first_sync?/1)
  end

  defp contains_first_sync?(_value), do: false

  defp bot_network_update?(%{"networkId" => network_id}, socket),
    do: bot_network_id?(network_id, socket)

  defp bot_network_update?(_update, _socket), do: false

  defp bot_sensitive_update?(%{"template" => "#remote-bot-avatar"}, _socket), do: true

  defp bot_sensitive_update?(update, socket) when is_map(update),
    do: bot_network_update?(update, socket)

  defp bot_sensitive_update?(_update, _socket), do: false

  defp malformed_bot_avatar_update?(update, socket) when is_map(update) do
    protected_network_id = bot_network_update?(update, socket)
    dedicated_template = update["template"] == "#remote-bot-avatar"

    (protected_network_id and (not dedicated_template or update["persistent"] !== false)) or
      (dedicated_template and (not protected_network_id or update["persistent"] !== false))
  end

  defp malformed_bot_avatar_update?(_update, _socket), do: false

  defp multi_update_permitted?(%{"template" => template}, socket),
    do: spawn_permitted?(template, socket)

  defp multi_update_permitted?(update, _socket), do: is_map(update)

  defp handle_entry_mode_change(socket, entry_mode) do
    hub = socket |> hub_for_socket
    account = Guardian.Phoenix.Socket.current_resource(socket)

    if account |> can?(close_hub(hub)) do
      result =
        hub
        |> Hub.changeset_for_entry_mode(entry_mode)
        |> BotConfigAdmission.update(account)

      case result do
        {:ok, updated_hub} ->
          updated_hub = Repo.preload(updated_hub, Hub.hub_preloads())
          sync_bot_orchestrator(updated_hub)
          broadcast_hub_refresh!(updated_hub, socket, ["entry_mode", "user_data"])
          {:noreply, socket}

        {:error, _changeset} ->
          reply_error(socket, "hub_close_rejected")
      end
    else
      {:noreply, socket}
    end
  end

  defp with_account(socket, handler) do
    case Guardian.Phoenix.Socket.current_resource(socket) do
      %Account{} = account ->
        handler.(account)
        {:reply, {:ok, %{}}, socket}

      _ ->
        # client should have signed-in at this point,
        # so if we still don't have an account, it must have been an invalid token
        {:reply, {:error, %{reason: :invalid_token}}, socket}
    end
  end

  defp secure_compare(value, expected)
       when is_binary(value) and is_binary(expected) and byte_size(expected) >= 32 do
    byte_size(value) == byte_size(expected) and Plug.Crypto.secure_compare(value, expected)
  end

  defp secure_compare(_, _), do: false

  defp authenticate_bot_runner_context(context, has_valid_bot_access_key) when is_map(context) do
    case Map.get(context, "bot_runner") do
      true when has_valid_bot_access_key ->
        {:ok, context |> Map.delete(:bot_runner) |> Map.put("bot_runner", true), true}

      true ->
        {:error, %{message: "Bot runner authentication failed", reason: "invalid_bot_access_key"}}

      value when value in [nil, false] ->
        {:ok, context |> Map.delete(:bot_runner) |> Map.put("bot_runner", false), false}

      _ ->
        {:error, %{message: "Invalid bot runner request", reason: "invalid_bot_runner_request"}}
    end
  end

  defp authenticate_bot_runner_context(_context, _has_valid_bot_access_key) do
    {:ok, %{"bot_runner" => false}, false}
  end

  defp authenticated_bot_runner?(socket) do
    socket.assigns.has_valid_bot_access_key === true and
      is_map(socket.assigns.context) and socket.assigns.context["bot_runner"] === true
  end

  defp bot_runner?(socket) do
    authenticated_bot_runner?(socket) and
      is_binary(socket.assigns[:bot_runner_lease_id]) and
      BotRunnerLease.authorized?(
        socket.assigns.hub_sid,
        socket.assigns.bot_runner_lease_id,
        socket.assigns[:bot_runner_authority_epoch]
      ) and bot_runner_presence_reflects_authority?(socket)
  end

  defp bot_runner_presence_reflects_authority?(socket) do
    socket
    |> Presence.get_by_key(socket.assigns.session_id)
    |> then(fn
      entry when is_map(entry) -> Map.get(entry, :metas) || Map.get(entry, "metas") || []
      _entry -> []
    end)
    |> then(fn
      metas when is_list(metas) -> metas
      _metas -> []
    end)
    |> Enum.any?(fn
      meta when is_map(meta) ->
        context = Map.get(meta, :context) || Map.get(meta, "context") || %{}

        is_map(context) and
          (Map.get(context, :bot_runner) === true or Map.get(context, "bot_runner") === true) and
          (Map.get(meta, :bot_runner_lease_id) || Map.get(meta, "bot_runner_lease_id")) ==
            socket.assigns.bot_runner_lease_id and
          (Map.get(meta, :bot_runner_authority_epoch) ||
             Map.get(meta, "bot_runner_authority_epoch")) ==
            socket.assigns.bot_runner_authority_epoch and
          (Map.get(meta, :bot_runner_authoritative) === true or
             Map.get(meta, "bot_runner_authoritative") === true)

      _meta ->
        false
    end)
  end

  defp bot_runner_rejection_reason(socket) do
    if authenticated_bot_runner?(socket),
      do: "bot_runner_not_authoritative",
      else: "bot_runner_required"
  end

  @doc false
  def authoritative_bot_runner_lease_id(presence_state) when is_map(presence_state) do
    presence_state
    |> Enum.flat_map(fn {session_id, entry} ->
      metas =
        if is_map(entry), do: Map.get(entry, :metas) || Map.get(entry, "metas") || [], else: []

      Enum.flat_map(metas, fn meta ->
        context =
          if is_map(meta), do: Map.get(meta, :context) || Map.get(meta, "context"), else: nil

        lease_id =
          if is_map(meta),
            do: Map.get(meta, :bot_runner_lease_id) || Map.get(meta, "bot_runner_lease_id"),
            else: nil

        authority_epoch =
          if is_map(meta),
            do:
              Map.get(meta, :bot_runner_authority_epoch) ||
                Map.get(meta, "bot_runner_authority_epoch"),
            else: nil

        authoritative =
          if is_map(meta),
            do:
              Map.get(meta, :bot_runner_authoritative) === true or
                Map.get(meta, "bot_runner_authoritative") === true,
            else: false

        if is_map(context) and
             (Map.get(context, "bot_runner") === true or Map.get(context, :bot_runner) === true) and
             is_binary(lease_id) and authoritative and is_integer(authority_epoch) and
             authority_epoch > 0 do
          [{authority_epoch, to_string(session_id), lease_id}]
        else
          []
        end
      end)
    end)
    |> Enum.sort_by(fn {authority_epoch, session_id, lease_id} ->
      {-authority_epoch, session_id, lease_id}
    end)
    |> List.first()
    |> case do
      {_authority_epoch, _session_id, lease_id} -> lease_id
      nil -> nil
    end
  end

  def authoritative_bot_runner_lease_id(_), do: nil

  defp account_for_socket(socket) do
    case Guardian.Phoenix.Socket.current_resource(socket) do
      nil ->
        {:error, :not_logged_in}

      account ->
        {:ok, account}
    end
  end

  defp auth(hub, account, :write_entity_state) do
    if can?(account, pin_objects(hub)) do
      :ok
    else
      {:error, :unauthorized}
    end
  end

  defp authorize(socket, :write_entity_state) do
    hub = hub_for_socket(socket)

    with {:ok, account} <- account_for_socket(socket),
         :ok <- auth(hub, account, :write_entity_state) do
      {:ok, hub, account}
    end
  end

  def handle_info({:begin_tracking, session_id, _hub_sid}, socket) do
    {:ok, _} = Presence.track(socket, session_id, socket |> presence_meta_for_socket)
    push(socket, "presence_state", socket |> Presence.list())

    {:noreply, assign(socket, :bot_runner_presence_tracked, true)}
  end

  def handle_info(
        {:bot_runner_lease_authority, lease_id, authority_epoch, authoritative},
        socket
      ) do
    if socket.assigns[:bot_runner_lease_id] == lease_id do
      socket =
        socket
        |> assign(:bot_runner_authority_epoch, authority_epoch)
        |> assign(:bot_runner_authoritative, authoritative)

      socket =
        if socket.assigns[:bot_runner_presence_tracked] do
          broadcast_presence_update(socket)
        else
          socket
        end

      {:noreply, socket}
    else
      {:noreply, socket}
    end
  end

  def handle_info({:waypoint_reservation_states, states}, socket) do
    broadcast_waypoint_states(socket, states)
    {:noreply, socket}
  end

  def handle_info(:close_channel, socket) do
    GenServer.cast(self(), :close)
    {:noreply, socket}
  end

  def handle_info(_message, socket) do
    {:noreply, socket}
  end

  defp perform_pin!(object_id, gltf_node, account, socket) do
    hub = socket |> hub_for_socket
    RoomObject.perform_pin!(hub, account, %{object_id: object_id, gltf_node: gltf_node})
    broadcast_pinned_media(socket, object_id, gltf_node)
  end

  def terminate(_reason, socket) do
    unregister_bot_runner_lease(socket)

    # enable_terminate_actions is set to false during tests. Since the GenServer is forcefully
    # terminated when a test ends, we want to avoid running into an error that would happen if we
    # invoked a DB mutation during termination.
    if Application.get_env(:ret, __MODULE__)[:enable_terminate_actions] !== false do
      socket
      |> SessionStat.stat_query_for_socket()
      |> Repo.update_all(set: [ended_at: NaiveDateTime.utc_now()])

      terminate_waypoint_reservation(socket)
    end

    :ok
  end

  defp set_presence_flag(socket, flag, value) do
    socket = socket |> assign(flag, value) |> broadcast_presence_update
    {:noreply, socket}
  end

  defp broadcast_presence_update(socket) do
    Presence.update(socket, socket.assigns.session_id, socket |> presence_meta_for_socket)
    socket
  end

  defp broadcast_pinned_media(socket, object_id, gltf_node) do
    broadcast!(socket, "pin", %{
      object_id: object_id,
      gltf_node: gltf_node,
      pinned_by: socket.assigns.session_id
    })
  end

  defp broadcast_waypoint_states(socket, states) do
    Enum.each(states, fn state ->
      broadcast!(socket, "waypoint_reservation:state", state)
    end)
  end

  defp terminate_waypoint_reservation(%{assigns: %{waypoint_reservation: reservation}} = socket)
       when reservation.supported do
    result =
      WaypointReservation.terminate_channel(
        reservation.hub_id,
        socket.assigns.session_id,
        reservation.client_instance_id,
        reservation.channel_id
      )

    Enum.each(result.states, fn state ->
      RetWeb.Endpoint.broadcast(socket.topic, "waypoint_reservation:state", state)
    end)
  end

  defp terminate_waypoint_reservation(_socket), do: :ok

  # Broadcasts the full hub info as well as an (optional) list of specific fields which
  # clients should consider stale and need to be updated in client state from the new
  # hub info
  #
  # Note this doesn't necessarily mean the fields have changed.
  #
  # For example, if the scene needs to be refreshed, this message indicates that by including
  # "scene" in the list of stale fields.
  defp broadcast_hub_refresh!(hub, socket, stale_fields) do
    account = Guardian.Phoenix.Socket.current_resource(socket)

    response =
      HubView.render("show.json", %{hub: hub, embeddable: account |> can?(embed_hub(hub))})
      |> Map.put(:session_id, socket.assigns.session_id)
      |> Map.put(:stale_fields, stale_fields)

    broadcast!(socket, "hub_refresh", response)
  end

  defp presence_meta_for_socket(socket) do
    hub = socket |> hub_for_socket
    account = Guardian.Phoenix.Socket.current_resource(socket)

    socket.assigns
    |> maybe_override_identifiers(account)
    |> Map.put(:roles, hub |> Hub.roles_for_account(account))
    |> Map.put(:permissions, hub |> Hub.perms_for_account(account))
    |> Map.take([
      :presence,
      :profile,
      :context,
      :roles,
      :permissions,
      :streaming,
      :recording,
      :hand_raised,
      :typing
    ])
    |> maybe_put_bot_runner_lease(socket.assigns)
  end

  defp maybe_put_bot_runner_lease(meta, %{has_valid_bot_access_key: true} = assigns) do
    meta
    |> Map.put(:bot_runner_lease_id, assigns.bot_runner_lease_id)
    |> Map.put(:bot_runner_join_order, assigns.bot_runner_join_order)
    |> Map.put(:bot_runner_authority_epoch, assigns.bot_runner_authority_epoch)
    |> Map.put(:bot_runner_authoritative, assigns.bot_runner_authoritative)
  end

  defp maybe_put_bot_runner_lease(meta, _assigns), do: meta

  # Hubs Bot can set their own display name.
  defp maybe_override_identifiers(
         %{
           hub_requires_oauth: true,
           has_valid_bot_access_key: true
         } = assigns,
         _account
       ),
       do: assigns

  # Do a direct display name lookup for OAuth users without a verified email (and thus, no Hubs account).
  defp maybe_override_identifiers(
         %{
           hub_requires_oauth: true,
           hub_sid: hub_sid,
           oauth_source: oauth_source,
           oauth_account_id: oauth_account_id
         } = assigns,
         _account
       )
       when not is_nil(oauth_source) and not is_nil(oauth_account_id) do
    hub = Hub |> Repo.get_by(hub_sid: hub_sid) |> Repo.preload(:hub_bindings)

    # Assume hubs only have a single hub binding for now.
    hub_binding = hub.hub_bindings |> Enum.at(0)

    oauth_provider = %Ret.OAuthProvider{
      source: oauth_source,
      provider_account_id: oauth_account_id
    }

    assigns |> override_display_name_via_binding(oauth_provider, hub_binding)
  end

  # If there isn't an oauth account id on the socket, we expect the user to have an account
  defp maybe_override_identifiers(
         %{
           hub_requires_oauth: true,
           hub_sid: hub_sid,
           oauth_account_id: oauth_account_id
         } = assigns,
         account
       )
       when is_nil(oauth_account_id) do
    hub = Hub |> Repo.get_by(hub_sid: hub_sid) |> Repo.preload(:hub_bindings)

    # Assume hubs only have a single hub binding for now.
    hub_binding = hub.hub_bindings |> Enum.at(0)

    # There's no way tell which oauth_provider a user would like to identify with. We're just going to pick
    # the first one for now.
    oauth_provider =
      account.oauth_providers
      |> Enum.filter(fn provider -> hub_binding.type == provider.source end)
      |> Enum.at(0)

    assigns |> override_display_name_via_binding(oauth_provider, hub_binding)
  end

  # For unbound hubs, set the identity name for the account.
  defp maybe_override_identifiers(
         %{
           hub_requires_oauth: false
         } = assigns,
         %Account{identity: %Identity{name: name}}
       ),
       do: put_in(assigns.profile["identityName"], name)

  defp maybe_override_identifiers(
         %{
           hub_requires_oauth: false
         } = assigns,
         _account
       ),
       do: assigns

  defp override_display_name_via_binding(assigns, oauth_provider, hub_binding) do
    display_name = oauth_provider |> Ret.HubBinding.fetch_display_name(hub_binding)
    community_identifier = oauth_provider |> Ret.HubBinding.fetch_community_identifier()

    overriden =
      assigns.profile
      |> Map.merge(%{
        "displayName" => display_name,
        "identityName" => community_identifier,
        # Deprecated
        "communityIdentifier" => community_identifier
      })

    assigns |> Map.put(:profile, overriden)
  end

  defp join_with_hub(%Hub{entry_mode: :deny}, _account, _socket, _context, _params) do
    {:error, %{message: "Hub no longer accessible", reason: "closed"}}
  end

  defp join_with_hub(
         %Hub{},
         %Account{},
         _socket,
         _context,
         %{
           hub_requires_oauth: false,
           account_can_join: false
         }
       ),
       do: deny_join()

  defp join_with_hub(
         %Hub{entry_mode: :invite},
         _account,
         _socket,
         _context,
         %{
           has_active_invite: false,
           account_can_update: false
         }
       ),
       do: deny_join()

  defp join_with_hub(
         %Hub{},
         %Account{},
         _socket,
         _context,
         %{
           hub_requires_oauth: true,
           account_has_provider_for_hub: true,
           account_can_join: false
         }
       ),
       do: deny_join()

  defp join_with_hub(
         %Hub{},
         nil = _account,
         _socket,
         _context,
         %{
           hub_requires_oauth: true,
           has_valid_bot_access_key: false,
           has_perms_token: true,
           perms_token_can_join: false
         }
       ),
       do: deny_join()

  defp join_with_hub(
         %Hub{} = hub,
         %Account{},
         _socket,
         _context,
         %{
           hub_requires_oauth: true,
           account_has_provider_for_hub: false
         }
       ),
       do: require_oauth(hub)

  defp join_with_hub(
         %Hub{} = hub,
         nil = _account,
         _socket,
         _context,
         %{
           hub_requires_oauth: true,
           has_valid_bot_access_key: false,
           has_perms_token: false
         }
       ),
       do: require_oauth(hub)

  # Join denied based upon account requirement
  defp join_with_hub(
         %Hub{},
         nil = _account,
         _socket,
         _context,
         %{
           hub_requires_oauth: false,
           has_valid_bot_access_key: false,
           has_perms_token: false,
           account_can_join: false
         }
       ),
       do: deny_join()

  defp join_with_hub(%Hub{} = hub, account, socket, context, params) do
    hub = hub |> Hub.ensure_host()

    hub =
      if context["embed"] && !hub.embedded do
        hub
        |> Hub.changeset_for_seen_embedded_hub()
        |> Repo.update!()
      else
        hub
      end

    # Each channel connection needs to be aware if there are, or ever have been,
    # embeddings of this hub (see internal_naf_event_for/2)
    socket = socket |> assign(:has_embeds, hub.embedded)

    push_subscription_endpoint = params["push_subscription_endpoint"]

    is_push_subscribed =
      push_subscription_endpoint &&
        hub.web_push_subscriptions |> Enum.any?(&(&1.endpoint == push_subscription_endpoint))

    is_favorited = AccountFavorite.timestamp_join_if_favorited(hub, account)

    socket = Guardian.Phoenix.Socket.put_current_resource(socket, account)

    with {:ok, socket} <-
           socket
           |> assign(:hub_sid, hub.hub_sid)
           |> assign(:hub_requires_oauth, params[:hub_requires_oauth])
           |> assign(:presence, :lobby)
           |> assign(:oauth_account_id, params[:oauth_account_id])
           |> assign(:oauth_source, params[:oauth_source])
           |> assign(:has_valid_bot_access_key, params[:has_valid_bot_access_key])
           |> assign_bot_runner_lease(params[:has_valid_bot_access_key], hub),
         {socket, waypoint_capability, waypoint_states} <-
           configure_waypoint_reservation(socket, hub, context, params),
         response <-
           HubView.render("show.json", %{hub: hub, embeddable: account |> can?(embed_hub(hub))}) do
      perms_token = params["perms_token"] || get_perms_token(hub, account)
      bot_chat_capability = if account, do: new_bot_chat_capability(), else: nil
      socket = assign(socket, :bot_chat_capability, bot_chat_capability)

      response =
        response
        |> Map.put(:session_id, socket.assigns.session_id)
        |> Map.put(
          :session_token,
          socket.assigns.session_id |> Ret.SessionToken.token_for_session()
        )
        |> Map.put(:subscriptions, %{web_push: is_push_subscribed, favorites: is_favorited})
        |> Map.put(:perms_token, perms_token)
        |> Map.put(:hub_requires_oauth, params[:hub_requires_oauth])
        |> Map.put(:bot_runner, params[:has_valid_bot_access_key] === true)
        |> Map.put(:bot_runner_lease_id, socket.assigns[:bot_runner_lease_id])
        |> Map.put(:bot_runner_authority_epoch, socket.assigns[:bot_runner_authority_epoch])
        |> Map.put(:bot_runner_authoritative, socket.assigns[:bot_runner_authoritative])
        |> Map.put(:bot_chat_capability, bot_chat_capability)
        |> Map.put(:waypoint_reservation, waypoint_capability)

      existing_stat_count =
        socket
        |> SessionStat.stat_query_for_socket()
        |> Repo.all()
        |> length

      unless existing_stat_count > 0 do
        with session_id <- socket.assigns.session_id,
             started_at <- socket.assigns.started_at,
             stat_attrs <- %{session_id: session_id, started_at: started_at},
             changeset <- %SessionStat{} |> SessionStat.changeset(stat_attrs) do
          Repo.insert(changeset)
        end
      end

      send(self(), {:begin_tracking, socket.assigns.session_id, hub.hub_sid})
      send(self(), {:waypoint_reservation_states, waypoint_states})

      # Send join push notification if this is the first joiner
      if Presence.list(socket.topic) |> Enum.count() == 0 do
        Task.start_link(fn ->
          hub |> Hub.send_push_messages_for_join(push_subscription_endpoint)
        end)
      end

      Statix.increment("ret.channels.hub.joins.ok")

      {:ok, response, socket}
    end
  end

  defp assign_bot_runner_lease(socket, true, %Hub{} = hub) do
    case BotConfigApproval.register_runtime_lease(hub.hub_sid) do
      {:ok, lease} ->
        {:ok, assign_registered_bot_runner_lease(socket, lease)}

      {:error, _reason} ->
        {:error,
         %{
           message: "Bot configuration is not approved",
           reason: "bot_config_unapproved"
         }}
    end
  end

  defp assign_bot_runner_lease(socket, _authenticated, %Hub{}) do
    {:ok, assign_no_bot_runner_lease(socket)}
  end

  defp assign_registered_bot_runner_lease(socket, lease) do
    socket
    |> assign(:bot_runner_lease_id, lease.lease_id)
    |> assign(:bot_runner_join_order, lease.join_order)
    |> assign(:bot_runner_authority_epoch, lease.authority_epoch)
    |> assign(:bot_runner_authoritative, lease.authoritative)
    |> assign(:bot_runner_presence_tracked, false)
  end

  defp assign_no_bot_runner_lease(socket) do
    socket
    |> assign(:bot_runner_lease_id, nil)
    |> assign(:bot_runner_join_order, nil)
    |> assign(:bot_runner_authority_epoch, nil)
    |> assign(:bot_runner_authoritative, false)
    |> assign(:bot_runner_presence_tracked, false)
  end

  defp unregister_bot_runner_lease(%{
         assigns: %{
           hub_sid: hub_sid,
           bot_runner_lease_id: lease_id,
           has_valid_bot_access_key: true
         }
       })
       when is_binary(lease_id) do
    BotRunnerLease.unregister(hub_sid, lease_id)
  end

  defp unregister_bot_runner_lease(_socket), do: :ok

  defp configure_waypoint_reservation(socket, hub, context, params) do
    channel_id = SecureRandom.uuid()
    bot_runner = context["bot_runner"] == true

    registration =
      if bot_runner do
        :unsupported
      else
        WaypointReservation.join_client_instance(params["waypoint_reservation"])
      end

    case registration do
      {:ok, client_instance_id} ->
        result =
          WaypointReservation.register_channel(
            hub.hub_id,
            socket.assigns.session_id,
            client_instance_id,
            channel_id
          )

        reservation = %{
          supported: true,
          hub_id: hub.hub_id,
          client_instance_id: client_instance_id,
          channel_id: channel_id,
          bot_runner: false
        }

        capability = %{
          protocol: WaypointReservation.protocol(),
          supported: true,
          lease_ms: WaypointReservation.lease_ms(),
          request_timeout_ms: WaypointReservation.request_timeout_ms(),
          snapshot_state_version: result.snapshot_state_version,
          active: result.active,
          current: result.current,
          request_seq: result.request_seq
        }

        {assign(socket, :waypoint_reservation, reservation), capability, result.states}

      :unsupported ->
        reservation = %{
          supported: false,
          hub_id: hub.hub_id,
          client_instance_id: nil,
          channel_id: channel_id,
          bot_runner: bot_runner
        }

        {assign(socket, :waypoint_reservation, reservation),
         WaypointReservation.unsupported_capability(), []}
    end
  end

  defp require_oauth(hub) do
    oauth_info = hub.hub_bindings |> get_oauth_info(hub.hub_sid)
    {:error, %{message: "OAuth required", reason: "oauth_required", oauth_info: oauth_info}}
  end

  defp deny_join do
    {:error, %{message: "Join denied", reason: "join_denied"}}
  end

  defp get_oauth_info(hub_bindings, hub_sid) do
    hub_bindings
    |> Enum.map(
      &case &1 do
        %{type: :discord} -> %{type: :discord, url: Ret.DiscordClient.get_oauth_url(hub_sid)}
        %{type: :slack} -> %{type: :slack, url: Ret.SlackClient.get_oauth_url(hub_sid)}
      end
    )
  end

  defp get_perms_token(
         hub,
         %Ret.OAuthProvider{provider_account_id: provider_account_id, source: source} = account
       ) do
    hub
    |> Hub.perms_for_account(account)
    |> Map.put(:oauth_account_id, provider_account_id)
    |> Map.put(:oauth_source, source)
    |> Map.put(:hub_id, hub.hub_sid)
    |> Ret.PermsToken.token_for_perms()
  end

  defp get_perms_token(hub, account) do
    account_id = if account, do: account.account_id, else: nil

    hub
    |> Hub.perms_for_account(account)
    |> Account.add_global_perms_for_account(account)
    |> Map.put(:account_id, account_id |> to_string)
    |> Map.put(:hub_id, hub.hub_sid)
    |> Ret.PermsToken.token_for_perms()
  end

  defp handle_entered_event(socket, payload) do
    stat_attributes = [
      entered_event_payload: payload,
      entered_event_received_at: NaiveDateTime.utc_now()
    ]

    # Flip context to have HMD if entered with display type
    socket =
      with %{"entryDisplayType" => display} when is_binary(display) and display != "Screen" <-
             payload,
           %{context: context} when is_map(context) <- socket.assigns do
        socket |> assign(:context, context |> Map.put("hmd", true))
      else
        _ -> socket
      end

    socket
    |> SessionStat.stat_query_for_socket()
    |> Repo.update_all(set: stat_attributes)

    context = socket.assigns.context || %{}

    socket =
      socket
      |> assign(:presence, :room)
      |> assign(:context, context |> Map.delete("entering"))
      |> broadcast_presence_update

    case Guardian.Phoenix.Socket.current_resource(socket) do
      %Ret.Account{account_id: account_id} ->
        :ok =
          BotChatPresence.track(
            self(),
            socket.assigns.hub_sid,
            account_id,
            socket.assigns.bot_chat_capability
          )

      _account ->
        :ok
    end

    socket
  end

  defp rotate_bot_chat_capability(socket, %Account{account_id: account_id}) do
    :ok = BotChatPresence.untrack(self())
    capability = new_bot_chat_capability()
    socket = assign(socket, :bot_chat_capability, capability)

    if socket.assigns[:presence] == :room do
      :ok = BotChatPresence.track(self(), socket.assigns.hub_sid, account_id, capability)
    end

    socket
  end

  defp new_bot_chat_capability do
    24
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  defp handle_max_occupant_update(socket, occupant_count) do
    socket
    |> hub_for_socket
    |> Hub.changeset_for_new_seen_occupant_count(occupant_count)
    |> Repo.update!()

    socket
  end

  defp handle_object_spawned(socket, object_type) do
    socket
    |> hub_for_socket
    |> Hub.changeset_for_new_spawned_object_type(object_type)
    |> Repo.update!()

    socket
  end

  defp hub_for_socket(socket) do
    Repo.get_by(Hub, hub_sid: socket.assigns.hub_sid)
    |> Repo.preload([:hub_bindings, :hub_role_memberships])
  end

  defp payload_with_from(payload, socket) do
    payload |> Map.put(:from_session_id, socket.assigns.session_id)
  end

  defp payload_without_from(payload) do
    payload |> Map.delete(:from_session_id)
  end

  defp assign_has_blocks(socket) do
    has_blocks =
      socket.assigns.blocked_session_ids |> Enum.any?() ||
        socket.assigns.blocked_by_session_ids |> Enum.any?()

    socket |> assign(:has_blocks, has_blocks)
  end

  # Normally, naf and nafr messages are sent as is. However, if this connection is blocking users,
  # has been blocked, or the hub itself has been seen in an iframe, we need to potentially filter
  # NAF messages. As such, we internally route messages via an intercepted handle_out for filtering.
  # This is done via the intercepted maybe-nafr and maybe-naf events.
  #
  # We avoid doing this in general because it's extremely expensive, since it re-encodes all outgoing messages.
  defp internal_naf_event_for("nafr", %Phoenix.Socket{
         assigns: %{has_blocks: false, has_embeds: false}
       }),
       do: "nafr"

  defp internal_naf_event_for("naf", %Phoenix.Socket{
         assigns: %{has_blocks: false, has_embeds: false}
       }),
       do: "naf"

  defp internal_naf_event_for("nafr", _socket), do: "maybe-nafr"
  defp internal_naf_event_for("naf", _socket), do: "maybe-naf"

  defp parse(%{"nid" => nid, "root_nid" => root_nid, "update_message" => update_message}) do
    %{nid: nid, root_nid: root_nid, update_message: Jason.encode!(update_message)}
  end

  defp parse(
         %{
           "nid" => nid,
           "create_message" => create_message,
           "updates" => updates
         } = params
       ) do
    %{
      nid: nid,
      create_message: Jason.encode!(create_message),
      updates: Enum.map(updates, &parse/1),
      promotion_token: Map.get(params, "promotion_token", nil),
      file_id: Map.get(params, "file_id", nil),
      file_access_token: Map.get(params, "file_access_token", nil)
    }
  end

  defp maybe_set_owned_file_inactive(%{"file_id" => file_id}, _account) do
    OwnedFile.set_inactive(file_id)
  end

  defp maybe_set_owned_file_inactive(_payload, _account) do
    {:ok, :no_file}
  end

  defp maybe_promote_file(%{file_id: nil} = _params, _account, _socket) do
    :ok
  end

  defp maybe_promote_file(params, account, _socket) do
    with {:ok, _owned_file} <-
           Storage.promote(
             params.file_id,
             params.file_access_token,
             params.promotion_token,
             account,
             false
           ) do
      OwnedFile.set_active(params.file_id, account.account_id)
      :ok
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp sync_bot_orchestrator(%Hub{} = hub) do
    bots = BotConfig.normalize(hub.user_data)

    if BotConfigApproval.runtime_enabled?(hub) do
      _ =
        BotOrchestrator.room_config(%{
          hub_sid: hub.hub_sid,
          bots: bots
        })
    else
      _ =
        BotOrchestrator.room_stop(%{
          hub_sid: hub.hub_sid
        })
    end

    :ok
  end

  defp reply_error(socket, reason) do
    {:reply, {:error, %{reason: reason}}, socket}
  end
end
