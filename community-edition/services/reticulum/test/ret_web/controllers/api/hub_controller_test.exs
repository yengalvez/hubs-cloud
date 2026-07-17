defmodule RetWeb.HubControllerTest do
  use RetWeb.ConnCase
  import Ret.TestHelpers

  alias Ret.{Hub, Scene, Repo, AppConfig}

  setup [:create_account, :create_owned_file, :create_scene]

  setup do
    original_orchestrator_config = Application.get_env(:ret, Ret.BotOrchestrator)
    original_test_receiver = Application.get_env(:ret, Ret.BotOrchestratorTestHttpClient)

    Application.put_env(
      :ret,
      Ret.BotOrchestrator,
      endpoint: "http://bot-orchestrator.test",
      access_key: String.duplicate("k", 32),
      http_client: Ret.BotOrchestratorTestHttpClient
    )

    Application.put_env(:ret, Ret.BotOrchestratorTestHttpClient, self())

    on_exit(fn ->
      restore_application_env(:ret, Ret.BotOrchestrator, original_orchestrator_config)
      restore_application_env(:ret, Ret.BotOrchestratorTestHttpClient, original_test_receiver)
    end)

    :ok
  end

  test "anyone can create a hub", %{conn: conn} do
    %{"status" => "ok"} =
      conn
      |> create_hub("Test Hub")
      |> json_response(200)
  end

  test "non-admins can't create a hub when creation disabled", %{conn: conn} do
    AppConfig.set_config_value("features|disable_room_creation", true)

    conn
    |> create_hub("Test Hub")
    |> response(401)

    AppConfig.set_config_value("features|disable_room_creation", false)
  end

  test "disabled accounts cannot create a hub", %{conn: conn} do
    disabled_account = create_account("disabled_account")
    disabled_account |> Ecto.Changeset.change(state: :disabled) |> Ret.Repo.update!()

    conn
    |> put_auth_header_for_email("disabled_account@mozilla.com")
    |> create_hub("Test Hub")
    |> response(401)
  end

  @tag :authenticated
  test "hub is assigned a creator when authenticated", %{conn: conn} do
    %{"hub_id" => hub_id} =
      conn
      |> create_hub("Test Hub")
      |> json_response(200)

    created_hub = Hub |> Repo.get_by(hub_sid: hub_id) |> Repo.preload(:created_by_account)

    created_account = Ret.Account.account_for_email("test@mozilla.com")
    assert created_hub.created_by_account.account_id == created_account.account_id
  end

  test "anyone can assign user_data to a hub", %{conn: conn} do
    %{"hub_id" => hub_id} =
      conn
      |> create_hub_with_attrs(%{name: "Test Hub", user_data: %{test: "Hello World"}})
      |> json_response(200)

    created_hub = Hub |> Repo.get_by(hub_sid: hub_id)

    assert created_hub.user_data["test"] == "Hello World"
  end

  test "a non-admin cannot create an active bot room", %{conn: conn} do
    conn
    |> create_hub_with_attrs(%{name: "Denied bot room", user_data: active_bot_user_data()})
    |> response(422)

    refute Repo.get_by(Hub, name: "Denied bot room")
  end

  test "a global admin can create an active bot room", %{conn: conn} do
    admin = create_account("hub-controller-bot-admin", true)

    %{"hub_id" => hub_id} =
      conn
      |> auth_with_account(admin)
      |> create_hub_with_attrs(%{name: "Admin bot room", user_data: active_bot_user_data()})
      |> json_response(200)

    assert Hub
           |> Repo.get_by!(hub_sid: hub_id)
           |> Map.fetch!(:user_data)
           |> get_in(["bots", "count"]) ==
             1
  end

  test "non-room owners can't update a hub", %{conn: conn} do
    %{"hub_id" => hub_id} =
      conn
      |> create_hub("Test Hub")
      |> json_response(200)

    conn
    |> update_hub(hub_id, %{name: "New Name"})
    |> response(401)
  end

  @tag :authenticated
  test "The room owner can update a hub", %{conn: conn} do
    %{"hub_id" => hub_id} =
      conn
      |> create_hub("Test Hub")
      |> json_response(200)

    %{"hubs" => hubs} =
      conn
      |> update_hub(hub_id, %{name: "New Name"})
      |> json_response(200)

    assert Enum.at(hubs, 0)["name"] === "New Name"
  end

  @tag :authenticated
  test "a non-admin room owner cannot activate bots through REST update", %{conn: conn} do
    %{"hub_id" => hub_id} =
      conn
      |> create_hub("REST denied bot room")
      |> json_response(200)

    conn
    |> update_hub(hub_id, %{user_data: active_bot_user_data()})
    |> response(422)

    refute Hub
           |> Repo.get_by!(hub_sid: hub_id)
           |> Map.fetch!(:user_data)
           |> get_in(["bots", "enabled"])
  end

  test "REST delete closes the room and disables its bots atomically", %{conn: conn} do
    admin = create_account("hub-delete-bot-admin", true)

    %{"hub_id" => hub_id} =
      conn
      |> auth_with_account(admin)
      |> create_hub_with_attrs(%{name: "Delete bot room", user_data: active_bot_user_data()})
      |> json_response(200)

    conn
    |> put_req_header("x-ret-admin-access-key", "test-admin-access-key-at-least-32bytes")
    |> delete(api_v1_hub_path(conn, :delete, hub_id))
    |> response(200)

    closed = Repo.get_by!(Hub, hub_sid: hub_id)
    assert closed.entry_mode == :deny
    refute closed.user_data["bots"]["enabled"]
    assert closed.user_data["bots"]["count"] == 1

    assert_receive {:bot_orchestrator_request, :post,
                    "http://bot-orchestrator.test/internal/bots/room-stop", body, headers,
                    options}

    assert Poison.decode!(body) == %{"hub_sid" => hub_id}
    assert {"x-ret-bot-orchestrator-access-key", String.duplicate("k", 32)} in headers
    assert options[:timeout] == 5_000
    assert options[:recv_timeout] == 5_000
  end

  @tag :authenticated
  test "The room owner can change the scene of a hub", %{conn: conn} do
    %{"hub_id" => hub_id} =
      conn
      |> create_hub("Test Hub")
      |> json_response(200)

    hub = Hub |> Repo.get_by(hub_sid: hub_id) |> Repo.preload(:scene)

    assert is_nil(hub.scene_id)

    scene = Scene |> Repo.one()

    assert !is_nil(scene)

    %{"hubs" => hubs} =
      conn
      |> update_hub(hub_id, %{scene_id: scene.scene_sid})
      |> json_response(200)

    hub_response = Enum.at(hubs, 0)

    assert hub_response["scene"]["scene_id"] === scene.scene_sid
  end

  @tag :authenticated
  test "The room owner can change the member_permissions of a hub", %{conn: conn} do
    %{"hub_id" => hub_id} =
      conn
      |> create_hub_with_attrs(%{name: "Test Hub"})
      |> json_response(200)

    hub = Hub |> Repo.get_by(hub_sid: hub_id) |> Repo.preload(:scene)

    assert Hub.has_member_permission?(hub, :spawn_camera) === false
    assert Hub.has_member_permission?(hub, :spawn_and_move_media) === false
    assert Hub.has_member_permission?(hub, :pin_objects) === false

    %{"hubs" => hubs} =
      conn
      |> update_hub(hub_id, %{member_permissions: %{spawn_camera: true, pin_objects: true}})
      |> json_response(200)

    hub_response = Enum.at(hubs, 0)

    assert hub_response["member_permissions"]["spawn_camera"] === true
    assert hub_response["member_permissions"]["spawn_and_move_media"] === false
    assert hub_response["member_permissions"]["pin_objects"] === true

    %{"hubs" => hubs} =
      conn
      |> update_hub(hub_id, %{
        member_permissions: %{spawn_and_move_media: true, pin_objects: false}
      })
      |> json_response(200)

    hub_response = Enum.at(hubs, 0)

    assert hub_response["member_permissions"]["spawn_camera"] === true
    assert hub_response["member_permissions"]["spawn_and_move_media"] === true
    assert hub_response["member_permissions"]["pin_objects"] === false
  end

  @tag :authenticated
  test "The room owner can allow the promotion of a hub", %{conn: conn} do
    %{"hub_id" => hub_id} =
      conn
      |> create_hub("Test Hub")
      |> json_response(200)

    hub = Hub |> Repo.get_by(hub_sid: hub_id) |> Repo.preload(:scene)

    assert hub.allow_promotion === false

    AppConfig.set_config_value("features|public_rooms", true)

    %{"hubs" => hubs} =
      conn
      |> update_hub(hub_id, %{allow_promotion: true})
      |> json_response(200)

    hub_response = Enum.at(hubs, 0)

    assert hub_response["allow_promotion"] === true

    %{"hubs" => hubs} =
      conn
      |> update_hub(hub_id, %{allow_promotion: false})
      |> json_response(200)

    hub_response = Enum.at(hubs, 0)

    assert hub_response["allow_promotion"] === false

    AppConfig.set_config_value("features|public_rooms", false)
  end

  @tag :authenticated
  test "An error is returned of the scene cannot be found", %{conn: conn} do
    %{"hub_id" => hub_id} =
      conn
      |> create_hub("Test Hub")
      |> json_response(200)

    conn
    |> update_hub(hub_id, %{name: "New Name", scene_id: "badscene"})
    |> response(422)
  end

  defp create_hub(conn, name) do
    create_hub_with_attrs(conn, %{name: name})
  end

  defp create_hub_with_attrs(conn, attrs) do
    req = conn |> api_v1_hub_path(:create, %{"hub" => attrs})
    conn |> post(req)
  end

  defp update_hub(conn, hub_id, attrs) do
    body = Poison.encode!(%{"hub" => attrs})

    conn
    |> put_req_header("content-type", "application/json")
    |> patch(api_v1_hub_path(conn, :update, hub_id), body)
  end

  defp active_bot_user_data do
    %{
      bots: %{
        enabled: true,
        count: 1,
        mobility: "static",
        chat_enabled: true,
        prompt: ""
      }
    }
  end

  defp restore_application_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_application_env(app, key, value), do: Application.put_env(app, key, value)
end
