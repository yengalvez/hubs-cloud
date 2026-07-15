defmodule RetWeb.ProjectsControllerTest do
  use RetWeb.ConnCase
  import Ret.TestHelpers

  alias Ret.{Account, OwnedFile, Project, Repo, Storage}

  setup [
    :create_account,
    :create_project_owned_file,
    :create_thumbnail_owned_file,
    :create_project,
    :create_model_owned_file
  ]

  setup do
    on_exit(fn ->
      clear_all_stored_files()
    end)
  end

  test "projects index 401's when not logged in", %{conn: conn} do
    conn |> get(api_v1_project_path(conn, :index)) |> response(401)
  end

  @tag :authenticated
  test "projects index works when logged in", %{conn: conn, project: _project} do
    response = conn |> get(api_v1_project_path(conn, :index)) |> json_response(200)

    %{
      "projects" => [
        %{
          "thumbnail_url" => thumbnail_url,
          "project_url" => project_url,
          "project_id" => project_id,
          "name" => name
        }
      ]
    } = response

    assert name == "Test Project"
    assert thumbnail_url != nil
    assert project_url != nil
    assert project_id != nil
  end

  test "projects show 401's when not logged in", %{conn: conn, project: project} do
    conn |> get(api_v1_project_path(conn, :show, project.project_sid)) |> response(401)
  end

  @tag :authenticated
  test "projects show works when logged in", %{conn: conn, project: project} do
    response =
      conn |> get(api_v1_project_path(conn, :show, project.project_sid)) |> json_response(200)

    %{
      "thumbnail_url" => thumbnail_url,
      "project_url" => project_url,
      "project_id" => project_id,
      "name" => name
    } = response

    assert name == "Test Project"
    assert thumbnail_url != nil
    assert project_url != nil
    assert project_id != nil
  end

  test "projects create 401's when not logged in", %{conn: conn} do
    conn |> post(api_v1_project_path(conn, :create)) |> response(401)
  end

  @tag :authenticated
  test "projects create works when logged in", %{
    conn: conn,
    project_owned_file: project_owned_file,
    thumbnail_owned_file: thumbnail_owned_file
  } do
    params = %{
      project: %{
        name: "Test Project",
        thumbnail_file_id: thumbnail_owned_file.owned_file_uuid,
        thumbnail_file_token: thumbnail_owned_file.key,
        project_file_id: project_owned_file.owned_file_uuid,
        project_file_token: project_owned_file.key
      }
    }

    response = conn |> post(api_v1_project_path(conn, :create, params)) |> json_response(200)

    %{
      "thumbnail_url" => thumbnail_url,
      "project_url" => project_url,
      "project_id" => project_id,
      "name" => name
    } = response

    assert name == "Test Project"
    assert thumbnail_url != nil
    assert project_url != nil
    assert project_id != nil
  end

  test "projects update 401's when not logged in", %{
    conn: conn,
    project: project,
    project_owned_file: project_owned_file,
    thumbnail_owned_file: thumbnail_owned_file
  } do
    params = %{
      project: %{
        name: "Test Project 2",
        thumbnail_file_id: thumbnail_owned_file.owned_file_uuid,
        thumbnail_file_token: thumbnail_owned_file.key,
        project_file_id: project_owned_file.owned_file_uuid,
        project_file_token: project_owned_file.key
      }
    }

    conn
    |> patch(api_v1_project_path(conn, :update, project.project_sid, params))
    |> response(401)
  end

  @tag :authenticated
  test "projects update works when logged in", %{
    conn: conn,
    project: project,
    project_owned_file: project_owned_file,
    thumbnail_owned_file: thumbnail_owned_file
  } do
    params = %{
      project: %{
        name: "Test Project 2",
        thumbnail_file_id: thumbnail_owned_file.owned_file_uuid,
        thumbnail_file_token: thumbnail_owned_file.key,
        project_file_id: project_owned_file.owned_file_uuid,
        project_file_token: project_owned_file.key
      }
    }

    response =
      conn
      |> patch(api_v1_project_path(conn, :update, project.project_sid, params))
      |> json_response(200)

    %{
      "thumbnail_url" => thumbnail_url,
      "project_url" => project_url,
      "project_id" => project_id,
      "name" => name
    } = response

    assert name == "Test Project 2"
    assert thumbnail_url != nil
    assert project_url != nil
    assert project_id != nil
  end

  @tag :authenticated
  test "projects update returns new file URLs and marks replaced files inactive", %{
    conn: conn,
    account: account,
    project: project,
    project_owned_file: old_project_file,
    thumbnail_owned_file: old_thumbnail_file
  } do
    new_project_file = generate_temp_owned_file(account)
    new_thumbnail_file = generate_temp_owned_file(account)

    assert Storage.mark_inactive_if_unreferenced(old_project_file) == :referenced
    assert Repo.get(OwnedFile, old_project_file.owned_file_id).state == :active

    params = %{
      project: %{
        name: "Updated Project",
        thumbnail_file_id: new_thumbnail_file.owned_file_uuid,
        thumbnail_file_token: new_thumbnail_file.key,
        project_file_id: new_project_file.owned_file_uuid,
        project_file_token: new_project_file.key
      }
    }

    response =
      conn
      |> patch(api_v1_project_path(conn, :update, project.project_sid, params))
      |> json_response(200)

    assert response["project_url"] =~ new_project_file.owned_file_uuid
    assert response["thumbnail_url"] =~ new_thumbnail_file.owned_file_uuid
    assert Repo.get(OwnedFile, old_project_file.owned_file_id).state == :inactive
    assert Repo.get(OwnedFile, old_thumbnail_file.owned_file_id).state == :inactive
    assert Repo.get(OwnedFile, new_project_file.owned_file_id).state == :active
    assert Repo.get(OwnedFile, new_thumbnail_file.owned_file_id).state == :active

    assert {:ok, _, _} = Storage.fetch(old_project_file)
    Storage.demote_inactive_owned_files(0)
    assert Repo.get(OwnedFile, old_project_file.owned_file_id) == nil
    assert Repo.get(OwnedFile, old_thumbnail_file.owned_file_id) == nil
  end

  @tag :authenticated
  test "a partial project promotion preserves pre-existing unreferenced files", %{
    conn: conn,
    account: account,
    project: project
  } do
    unreferenced_project_file = generate_temp_owned_file(account)

    params = %{
      project: %{
        name: "Failed Update",
        thumbnail_file_id: Ecto.UUID.generate(),
        thumbnail_file_token: "missing",
        project_file_id: unreferenced_project_file.owned_file_uuid,
        project_file_token: unreferenced_project_file.key
      }
    }

    conn
    |> patch(api_v1_project_path(conn, :update, project.project_sid, params))
    |> response(404)

    assert Repo.get(OwnedFile, unreferenced_project_file.owned_file_id).state == :active
    assert {:ok, _, _} = Storage.fetch(unreferenced_project_file)
  end

  @tag :authenticated
  test "a partial project promotion marks only files created by that request inactive", %{
    conn: conn,
    project: project
  } do
    project_path = generate_temp_file("new project")

    {:ok, project_file_uuid} =
      Storage.store(%Plug.Upload{path: project_path}, "application/json", "new-secret")

    params = %{
      project: %{
        name: "Failed Update",
        thumbnail_file_id: Ecto.UUID.generate(),
        thumbnail_file_token: "missing",
        project_file_id: project_file_uuid,
        project_file_token: "new-secret"
      }
    }

    conn
    |> patch(api_v1_project_path(conn, :update, project.project_sid, params))
    |> json_response(404)

    created_file = Repo.get_by!(OwnedFile, owned_file_uuid: project_file_uuid)
    assert created_file.state == :inactive
    assert {:ok, _, _} = Storage.fetch(created_file)
  end

  @tag :authenticated
  test "updating a project keeps replaced files active when another project references them", %{
    conn: conn,
    account: account,
    project: project,
    project_owned_file: shared_project_file,
    thumbnail_owned_file: shared_thumbnail_file
  } do
    {:ok, _other_project} =
      %Project{}
      |> Project.changeset(account, shared_project_file, shared_thumbnail_file, %{
        name: "Shared Files Project"
      })
      |> Repo.insert()

    new_project_file = generate_temp_owned_file(account)
    new_thumbnail_file = generate_temp_owned_file(account)

    params = %{
      project: %{
        name: "Updated Project",
        thumbnail_file_id: new_thumbnail_file.owned_file_uuid,
        thumbnail_file_token: new_thumbnail_file.key,
        project_file_id: new_project_file.owned_file_uuid,
        project_file_token: new_project_file.key
      }
    }

    conn
    |> patch(api_v1_project_path(conn, :update, project.project_sid, params))
    |> json_response(200)

    assert Repo.get(OwnedFile, shared_project_file.owned_file_id).state == :active
    assert Repo.get(OwnedFile, shared_thumbnail_file.owned_file_id).state == :active
  end

  @tag :authenticated
  test "projects cannot attach files owned by another account", %{
    conn: conn,
    project: project
  } do
    other_account = Account.find_or_create_account_for_email("project-owner-2@mozilla.com")
    other_file = generate_temp_owned_file(other_account)

    params = %{
      project: %{
        name: "Cross Account Update",
        thumbnail_file_id: other_file.owned_file_uuid,
        thumbnail_file_token: other_file.key,
        project_file_id: other_file.owned_file_uuid,
        project_file_token: other_file.key
      }
    }

    conn
    |> patch(api_v1_project_path(conn, :update, project.project_sid, params))
    |> json_response(404)

    assert Repo.get(OwnedFile, other_file.owned_file_id).state == :active
  end

  @tag :authenticated
  test "projects with no scene creates a new scene on publish", %{
    conn: conn,
    project: project,
    project_owned_file: scene_owned_file,
    thumbnail_owned_file: screenshot_owned_file,
    model_owned_file: model_owned_file
  } do
    params = %{
      scene: %{
        name: "Test Publish",
        allow_promotion: true,
        model_file_id: model_owned_file.owned_file_uuid,
        model_file_token: model_owned_file.key,
        screenshot_file_id: screenshot_owned_file.owned_file_uuid,
        screenshot_file_token: screenshot_owned_file.key,
        scene_file_id: scene_owned_file.owned_file_uuid,
        scene_file_token: scene_owned_file.key
      }
    }

    # Publishing the first time should create a new scene
    project = project |> Repo.preload([:scene])
    assert project.scene == nil

    response_project =
      conn
      |> post(api_v1_project_project_path(conn, :publish, project.project_sid, params))
      |> json_response(200)

    new_scene_sid = response_project["scene"]["scene_id"]

    updated_project =
      Project |> Repo.get_by(project_sid: project.project_sid) |> Repo.preload([:scene])

    assert updated_project.scene.scene_sid == new_scene_sid
    assert response_project["name"] == "Test Project"
    assert response_project["scene"]["name"] == "Test Publish"

    # Republishing should not create a new scene
    params = params |> put_in([:scene, :name], "Test Republish")

    response_project =
      conn
      |> post(api_v1_project_project_path(conn, :publish, project.project_sid, params))
      |> json_response(200)

    assert response_project["scene"]["name"] == "Test Republish"
    assert response_project["scene"]["scene_id"] == new_scene_sid
  end

  @tag :authenticated
  test "republishing marks replaced scene files inactive", %{
    conn: conn,
    account: account,
    project: project
  } do
    old_model_file = generate_temp_owned_file(account)
    old_screenshot_file = generate_temp_owned_file(account)
    old_scene_file = generate_temp_owned_file(account)

    first_params = %{
      scene: %{
        name: "First Publish",
        model_file_id: old_model_file.owned_file_uuid,
        model_file_token: old_model_file.key,
        screenshot_file_id: old_screenshot_file.owned_file_uuid,
        screenshot_file_token: old_screenshot_file.key,
        scene_file_id: old_scene_file.owned_file_uuid,
        scene_file_token: old_scene_file.key
      }
    }

    conn
    |> post(api_v1_project_project_path(conn, :publish, project.project_sid, first_params))
    |> json_response(200)

    new_model_file = generate_temp_owned_file(account)
    new_screenshot_file = generate_temp_owned_file(account)
    new_scene_file = generate_temp_owned_file(account)

    second_params = %{
      scene: %{
        name: "Second Publish",
        model_file_id: new_model_file.owned_file_uuid,
        model_file_token: new_model_file.key,
        screenshot_file_id: new_screenshot_file.owned_file_uuid,
        screenshot_file_token: new_screenshot_file.key,
        scene_file_id: new_scene_file.owned_file_uuid,
        scene_file_token: new_scene_file.key
      }
    }

    response =
      conn
      |> post(api_v1_project_project_path(conn, :publish, project.project_sid, second_params))
      |> json_response(200)

    assert response["scene"]["model_url"] =~ new_model_file.owned_file_uuid
    assert response["scene"]["screenshot_url"] =~ new_screenshot_file.owned_file_uuid
    assert Repo.get(OwnedFile, old_model_file.owned_file_id).state == :inactive
    assert Repo.get(OwnedFile, old_screenshot_file.owned_file_id).state == :inactive
    assert Repo.get(OwnedFile, old_scene_file.owned_file_id).state == :inactive
    assert Repo.get(OwnedFile, new_model_file.owned_file_id).state == :active
    assert Repo.get(OwnedFile, new_screenshot_file.owned_file_id).state == :active
    assert Repo.get(OwnedFile, new_scene_file.owned_file_id).state == :active
  end

  test "projects delete 401's when not logged in", %{conn: conn, project: project} do
    conn |> delete(api_v1_project_path(conn, :delete, project.project_sid)) |> response(401)
  end

  @tag :authenticated
  test "projects delete marks its unreferenced files inactive", %{
    conn: conn,
    project: project,
    project_owned_file: project_owned_file,
    thumbnail_owned_file: thumbnail_owned_file
  } do
    conn |> delete(api_v1_project_path(conn, :delete, project.project_sid)) |> response(200)

    deleted_project = Project |> Repo.get_by(project_sid: project.project_sid)

    assert deleted_project == nil
    assert Repo.get(OwnedFile, project_owned_file.owned_file_id).state == :inactive
    assert Repo.get(OwnedFile, thumbnail_owned_file.owned_file_id).state == :inactive
  end

  @tag :authenticated
  test "demotion reactivates an inactive file that is still referenced", %{
    project_owned_file: project_owned_file
  } do
    {:ok, _inactive_file} = OwnedFile.set_inactive(project_owned_file)

    Storage.demote_inactive_owned_files(0)

    assert Repo.get(OwnedFile, project_owned_file.owned_file_id).state == :active
    assert {:ok, _, _} = Storage.fetch(project_owned_file)
  end

  @tag :authenticated
  test "projects delete shows a 404 when the user does not own the project", %{
    conn: conn,
    project_owned_file: project_owned_file,
    thumbnail_owned_file: thumbnail_owned_file
  } do
    other_account = Account.find_or_create_account_for_email("test2@mozilla.com")

    {:ok, project} =
      %Project{}
      |> Project.changeset(other_account, project_owned_file, thumbnail_owned_file, %{
        name: "Test Project"
      })
      |> Repo.insert_or_update()

    conn |> delete(api_v1_project_path(conn, :delete, project.project_sid)) |> response(404)

    deleted_project = Project |> Repo.get_by(project_sid: project.project_sid)

    assert deleted_project != nil
  end
end
