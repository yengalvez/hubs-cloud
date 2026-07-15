defmodule RetWeb.Api.V1.ProjectController do
  use RetWeb, :controller
  require Logger

  alias Ret.{OwnedFile, Project, Repo, Storage, Scene, SceneListing}

  # Limit to 1 TPS
  plug RetWeb.Plugs.RateLimit when action in [:create]

  defp preload(project) do
    project
    |> Repo.preload(
      [
        :project_owned_file,
        :thumbnail_owned_file,
        scene: Scene.scene_preloads(),
        parent_scene: Scene.scene_preloads(),
        parent_scene_listing: [
          :model_owned_file,
          :screenshot_owned_file,
          :scene_owned_file,
          :project,
          :account,
          scene: Scene.scene_preloads()
        ]
      ],
      force: true
    )
  end

  def index(conn, %{} = _params) do
    account = Guardian.Plug.current_resource(conn)
    projects = Project.projects_for_account(account)
    render(conn, "index.json", projects: projects)
  end

  def show(conn, %{"id" => project_sid}) do
    account = Guardian.Plug.current_resource(conn)

    case Project.project_by_sid_for_account(project_sid, account) do
      %Project{} = project -> render(conn, "show.json", project: project)
      nil -> render_error_json(conn, :not_found)
    end
  end

  def create(conn, %{"project" => %{"parent_scene_id" => parent_scene_sid} = params}) do
    account = Guardian.Plug.current_resource(conn)

    case parent_scene_sid |> Scene.scene_or_scene_listing_by_sid() do
      %t{} = s when t in [Scene, SceneListing] ->
        conn |> create_or_update_project(account, params, %Project{}, s)

      _ ->
        conn |> send_resp(404, "not found")
    end
  end

  def create(conn, %{"project" => params}) do
    account = Guardian.Plug.current_resource(conn)
    create_or_update_project(conn, account, params, %Project{})
  end

  def update(conn, %{"id" => project_sid, "project" => params}) do
    account = Guardian.Plug.current_resource(conn)

    with %Project{} = project <- Project.project_by_sid_for_account(project_sid, account) do
      create_or_update_project(conn, account, params, project)
    else
      nil -> render_error_json(conn, :not_found)
    end
  end

  defp create_or_update_project(conn, account, params, project, parent_scene \\ nil) do
    promotion_params = %{
      project: {params["project_file_id"], params["project_file_token"]},
      thumbnail: {params["thumbnail_file_id"], params["thumbnail_file_token"]}
    }

    promotions = Storage.promote_with_status(promotion_params, account)

    case promotions do
      %{
        project: {:ok, project_file, _project_file_status},
        thumbnail: {:ok, thumbnail_file, _thumbnail_file_status}
      } ->
        case persist_project_with_lock(
               project,
               account,
               project_file,
               thumbnail_file,
               parent_scene,
               params
             ) do
          {:ok, updated_project, replaced_files} ->
            mark_files_inactive(replaced_files)
            render(conn, "show.json", project: updated_project |> preload())

          {:error, error} ->
            mark_created_promotions_inactive(promotions)
            render_error_json(conn, error)
        end

      _ ->
        mark_created_promotions_inactive(promotions)
        render_project_promotion_error(conn, promotion_error(promotions))
    end
  end

  defp persist_project_with_lock(
         %Project{project_id: nil} = project,
         account,
         project_file,
         thumbnail_file,
         parent_scene,
         params
       ) do
    case project
         |> Project.changeset(account, project_file, thumbnail_file, parent_scene, params)
         |> Repo.insert() do
      {:ok, updated_project} -> {:ok, updated_project, []}
      {:error, error} -> {:error, error}
    end
  end

  defp persist_project_with_lock(
         %Project{project_id: project_id},
         account,
         project_file,
         thumbnail_file,
         parent_scene,
         params
       ) do
    case Repo.transaction(fn ->
           case Project.lock_by_id_for_account(project_id, account) do
             nil ->
               Repo.rollback(:not_found)

             locked_project ->
               replaced_files = [
                 locked_project.project_owned_file,
                 locked_project.thumbnail_owned_file
               ]

               case locked_project
                    |> Project.changeset(
                      account,
                      project_file,
                      thumbnail_file,
                      parent_scene,
                      params
                    )
                    |> Repo.update() do
                 {:ok, updated_project} -> {updated_project, replaced_files}
                 {:error, error} -> Repo.rollback(error)
               end
           end
         end) do
      {:ok, {updated_project, replaced_files}} ->
        {:ok, updated_project, replaced_files}

      {:error, error} ->
        {:error, error}
    end
  end

  def delete(conn, %{"id" => project_sid}) do
    account = Guardian.Plug.current_resource(conn)

    case Project.project_by_sid_for_account(project_sid, account) do
      %Project{} = project ->
        case Repo.transaction(fn ->
               case Project.lock_by_id_for_account(project.project_id, account) do
                 nil ->
                   Repo.rollback(:not_found)

                 locked_project ->
                   replaced_files = [
                     locked_project.project_owned_file,
                     locked_project.thumbnail_owned_file
                   ]

                   case Repo.delete(locked_project) do
                     {:ok, _deleted_project} -> replaced_files
                     {:error, error} -> Repo.rollback(error)
                   end
               end
             end) do
          {:ok, replaced_files} ->
            mark_files_inactive(replaced_files)
            send_resp(conn, 200, "OK")

          {:error, :not_found} ->
            render_error_json(conn, :not_found)

          {:error, error} ->
            render_error_json(conn, error)
        end

      nil ->
        render_error_json(conn, :not_found)
    end
  end

  def publish(conn, %{"project_id" => project_sid, "scene" => scene_params}) do
    account = Guardian.Plug.current_resource(conn)

    conn
    |> publish_project(
      scene_params,
      project_sid |> Project.project_by_sid_for_account(account),
      account
    )
  end

  def publish(conn, %{"project_id" => _project_sid}) do
    conn |> render_error_json(400, "You must provide a valid scene to publish")
  end

  defp publish_project(conn, _scene_params, _project = nil, _account) do
    conn |> render_error_json(:not_found)
  end

  defp publish_project(conn, scene_params, project = %Project{scene: nil}, account) do
    conn |> publish_project(scene_params, project, %Scene{}, account)
  end

  defp publish_project(conn, scene_params, project = %Project{scene: scene}, account) do
    conn |> publish_project(scene_params, project, scene, account)
  end

  defp publish_project(conn, scene_params, project = %Project{}, _scene = %Scene{}, account) do
    promotion_params = %{
      model: {scene_params["model_file_id"], scene_params["model_file_token"]},
      screenshot: {scene_params["screenshot_file_id"], scene_params["screenshot_file_token"]},
      scene: {scene_params["scene_file_id"], scene_params["scene_file_token"]}
    }

    promotions = Storage.promote_with_status(promotion_params, account)

    case promotions do
      %{
        model: {:ok, model_owned_file, _model_file_status},
        screenshot: {:ok, screenshot_owned_file, _screenshot_file_status},
        scene: {:ok, scene_owned_file, _scene_file_status}
      } ->
        transaction_result =
          Repo.transaction(fn ->
            locked_project = Project.lock_by_id_for_account(project.project_id, account)

            if is_nil(locked_project) do
              Repo.rollback(:not_found)
            end

            locked_scene = locked_project.scene || %Scene{}

            replaced_files = [
              locked_scene.model_owned_file,
              locked_scene.screenshot_owned_file,
              locked_scene.scene_owned_file
            ]

            scene_changes =
              locked_scene
              |> Scene.changeset(
                account,
                model_owned_file,
                screenshot_owned_file,
                scene_owned_file,
                locked_project.parent_scene_listing || locked_project.parent_scene,
                scene_params
              )

            case Project.add_scene_to_project(locked_project, scene_changes) do
              {:ok, updated_project} -> {updated_project, replaced_files}
              {:error, error} -> Repo.rollback(error)
            end
          end)

        case transaction_result do
          {:ok, {updated_project, replaced_files}} ->
            mark_files_inactive(replaced_files)
            updated_project = preload(updated_project)
            updated_scene = updated_project.scene

            if updated_scene.allow_promotion,
              do: Task.async(fn -> Ret.Support.send_notification_of_new_scene(updated_scene) end)

            render(conn, "show.json", project: updated_project)

          {:error, error} ->
            mark_created_promotions_inactive(promotions)
            render_error_json(conn, error)
        end

      _ ->
        mark_created_promotions_inactive(promotions)

        case promotion_error(promotions) do
          error when error in [:not_found, :not_allowed] ->
            render_error_json(
              conn,
              400,
              "You must provide a valid model, screenshot, and scene file"
            )

          error ->
            render_project_promotion_error(conn, error)
        end
    end
  end

  defp mark_created_promotions_inactive(promotions) do
    promotions
    |> Map.values()
    |> Enum.flat_map(fn
      {:ok, %OwnedFile{} = owned_file, :created} -> [owned_file]
      _ -> []
    end)
    |> mark_files_inactive()
  end

  defp mark_files_inactive(files) do
    files
    |> Enum.filter(&match?(%OwnedFile{}, &1))
    |> Enum.uniq_by(& &1.owned_file_id)
    |> Enum.each(fn owned_file ->
      case Storage.mark_inactive_if_unreferenced(owned_file) do
        result when result in [:ok, :referenced, :marked_inactive] ->
          :ok

        {:error, error} ->
          Logger.error(
            "Failed to mark unreferenced owned file #{owned_file.owned_file_uuid} inactive: #{inspect(error)}"
          )
      end
    end)
  end

  defp promotion_error(promotions) do
    errors =
      promotions
      |> Map.values()
      |> Enum.flat_map(fn
        {:error, error} -> [error]
        _ -> []
      end)

    cond do
      :not_allowed in errors -> :not_allowed
      :not_found in errors -> :not_found
      true -> List.first(errors) || :not_found
    end
  end

  defp render_project_promotion_error(conn, error) when error in [:not_found, :not_allowed],
    do: render_error_json(conn, :not_found)

  defp render_project_promotion_error(conn, :storage_error),
    do: render_error_json(conn, 500, "Storage operation failed")

  defp render_project_promotion_error(conn, error), do: render_error_json(conn, error)
end
