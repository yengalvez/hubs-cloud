defmodule Ret.OwnedFile do
  use Ecto.Schema
  import Ecto.Query
  import Ecto.Changeset
  require Logger
  alias Ret.{Repo, OwnedFile, Account}

  @type t :: %__MODULE__{}

  @schema_prefix "ret0"
  @primary_key {:owned_file_id, :id, autogenerate: true}

  @reference_columns [
    {"app_configs", "owned_file_id"},
    {"assets", "asset_owned_file_id"},
    {"assets", "thumbnail_owned_file_id"},
    {"avatar_listings", "base_map_owned_file_id"},
    {"avatar_listings", "bin_owned_file_id"},
    {"avatar_listings", "emissive_map_owned_file_id"},
    {"avatar_listings", "gltf_owned_file_id"},
    {"avatar_listings", "normal_map_owned_file_id"},
    {"avatar_listings", "orm_map_owned_file_id"},
    {"avatar_listings", "thumbnail_owned_file_id"},
    {"avatars", "base_map_owned_file_id"},
    {"avatars", "bin_owned_file_id"},
    {"avatars", "emissive_map_owned_file_id"},
    {"avatars", "gltf_owned_file_id"},
    {"avatars", "normal_map_owned_file_id"},
    {"avatars", "orm_map_owned_file_id"},
    {"avatars", "thumbnail_owned_file_id"},
    {"projects", "project_owned_file_id"},
    {"projects", "thumbnail_owned_file_id"},
    {"scene_listings", "model_owned_file_id"},
    {"scene_listings", "scene_owned_file_id"},
    {"scene_listings", "screenshot_owned_file_id"},
    {"scenes", "model_owned_file_id"},
    {"scenes", "scene_owned_file_id"},
    {"scenes", "screenshot_owned_file_id"}
  ]

  @reference_query "SELECT EXISTS (" <>
                     Enum.map_join(@reference_columns, " UNION ALL ", fn {table, column} ->
                       "SELECT 1 FROM ret0.#{table} WHERE #{column} = $1"
                     end) <> ")"

  schema "owned_files" do
    field :owned_file_uuid, :string
    field :key, :string
    field :content_type, :string
    field :content_length, :integer
    field :state, OwnedFile.State

    belongs_to :account, Account, references: :account_id

    timestamps()
  end

  def url_or_nil_for(%Ret.OwnedFile{} = f), do: f |> OwnedFile.uri_for() |> URI.to_string()
  def url_or_nil_for(_), do: nil

  def uri_for(%OwnedFile{owned_file_uuid: file_uuid, content_type: content_type}) do
    Ret.Storage.uri_for(file_uuid, content_type)
  end

  def changeset(struct, account, params \\ %{}) do
    struct
    |> cast(params, [:owned_file_uuid, :key, :content_type, :content_length, :state])
    |> validate_required([:owned_file_uuid, :key, :content_type, :content_length])
    |> unique_constraint(:owned_file_uuid)
    |> put_assoc(:account, account)
  end

  def inactive() do
    Repo.all(from OwnedFile, where: [state: ^:inactive])
  end

  def inactive_before(%NaiveDateTime{} = cutoff) do
    Repo.all(
      from owned_file in OwnedFile,
        where: owned_file.state == ^:inactive,
        where: owned_file.updated_at <= ^cutoff
    )
  end

  def set_active(owned_file_uuid, account_id) do
    case get_by_uuid_and_account(owned_file_uuid, account_id) do
      nil ->
        {:error, :file_not_found}

      owned_file ->
        set_state(owned_file, :active)
    end
  end

  def set_inactive(owned_file_uuid, account_id) do
    case get_by_uuid_and_account(owned_file_uuid, account_id) do
      nil ->
        {:error, :file_not_found}

      owned_file ->
        set_state(owned_file, :inactive)
    end
  end

  def set_inactive(owned_file_uuid) when is_binary(owned_file_uuid) do
    case get_by_uuid(owned_file_uuid) do
      nil ->
        {:error, :file_not_found}

      owned_file ->
        set_state(owned_file, :inactive)
    end
  end

  @spec set_inactive(Ret.OwnedFile.t()) :: any()
  def set_inactive(%OwnedFile{} = owned_file) do
    set_state(owned_file, :inactive)
  end

  def get_by_uuid_and_account(owned_file_uuid, account_id) do
    Repo.one(from OwnedFile, where: [owned_file_uuid: ^owned_file_uuid, account_id: ^account_id])
  end

  def get_by_uuid(owned_file_uuid) do
    Repo.one(from OwnedFile, where: [owned_file_uuid: ^owned_file_uuid])
  end

  def reference_columns, do: @reference_columns

  def referenced?(%OwnedFile{owned_file_id: owned_file_id}), do: referenced?(owned_file_id)

  def referenced?(owned_file_id) when is_integer(owned_file_id) do
    case Ecto.Adapters.SQL.query(Repo, @reference_query, [owned_file_id]) do
      {:ok, %{rows: [[referenced]]}} ->
        referenced

      {:error, error} ->
        Logger.error("Failed to check owned-file references: #{inspect(error)}")
        true
    end
  end

  defp set_state(nil, _state), do: nil

  defp set_state(%OwnedFile{} = owned_file, state) do
    owned_file
    |> change(%{state: state})
    |> Repo.update()
  end
end
