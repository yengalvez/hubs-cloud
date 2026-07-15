defmodule Ret.StorageTest do
  use Ret.DataCase
  import Ret.TestHelpers

  alias Ret.{OwnedFile, Storage}

  setup do
    on_exit(fn ->
      clear_all_stored_files()
    end)
  end

  setup _context do
    %{temp_file: generate_temp_file("test"), temp_file_2: generate_temp_file("test2")}
  end

  test "store a file", %{temp_file: temp_file} do
    {:ok, uuid} = Storage.store(%Plug.Upload{path: temp_file}, "text/plain", "secret")
    result = Storage.fetch(uuid, "secret")

    assert_fetch_result(result, "text/plain", "test")
  end

  test "bad key should fail fetch", %{temp_file: temp_file} do
    {:ok, uuid} = Storage.store(%Plug.Upload{path: temp_file}, "text/plain", "secret")
    {result, message} = Storage.fetch(uuid, "secret2")

    assert result == :error
    assert message == :not_allowed
  end

  test "promote a stored file", %{temp_file: temp_file} do
    account = Ret.Repo.insert!(%Ret.Account{})

    {:ok, uuid} = Storage.store(%Plug.Upload{path: temp_file}, "text/plain", "secret")
    {:ok, owned_file} = Storage.promote(uuid, "secret", nil, account)
    result = Storage.fetch(owned_file)

    assert_fetch_result(result, "text/plain", "test")
  end

  test "should not be able to promote a file with an invalid promotion token", %{
    temp_file: temp_file
  } do
    account = Ret.Repo.insert!(%Ret.Account{})

    {:ok, uuid} =
      Storage.store(%Plug.Upload{path: temp_file}, "text/plain", "secret", "promotion_secret")

    {:error, :not_allowed} = Storage.promote(uuid, "secret", "invalid_promotion_secret", account)
  end

  test "should be able to re-promote without failure", %{temp_file: temp_file} do
    account = Ret.Repo.insert!(%Ret.Account{})

    {:ok, uuid} = Storage.store(%Plug.Upload{path: temp_file}, "text/plain", "secret")
    {:ok, _owned_file} = Storage.promote(uuid, "secret", nil, account)
    {:ok, owned_file} = Storage.promote(uuid, "secret", nil, account)

    owned_file_id = owned_file.owned_file_id

    {:ok, %OwnedFile{owned_file_id: ^owned_file_id}} =
      Storage.promote(uuid, "secret", nil, account)
  end

  test "promote_with_status distinguishes new and existing files", %{temp_file: temp_file} do
    account = Ret.Repo.insert!(%Ret.Account{})

    {:ok, uuid} = Storage.store(%Plug.Upload{path: temp_file}, "text/plain", "secret")
    assert {:ok, owned_file, :created} = Storage.promote_with_status(uuid, "secret", nil, account)

    assert {:ok, same_owned_file, :existing} =
             Storage.promote_with_status(uuid, "secret", nil, account)

    assert same_owned_file.owned_file_id == owned_file.owned_file_id
  end

  test "concurrent promotion of one UUID creates one owned file", %{temp_file: temp_file} do
    account = Ret.Repo.insert!(%Ret.Account{})
    {:ok, uuid} = Storage.store(%Plug.Upload{path: temp_file}, "text/plain", "secret")
    parent = self()

    tasks =
      for _ <- 1..2 do
        Task.async(fn ->
          send(parent, {:ready, self()})

          receive do
            :promote -> Storage.promote_with_status(uuid, "secret", nil, account)
          end
        end)
      end

    Enum.each(tasks, fn task ->
      task_pid = task.pid
      assert_receive {:ready, ^task_pid}
      Ecto.Adapters.SQL.Sandbox.allow(Ret.Repo, self(), task_pid)
    end)

    Enum.each(tasks, &send(&1.pid, :promote))
    results = Enum.map(tasks, &Task.await(&1, 5_000))

    assert Enum.sort(Enum.map(results, fn {:ok, _owned_file, status} -> status end)) ==
             [:created, :existing]

    assert Ret.Repo.aggregate(OwnedFile, :count, :owned_file_id) == 1
  end

  test "a destination collision leaves the expiring upload intact", %{temp_file: temp_file} do
    account = Ret.Repo.insert!(%Ret.Account{})
    {:ok, uuid} = Storage.store(%Plug.Upload{path: temp_file}, "text/plain", "secret")

    [owned_directory, owned_meta_path, _owned_blob_path] =
      Storage.paths_for_owned_file(%OwnedFile{owned_file_uuid: uuid})

    File.mkdir_p!(owned_directory)
    File.write!(owned_meta_path, "collision")

    assert {:error, :storage_error} =
             Storage.promote_with_status(uuid, "secret", nil, account)

    assert {:ok, _, _} = Storage.fetch(uuid, "secret")
    assert Ret.Repo.get_by(OwnedFile, owned_file_uuid: uuid) == nil
  end

  test "re-promoting an inactive file safely reactivates it", %{temp_file: temp_file} do
    account = Ret.Repo.insert!(%Ret.Account{})

    {:ok, uuid} = Storage.store(%Plug.Upload{path: temp_file}, "text/plain", "secret")
    {:ok, owned_file} = Storage.promote(uuid, "secret", nil, account)
    {:ok, _inactive_file} = OwnedFile.set_inactive(owned_file)

    assert {:ok, reactivated_file, :existing} =
             Storage.promote_with_status(uuid, "secret", nil, account)

    assert reactivated_file.state == :active
    assert {:ok, _, _} = Storage.fetch(reactivated_file)
  end

  test "re-promoting requires the original owner and key", %{temp_file: temp_file} do
    account = Ret.Repo.insert!(%Ret.Account{})
    other_account = Ret.Repo.insert!(%Ret.Account{})

    {:ok, uuid} = Storage.store(%Plug.Upload{path: temp_file}, "text/plain", "secret")
    {:ok, _owned_file} = Storage.promote(uuid, "secret", nil, account)

    assert {:error, :not_allowed} = Storage.promote(uuid, "wrong-secret", nil, account)
    assert {:error, :not_allowed} = Storage.promote(uuid, "secret", nil, other_account)
  end

  test "owned-file reference inventory covers every schema column" do
    {:ok, %{rows: rows}} =
      Ecto.Adapters.SQL.query(
        Ret.Repo,
        """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'ret0'
          AND column_name LIKE '%owned_file_id'
          AND table_name <> 'owned_files'
        ORDER BY table_name, column_name
        """,
        []
      )

    schema_columns = rows |> Enum.map(&List.to_tuple/1) |> MapSet.new()
    assert schema_columns == MapSet.new(OwnedFile.reference_columns())
  end

  test "demotion keeps an inactive database row when its physical files are missing", %{
    temp_file: temp_file
  } do
    account = Ret.Repo.insert!(%Ret.Account{})

    {:ok, uuid} = Storage.store(%Plug.Upload{path: temp_file}, "text/plain", "secret")
    {:ok, owned_file} = Storage.promote(uuid, "secret", nil, account)
    :ok = Storage.rm_files_for_owned_file(owned_file)
    {:ok, _inactive_file} = OwnedFile.set_inactive(owned_file)

    Storage.demote_inactive_owned_files(0)

    assert Ret.Repo.get(OwnedFile, owned_file.owned_file_id).state == :inactive
  end

  test "demotion does not delete the remaining half of an incomplete file pair", %{
    temp_file: temp_file
  } do
    account = Ret.Repo.insert!(%Ret.Account{})

    {:ok, uuid} = Storage.store(%Plug.Upload{path: temp_file}, "text/plain", "secret")
    {:ok, owned_file} = Storage.promote(uuid, "secret", nil, account)
    [_, meta_path, blob_path] = Storage.paths_for_owned_file(owned_file)
    File.rm!(meta_path)
    {:ok, _inactive_file} = OwnedFile.set_inactive(owned_file)

    Storage.demote_inactive_owned_files(0)

    assert Ret.Repo.get(OwnedFile, owned_file.owned_file_id).state == :inactive
    assert File.exists?(blob_path)
  end

  test "should be able to promote multiple files", %{
    temp_file: temp_file,
    temp_file_2: temp_file_2
  } do
    account = Ret.Repo.insert!(%Ret.Account{})

    {:ok, uuid_1} = Storage.store(%Plug.Upload{path: temp_file}, "text/plain", "secret")
    {:ok, uuid_2} = Storage.store(%Plug.Upload{path: temp_file_2}, "text/plain", "secret2")

    %{t1: {:ok, owned_file_t1}, t2: {:ok, owned_file_t2}} =
      Storage.promote(%{t1: {uuid_1, "secret"}, t2: {uuid_2, "secret2"}}, account)

    r1 = Storage.fetch(owned_file_t1)
    r2 = Storage.fetch(owned_file_t2)

    assert_fetch_result(r1, "text/plain", "test")
    assert_fetch_result(r2, "text/plain", "test2")
  end

  test "duplicating a missing owned file returns not_found instead of crashing" do
    account = Ret.Repo.insert!(%Ret.Account{})
    missing_file = %OwnedFile{owned_file_uuid: Ecto.UUID.generate(), key: "missing"}

    assert {:error, :not_found} = Storage.duplicate(missing_file, account)
  end

  defp assert_fetch_result(result, expected_content_type, expected_content) do
    {:ok, %{"content_type" => content_type}, stream} = result

    assert content_type == expected_content_type
    assert stream |> Enum.map(& &1) |> Enum.join() == expected_content
  end
end
