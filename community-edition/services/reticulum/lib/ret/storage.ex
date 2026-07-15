defmodule Ret.Storage do
  require Logger

  import Ret.HttpUtils

  def expiring_file_path, do: "expiring"
  def owned_file_path, do: "owned"
  def cached_file_path, do: "cached"

  @chunk_size 1024 * 1024

  alias Ret.{OwnedFile, CachedFile, Repo, Account}

  def store(path, content_type, key, promotion_token \\ nil)

  # Given a Plug.Upload, a content-type, and an optional encryption key, returns an id
  # that can be used to fetch a stream to the uploaded file after this call.
  def store(%Plug.Upload{path: path}, content_type, key, promotion_token) do
    store(path, content_type, key, promotion_token)
  end

  def store(path, content_type, key, promotion_token) do
    store(
      path,
      content_type,
      key,
      promotion_token,
      expiring_file_path()
    )
  end

  # Given a path to a file, a content-type, and an optional encryption key, returns an id
  # that can be used to fetch a stream to the uploaded file after this call.
  def store(path, content_type, key, promotion_token, file_path) do
    if in_quota?() do
      case File.stat(path) do
        {:ok, %{size: source_size}} ->
          source_stream = path |> File.stream!([], @chunk_size)
          store_stream(source_stream, source_size, content_type, key, promotion_token, file_path)

        {:error, _reason} = err ->
          err
      end
    else
      {:error, :quota}
    end
  end

  # Given a stream, a content-type, an optional encryption key, and a storage subpath, returns an id
  # that can be used to fetch a stream to the uploaded file after this call.
  def store_stream(source_stream, source_size, content_type, key, promotion_token, subpath) do
    with storage_path when is_binary(storage_path) <- module_config(:storage_path) do
      uuid = Ecto.UUID.generate()
      [file_directory, meta_file_path, blob_file_path] = paths_for_uuid(uuid, subpath)

      File.mkdir_p!(file_directory)
      source_stream |> encrypt_stream_to_file(source_size, blob_file_path, key)

      meta_file_path
      |> File.write!(
        Poison.encode!(%{
          content_type: content_type,
          content_length: source_size,
          promotion_token: promotion_token
        })
      )

      {:ok, uuid}
    else
      _ -> {:error, :not_allowed}
    end
  end

  defp store_string_as_owned_file(str, content_type, %Account{} = account) when is_binary(str) do
    {:ok, stringio_device} = StringIO.open(str)
    str_stream = IO.binstream(stringio_device, :line)

    content_length = byte_size(str)

    key = SecureRandom.hex()
    # This will immediately become an OwnedFile, so we don't need a promotion key.
    promotion_key = nil

    {:ok, file_uuid} =
      store_stream(
        str_stream,
        content_length,
        content_type,
        key,
        promotion_key,
        owned_file_path()
      )

    owned_file_params = %{
      owned_file_uuid: file_uuid,
      key: key,
      content_type: content_type,
      content_length: content_length
    }

    %OwnedFile{}
    |> OwnedFile.changeset(account, owned_file_params)
    |> Repo.insert!()
  end

  def create_new_owned_file_with_replaced_string(
        %OwnedFile{} = owned_file,
        %Account{} = account,
        search_string,
        replacement_string
      ) do
    {:ok, %{"content_type" => content_type}, file_stream} = fetch(owned_file)

    file_stream
    |> Enum.to_list()
    |> Enum.join("")
    |> String.replace(search_string, replacement_string)
    |> store_string_as_owned_file(content_type, account)
  end

  def fetch(id, key) when is_binary(id) and is_binary(key) do
    fetch_blob(id, key, expiring_file_path())
  end

  # TODO: Should this function not exist because it is only used in testing?
  def fetch(id, key, file_path) when is_binary(id) and is_binary(key) do
    fetch_blob(id, key, file_path)
  end

  def fetch(%OwnedFile{owned_file_uuid: id, key: key}) do
    fetch_blob(id, key, owned_file_path())
  end

  def fetch(%CachedFile{} = cached_file) do
    fetch_at(
      cached_file,
      Timex.now() |> Timex.to_naive_datetime() |> NaiveDateTime.truncate(:second)
    )
  end

  defp maybe_bump_accessed_at(%CachedFile{accessed_at: accessed_at} = cached_file, time) do
    one_day_ago = Timex.shift(time, days: -1)

    # Save a trip to the database if this file was recently accessed.
    # If the vacuum window for CachedFiles is on the order of days, only update
    # the accessed_at time daily. If vacuuming happens on the order of weeks,
    # we can update accessed_at even less often. This is a minor (perhaps unnecessary)
    # performance optimization.
    if Timex.before?(accessed_at, one_day_ago) do
      cached_file |> Ecto.Changeset.change(accessed_at: time) |> Ret.Repo.update()
    end
  end

  defp lock_and_migrate(%CachedFile{file_uuid: id, file_key: key} = cached_file) do
    Ret.Locking.exec_after_lock(
      "migrate_" <> id,
      fn ->
        # Try fetch_blob again in case the file has been migrated
        # while waiting for the lock
        case fetch_blob(id, key, cached_file_path()) do
          {:ok, meta, stream} ->
            {:ok, meta, stream}

          {:error, _} ->
            migrate(cached_file)
        end
      end
    )
  end

  def fetch_at(%CachedFile{file_uuid: id, file_key: key} = cached_file, time) do
    maybe_bump_accessed_at(cached_file, time)

    case fetch_blob(id, key, cached_file_path()) do
      {:ok, meta, stream} ->
        {:ok, meta, stream}

      {:error, _} ->
        case lock_and_migrate(cached_file) do
          {:ok, {:ok, meta, stream}} ->
            # Got lock, but file had already been migrated
            {:ok, meta, stream}

          {:ok, {:ok, _cached_file}} ->
            # Got lock, then migrated
            fetch_blob(id, key, cached_file_path())

          {:ok, {:error, reason}} ->
            # Got lock, failed to migrate the file
            # Delete the cached_file record. The user will have to re-request this file.
            Repo.delete(cached_file)
            {:error, reason}

          _ ->
            {:error, "Failed to acquire database lock."}
        end
    end
  end

  # TODO: Remove this code once all the CachedFiles are stored in the cached_file_path().
  defp migrate(%CachedFile{file_uuid: id, file_key: file_key} = cached_file) do
    with {:ok, _meta, _stream} <- fetch_blob(id, file_key, expiring_file_path()),
         {:ok, uuid} <- Ecto.UUID.cast(id),
         [_src_file_directory, src_meta_file_path, src_blob_file_path] <-
           paths_for_uuid(uuid, expiring_file_path()),
         [dest_file_directory, dest_meta_file_path, dest_blob_file_path] <-
           paths_for_uuid(uuid, cached_file_path()),
         :ok <- File.mkdir_p(dest_file_directory),
         :ok <- File.cp(src_meta_file_path, dest_meta_file_path),
         :ok <- File.cp(src_blob_file_path, dest_blob_file_path) do
      {:ok, cached_file}
    else
      {:error, reason} -> {:error, "Migration failed: #{reason}"}
      _ -> {:error, "Migration failed"}
    end
  end

  defp fetch_blob(id, key, subpath) do
    with storage_path when is_binary(storage_path) <- module_config(:storage_path),
         {:ok, uuid} <- Ecto.UUID.cast(id),
         [_file_path, meta_file_path, blob_file_path] <- paths_for_uuid(uuid, subpath),
         {:ok, _} <- File.stat(meta_file_path),
         {:ok, _} <- File.stat(blob_file_path),
         {:ok, meta_file_data} <- File.read(meta_file_path),
         {:ok, meta} <- Poison.decode(meta_file_data),
         {:ok, stream} <- decrypt_file_to_stream(blob_file_path, meta, key) do
      {:ok, meta, stream}
    else
      {:error, :invalid_key} -> {:error, :not_allowed}
      {:error, :enoent} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :not_allowed}
    end
  end

  def promote(id, key, promotion_token, account, require_token \\ true)

  # Promotes an expiring stored file to a permanently stored file in the specified Account.
  def promote(id, key, promotion_token, %Account{} = account, require_token) do
    case promote_with_status(id, key, promotion_token, account, require_token) do
      {:ok, owned_file, _status} -> {:ok, owned_file}
      {:error, _reason} = error -> error
    end
  end

  def promote_with_status(id, key, promotion_token, account, require_token \\ true)

  def promote_with_status(_id, nil, nil, _account, _require_token),
    do: {:error, :not_allowed}

  def promote_with_status(nil, _key, _promotion_token, _account, _require_token),
    do: {:error, :not_found}

  def promote_with_status(_id, nil, _promotion_token, _account, _require_token),
    do: {:error, :not_allowed}

  def promote_with_status(id, key, promotion_token, %Account{} = account, require_token) do
    case Ret.Locking.exec_after_lock("storage_promote_#{id}", fn ->
           OwnedFile
           |> Repo.get_by(owned_file_uuid: id)
           |> promote_or_return_owned_file(id, key, promotion_token, account, require_token)
         end) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
      _ -> {:error, :storage_error}
    end
  end

  # Promotes multiple files into the given account.
  #
  # Given a map that has { id, key } or { id, key, promotion_token} tuple values, returns a similarly-keyed map
  # that has the return values of promote as values.
  def promote(map, %Account{} = account) when is_map(map) do
    map
    |> Enum.map(fn
      {k, {id, key}} -> {k, promote(id, key, nil, account)}
      {k, {id, key, promotion_token}} -> {k, promote(id, key, promotion_token, account)}
    end)
    |> Enum.into(%{})
  end

  def promote_with_status(map, %Account{} = account) when is_map(map) do
    map
    |> Enum.map(fn
      {k, {id, key}} ->
        {k, promote_with_status(id, key, nil, account)}

      {k, {id, key, promotion_token}} ->
        {k, promote_with_status(id, key, promotion_token, account)}
    end)
    |> Enum.into(%{})
  end

  # Similar to promote above, but allows for passing nil. Useful for optional upload fields
  def promote_optional(map, %Account{} = account) when is_map(map) do
    map
    |> Enum.map(fn
      {k, {nil, nil}} -> {k, {:ok, nil}}
      {k, {nil, nil, nil}} -> {k, {:ok, nil}}
      {k, {id, key}} -> {k, promote(id, key, nil, account)}
      {k, {id, key, promotion_token}} -> {k, promote(id, key, promotion_token, account)}
    end)
    |> Enum.into(%{})
  end

  defp promote_or_return_owned_file(
         %OwnedFile{} = owned_file,
         _id,
         key,
         _promotion_token,
         %Account{} = account,
         _require_token
       ) do
    with true <-
           owned_file.account_id == account.account_id and owned_file.key == key and
             owned_file.state in [:active, :inactive],
         :ok <- validate_owned_file_pair(owned_file) do
      case owned_file.state do
        :active ->
          {:ok, owned_file, :existing}

        :inactive ->
          case OwnedFile.set_active(owned_file.owned_file_uuid, account.account_id) do
            {:ok, active_owned_file} -> {:ok, active_owned_file, :existing}
            {:error, reason} -> {:error, reason}
          end
      end
    else
      false -> {:error, :not_allowed}
      {:error, reason} -> {:error, reason}
    end
  end

  # Promoting a stored file to being owned has two side effects: the file is moved
  # into the owned files directory (which prevents it from being vacuumed) and an
  # OwnedFile record is inserted into the database which includes the decryption key.
  # If the stored file has an associated promotion token, the given promotion token is verified against it.
  # If the given promotion token fails verification, the file is not promoted.
  defp promote_or_return_owned_file(nil, id, key, promotion_token, account, require_token) do
    with(
      storage_path when is_binary(storage_path) <- module_config(:storage_path),
      {:ok, uuid} <- Ecto.UUID.cast(id),
      [_, meta_file_path, blob_file_path] <- paths_for_uuid(uuid, expiring_file_path()),
      [dest_path, dest_meta_file_path, dest_blob_file_path] <-
        paths_for_uuid(uuid, owned_file_path()),
      [{:ok, _}, {:ok, _}] <- [File.stat(meta_file_path), File.stat(blob_file_path)],
      {:ok,
       %{
         "content_type" => content_type,
         "content_length" => content_length,
         "promotion_token" => actual_promotion_token
       }} <- read_storage_metadata(meta_file_path),
      {:ok} <-
        if(require_token,
          do: check_promotion_token(actual_promotion_token, promotion_token),
          else: {:ok}
        ),
      {:ok} <- check_blob_file_key(blob_file_path, key)
    ) do
      owned_file_params = %{
        owned_file_uuid: id,
        key: key,
        content_type: content_type,
        content_length: content_length
      }

      changeset =
        %OwnedFile{}
        |> OwnedFile.changeset(account, owned_file_params)

      promotion_result =
        with :ok <- File.mkdir_p(dest_path),
             :ok <-
               move_file_pair(
                 meta_file_path,
                 blob_file_path,
                 dest_meta_file_path,
                 dest_blob_file_path
               ) do
          case Repo.insert(changeset) do
            {:ok, owned_file} ->
              {:ok, owned_file, :created}

            {:error, changeset} ->
              restore_promoted_file_pair(
                dest_meta_file_path,
                dest_blob_file_path,
                meta_file_path,
                blob_file_path
              )

              {:error, changeset}
          end
        end

      case promotion_result do
        {:error, %Ecto.Changeset{}} = error ->
          error

        {:error, reason} ->
          Logger.error("Failed to move stored file #{id} during promotion: #{inspect(reason)}")
          {:error, :storage_error}

        result ->
          result
      end
    else
      {:error, :invalid_key} ->
        {:error, :not_allowed}

      {:error, :enoent} ->
        {:error, :not_found}

      {:error, :not_found} ->
        {:error, :not_found}

      {:error, reason} ->
        Logger.error("Failed to promote stored file #{id}: #{inspect(reason)}")
        {:error, :storage_error}

      _ ->
        {:error, :not_found}
    end
  end

  defp read_storage_metadata(path) do
    with {:ok, contents} <- File.read(path),
         {:ok, metadata} <- Poison.decode(contents) do
      {:ok, metadata}
    end
  end

  defp move_file_pair(source_meta, source_blob, destination_meta, destination_blob) do
    with :ok <- ensure_destinations_absent(destination_meta, destination_blob),
         :ok <- File.rename(source_meta, destination_meta) do
      case File.rename(source_blob, destination_blob) do
        :ok ->
          :ok

        {:error, reason} ->
          case File.rename(destination_meta, source_meta) do
            :ok ->
              :ok

            {:error, restore_reason} ->
              Logger.error(
                "Failed to restore metadata after paired file move failed: #{inspect(restore_reason)}"
              )
          end

          {:error, reason}
      end
    end
  end

  defp ensure_destinations_absent(destination_meta, destination_blob) do
    if File.exists?(destination_meta) or File.exists?(destination_blob) do
      {:error, :destination_exists}
    else
      :ok
    end
  end

  defp restore_promoted_file_pair(source_meta, source_blob, destination_meta, destination_blob) do
    case move_file_pair(source_meta, source_blob, destination_meta, destination_blob) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("Failed to roll back promoted file pair: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # If an owned file does not have a promotion token associated with it, it can be promoted with any given
  # promotion token, including nil.
  defp check_promotion_token(nil, _token), do: {:ok}
  defp check_promotion_token(actual_token, token) when actual_token == token, do: {:ok}

  defp check_promotion_token(actual_token, token) when actual_token != token,
    do: {:error, :invalid_key}

  # Vacuums up TTLed out files
  def vacuum do
    Logger.info("Stored Files: Attempting Vacuum.")

    Ret.Locking.exec_if_lockable(:storage_vacuum, fn ->
      Logger.info("Stored Files: Beginning Vacuum.")

      with storage_path when is_binary(storage_path) <- module_config(:storage_path),
           ttl when is_integer(ttl) <- module_config(:ttl) do
        process_blob = fn blob_file, _acc ->
          {:ok, %{atime: atime}} = File.stat(blob_file)

          now = DateTime.utc_now()
          atime_datetime = atime |> NaiveDateTime.from_erl!() |> DateTime.from_naive!("Etc/UTC")
          seconds_since_access = DateTime.diff(now, atime_datetime)

          if seconds_since_access > ttl do
            Logger.info(
              "Stored Files: Removing #{blob_file} after #{seconds_since_access}s since last access."
            )

            File.rm!(blob_file)
            File.rm(blob_file |> String.replace_suffix(".blob", ".meta.json"))
          end
        end

        :filelib.fold_files(
          Path.join(storage_path, expiring_file_path()),
          "\\.blob$",
          true,
          process_blob,
          nil
        )

        # TODO figure out what to do about owned files -- that structure increase over time
        root_path = "#{storage_path}/#{expiring_file_path()}"
        clean_empty_dirs(root_path)
      end

      Logger.info("Stored Files: Vacuum Finished.")
    end)
  end

  defp remove_underlying_assets(%{
         cached_file: %CachedFile{file_uuid: id} = cached_file,
         path: path
       }) do
    try do
      {:ok, uuid} = Ecto.UUID.cast(id)
      [_file_path, meta_file_path, blob_file_path] = paths_for_uuid(uuid, path)
      File.rm!(blob_file_path)
      File.rm!(meta_file_path)
      {:ok, cached_file}
    rescue
      _ ->
        {:error, "Failed to remove underlying assets for cached file.", cached_file}
    end
  end

  def vacuum(%{cached_files: cached_files}) do
    Logger.info("Stored Files: Attempting Vacuum.")

    Ret.Locking.exec_if_lockable(:storage_vacuum, fn ->
      Logger.info("Stored Files: Beginning Vacuum.")

      results =
        with storage_path when is_binary(storage_path) <- module_config(:storage_path) do
          results =
            cached_files
            |> Enum.map(fn cached_file ->
              remove_underlying_assets(%{
                cached_file: cached_file,
                path: cached_file_path()
              })
            end)
            |> Enum.reduce(%{vacuumed: [], errors: []}, fn
              {:ok, cached_file}, %{vacuumed: vacuumed, errors: errors} ->
                %{vacuumed: vacuumed ++ [cached_file], errors: errors}

              {:error, _, cached_file}, %{vacuumed: vacuumed, errors: errors} ->
                %{vacuumed: vacuumed, errors: errors ++ [cached_file]}
            end)

          root_path = "#{storage_path}/#{cached_file_path()}"
          clean_empty_dirs(root_path)
          results
        end

      Logger.info("Stored Files: Vacuum Finished.")

      results
    end)
  end

  defp clean_empty_dirs(root_path) do
    with {:ok, dirs} <- :file.list_dir(root_path) do
      # Walk sub directories and remove them if they are empty.
      for d <- dirs do
        sub_path = Path.join(root_path, d)

        with {:ok, subdirs} <- :file.list_dir(sub_path) do
          for sd <- subdirs do
            path = Path.join(sub_path, sd)

            with {:ok, files} <- :file.list_dir(path) do
              if files |> length === 0 do
                File.rmdir(path)
              end
            end
          end
        end
      end

      # Check if we've removed all the sub directories.
      for d <- dirs do
        sub_path = Path.join(root_path, d)

        with {:ok, subdirs} <- :file.list_dir(sub_path) do
          if subdirs |> length === 0 do
            File.rmdir(sub_path)
          end
        end
      end
    end
  end

  def reconcile_owned_files(grace_seconds \\ nil) do
    Ret.Locking.exec_if_session_lockable(:storage_owned_file_reconcile, fn ->
      mark_unreferenced_active_owned_files(grace_seconds)
      reconcile_untracked_owned_file_pairs(grace_seconds)
      demote_inactive_owned_files(grace_seconds)
    end)
  end

  def mark_unreferenced_active_owned_files(grace_seconds \\ nil) do
    grace_seconds = grace_seconds || module_config(:owned_file_orphan_grace_seconds) || 86_400
    cutoff = NaiveDateTime.utc_now() |> NaiveDateTime.add(-grace_seconds, :second)

    OwnedFile.active_before(cutoff)
    |> Enum.each(&mark_inactive_if_unreferenced/1)
  end

  def reconcile_untracked_owned_file_pairs(grace_seconds \\ nil) do
    grace_seconds = grace_seconds || module_config(:owned_file_orphan_grace_seconds) || 86_400

    cutoff_unix =
      DateTime.utc_now() |> DateTime.add(-grace_seconds, :second) |> DateTime.to_unix()

    with storage_path when is_binary(storage_path) <- module_config(:storage_path) do
      blob_uuids =
        [storage_path, owned_file_path(), "**", "*.blob"]
        |> Path.join()
        |> Path.wildcard()
        |> Enum.map(&Path.basename(&1, ".blob"))

      metadata_uuids =
        [storage_path, owned_file_path(), "**", "*.meta.json"]
        |> Path.join()
        |> Path.wildcard()
        |> Enum.map(&Path.basename(&1, ".meta.json"))

      blob_uuids
      |> Enum.concat(metadata_uuids)
      |> Enum.uniq()
      |> Enum.each(&reconcile_untracked_owned_uuid(&1, cutoff_unix))
    end
  end

  def demote_inactive_owned_files(grace_seconds \\ nil) do
    grace_seconds = grace_seconds || module_config(:owned_file_demotion_grace_seconds) || 86_400
    cutoff = NaiveDateTime.utc_now() |> NaiveDateTime.add(-grace_seconds, :second)

    OwnedFile.inactive_before(cutoff)
    |> Enum.each(&demote_inactive_owned_file/1)
  end

  def mark_inactive_if_unreferenced(nil), do: :ok

  def mark_inactive_if_unreferenced(%OwnedFile{} = owned_file) do
    case Ret.Locking.exec_after_lock("storage_promote_#{owned_file.owned_file_uuid}", fn ->
           case Repo.get(OwnedFile, owned_file.owned_file_id) do
             nil ->
               :ok

             current_owned_file ->
               if OwnedFile.referenced?(current_owned_file) do
                 case reactivate_if_needed(current_owned_file) do
                   :ok -> :referenced
                   {:error, reason} -> Repo.rollback({:reactivation_failed, reason})
                 end
               else
                 case OwnedFile.set_inactive(current_owned_file) do
                   {:ok, _inactive_owned_file} -> :marked_inactive
                   {:error, reason} -> {:error, reason}
                 end
               end
           end
         end) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
      _ -> {:error, :storage_error}
    end
  rescue
    error -> {:error, error}
  end

  defp demote_inactive_owned_file(%OwnedFile{} = owned_file) do
    result =
      Ret.Locking.exec_after_lock("storage_promote_#{owned_file.owned_file_uuid}", fn ->
        case Repo.get(OwnedFile, owned_file.owned_file_id) do
          nil ->
            :ok

          %OwnedFile{state: state} when state != :inactive ->
            :active

          current_owned_file ->
            if OwnedFile.referenced?(current_owned_file) do
              case reactivate_if_needed(current_owned_file) do
                :ok -> :reactivated
                {:error, reason} -> Repo.rollback({:reactivation_failed, reason})
              end
            else
              demote_owned_file(current_owned_file)
            end
        end
      end)

    case result do
      {:ok, status} when status in [:ok, :active, :reactivated, :demoted, :missing_files] ->
        status

      {:error, reason} ->
        Logger.error(
          "Failed to demote inactive owned file #{owned_file.owned_file_uuid}: #{inspect(reason)}"
        )

        {:error, reason}

      other ->
        Logger.error(
          "Unexpected demotion result for owned file #{owned_file.owned_file_uuid}: #{inspect(other)}"
        )

        {:error, :storage_error}
    end
  end

  defp demote_owned_file(%OwnedFile{} = owned_file) do
    [_, source_meta, source_blob] = paths_for_uuid(owned_file.owned_file_uuid, owned_file_path())

    [destination_path, destination_meta, destination_blob] =
      paths_for_uuid(owned_file.owned_file_uuid, expiring_file_path())

    case file_pair_status(source_meta, source_blob) do
      :missing ->
        :missing_files

      :partial ->
        Repo.rollback({:file_move_failed, :incomplete_source_pair})

      :complete ->
        with :ok <- File.mkdir_p(destination_path),
             :ok <- move_file_pair(source_meta, source_blob, destination_meta, destination_blob) do
          case Repo.delete(owned_file) do
            {:ok, _deleted_owned_file} ->
              :demoted

            {:error, reason} ->
              restore_promoted_file_pair(
                destination_meta,
                destination_blob,
                source_meta,
                source_blob
              )

              Repo.rollback({:owned_file_delete_failed, reason})
          end
        else
          {:error, reason} -> Repo.rollback({:file_move_failed, reason})
        end
    end
  end

  defp file_pair_status(meta_path, blob_path) do
    case {File.exists?(meta_path), File.exists?(blob_path)} do
      {true, true} -> :complete
      {false, false} -> :missing
      _ -> :partial
    end
  end

  defp validate_owned_file_pair(%OwnedFile{} = owned_file) do
    [_, meta_path, blob_path] = paths_for_owned_file(owned_file)

    with :complete <- file_pair_status(meta_path, blob_path),
         {:ok, metadata} <- read_storage_metadata(meta_path),
         true <- metadata["content_type"] == owned_file.content_type,
         true <- metadata["content_length"] == owned_file.content_length,
         {:ok} <- check_blob_file_key(blob_path, owned_file.key) do
      :ok
    else
      :missing -> {:error, :not_found}
      :partial -> {:error, :storage_error}
      false -> {:error, :storage_error}
      {:error, :enoent} -> {:error, :not_found}
      {:error, _reason} -> {:error, :storage_error}
      _ -> {:error, :storage_error}
    end
  end

  defp reconcile_untracked_owned_uuid(uuid, cutoff_unix) do
    [_, meta_path, blob_path] = paths_for_uuid(uuid, owned_file_path())

    with {:ok, _uuid} <- Ecto.UUID.cast(uuid),
         :complete <- file_pair_status(meta_path, blob_path),
         true <- file_pair_older_than?(meta_path, blob_path, cutoff_unix) do
      case Ret.Locking.exec_after_lock("storage_promote_#{uuid}", fn ->
             reconcile_untracked_owned_pair(uuid, meta_path, blob_path)
           end) do
        {:ok, result} when result in [:tracked, :moved_to_expiring] ->
          result

        {:error, reason} ->
          Logger.error("Failed to reconcile untracked owned file #{uuid}: #{inspect(reason)}")
          {:error, reason}

        other ->
          Logger.error("Unexpected untracked owned-file result for #{uuid}: #{inspect(other)}")
          {:error, :storage_error}
      end
    else
      :partial ->
        Logger.error("Incomplete owned-file pair found for #{uuid}; leaving it untouched")
        :partial

      _ ->
        :skipped
    end
  end

  defp reconcile_untracked_owned_pair(uuid, meta_path, blob_path) do
    case Repo.get_by(OwnedFile, owned_file_uuid: uuid) do
      %OwnedFile{} ->
        :tracked

      nil ->
        [destination_path, destination_meta, destination_blob] =
          paths_for_uuid(uuid, expiring_file_path())

        with :complete <- file_pair_status(meta_path, blob_path),
             :ok <- File.mkdir_p(destination_path),
             :ok <- move_file_pair(meta_path, blob_path, destination_meta, destination_blob) do
          :moved_to_expiring
        else
          :missing -> :tracked
          :partial -> Repo.rollback({:file_move_failed, :incomplete_source_pair})
          {:error, reason} -> Repo.rollback({:file_move_failed, reason})
        end
    end
  end

  defp file_pair_older_than?(meta_path, blob_path, cutoff_unix) do
    with {:ok, meta_stat} <- File.stat(meta_path, time: :posix),
         {:ok, blob_stat} <- File.stat(blob_path, time: :posix) do
      max(meta_stat.mtime, blob_stat.mtime) <= cutoff_unix
    else
      _ -> false
    end
  end

  defp reactivate_if_needed(%OwnedFile{state: :inactive} = owned_file) do
    with :ok <- validate_owned_file_pair(owned_file) do
      case OwnedFile.set_active(owned_file.owned_file_uuid, owned_file.account_id) do
        {:ok, _active_owned_file} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp reactivate_if_needed(%OwnedFile{}), do: :ok

  @spec rm_files_for_owned_file(OwnedFile.t()) :: :ok
  def rm_files_for_owned_file(%OwnedFile{} = owned_file) do
    [_, meta_file_path, blob_file_path] =
      paths_for_uuid(owned_file.owned_file_uuid, owned_file_path())

    File.rm!(meta_file_path)
    File.rm!(blob_file_path)
  end

  def uri_for(id, content_type, token \\ nil) do
    file_host = Application.get_env(:ret, Ret.Storage)[:host] || RetWeb.Endpoint.url()
    ext = MIME.extensions(content_type) |> List.first()
    filename = [id, ext] |> Enum.reject(&is_nil/1) |> Enum.join(".")

    "#{file_host}/files/#{filename}#{if token do
      "?" <> URI.encode_query(token: token)
    else
      ""
    end}"
    |> URI.parse()
  end

  defp check_blob_file_key(_source_path, nil) do
    {:error, :invalid_key}
  end

  defp check_blob_file_key(source_path, key) do
    Ret.Crypto.stream_check_key(source_path, key |> Ret.Crypto.hash())
  end

  defp decrypt_file_to_stream(source_path, _meta, key) do
    Ret.Crypto.decrypt_file_to_stream(source_path, key |> Ret.Crypto.hash())
  end

  defp encrypt_stream_to_file(source_stream, source_size, destination_path, key) do
    Ret.Crypto.encrypt_stream_to_file(
      source_stream,
      source_size,
      destination_path,
      key |> Ret.Crypto.hash()
    )
  end

  @spec paths_for_owned_file(OwnedFile.t()) :: [String.t(), ...]
  def paths_for_owned_file(%OwnedFile{} = owned_file) do
    paths_for_uuid(owned_file.owned_file_uuid, owned_file_path())
  end

  defp paths_for_uuid(uuid, subpath) do
    path =
      "#{module_config(:storage_path)}/#{subpath}/#{String.slice(uuid, 0, 2)}/#{String.slice(uuid, 2, 2)}"

    blob_file_path = "#{path}/#{uuid}.blob"
    meta_file_path = "#{path}/#{uuid}.meta.json"

    [path, meta_file_path, blob_file_path]
  end

  def duplicate_and_transform(
        %OwnedFile{owned_file_uuid: id, key: key},
        %Account{} = account,
        transform
      )
      when is_function(transform, 2) do
    with {:ok,
          %{
            "content_type" => content_type,
            "content_length" => content_length
          }, source_stream} <- fetch_blob(id, key, owned_file_path()) do
      {transformed_stream, transformed_length} = transform.(source_stream, content_length)

      new_key = SecureRandom.hex()
      new_promotion_token = SecureRandom.hex()

      {:ok, new_id} =
        store_stream(
          transformed_stream,
          transformed_length,
          content_type,
          new_key,
          new_promotion_token,
          owned_file_path()
        )

      owned_file_params = %{
        owned_file_uuid: new_id,
        key: new_key,
        content_type: content_type,
        content_length: transformed_length
      }

      owned_file =
        %OwnedFile{}
        |> OwnedFile.changeset(account, owned_file_params)
        |> Repo.insert!()

      {:ok, owned_file}
    end
  end

  def duplicate(%OwnedFile{} = owned_file, %Account{} = account) do
    duplicate_and_transform(owned_file, account, fn stream, content_length ->
      {stream, content_length}
    end)
  end

  defp download!(url) do
    {:ok, content_type} = fetch_content_type(url)
    {:ok, download_path} = Temp.path()

    case Download.from(url, path: download_path) do
      {:ok, _path} ->
        {download_path, content_type}

      _error ->
        throw("Error downloading #{url}")
    end
  end

  def owned_files_from_urls!(urls, account) do
    urls
    |> Enum.map(&download!/1)
    |> Enum.map(fn {download_path, content_type} ->
      access_token = SecureRandom.hex()
      promotion_token = SecureRandom.hex()

      {:ok, file_uuid} = store(download_path, content_type, access_token, promotion_token)
      {file_uuid, access_token, promotion_token}

      {:ok, owned_file} = promote(file_uuid, access_token, promotion_token, account)

      owned_file
    end)
  end

  def storage_used() do
    Cachex.get(:storage_used, :storage_used)
  end

  def in_quota?() do
    with storage_path when is_binary(storage_path) <- module_config(:storage_path),
         quota_gb when is_integer(quota_gb) and quota_gb > 0 <- module_config(:quota_gb) do
      case storage_used() do
        {:ok, 0} -> true
        {:ok, kbytes} -> kbytes < quota_gb * 1024 * 1024
        _ -> false
      end
    else
      _ -> true
    end
  end

  defp module_config(key), do: Application.get_env(:ret, __MODULE__)[key]
end
