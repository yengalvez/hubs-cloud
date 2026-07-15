defmodule Ret.AvatarTest do
  use Ret.DataCase

  alias Ret.{Account, Avatar, OwnedFile, Repo}

  defp owned_file(account, content_length) do
    %OwnedFile{}
    |> OwnedFile.changeset(account, %{
      owned_file_uuid: Ecto.UUID.generate(),
      key: "test-key",
      content_type: "application/octet-stream",
      content_length: content_length
    })
    |> Repo.insert!()
  end

  test "rejects an avatar model larger than 64 MiB" do
    account = Account.find_or_create_account_for_email("avatar-size-model@mozilla.com")

    changeset =
      Avatar.changeset(
        %Avatar{},
        account,
        %{
          gltf_owned_file: owned_file(account, 1),
          bin_owned_file: owned_file(account, 64 * 1024 * 1024)
        },
        nil,
        nil,
        %{name: "Oversized model"}
      )

    refute changeset.valid?
    assert {"avatar model exceeds 64 MiB", _} = changeset.errors[:gltf_owned_file]
  end

  test "rejects oversized avatar textures and thumbnails" do
    account = Account.find_or_create_account_for_email("avatar-size-images@mozilla.com")

    changeset =
      Avatar.changeset(
        %Avatar{},
        account,
        %{
          base_map_owned_file: owned_file(account, 16 * 1024 * 1024 + 1),
          thumbnail_owned_file: owned_file(account, 5 * 1024 * 1024 + 1)
        },
        nil,
        nil,
        %{name: "Oversized images"}
      )

    refute changeset.valid?
    assert {"avatar texture exceeds 16 MiB", _} = changeset.errors[:base_map_owned_file]
    assert {"avatar thumbnail exceeds 5 MiB", _} = changeset.errors[:thumbnail_owned_file]
  end
end
