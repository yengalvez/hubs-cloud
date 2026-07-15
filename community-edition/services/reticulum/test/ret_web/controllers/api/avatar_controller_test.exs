defmodule RetWeb.AvatarControllerTest do
  use RetWeb.ConnCase
  import Ret.TestHelpers

  alias Ret.Account

  setup do
    %{account: Account.find_or_create_account_for_email("avatar-controller@mozilla.com")}
  end

  test "rejects unknown avatar file keys without creating atoms", %{conn: conn, account: account} do
    conn
    |> auth_with_account(account)
    |> post(api_v1_avatar_path(conn, :create), %{
      avatar: %{name: "Invalid avatar", files: %{"unbounded_user_atom" => nil}}
    })
    |> response(422)
  end

  test "rejects malformed avatar file credentials", %{conn: conn, account: account} do
    conn
    |> auth_with_account(account)
    |> post(api_v1_avatar_path(conn, :create), %{
      avatar: %{name: "Invalid avatar", files: %{"gltf" => ["id", "key"]}}
    })
    |> response(422)
  end
end
