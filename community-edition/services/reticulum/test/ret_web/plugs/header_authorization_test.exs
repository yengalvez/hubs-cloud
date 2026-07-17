defmodule RetWeb.Plugs.HeaderAuthorizationTest do
  use ExUnit.Case, async: false

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias RetWeb.Plugs.HeaderAuthorization

  @header_name "x-ret-admin-access-key"
  @access_key "test-admin-access-key-at-least-32bytes"

  setup do
    original_config = Application.get_env(:ret, HeaderAuthorization)

    Application.put_env(:ret, HeaderAuthorization,
      header_name: @header_name,
      header_value: @access_key
    )

    on_exit(fn -> restore_application_env(:ret, HeaderAuthorization, original_config) end)
  end

  test "accepts exactly one constant-time administrative credential" do
    conn = conn(:get, "/") |> put_req_header(@header_name, @access_key)

    refute HeaderAuthorization.call(conn, []).halted
  end

  test "fails closed for missing, wrong, short, and duplicate credentials" do
    assert_denied(conn(:get, "/"))
    assert_denied(conn(:get, "/") |> put_req_header(@header_name, String.duplicate("x", 32)))

    Application.put_env(:ret, HeaderAuthorization,
      header_name: @header_name,
      header_value: "too-short"
    )

    assert_denied(conn(:get, "/") |> put_req_header(@header_name, "too-short"))

    Application.put_env(:ret, HeaderAuthorization,
      header_name: @header_name,
      header_value: @access_key
    )

    duplicate_conn = %{
      conn(:get, "/")
      | req_headers: [{@header_name, @access_key}, {@header_name, @access_key}]
    }

    assert_denied(duplicate_conn)
  end

  defp assert_denied(conn) do
    denied = HeaderAuthorization.call(conn, [])
    assert denied.halted
    assert denied.status == 401
  end

  defp restore_application_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_application_env(app, key, value), do: Application.put_env(app, key, value)
end
