defmodule RetWeb.PageControllerTest do
  use RetWeb.ConnCase

  test "does not redirect with an invalid hub sid", %{conn: conn} do
    resp = conn |> get("/link/123456")
    assert resp |> response(404)
  end

  test "redirect with a valid hub sid", %{conn: conn} do
    {:ok, hub} = Ret.Hub.create_new_room(%{"name" => "test hub"}, true)
    resp = conn |> get("/link/#{hub.hub_sid}")
    assert resp |> response(302)
  end

  test "pages are served with csp headers", %{conn: conn} do
    resp = conn |> get("/")
    [csp] = resp |> Plug.Conn.get_resp_header("content-security-policy")

    assert csp |> String.contains?("google-analytics")
  end

  test "cors proxy rejects internal targets without making a request", %{conn: conn} do
    conn = %{conn | scheme: :https, host: "hubs-proxy.local", port: 4000}
    resp = get(conn, "/http://127.0.0.1/secret")

    assert response(resp, 401) == "Bad request."
  end
end
