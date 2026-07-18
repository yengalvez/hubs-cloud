defmodule RetWeb.HealthControllerTest do
  use RetWeb.ConnCase

  test "exposes the exact waypoint reservation wire-semantics contract", %{conn: conn} do
    conn =
      conn
      |> get("/health/capabilities")

    assert get_resp_header(conn, "cache-control") == ["no-store"]

    response = json_response(conn, 200)

    assert response == %{
             "bot_config_approval" => %{
               "legacy_default" => "quarantined",
               "protocol" => 1,
               "runtime_match" => "exact_jsonb"
             },
             "waypoint_reservation" => %{
               "protocol" => 2,
               "snapshot_state_version" => "strictly_greater_than_events",
               "state_version" => "monotonic_safe_integer"
             }
           }
  end
end
