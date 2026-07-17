defmodule RetWeb.HealthControllerTest do
  use RetWeb.ConnCase

  test "exposes the exact waypoint reservation wire-semantics contract", %{conn: conn} do
    response =
      conn
      |> get("/health/capabilities")
      |> json_response(200)

    assert response == %{
             "waypoint_reservation" => %{
               "protocol" => 2,
               "snapshot_state_version" => "strictly_greater_than_events",
               "state_version" => "monotonic_safe_integer"
             }
           }
  end
end
