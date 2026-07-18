defmodule RetWeb.Plugs.BotOrchestratorHeaderAuthorizationTest do
  use ExUnit.Case, async: false

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias RetWeb.Plugs.BotOrchestratorHeaderAuthorization

  @header_name "x-ret-bot-orchestrator-access-key"
  @access_key "test-bot-orchestrator-access-key-at-least-32"

  setup do
    original = Application.get_env(:ret, :bot_orchestrator_access_key)
    Application.put_env(:ret, :bot_orchestrator_access_key, @access_key)

    on_exit(fn ->
      if is_nil(original),
        do: Application.delete_env(:ret, :bot_orchestrator_access_key),
        else: Application.put_env(:ret, :bot_orchestrator_access_key, original)
    end)
  end

  test "accepts exactly one strong orchestrator credential" do
    conn = conn(:get, "/") |> put_req_header(@header_name, @access_key)
    refute BotOrchestratorHeaderAuthorization.call(conn, []).halted
  end

  test "fails closed for missing, wrong, weak, and duplicate credentials" do
    assert_denied(conn(:get, "/"))
    assert_denied(conn(:get, "/") |> put_req_header(@header_name, String.duplicate("x", 32)))

    Application.put_env(:ret, :bot_orchestrator_access_key, "weak")
    assert_denied(conn(:get, "/") |> put_req_header(@header_name, "weak"))
    Application.put_env(:ret, :bot_orchestrator_access_key, @access_key)

    duplicate = %{
      conn(:get, "/")
      | req_headers: [{@header_name, @access_key}, {@header_name, @access_key}]
    }

    assert_denied(duplicate)
  end

  defp assert_denied(conn) do
    denied = BotOrchestratorHeaderAuthorization.call(conn, [])
    assert denied.halted
    assert denied.status == 401
  end
end
