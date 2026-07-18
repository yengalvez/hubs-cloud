defmodule RetWeb.Plugs.BotOrchestratorHeaderAuthorization do
  import Plug.Conn

  @header_name "x-ret-bot-orchestrator-access-key"

  def init(default), do: default

  def call(conn, _default) do
    expected_value = Application.get_env(:ret, :bot_orchestrator_access_key)
    provided_values = get_req_header(conn, @header_name)

    if single_secure_match?(provided_values, expected_value) do
      conn
    else
      conn |> send_resp(401, "") |> halt()
    end
  end

  defp single_secure_match?([provided], expected)
       when is_binary(provided) and is_binary(expected) and byte_size(expected) >= 32 and
              byte_size(provided) == byte_size(expected),
       do: Plug.Crypto.secure_compare(provided, expected)

  defp single_secure_match?(_, _), do: false
end
