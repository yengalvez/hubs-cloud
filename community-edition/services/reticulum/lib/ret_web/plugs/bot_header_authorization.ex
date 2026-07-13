defmodule RetWeb.Plugs.BotHeaderAuthorization do
  import Plug.Conn
  @header_name "x-ret-bot-access-key"

  def init(default), do: default

  def call(conn, _default) do
    expected_value = Application.get_env(:ret, :bot_access_key)
    provided_value = conn |> get_req_header(@header_name) |> List.first()

    if secure_match?(provided_value, expected_value) do
      conn
    else
      conn |> send_resp(401, "") |> halt()
    end
  end

  defp secure_match?(provided, expected)
       when is_binary(provided) and is_binary(expected) and byte_size(expected) >= 32 and
              byte_size(provided) == byte_size(expected),
       do: Plug.Crypto.secure_compare(provided, expected)

  defp secure_match?(_, _), do: false
end
