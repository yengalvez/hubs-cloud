defmodule RetWeb.Plugs.HeaderAuthorization do
  import Plug.Conn

  def init(default), do: default

  def call(conn, _default) do
    env = Application.get_env(:ret, __MODULE__, [])
    header_name = env[:header_name]
    expected_value = env[:header_value]

    provided_values =
      if is_binary(header_name), do: get_req_header(conn, header_name), else: []

    if authorized?(provided_values, expected_value) do
      conn
    else
      conn |> send_resp(401, "") |> halt()
    end
  end

  defp authorized?([provided], expected)
       when is_binary(provided) and is_binary(expected) and byte_size(expected) >= 32 and
              byte_size(provided) == byte_size(expected),
       do: Plug.Crypto.secure_compare(provided, expected)

  defp authorized?(_, _), do: false
end
