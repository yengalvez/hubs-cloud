defmodule RetWeb.CorsProxy do
  @moduledoc false

  alias Ret.HttpUtils

  @allowed_ports %{"http" => 80, "https" => 443}

  def resolve_target(url, resolver \\ &HttpUtils.resolve_ips/1)

  def resolve_target(url, resolver) when is_binary(url) do
    uri = URI.parse(url)

    with true <- valid_uri?(uri),
         addresses when is_list(addresses) and addresses != [] <- resolver.(uri.host),
         true <- Enum.all?(addresses, &(not HttpUtils.internal_ip?(&1))) do
      address = hd(addresses)
      pinned_uri = HttpUtils.replace_host(uri, address)

      {:ok,
       %{
         host: uri.host,
         host_header: uri.host,
         pinned_url: URI.to_string(pinned_uri),
         scheme: uri.scheme
       }}
    else
      _ -> {:error, :invalid_target}
    end
  rescue
    _ -> {:error, :invalid_target}
  end

  def resolve_target(_, _), do: {:error, :invalid_target}

  @doc false
  def client_options(target) do
    options = [
      target_host: target.host_header,
      proxy: nil,
      follow_redirect: false,
      timeout: 5_000,
      recv_timeout: 30_000,
      hackney: [pool: false, protocols: [:http1]]
    ]

    case target do
      %{scheme: "https", host: host} ->
        Keyword.put(options, :ssl,
          server_name_indication: to_charlist(host),
          versions: [:"tlsv1.2", :"tlsv1.3"]
        )

      _ ->
        options
    end
  end

  defp valid_uri?(%URI{scheme: scheme, host: host, port: port, userinfo: nil, fragment: nil})
       when is_binary(host) and host != "" do
    Map.get(@allowed_ports, scheme) == port and safe_host?(host)
  end

  defp valid_uri?(_), do: false

  defp safe_host?(host) do
    not String.contains?(host, ["\r", "\n", <<0>>, "/", "\\"])
  end
end
