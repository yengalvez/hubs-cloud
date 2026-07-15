defmodule Ret.HttpUtils do
  use Retry

  @internal_ipv4_cidr_list [
    InetCidr.parse_cidr!("0.0.0.0/8"),
    InetCidr.parse_cidr!("10.0.0.0/8"),
    InetCidr.parse_cidr!("100.64.0.0/10"),
    InetCidr.parse_cidr!("127.0.0.0/8"),
    InetCidr.parse_cidr!("169.254.0.0/16"),
    InetCidr.parse_cidr!("172.16.0.0/12"),
    InetCidr.parse_cidr!("192.0.0.0/24"),
    InetCidr.parse_cidr!("192.0.2.0/24"),
    InetCidr.parse_cidr!("192.168.0.0/16"),
    InetCidr.parse_cidr!("198.18.0.0/15"),
    InetCidr.parse_cidr!("198.51.100.0/24"),
    InetCidr.parse_cidr!("203.0.113.0/24"),
    InetCidr.parse_cidr!("224.0.0.0/4"),
    InetCidr.parse_cidr!("240.0.0.0/4")
  ]

  def retry_head_until_success(url, options \\ []),
    do: retry_until_success(:head, url, "", options)

  def retry_get_until_success(url, options \\ []),
    do: retry_until_success(:get, url, "", options)

  def retry_post_until_success(url, body, options \\ []),
    do: retry_until_success(:post, url, body, options)

  def retry_put_until_success(url, body, options \\ []),
    do: retry_until_success(:put, url, body, options)

  def retry_head_then_get_until_success(url, options \\ []) do
    case url |> retry_head_until_success(options) do
      :error ->
        url |> retry_get_until_success(options)

      res ->
        res
    end
  end

  defp retry_until_success(verb, url, body, options) do
    default_options = [
      headers: [],
      cap_ms: 5_000,
      expiry_ms: 10_000,
      append_browser_user_agent: false
    ]

    options = Keyword.merge(default_options, options)

    headers =
      if options[:append_browser_user_agent] do
        options[:headers] ++
          [
            {"User-Agent",
             "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0"}
          ]
      else
        options[:headers]
      end

    request_options = [
      follow_redirect: true,
      timeout: options[:cap_ms],
      recv_timeout: options[:cap_ms]
    ]

    request_options =
      if module_config(:insecure_ssl) == true do
        Keyword.put(request_options, :ssl, verify: :verify_none)
      else
        request_options
      end

    retry with:
            exponential_backoff()
            |> randomize
            |> cap(options[:cap_ms])
            |> expiry(options[:expiry_ms]) do
      http_client = module_config(:http_client) || HTTPoison

      case http_client.request(verb, url, body, headers, request_options) do
        {:ok, %HTTPoison.Response{status_code: status_code} = resp}
        when status_code >= 200 and status_code < 300 ->
          resp

        {:ok, %HTTPoison.Response{status_code: status_code}}
        when status_code >= 400 and status_code < 500 ->
          :unauthorized

        _ ->
          :error
      end
    after
      result ->
        case result do
          :unauthorized -> :error
          _ -> result
        end
    else
      error -> error
    end
  end

  def get_http_header(headers, header) do
    header = headers |> Enum.find(fn h -> h |> elem(0) |> String.downcase() === header end)

    if header do
      header |> elem(1)
    else
      nil
    end
  end

  def content_type_from_headers(headers) do
    headers |> get_http_header("content-type")
  end

  def fetch_content_type(url) do
    case url |> retry_head_then_get_until_success(headers: [{"Range", "bytes=0-32768"}]) do
      :error -> {:error, "Could not get content-type"}
      %HTTPoison.Response{headers: headers} -> {:ok, headers |> content_type_from_headers}
    end
  end

  def get_forwarded_ip(headers) do
    origin_ips_csv = headers |> get_http_header("x-forwarded-for")

    if origin_ips_csv do
      origin_ips = origin_ips_csv |> String.split(",") |> Enum.map(&String.trim/1)

      # AWS' Application Load Balancer (ALB) will append the client's actual IP Address
      # at the end of the list of forwarded headers. Since we want to ignore any
      # spoofed IPs, we take the last IP in the list.
      # See https://docs.aws.amazon.com/elasticloadbalancing/latest/application/x-forwarded-headers.html#x-forwarded-for
      ip_str = origin_ips |> Enum.at(length(origin_ips) - 1)

      case :inet.parse_address(to_charlist(ip_str)) do
        {:ok, ip} -> ip
        _ -> nil
      end
    else
      nil
    end
  end

  def resolve_ip(host) do
    case resolve_ips(host) do
      [] -> nil
      results -> Enum.random(results)
    end
  end

  def resolve_ips(host) do
    try do
      [InetCidr.parse_address!(host)]
    rescue
      _ ->
        case DNS.resolve(host) do
          {:ok, results} -> results |> Enum.filter(&InetCidr.v4?/1) |> Enum.uniq()
          _ -> []
        end
    end
  end

  def internal_ip?(ip_address) do
    case ip_address do
      nil ->
        # Default to true for safety.
        true

      {_, _, _, _} = ipv4_address ->
        Enum.any?(@internal_ipv4_cidr_list, fn cidr -> InetCidr.contains?(cidr, ipv4_address) end)

      _ ->
        # For safety, assume anything else is internal.
        true
    end
  end

  def replace_host(uri, ip_address) do
    %{uri | authority: nil, host: to_string(:inet.ntoa(ip_address)), userinfo: nil}
  end

  defp module_config(key) do
    Application.get_env(:ret, __MODULE__)[key]
  end
end
