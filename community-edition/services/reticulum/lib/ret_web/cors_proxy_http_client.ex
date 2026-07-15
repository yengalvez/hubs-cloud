defmodule RetWeb.CorsProxyHTTPClient do
  @moduledoc false

  alias ReverseProxyPlug.HTTPClient
  alias ReverseProxyPlug.HTTPClient.Adapters.HTTPoison, as: Adapter

  @behaviour HTTPClient

  @allowed_request_headers MapSet.new([
                             "accept",
                             "accept-encoding",
                             "accept-language",
                             "cache-control",
                             "if-match",
                             "if-modified-since",
                             "if-none-match",
                             "if-range",
                             "if-unmodified-since",
                             "range",
                             "user-agent"
                           ])

  @blocked_response_headers MapSet.new([
                              "access-control-allow-credentials",
                              "access-control-allow-headers",
                              "access-control-allow-methods",
                              "access-control-allow-origin",
                              "access-control-expose-headers",
                              "access-control-max-age",
                              "alt-svc",
                              "clear-site-data",
                              "connection",
                              "keep-alive",
                              "location",
                              "nel",
                              "proxy-authenticate",
                              "proxy-authorization",
                              "refresh",
                              "report-to",
                              "reporting-endpoints",
                              "set-cookie",
                              "set-cookie2",
                              "te",
                              "trailer",
                              "transfer-encoding",
                              "upgrade",
                              "www-authenticate"
                            ])

  @redirect_statuses [301, 302, 303, 307, 308]

  @impl HTTPClient
  def request(request) do
    request
    |> sanitize_request()
    |> Adapter.request()
    |> sanitize_response()
  end

  @impl HTTPClient
  def request_stream(request) do
    case request |> sanitize_request() |> Adapter.request_stream() do
      {:ok, stream} -> {:ok, Stream.map(stream, &sanitize_stream_item/1)}
      error -> error
    end
  end

  @doc false
  def sanitize_request(%HTTPClient.Request{} = request) do
    {target_host, options} = Keyword.pop!(request.options, :target_host)

    headers =
      request.headers
      |> Enum.map(fn {name, value} -> {String.downcase(to_string(name)), value} end)
      |> Enum.filter(fn {name, _value} -> MapSet.member?(@allowed_request_headers, name) end)
      |> List.keystore("host", 0, {"host", target_host})

    %{
      request
      | headers: headers,
        cookies: "",
        options:
          options
          |> Keyword.put(:follow_redirect, false)
          |> Keyword.put(:proxy, nil)
    }
  end

  @doc false
  def sanitize_response_headers(headers) do
    dynamic_hop_headers = connection_header_tokens(headers)

    Enum.reject(headers, fn {name, _value} ->
      name = String.downcase(to_string(name))
      MapSet.member?(@blocked_response_headers, name) or MapSet.member?(dynamic_hop_headers, name)
    end)
  end

  defp sanitize_response({:ok, %HTTPClient.Response{} = response}) do
    {:ok,
     %{
       response
       | status_code: sanitize_status(response.status_code),
         headers: sanitize_response_headers(response.headers)
     }}
  end

  defp sanitize_response(other), do: other

  defp sanitize_stream_item({:status, status}), do: {:status, sanitize_status(status)}

  defp sanitize_stream_item({:headers, headers}),
    do: {:headers, sanitize_response_headers(headers)}

  defp sanitize_stream_item(other), do: other

  defp sanitize_status(status) when status in @redirect_statuses, do: 502
  defp sanitize_status(status), do: status

  defp connection_header_tokens(headers) do
    headers
    |> Enum.filter(fn {name, _value} -> String.downcase(to_string(name)) == "connection" end)
    |> Enum.flat_map(fn {_name, value} -> String.split(value, ",") end)
    |> Enum.map(&(&1 |> String.trim() |> String.downcase()))
    |> MapSet.new()
  end
end
