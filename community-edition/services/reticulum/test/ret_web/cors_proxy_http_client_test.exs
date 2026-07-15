defmodule RetWeb.CorsProxyHTTPClientTest do
  use ExUnit.Case, async: true

  alias ReverseProxyPlug.HTTPClient.Request
  alias RetWeb.CorsProxyHTTPClient

  test "the real adapter connects to the pinned IP with the original Host" do
    {listener, port, server} =
      listen_once("HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 2\r\n\r\nok")

    request = %Request{
      method: :get,
      url: "http://127.0.0.1:#{port}/probe",
      headers: [{"cookie", "session=secret"}, {"x-forwarded-for", "10.0.0.1"}],
      cookies: "session=secret",
      options: [target_host: "origin.test", proxy: nil, follow_redirect: false, timeout: 1_000]
    }

    assert {:ok, response} = CorsProxyHTTPClient.request(request)
    assert response.status_code == 200
    assert response.body == "ok"

    raw_request = Task.await(server)
    :gen_tcp.close(listener)

    assert raw_request =~ "GET /probe HTTP/1.1"
    assert String.downcase(raw_request) =~ "host: origin.test"
    refute String.downcase(raw_request) =~ "cookie:"
    refute String.downcase(raw_request) =~ "x-forwarded-for:"
  end

  test "redirect responses are not followed" do
    {:ok, redirect_target} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_address, redirect_port}} = :inet.sockname(redirect_target)

    redirect_response =
      "HTTP/1.1 302 Found\r\nlocation: http://127.0.0.1:#{redirect_port}/secret\r\ncontent-length: 0\r\n\r\n"

    {listener, port, server} = listen_once(redirect_response)

    request = %Request{
      method: :get,
      url: "http://127.0.0.1:#{port}/redirect",
      options: [target_host: "origin.test", proxy: nil, follow_redirect: false, timeout: 1_000]
    }

    assert {:ok, response} = CorsProxyHTTPClient.request(request)
    assert response.status_code == 502

    refute Enum.any?(response.headers, fn {name, _value} ->
             String.downcase(to_string(name)) == "location"
           end)

    Task.await(server)
    :gen_tcp.close(listener)
    assert {:error, :timeout} = :gen_tcp.accept(redirect_target, 100)
    :gen_tcp.close(redirect_target)
  end

  test "pins Host and removes browser credentials and forwarding headers" do
    request = %Request{
      url: "https://93.184.216.34/avatar.glb",
      headers: [
        {"accept", "model/gltf-binary"},
        {"authorization", "Bearer secret"},
        {"cookie", "session=secret"},
        {"origin", "https://meta-hubs.org"},
        {"referer", "https://meta-hubs.org/private-room"},
        {"x-forwarded-for", "127.0.0.1"}
      ],
      cookies: "session=secret",
      options: [
        target_host: "media.example",
        proxy: "http://proxy.invalid",
        follow_redirect: true
      ]
    }

    sanitized = CorsProxyHTTPClient.sanitize_request(request)

    assert {"host", "media.example"} in sanitized.headers
    assert {"accept", "model/gltf-binary"} in sanitized.headers
    refute Enum.any?(sanitized.headers, fn {name, _} -> name == "authorization" end)
    refute Enum.any?(sanitized.headers, fn {name, _} -> name == "cookie" end)
    refute Enum.any?(sanitized.headers, fn {name, _} -> name == "x-forwarded-for" end)
    assert sanitized.cookies == ""
    assert sanitized.options[:proxy] == nil
    assert sanitized.options[:follow_redirect] == false
    refute Keyword.has_key?(sanitized.options, :target_host)
  end

  test "removes unsafe and connection-nominated response headers" do
    headers = [
      {"content-type", "model/gltf-binary"},
      {"connection", "x-private, keep-alive"},
      {"x-private", "secret"},
      {"set-cookie", "session=attacker"},
      {"location", "http://127.0.0.1/secret"},
      {"access-control-allow-origin", "https://attacker.invalid"}
    ]

    sanitized = CorsProxyHTTPClient.sanitize_response_headers(headers)

    assert sanitized == [{"content-type", "model/gltf-binary"}]
  end

  defp listen_once(response) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_address, port}} = :inet.sockname(listener)

    server =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 1_000)
        {:ok, request} = :gen_tcp.recv(socket, 0, 1_000)
        :ok = :gen_tcp.send(socket, response)
        :gen_tcp.close(socket)
        request
      end)

    {listener, port, server}
  end
end
