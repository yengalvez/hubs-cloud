defmodule RetWeb.CorsProxyTest do
  use ExUnit.Case, async: true

  alias RetWeb.CorsProxy

  test "pins a valid HTTPS target to the validated address" do
    resolver = fn "media.example" -> [{93, 184, 216, 34}] end

    assert {:ok, target} =
             CorsProxy.resolve_target("https://media.example/models/avatar.glb?v=1", resolver)

    assert target.host == "media.example"
    assert target.host_header == "media.example"
    assert target.scheme == "https"
    assert target.pinned_url == "https://93.184.216.34/models/avatar.glb?v=1"

    options = CorsProxy.client_options(target)
    assert options[:target_host] == "media.example"
    assert options[:proxy] == nil
    assert options[:follow_redirect] == false
    assert options[:hackney] == [pool: false, protocols: [:http1]]
    assert options[:ssl][:server_name_indication] == ~c"media.example"
    assert options[:ssl][:versions] == [:"tlsv1.2", :"tlsv1.3"]
  end

  test "rejects malformed and non-web targets" do
    resolver = fn _ -> [{93, 184, 216, 34}] end

    invalid_urls = [
      "ftp://media.example/file",
      "https://user:pass@media.example/file",
      "https://media.example:8443/file",
      "https://media.example/file#fragment",
      "https:///missing-host"
    ]

    assert Enum.all?(invalid_urls, fn url ->
             CorsProxy.resolve_target(url, resolver) == {:error, :invalid_target}
           end)
  end

  test "rejects internal, unresolved, and mixed DNS answers" do
    assert {:error, :invalid_target} =
             CorsProxy.resolve_target("https://media.example/file", fn _ -> [{127, 0, 0, 1}] end)

    assert {:error, :invalid_target} =
             CorsProxy.resolve_target("https://media.example/file", fn _ -> [] end)

    assert {:error, :invalid_target} =
             CorsProxy.resolve_target("https://media.example/file", fn _ ->
               [{93, 184, 216, 34}, {10, 0, 0, 1}]
             end)
  end
end
