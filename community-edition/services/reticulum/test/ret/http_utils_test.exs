defmodule Ret.HttpUtilsTest do
  use ExUnit.Case

  setup_all do
    Mox.defmock(Ret.HttpMock, for: HTTPoison.Base)
    Ret.TestHelpers.merge_module_config(:ret, Ret.HttpUtils, %{:http_client => Ret.HttpMock})

    on_exit(fn ->
      Ret.TestHelpers.merge_module_config(:ret, Ret.HttpUtils, %{:http_client => nil})
    end)
  end

  test "fetch_content_type should attempt a request and return the response content type" do
    Ret.HttpMock
    |> Mox.expect(:request, 1, fn _verb, _url, _body, _headers, _options ->
      {:ok, %HTTPoison.Response{status_code: 200, headers: %{"content-type" => "foo/bar"}}}
    end)

    {:ok, "foo/bar"} = Ret.HttpUtils.fetch_content_type("http://foo.local/")
  end

  test "classifies non-global IPv4 ranges as internal" do
    internal_addresses = [
      {0, 0, 0, 1},
      {10, 1, 2, 3},
      {100, 64, 0, 1},
      {127, 0, 0, 1},
      {169, 254, 1, 1},
      {172, 16, 0, 1},
      {192, 0, 0, 1},
      {192, 0, 2, 1},
      {192, 168, 1, 1},
      {198, 18, 0, 1},
      {198, 51, 100, 1},
      {203, 0, 113, 1},
      {224, 0, 0, 1},
      {255, 255, 255, 255}
    ]

    assert Enum.all?(internal_addresses, &Ret.HttpUtils.internal_ip?/1)
    refute Ret.HttpUtils.internal_ip?({8, 8, 8, 8})
  end

  test "fails closed for nil and IPv6 addresses" do
    assert Ret.HttpUtils.internal_ip?(nil)
    assert Ret.HttpUtils.internal_ip?({0, 0, 0, 0, 0, 0, 0, 1})
  end
end
