defmodule Ret.CowlibSecurityContractTest do
  use ExUnit.Case, async: true

  test "pinned cowlib rejects Link directive injection vectors" do
    valid = [
      %{
        target: "/scene",
        rel: "preload",
        attributes: [{"as", "fetch"}]
      }
    ]

    assert IO.iodata_to_binary(:cow_link.link(valid)) ==
             ~s(</scene>; rel="preload"; as="fetch")

    assert catch_error(
             :cow_link.link([
               %{
                 target: ~s(/>; rel="preconnect", <https://attacker.invalid/),
                 rel: "self",
                 attributes: []
               }
             ])
           )

    assert catch_error(
             :cow_link.link([
               %{target: "/", rel: "self\r\nX-Injected: yes", attributes: []}
             ])
           )

    assert catch_error(
             :cow_link.link([
               %{
                 target: "/",
                 rel: "self",
                 attributes: [{~s(a"; rel="preconnect), "value"}]
               }
             ])
           )
  end
end
