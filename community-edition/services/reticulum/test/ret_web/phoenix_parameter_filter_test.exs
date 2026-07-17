defmodule RetWeb.PhoenixParameterFilterTest do
  use ExUnit.Case, async: true

  test "filters bot credentials and common secrets from Phoenix logs" do
    filter_parameters = Application.fetch_env!(:phoenix, :filter_parameters)

    values = %{
      "bot_access_key" => "internal-bot-key",
      "bot_chat_capability" => "private-channel-capability",
      "access_token" => "session-token",
      "password" => "account-password",
      "client_secret" => "oauth-secret",
      "message" => "private conversation",
      "prompt" => "private room prompt"
    }

    assert Phoenix.Logger.filter_values(values, filter_parameters) == %{
             "bot_access_key" => "[FILTERED]",
             "bot_chat_capability" => "[FILTERED]",
             "access_token" => "[FILTERED]",
             "password" => "[FILTERED]",
             "client_secret" => "[FILTERED]",
             "message" => "[FILTERED]",
             "prompt" => "[FILTERED]"
           }
  end

  test "filters credentials inside channel join context" do
    filter_parameters = Application.fetch_env!(:phoenix, :filter_parameters)

    values = %{
      "context" => %{"bot_access_key" => "nested-key", "mobile" => false},
      "profile" => %{"displayName" => "bot-runner"}
    }

    assert get_in(Phoenix.Logger.filter_values(values, filter_parameters), [
             "context",
             "bot_access_key"
           ]) ==
             "[FILTERED]"
  end
end
