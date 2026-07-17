defmodule Ret.BotOrchestratorTestHttpClient do
  @moduledoc false

  def request(method, url, body, headers, options) do
    case Application.get_env(:ret, __MODULE__) do
      pid when is_pid(pid) ->
        send(pid, {:bot_orchestrator_request, method, url, body, headers, options})
        {:ok, %HTTPoison.Response{status_code: 200, body: "{}"}}

      _ ->
        {:error, %HTTPoison.Error{reason: :test_receiver_unavailable}}
    end
  end
end
