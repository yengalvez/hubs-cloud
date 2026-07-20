defmodule Ret.BotRuntimeOutboxDispatcherTestStore do
  @moduledoc false

  def claim_next(claim_owner, opts), do: call({:claim_next, claim_owner, opts})

  def complete_claim(id, runtime_revision, claim_owner, claim_token) do
    call({:complete_claim, id, runtime_revision, claim_owner, claim_token})
  end

  def retry_claim(
        id,
        runtime_revision,
        claim_owner,
        claim_token,
        failure_code,
        delay_seconds
      ) do
    call({
      :retry_claim,
      id,
      runtime_revision,
      claim_owner,
      claim_token,
      failure_code,
      delay_seconds
    })
  end

  defp call(message) do
    case Application.get_env(:ret, __MODULE__) do
      handler when is_function(handler, 1) -> handler.(message)
      _ -> {:error, :database_unavailable}
    end
  end
end

defmodule Ret.BotRuntimeOutboxDispatcherTestDelivery do
  @moduledoc false

  def deliver_runtime_event(event) do
    case Application.get_env(:ret, __MODULE__) do
      handler when is_function(handler, 1) -> handler.(event)
      _ -> {:pending, :test_delivery_unavailable}
    end
  end
end
