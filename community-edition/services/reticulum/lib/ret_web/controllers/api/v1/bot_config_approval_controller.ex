defmodule RetWeb.Api.V1.BotConfigApprovalController do
  use RetWeb, :controller

  alias Ret.{BotConfig, BotConfigApproval, BotOrchestrator}

  def index(conn, params) do
    conn
    |> no_store()
    |> json(BotConfigApproval.inventory(params))
  end

  def approve(conn, %{
        "hub_sid" => hub_sid,
        "expected_config_fingerprint" => expected_fingerprint
      }) do
    actor = Guardian.Plug.current_resource(conn)

    case BotConfigApproval.approve_candidate(hub_sid, expected_fingerprint, actor) do
      {:ok, hub} ->
        sync_runtime(hub)

        conn
        |> no_store()
        |> json(%{status: "approved", hub_sid: hub.hub_sid})

      {:error, reason} ->
        send_error(conn, reason)
    end
  end

  def approve(conn, _params), do: send_error(conn, :invalid_request)

  def quarantine(conn, %{"hub_sid" => hub_sid}) do
    actor = Guardian.Plug.current_resource(conn)

    case BotConfigApproval.quarantine(hub_sid, actor) do
      {:ok, hub} ->
        _ = BotOrchestrator.room_stop(%{hub_sid: hub.hub_sid})

        conn
        |> no_store()
        |> json(%{status: "quarantined", hub_sid: hub.hub_sid})

      {:error, reason} ->
        send_error(conn, reason)
    end
  end

  def quarantine(conn, _params), do: send_error(conn, :invalid_request)

  defp sync_runtime(hub) do
    if BotConfigApproval.runtime_enabled?(hub) do
      _ =
        BotOrchestrator.room_config(%{
          hub_sid: hub.hub_sid,
          bots: BotConfig.normalize(hub.user_data)
        })
    else
      _ = BotOrchestrator.room_stop(%{hub_sid: hub.hub_sid})
    end

    :ok
  end

  defp send_error(conn, reason) do
    status =
      case reason do
        :not_found -> 404
        :forbidden -> 403
        :fingerprint_mismatch -> 409
        :room_limit -> 409
        :config_too_large -> 422
        :inactive_candidate -> 422
        :approval_unavailable -> 503
        _ -> 400
      end

    conn
    |> no_store()
    |> put_status(status)
    |> json(%{error: Atom.to_string(reason)})
  end

  defp no_store(conn), do: put_resp_header(conn, "cache-control", "no-store")
end
