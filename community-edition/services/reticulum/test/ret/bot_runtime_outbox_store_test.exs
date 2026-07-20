defmodule Ret.BotRuntimeOutbox.StoreTest do
  use Ret.DataCase

  import Ret.TestHelpers

  alias Ecto.Adapters.SQL
  alias Ret.{BotRuntimeOutbox, Hub, Repo}
  alias Ret.BotRuntimeOutbox.Store

  setup [:create_account, :create_owned_file, :create_scene, :create_hub]

  setup do
    Repo.update_all(BotRuntimeOutbox,
      set: [
        delivered_at: DateTime.utc_now(),
        claim_owner: nil,
        claim_token: nil,
        claim_expires_at: nil
      ]
    )

    Repo.delete_all(BotRuntimeOutbox)
    :ok
  end

  test "config and stop events are immutable, typed and revision ordered", %{hub: hub} do
    bots = %{
      "count" => 1,
      "enabled" => true,
      "mobility" => "static",
      "chat_enabled" => 1.0
    }

    assert {:ok, config} =
             Store.enqueue_config(%{
               hub_id: hub.hub_id,
               hub_sid: hub.hub_sid,
               runtime_revision: 1,
               bots: bots,
               runtime_chat_enabled: true
             })

    assert config.operation_id
    assert config.event_kind == "config"
    assert config.bots == bots
    assert config.bots["chat_enabled"] === 1.0
    refute config.runtime_chat_enabled
    assert config.revoke_epoch == nil
    assert config.attempt_count == 0
    assert config.next_attempt_at

    assert {:ok, stop} =
             Store.enqueue_stop(%{
               "hub_id" => hub.hub_id,
               "hub_sid" => hub.hub_sid,
               "runtime_revision" => 2,
               "revoke_epoch" => 91
             })

    assert stop.event_kind == "stop"
    assert stop.bots == nil
    assert stop.runtime_chat_enabled == nil
    assert stop.revoke_epoch == 91
    refute stop.operation_id == config.operation_id

    assert Store.pending_before?(hub.hub_id, 1) == false
    assert Store.pending_before?(hub.hub_id, 2)
    assert Store.pending_through?(hub.hub_id, 2)
    assert {:error, :invalid_pending_query} = Store.pending_through?(hub.hub_id, 0)
  end

  test "enqueue participates in the caller transaction and never commits independently", %{
    hub: hub
  } do
    assert {:error, :forced_rollback} =
             Repo.transaction(fn ->
               assert {:ok, _event} =
                        Store.enqueue_config(%{
                          hub_id: hub.hub_id,
                          hub_sid: hub.hub_sid,
                          runtime_revision: 1,
                          bots: %{"enabled" => true}
                        })

               Repo.rollback(:forced_rollback)
             end)

    refute Repo.exists?(BotRuntimeOutbox)
  end

  test "database and changeset constraints reject unsafe revisions, duplicate revisions and alien hubs",
       %{
         hub: hub
       } do
    maximum = BotRuntimeOutbox.max_javascript_integer()

    assert {:ok, _event} =
             Store.enqueue_config(%{
               hub_id: hub.hub_id,
               hub_sid: hub.hub_sid,
               runtime_revision: maximum,
               bots: %{}
             })

    assert {:error, overflow} =
             Store.enqueue_stop(%{
               hub_id: hub.hub_id,
               hub_sid: hub.hub_sid,
               runtime_revision: maximum + 1,
               revoke_epoch: 1
             })

    assert "must be less than or equal to #{maximum}" in errors_on(overflow).runtime_revision

    assert {:error, duplicate} =
             Store.enqueue_stop(%{
               hub_id: hub.hub_id,
               hub_sid: hub.hub_sid,
               runtime_revision: maximum,
               revoke_epoch: 2
             })

    assert errors_on(duplicate).runtime_revision

    assert {:error, alien_hub} =
             Store.enqueue_config(%{
               hub_id: hub.hub_id + 10_000_000,
               hub_sid: "missing-hub",
               runtime_revision: 1,
               bots: %{}
             })

    assert errors_on(alien_hub).hub_id

    assert {:error, mixed_event} =
             Store.enqueue_config(%{
               hub_id: hub.hub_id,
               hub_sid: hub.hub_sid,
               runtime_revision: 2,
               bots: %{},
               revoke_epoch: 10
             })

    assert errors_on(mixed_event).revoke_epoch
  end

  test "hub sid and operation id are canonical before persistence", %{hub: hub} do
    for invalid_sid <- [
          "",
          "contains space",
          "contains/slash",
          "contains.dot",
          "á",
          String.duplicate("a", 65)
        ] do
      assert {:error, changeset} =
               Store.enqueue_config(%{
                 hub_id: hub.hub_id,
                 hub_sid: invalid_sid,
                 runtime_revision: 1,
                 bots: %{}
               })

      sid_errors = errors_on(changeset).hub_sid

      assert "must match ^[A-Za-z0-9_-]{1,64}$" in sid_errors or
               (invalid_sid == "" and "can't be blank" in sid_errors)
    end

    valid_boundary =
      BotRuntimeOutbox.changeset(%BotRuntimeOutbox{}, %{
        operation_id: Ecto.UUID.generate(),
        hub_id: hub.hub_id,
        hub_sid: String.duplicate("A", 64),
        runtime_revision: 1,
        event_kind: "config",
        bots: %{},
        runtime_chat_enabled: false
      })

    assert valid_boundary.valid?

    for invalid_uuid <- [
          "11111111-1111-1111-8111-111111111111",
          "11111111-1111-4111-7111-111111111111",
          "not-a-uuid"
        ] do
      changeset =
        BotRuntimeOutbox.changeset(%BotRuntimeOutbox{}, %{
          operation_id: invalid_uuid,
          hub_id: hub.hub_id,
          hub_sid: hub.hub_sid,
          runtime_revision: 1,
          event_kind: "config",
          bots: %{},
          runtime_chat_enabled: false
        })

      refute changeset.valid?
      assert errors_on(changeset).operation_id
    end

    uppercase_uuid = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"

    assert {:ok, canonical} =
             Store.enqueue_config(%{
               operation_id: uppercase_uuid,
               hub_id: hub.hub_id,
               hub_sid: hub.hub_sid,
               runtime_revision: 1,
               bots: %{}
             })

    assert canonical.operation_id == String.downcase(uppercase_uuid)
    assert BotRuntimeOutbox.canonical_uuid_v4?(canonical.operation_id)
  end

  test "config payload rejects oversized or JavaScript-inexact JSON recursively", %{hub: hub} do
    safe_integer = BotRuntimeOutbox.max_javascript_integer()

    valid =
      config_changeset(hub, %{
        "integer_bounds" => [safe_integer, -safe_integer],
        "roundtrip_floats" => [1.0, -0.0, 5.0e-324, 1.2345678901234567],
        "nested" => %{"values" => [true, false, nil, "utf8-✓"]}
      })

    assert valid.valid?

    assert config_changeset(hub, %{"depth" => nested_lists(63)}).valid?

    projection_boundary = %{
      "enabled" => true,
      "count" => 1,
      "padding" => String.duplicate("x", 16_324)
    }

    projected_boundary = BotRuntimeOutbox.runtime_bots_projection(projection_boundary, false)
    assert byte_size(Jason.encode!(projected_boundary)) == BotRuntimeOutbox.max_config_bytes()
    assert byte_size(Poison.encode!(projected_boundary)) == BotRuntimeOutbox.max_config_bytes()
    assert config_changeset(hub, projection_boundary).valid?

    projection_overflow = Map.put(projection_boundary, "padding", String.duplicate("x", 16_325))
    projected_overflow = BotRuntimeOutbox.runtime_bots_projection(projection_overflow, false)
    assert byte_size(Jason.encode!(projected_overflow)) == BotRuntimeOutbox.max_config_bytes() + 1

    overflow_changeset = config_changeset(hub, projection_overflow)
    refute overflow_changeset.valid?

    assert "runtime projection with normalized chat_enabled must be interoperable JSON within #{BotRuntimeOutbox.max_config_bytes()} encoded bytes" in errors_on(
             overflow_changeset
           ).bots

    too_deep = config_changeset(hub, %{"depth" => nested_lists(64)})
    refute too_deep.valid?

    assert "must not exceed #{BotRuntimeOutbox.max_json_depth()} nested JSON containers" in errors_on(
             too_deep
           ).bots

    for unsafe_bots <- [
          %{"unsafe" => safe_integer + 1},
          %{"nested" => [%{"unsafe" => -safe_integer - 1}]},
          %{"integral-float-outside-safe-range" => 9_007_199_254_740_992.0},
          %{"atom-key-is-not-json" => %{unsafe_key: 1}},
          %{"tuple-is-not-json" => {:not, :json}},
          %{"invalid-utf8" => <<255>>}
        ] do
      changeset = config_changeset(hub, unsafe_bots)
      refute changeset.valid?
      assert errors_on(changeset).bots
    end

    oversized =
      config_changeset(hub, %{
        "prompt" => String.duplicate("x", BotRuntimeOutbox.max_config_bytes())
      })

    refute oversized.valid?

    assert "must not exceed #{BotRuntimeOutbox.max_config_bytes()} encoded bytes" in errors_on(
             oversized
           ).bots
  end

  test "loaded event identity and payload are immutable through changesets", %{hub: hub} do
    event = enqueue_config!(hub, 1)

    for {field, attrs} <- [
          {:operation_id, %{operation_id: Ecto.UUID.generate()}},
          {:hub_id, %{hub_id: hub.hub_id + 1}},
          {:hub_sid, %{hub_sid: "different_sid"}},
          {:runtime_revision, %{runtime_revision: 2}},
          {:event_kind, %{event_kind: "stop", revoke_epoch: 8}},
          {:bots, %{bots: %{"enabled" => false}}},
          {:runtime_chat_enabled, %{runtime_chat_enabled: true}}
        ] do
      changeset = BotRuntimeOutbox.changeset(event, attrs)
      refute changeset.valid?
      assert "is immutable once inserted" in Map.fetch!(errors_on(changeset), field)
    end

    unchanged = BotRuntimeOutbox.changeset(event, %{bots: event.bots})
    assert unchanged.valid?
    assert unchanged.changes == %{}

    numeric_event = %{event | bots: %{"typed" => 1}}
    numeric_drift = BotRuntimeOutbox.changeset(numeric_event, %{bots: %{"typed" => 1.0}})
    refute numeric_drift.valid?
    assert "is immutable once inserted" in errors_on(numeric_drift).bots
    assert get_change(numeric_drift, :bots) === %{"typed" => 1.0}

    stop = enqueue_stop!(hub, 2, 10)
    changed_stop = BotRuntimeOutbox.changeset(stop, %{revoke_epoch: 11})
    refute changed_stop.valid?
    assert "is immutable once inserted" in errors_on(changed_stop).revoke_epoch
  end

  test "claims never let a later revision cross a claimed or backed-off predecessor", %{
    hub: hub,
    scene: scene
  } do
    other_hub = insert_hub!(scene, "Other claim room")

    first = enqueue_config!(hub, 1)
    later = enqueue_stop!(hub, 2, 501)
    other = enqueue_config!(other_hub, 1)

    first_token = Ecto.UUID.generate()

    assert {:ok, claimed_first} =
             Store.claim_next("dispatcher-a",
               claim_token: first_token,
               claim_ttl_seconds: 60
             )

    assert claimed_first.id == first.id
    assert claimed_first.runtime_revision == 1
    assert claimed_first.claim_owner == "dispatcher-a"
    assert claimed_first.claim_token == first_token
    assert claimed_first.attempt_count == 1

    assert {:ok, claimed_other} = Store.claim_next("dispatcher-b", claim_ttl_seconds: 60)
    assert claimed_other.id == other.id
    refute claimed_other.id == later.id

    assert :ok =
             Store.complete_claim(
               claimed_other.id,
               claimed_other.runtime_revision,
               claimed_other.claim_owner,
               claimed_other.claim_token
             )

    assert :ok =
             Store.retry_claim(
               claimed_first.id,
               claimed_first.runtime_revision,
               claimed_first.claim_owner,
               claimed_first.claim_token,
               "http_timeout",
               60
             )

    assert :empty = Store.claim_next("dispatcher-c", claim_ttl_seconds: 60)

    reloaded_first = Repo.get!(BotRuntimeOutbox, first.id)
    assert reloaded_first.last_failure_code == "http_timeout"
    assert reloaded_first.claim_owner == nil
    assert reloaded_first.claim_token == nil
    assert reloaded_first.claim_expires_at == nil
    assert Repo.get!(BotRuntimeOutbox, later.id).attempt_count == 0
  end

  test "expired claims are recoverable and stale owners cannot complete or retry", %{hub: hub} do
    event = enqueue_config!(hub, 1)
    stale_token = Ecto.UUID.generate()

    assert {:ok, stale_claim} =
             Store.claim_next("stale-dispatcher",
               claim_token: stale_token,
               claim_ttl_seconds: 60
             )

    SQL.query!(
      Repo,
      """
      UPDATE ret0.bot_runtime_outbox
      SET claim_expires_at = timezone('UTC', clock_timestamp()) - interval '1 second'
      WHERE id = $1
      """,
      [event.id]
    )

    fresh_token = Ecto.UUID.generate()

    assert {:ok, fresh_claim} =
             Store.claim_next("fresh-dispatcher",
               claim_token: fresh_token,
               claim_ttl_seconds: 60
             )

    assert fresh_claim.id == event.id
    assert fresh_claim.attempt_count == 2

    assert {:error, :claim_lost} =
             Store.complete_claim(
               stale_claim.id,
               stale_claim.runtime_revision,
               stale_claim.claim_owner,
               stale_claim.claim_token
             )

    assert {:error, :claim_lost} =
             Store.retry_claim(
               fresh_claim.id,
               fresh_claim.runtime_revision + 1,
               fresh_claim.claim_owner,
               fresh_claim.claim_token,
               "wrong_revision",
               0
             )

    assert {:error, :claim_lost} =
             Store.complete_claim(
               fresh_claim.id,
               fresh_claim.runtime_revision,
               "wrong-owner",
               fresh_claim.claim_token
             )

    assert :ok =
             Store.complete_claim(
               fresh_claim.id,
               fresh_claim.runtime_revision,
               fresh_claim.claim_owner,
               fresh_claim.claim_token
             )

    delivered = Repo.get!(BotRuntimeOutbox, event.id)
    assert delivered.delivered_at
    assert delivered.claim_owner == nil
    assert delivered.claim_token == nil
    assert delivered.claim_expires_at == nil
    refute Store.pending_through?(hub.hub_id, 1)
  end

  test "retry accepts bounded machine codes only", %{hub: hub} do
    event = enqueue_config!(hub, 1)
    assert {:ok, claim} = Store.claim_next("dispatcher", claim_ttl_seconds: 60)

    assert {:error, :invalid_retry} =
             Store.retry_claim(
               claim.id,
               claim.runtime_revision,
               claim.claim_owner,
               claim.claim_token,
               "HTTP body with private payload",
               0
             )

    assert Repo.get!(BotRuntimeOutbox, event.id).claim_token == claim.claim_token
  end

  defp enqueue_config!(hub, runtime_revision) do
    {:ok, event} =
      Store.enqueue_config(%{
        hub_id: hub.hub_id,
        hub_sid: hub.hub_sid,
        runtime_revision: runtime_revision,
        bots: %{"enabled" => true}
      })

    event
  end

  defp config_changeset(hub, bots) do
    BotRuntimeOutbox.changeset(%BotRuntimeOutbox{}, %{
      operation_id: Ecto.UUID.generate(),
      hub_id: hub.hub_id,
      hub_sid: hub.hub_sid,
      runtime_revision: 1,
      event_kind: "config",
      bots: bots,
      runtime_chat_enabled: false
    })
  end

  defp nested_lists(count) do
    Enum.reduce(1..count, 0, fn _index, nested -> [nested] end)
  end

  defp enqueue_stop!(hub, runtime_revision, revoke_epoch) do
    {:ok, event} =
      Store.enqueue_stop(%{
        hub_id: hub.hub_id,
        hub_sid: hub.hub_sid,
        runtime_revision: runtime_revision,
        revoke_epoch: revoke_epoch
      })

    event
  end

  defp insert_hub!(scene, name) do
    %Hub{}
    |> Hub.changeset(scene, %{name: name})
    |> Repo.insert!()
  end
end
