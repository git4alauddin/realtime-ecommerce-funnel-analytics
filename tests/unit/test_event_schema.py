from __future__ import annotations

from de_pipeline.schemas.event import coerce_event_record, validate_event_payload


def build_payload(**overrides):
    payload = {
        "event_id": "evt-100",
        "user_id": "user-100",
        "session_id": "session-100",
        "event_type": "product_view",
        "event_time": "2025-01-01T10:00:00+00:00",
        "ingest_time": "2025-01-01T10:00:01+00:00",
        "product_id": "sku-100",
        "order_id": None,
        "order_amount": None,
        "device_type": "mobile",
        "traffic_source": "organic",
        "country_code": "US",
        "schema_version": 1,
    }
    payload.update(overrides)
    return payload


def test_validate_event_payload_accepts_valid_event() -> None:
    assert validate_event_payload(build_payload()) == []


def test_validate_event_payload_rejects_invalid_event_type() -> None:
    errors = validate_event_payload(build_payload(event_type="login"))
    assert any("event_type" in error for error in errors)


def test_validate_event_payload_requires_purchase_order_fields() -> None:
    errors = validate_event_payload(
        build_payload(event_type="purchase", order_id=None, order_amount=None)
    )
    assert "order_id is required for purchase events" in errors
    assert "order_amount must be > 0 for purchase events" in errors


def test_coerce_event_record_parses_timestamps() -> None:
    event = coerce_event_record(build_payload())
    assert event.event_time.isoformat() == "2025-01-01T10:00:00+00:00"
    assert event.schema_version == 1
