from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

EVENT_TYPES = frozenset(
    {
        "home_view",
        "product_view",
        "add_to_cart",
        "checkout_start",
        "purchase",
    }
)

REQUIRED_FIELDS = (
    "event_id",
    "user_id",
    "session_id",
    "event_type",
    "event_time",
    "ingest_time",
    "product_id",
    "device_type",
    "traffic_source",
    "country_code",
    "schema_version",
)


@dataclass(frozen=True)
class EventRecord:
    event_id: str
    user_id: str
    session_id: str
    event_type: str
    event_time: datetime
    ingest_time: datetime
    product_id: str
    order_id: str | None
    order_amount: float | None
    device_type: str
    traffic_source: str
    country_code: str
    schema_version: int = 1


def _parse_timestamp(raw_value: Any) -> datetime:
    if isinstance(raw_value, datetime):
        dt = raw_value
    elif isinstance(raw_value, str):
        normalized = raw_value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
    else:
        raise ValueError(f"unsupported timestamp value: {raw_value!r}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def validate_event_payload(payload: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []

    for field_name in REQUIRED_FIELDS:
        value = payload.get(field_name)
        if value is None or (isinstance(value, str) and not value.strip()):
            errors.append(f"{field_name} is required")

    event_type = payload.get("event_type")
    if event_type and event_type not in EVENT_TYPES:
        errors.append(f"event_type must be one of {sorted(EVENT_TYPES)}")

    for field_name in ("event_time", "ingest_time"):
        raw_value = payload.get(field_name)
        if raw_value is None:
            continue
        try:
            _parse_timestamp(raw_value)
        except ValueError as exc:
            errors.append(f"{field_name} is invalid: {exc}")

    schema_version = payload.get("schema_version")
    if schema_version is not None:
        try:
            if int(schema_version) < 1:
                errors.append("schema_version must be >= 1")
        except (TypeError, ValueError):
            errors.append("schema_version must be an integer")

    if event_type == "purchase":
        if not payload.get("order_id"):
            errors.append("order_id is required for purchase events")
        order_amount = payload.get("order_amount")
        try:
            if order_amount is None or float(order_amount) <= 0:
                errors.append("order_amount must be > 0 for purchase events")
        except (TypeError, ValueError):
            errors.append("order_amount must be numeric for purchase events")

    return errors


def coerce_event_record(payload: Mapping[str, Any]) -> EventRecord:
    errors = validate_event_payload(payload)
    if errors:
        raise ValueError("; ".join(errors))

    order_amount = payload.get("order_amount")
    coerced_amount = float(order_amount) if order_amount is not None else None

    return EventRecord(
        event_id=str(payload["event_id"]),
        user_id=str(payload["user_id"]),
        session_id=str(payload["session_id"]),
        event_type=str(payload["event_type"]),
        event_time=_parse_timestamp(payload["event_time"]),
        ingest_time=_parse_timestamp(payload["ingest_time"]),
        product_id=str(payload["product_id"]),
        order_id=str(payload["order_id"]) if payload.get("order_id") else None,
        order_amount=coerced_amount,
        device_type=str(payload["device_type"]),
        traffic_source=str(payload["traffic_source"]),
        country_code=str(payload["country_code"]),
        schema_version=int(payload["schema_version"]),
    )


def event_to_payload(record: EventRecord) -> dict[str, Any]:
    payload = asdict(record)
    payload["event_time"] = record.event_time.isoformat()
    payload["ingest_time"] = record.ingest_time.isoformat()
    return payload

