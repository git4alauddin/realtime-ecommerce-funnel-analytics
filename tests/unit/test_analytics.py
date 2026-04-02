from __future__ import annotations

from datetime import UTC, datetime, timedelta

from de_pipeline.schemas.event import EventRecord
from de_pipeline.utils.analytics import (
    compute_daily_kpis,
    compute_session_metrics,
    deduplicate_events,
)


def build_event(
    event_id: str,
    session_id: str,
    event_type: str,
    offset_seconds: int,
    order_id: str | None = None,
    order_amount: float | None = None,
) -> EventRecord:
    event_time = datetime(2025, 1, 1, 10, 0, tzinfo=UTC) + timedelta(seconds=offset_seconds)
    return EventRecord(
        event_id=event_id,
        user_id="user-100",
        session_id=session_id,
        event_type=event_type,
        event_time=event_time,
        ingest_time=event_time + timedelta(seconds=1),
        product_id="sku-100",
        order_id=order_id,
        order_amount=order_amount,
        device_type="mobile",
        traffic_source="organic",
        country_code="US",
        schema_version=1,
    )


def test_deduplicate_events_keeps_latest_ingest_time() -> None:
    first = build_event("evt-1", "session-1", "product_view", 0)
    second = EventRecord(
        **{
            **first.__dict__,
            "ingest_time": first.ingest_time + timedelta(seconds=5),
        }
    )

    deduped = deduplicate_events([first, second])
    assert len(deduped) == 1
    assert deduped[0].ingest_time == second.ingest_time


def test_compute_daily_kpis_aggregates_orders_and_revenue() -> None:
    events = [
        build_event("evt-1", "session-1", "product_view", 0),
        build_event("evt-2", "session-1", "purchase", 60, order_id="order-1", order_amount=19.0),
    ]

    rows = compute_daily_kpis(events)
    assert len(rows) == 1
    assert rows[0].daily_active_users == 1
    assert rows[0].total_orders == 1
    assert rows[0].total_revenue == 19.0


def test_compute_session_metrics_measures_duration() -> None:
    events = [
        build_event("evt-1", "session-1", "home_view", 0),
        build_event("evt-2", "session-1", "purchase", 75, order_id="order-1", order_amount=19.0),
    ]

    rows = compute_session_metrics(events)
    assert len(rows) == 1
    assert rows[0].event_count == 2
    assert rows[0].purchase_count == 1
    assert rows[0].session_duration_seconds == 75

