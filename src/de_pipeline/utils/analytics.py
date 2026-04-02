from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date

from de_pipeline.schemas.event import EventRecord


@dataclass(frozen=True)
class DailyKPI:
    event_date: date
    daily_active_users: int
    total_events: int
    total_sessions: int
    total_orders: int
    total_revenue: float


@dataclass(frozen=True)
class SessionMetric:
    session_id: str
    user_id: str
    event_date: date
    session_duration_seconds: int
    event_count: int
    purchase_count: int
    total_revenue: float


def deduplicate_events(events: list[EventRecord]) -> list[EventRecord]:
    latest_by_event_id: dict[str, EventRecord] = {}
    for event in events:
        current = latest_by_event_id.get(event.event_id)
        if current is None or event.ingest_time >= current.ingest_time:
            latest_by_event_id[event.event_id] = event
    return list(latest_by_event_id.values())


def compute_daily_kpis(events: list[EventRecord]) -> list[DailyKPI]:
    by_date: dict[date, dict[str, object]] = defaultdict(
        lambda: {
            "users": set(),
            "sessions": set(),
            "orders": set(),
            "revenue": 0.0,
            "events": 0,
        }
    )

    for event in deduplicate_events(events):
        event_date = event.event_time.date()
        bucket = by_date[event_date]
        bucket["events"] = int(bucket["events"]) + 1
        cast_users = bucket["users"]
        cast_sessions = bucket["sessions"]
        cast_orders = bucket["orders"]
        assert isinstance(cast_users, set)
        assert isinstance(cast_sessions, set)
        assert isinstance(cast_orders, set)
        cast_users.add(event.user_id)
        cast_sessions.add(event.session_id)
        if event.order_id:
            cast_orders.add(event.order_id)
        if event.order_amount:
            bucket["revenue"] = float(bucket["revenue"]) + event.order_amount

    return sorted(
        [
            DailyKPI(
                event_date=event_date,
                daily_active_users=len(values["users"]),
                total_events=int(values["events"]),
                total_sessions=len(values["sessions"]),
                total_orders=len(values["orders"]),
                total_revenue=round(float(values["revenue"]), 2),
            )
            for event_date, values in by_date.items()
        ],
        key=lambda row: row.event_date,
    )


def compute_session_metrics(events: list[EventRecord]) -> list[SessionMetric]:
    session_buckets: dict[str, list[EventRecord]] = defaultdict(list)
    for event in deduplicate_events(events):
        session_buckets[event.session_id].append(event)

    rows: list[SessionMetric] = []
    for session_id, session_events in session_buckets.items():
        sorted_events = sorted(session_events, key=lambda row: row.event_time)
        start = sorted_events[0]
        end = sorted_events[-1]
        purchase_count = sum(1 for row in sorted_events if row.event_type == "purchase")
        revenue = round(sum(row.order_amount or 0.0 for row in sorted_events), 2)
        duration_seconds = int((end.event_time - start.event_time).total_seconds())
        rows.append(
            SessionMetric(
                session_id=session_id,
                user_id=start.user_id,
                event_date=start.event_time.date(),
                session_duration_seconds=duration_seconds,
                event_count=len(sorted_events),
                purchase_count=purchase_count,
                total_revenue=revenue,
            )
        )

    return sorted(rows, key=lambda row: (row.event_date, row.session_id))

