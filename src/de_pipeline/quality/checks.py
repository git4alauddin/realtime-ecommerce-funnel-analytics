from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from de_pipeline.schemas.event import validate_event_payload


@dataclass(frozen=True)
class DataQualityReport:
    total_events: int
    valid_events: int
    invalid_events: int
    duplicate_event_ids: int
    invalid_event_types: int
    missing_required_fields: int


def collect_data_quality_metrics(payloads: Iterable[Mapping[str, Any]]) -> DataQualityReport:
    seen_event_ids: set[str] = set()
    duplicate_event_ids = 0
    invalid_event_types = 0
    missing_required_fields = 0
    invalid_events = 0
    total_events = 0

    for payload in payloads:
        total_events += 1
        event_id = payload.get("event_id")
        if isinstance(event_id, str):
            if event_id in seen_event_ids:
                duplicate_event_ids += 1
            else:
                seen_event_ids.add(event_id)

        errors = validate_event_payload(payload)
        if errors:
            invalid_events += 1
            for error in errors:
                if error.startswith("event_type"):
                    invalid_event_types += 1
                if error.endswith("is required"):
                    missing_required_fields += 1

    return DataQualityReport(
        total_events=total_events,
        valid_events=total_events - invalid_events,
        invalid_events=invalid_events,
        duplicate_event_ids=duplicate_event_ids,
        invalid_event_types=invalid_event_types,
        missing_required_fields=missing_required_fields,
    )

