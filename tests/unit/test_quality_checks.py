from __future__ import annotations

from de_pipeline.quality import collect_data_quality_metrics


def test_collect_data_quality_metrics_detects_duplicates_and_invalid_rows() -> None:
    payloads = [
        {
            "event_id": "evt-1",
            "user_id": "user-1",
            "session_id": "session-1",
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
        },
        {
            "event_id": "evt-1",
            "user_id": "user-2",
            "session_id": "session-2",
            "event_type": "signup",
            "event_time": "2025-01-01T10:01:00+00:00",
            "ingest_time": "2025-01-01T10:01:01+00:00",
            "product_id": "sku-100",
            "order_id": None,
            "order_amount": None,
            "device_type": "desktop",
            "traffic_source": "paid_search",
            "country_code": "US",
            "schema_version": 1,
        },
    ]

    report = collect_data_quality_metrics(payloads)

    assert report.total_events == 2
    assert report.valid_events == 1
    assert report.invalid_events == 1
    assert report.duplicate_event_ids == 1
    assert report.invalid_event_types == 1

