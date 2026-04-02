from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from jobs.streaming.process_events import (  # noqa: E402
    build_bronze_frame,
    build_parsed_frame,
    split_valid_invalid,
)


def test_streaming_transform_routes_invalid_events_to_quarantine():
    spark = SparkSession.builder.master("local[1]").appName(
        "test_streaming_transform"
    ).getOrCreate()
    try:
        kafka_like = spark.createDataFrame(
            [
                (
                    "evt-1",
                    '{"event_id":"evt-1","user_id":"user-1","session_id":"session-1","event_type":"product_view","event_time":"2025-01-01T10:00:00Z","ingest_time":"2025-01-01T10:00:01Z","product_id":"sku-100","order_id":null,"order_amount":null,"device_type":"mobile","traffic_source":"organic","country_code":"US","schema_version":1}',
                    "user_events_v1",
                    0,
                    10,
                    "2025-01-01T10:00:01Z",
                ),
                (
                    "evt-2",
                    '{"event_id":"evt-2","user_id":"user-1","session_id":"session-1","event_type":"signup","event_time":"2025-01-01T10:01:00Z","ingest_time":"2025-01-01T10:01:01Z","product_id":"sku-100","order_id":null,"order_amount":null,"device_type":"mobile","traffic_source":"organic","country_code":"US","schema_version":1}',
                    "user_events_v1",
                    0,
                    11,
                    "2025-01-01T10:01:01Z",
                ),
            ],
            ["key", "value", "topic", "partition", "offset", "timestamp"],
        )
        bronze_df = build_bronze_frame(kafka_like)
        parsed_df = build_parsed_frame(bronze_df)
        valid_df, invalid_df = split_valid_invalid(parsed_df, 15)

        assert valid_df.count() == 1
        assert invalid_df.count() == 1
    finally:
        spark.stop()
