from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from jobs.batch.build_daily_analytics import (  # noqa: E402
    build_conversion_funnel_frame,
    build_daily_kpis_frame,
    build_product_daily_metrics_frame,
    build_session_metrics_frame,
)


def test_batch_aggregation_frames_produce_expected_counts():
    spark = SparkSession.builder.master("local[1]").appName(
        "test_batch_aggregations"
    ).getOrCreate()
    try:
        silver_df = spark.createDataFrame(
            [
                (
                    "evt-1",
                    "user-1",
                    "session-1",
                    "product_view",
                    "2025-01-01 10:00:00",
                    "sku-100",
                    None,
                    None,
                    "mobile",
                    "organic",
                    "US",
                    1,
                    "2025-01-01",
                ),
                (
                    "evt-2",
                    "user-1",
                    "session-1",
                    "add_to_cart",
                    "2025-01-01 10:01:00",
                    "sku-100",
                    None,
                    None,
                    "mobile",
                    "organic",
                    "US",
                    1,
                    "2025-01-01",
                ),
                (
                    "evt-3",
                    "user-1",
                    "session-1",
                    "purchase",
                    "2025-01-01 10:02:00",
                    "sku-100",
                    "order-1",
                    19.0,
                    "mobile",
                    "organic",
                    "US",
                    1,
                    "2025-01-01",
                ),
            ],
            [
                "event_id",
                "user_id",
                "session_id",
                "event_type",
                "event_time",
                "product_id",
                "order_id",
                "order_amount",
                "device_type",
                "traffic_source",
                "country_code",
                "schema_version",
                "event_date",
            ],
        )
        silver_df = silver_df.withColumn(
            "event_time",
            pyspark.sql.functions.to_timestamp("event_time"),
        )
        silver_df = silver_df.withColumn("event_date", pyspark.sql.functions.to_date("event_date"))
        products_df = spark.createDataFrame(
            [("sku-100", "accessories", "low")],
            ["product_id", "category", "price_band"],
        )

        daily = build_daily_kpis_frame(silver_df).collect()[0]
        funnel = build_conversion_funnel_frame(silver_df).collect()[0]
        session = build_session_metrics_frame(silver_df).collect()[0]
        product = build_product_daily_metrics_frame(silver_df, products_df).collect()[0]

        assert daily.total_orders == 1
        assert float(daily.total_revenue) == 19.0
        assert funnel.purchase_users == 1
        assert session.purchase_count == 1
        assert product.purchases == 1
    finally:
        spark.stop()
