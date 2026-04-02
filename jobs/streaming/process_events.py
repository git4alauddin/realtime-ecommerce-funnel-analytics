from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from de_pipeline.config import load_settings
from de_pipeline.logging import configure_logging, get_logger
from de_pipeline.schemas.event import EVENT_TYPES
from de_pipeline.utils import build_spark_session, ensure_runtime_directories

LOGGER = get_logger(__name__)


def event_struct_type() -> T.StructType:
    return T.StructType(
        [
            T.StructField("event_id", T.StringType(), False),
            T.StructField("user_id", T.StringType(), False),
            T.StructField("session_id", T.StringType(), False),
            T.StructField("event_type", T.StringType(), False),
            T.StructField("event_time", T.TimestampType(), False),
            T.StructField("ingest_time", T.TimestampType(), False),
            T.StructField("product_id", T.StringType(), False),
            T.StructField("order_id", T.StringType(), True),
            T.StructField("order_amount", T.DoubleType(), True),
            T.StructField("device_type", T.StringType(), False),
            T.StructField("traffic_source", T.StringType(), False),
            T.StructField("country_code", T.StringType(), False),
            T.StructField("schema_version", T.IntegerType(), False),
        ]
    )


def build_bronze_frame(kafka_df: DataFrame) -> DataFrame:
    return (
        kafka_df.selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS raw_payload",
            "topic",
            "partition",
            "offset",
            "timestamp AS kafka_timestamp",
        )
        .withColumn("stream_ingest_time", F.current_timestamp())
        .withColumn("event_date", F.to_date("kafka_timestamp"))
    )


def build_parsed_frame(bronze_df: DataFrame) -> DataFrame:
    parsed = bronze_df.withColumn("payload", F.from_json(F.col("raw_payload"), event_struct_type()))
    flattened = parsed.select(
        "raw_payload",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "stream_ingest_time",
        F.col("payload.*"),
    )

    required_fields = [
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
    ]

    error_checks = [F.when(F.col("event_id").isNull(), F.lit("malformed_json"))]
    error_checks.extend(
        F.when(F.col(field_name).isNull(), F.lit(f"{field_name}_missing"))
        for field_name in required_fields
    )
    error_checks.append(
        F.when(~F.col("event_type").isin(*sorted(EVENT_TYPES)), F.lit("invalid_event_type"))
    )
    error_checks.append(
        F.when(
            (F.col("event_type") == F.lit("purchase"))
            & (
                F.col("order_id").isNull()
                | (F.col("order_amount").isNull())
                | (F.col("order_amount") <= 0)
            ),
            F.lit("purchase_missing_order_fields"),
        )
    )

    return (
        flattened.withColumn(
            "validation_errors",
            F.array_remove(F.array(*error_checks), F.lit(None)),
        )
        .withColumn("event_date", F.coalesce(F.to_date("event_time"), F.to_date("kafka_timestamp")))
    )


def split_valid_invalid(
    parsed_df: DataFrame,
    watermark_minutes: int,
) -> tuple[DataFrame, DataFrame]:
    invalid = parsed_df.filter(F.size("validation_errors") > 0)
    valid = parsed_df.filter(F.size("validation_errors") == 0).drop(
        "raw_payload",
        "validation_errors",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
    )
    if parsed_df.isStreaming:
        valid = valid.withWatermark("event_time", f"{watermark_minutes} minutes")
    valid = valid.dropDuplicates(["event_id"])
    return valid, invalid


def main() -> None:
    configure_logging()
    settings = load_settings()
    ensure_runtime_directories(settings)
    spark = build_spark_session("stream_user_events_to_lake", settings)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", settings.kafka.topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    bronze_df = build_bronze_frame(kafka_df)
    parsed_df = build_parsed_frame(bronze_df)
    silver_df, quarantine_df = split_valid_invalid(parsed_df, settings.spark.watermark_minutes)

    bronze_query = (
        bronze_df.writeStream.format("parquet")
        .option("path", str(settings.storage.bronze_path))
        .option(
            "checkpointLocation",
            str(settings.storage.checkpoint_root / "streaming" / "bronze_user_events"),
        )
        .partitionBy("event_date")
        .outputMode("append")
        .queryName("write_bronze_user_events")
        .start()
    )

    silver_query = (
        silver_df.writeStream.format("parquet")
        .option("path", str(settings.storage.silver_path))
        .option(
            "checkpointLocation",
            str(settings.storage.checkpoint_root / "streaming" / "silver_user_events"),
        )
        .partitionBy("event_date")
        .outputMode("append")
        .queryName("write_silver_user_events")
        .start()
    )

    quarantine_query = (
        quarantine_df.writeStream.format("parquet")
        .option("path", str(settings.storage.quarantine_path))
        .option(
            "checkpointLocation",
            str(settings.storage.checkpoint_root / "streaming" / "quarantine_user_events"),
        )
        .partitionBy("event_date")
        .outputMode("append")
        .queryName("write_quarantine_user_events")
        .start()
    )

    LOGGER.info(
        "streaming queries started",
        extra={
            "bronze_path": settings.storage.bronze_path,
            "silver_path": settings.storage.silver_path,
            "quarantine_path": settings.storage.quarantine_path,
        },
    )

    for query in (bronze_query, silver_query, quarantine_query):
        query.awaitTermination()


if __name__ == "__main__":
    main()
