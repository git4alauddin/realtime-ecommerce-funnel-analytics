from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from de_pipeline.config import load_settings
from de_pipeline.logging import configure_logging, get_logger
from de_pipeline.utils import build_spark_session, ensure_runtime_directories

LOGGER = get_logger(__name__)


def read_silver_events(spark, silver_path: Path) -> DataFrame:
    return spark.read.parquet(str(silver_path)).withColumn("event_date", F.to_date("event_time"))


def read_products_dimension(spark, products_path: Path) -> DataFrame:
    return spark.read.option("header", True).csv(str(products_path))


def build_daily_kpis_frame(silver_df: DataFrame) -> DataFrame:
    return silver_df.groupBy("event_date").agg(
        F.countDistinct("user_id").alias("daily_active_users"),
        F.count("*").alias("total_events"),
        F.countDistinct("session_id").alias("total_sessions"),
        F.countDistinct(
            F.when(F.col("event_type") == "purchase", F.col("order_id"))
        ).alias("total_orders"),
        F.round(
            F.sum(
                F.when(F.col("event_type") == "purchase", F.col("order_amount")).otherwise(
                    F.lit(0.0)
                )
            ),
            2,
        ).alias("total_revenue"),
        F.countDistinct(
            F.when(F.col("event_type") == "purchase", F.col("user_id"))
        ).alias("purchase_users"),
    )


def build_conversion_funnel_frame(silver_df: DataFrame) -> DataFrame:
    funnel = silver_df.groupBy("event_date").agg(
        F.countDistinct(F.when(F.col("event_type") == "home_view", F.col("user_id"))).alias(
            "home_view_users"
        ),
        F.countDistinct(
            F.when(F.col("event_type") == "product_view", F.col("user_id"))
        ).alias("product_view_users"),
        F.countDistinct(F.when(F.col("event_type") == "add_to_cart", F.col("user_id"))).alias(
            "add_to_cart_users"
        ),
        F.countDistinct(
            F.when(F.col("event_type") == "checkout_start", F.col("user_id"))
        ).alias("checkout_start_users"),
        F.countDistinct(F.when(F.col("event_type") == "purchase", F.col("user_id"))).alias(
            "purchase_users"
        ),
    )

    return (
        funnel.withColumn(
            "product_view_to_purchase_rate",
            F.when(F.col("product_view_users") == 0, F.lit(0.0)).otherwise(
                F.round(F.col("purchase_users") / F.col("product_view_users"), 4)
            ),
        )
        .withColumn(
            "add_to_cart_to_purchase_rate",
            F.when(F.col("add_to_cart_users") == 0, F.lit(0.0)).otherwise(
                F.round(F.col("purchase_users") / F.col("add_to_cart_users"), 4)
            ),
        )
        .withColumn(
            "checkout_to_purchase_rate",
            F.when(F.col("checkout_start_users") == 0, F.lit(0.0)).otherwise(
                F.round(F.col("purchase_users") / F.col("checkout_start_users"), 4)
            ),
        )
    )


def build_session_metrics_frame(silver_df: DataFrame) -> DataFrame:
    return silver_df.groupBy("event_date", "session_id", "user_id").agg(
        F.min("event_time").alias("session_start_time"),
        F.max("event_time").alias("session_end_time"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("event_type") == "purchase", F.lit(1)).otherwise(F.lit(0))).alias(
            "purchase_count"
        ),
        F.round(F.sum(F.coalesce("order_amount", F.lit(0.0))), 2).alias("total_revenue"),
    ).withColumn(
        "session_duration_seconds",
        F.col("session_end_time").cast("long") - F.col("session_start_time").cast("long"),
    )


def build_product_daily_metrics_frame(silver_df: DataFrame, products_df: DataFrame) -> DataFrame:
    return (
        silver_df.join(products_df, on="product_id", how="left")
        .groupBy("event_date", "product_id", "category", "price_band")
        .agg(
            F.sum(
                F.when(F.col("event_type") == "product_view", F.lit(1)).otherwise(F.lit(0))
            ).alias("views"),
            F.sum(
                F.when(F.col("event_type") == "add_to_cart", F.lit(1)).otherwise(F.lit(0))
            ).alias("add_to_carts"),
            F.sum(
                F.when(F.col("event_type") == "purchase", F.lit(1)).otherwise(F.lit(0))
            ).alias("purchases"),
            F.round(F.sum(F.coalesce("order_amount", F.lit(0.0))), 2).alias("revenue"),
        )
    )


def write_gold_dataset(df: DataFrame, gold_root: Path, name: str) -> None:
    (
        df.write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(str(gold_root / name))
    )


def ensure_database_schemas(settings) -> None:
    try:
        import psycopg
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg is not installed. Install the postgres extra before loading marts."
        ) from exc

    with psycopg.connect(settings.warehouse.psycopg_dsn, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {settings.warehouse.staging_schema}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {settings.warehouse.analytics_schema}")


def write_postgres_table(df: DataFrame, settings, table_name: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", settings.warehouse.jdbc_url)
        .option("dbtable", table_name)
        .option("user", settings.warehouse.user)
        .option("password", settings.warehouse.password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )


def main() -> None:
    configure_logging()
    settings = load_settings()
    ensure_runtime_directories(settings)
    spark = build_spark_session("build_daily_analytics_marts", settings)

    silver_df = read_silver_events(spark, settings.storage.silver_path)
    products_df = read_products_dimension(spark, settings.warehouse.product_seed_path)

    daily_kpis_df = build_daily_kpis_frame(silver_df)
    conversion_funnel_df = build_conversion_funnel_frame(silver_df)
    session_metrics_df = build_session_metrics_frame(silver_df)
    product_daily_metrics_df = build_product_daily_metrics_frame(silver_df, products_df)

    write_gold_dataset(daily_kpis_df, settings.storage.gold_path, "daily_kpis")
    write_gold_dataset(conversion_funnel_df, settings.storage.gold_path, "conversion_funnel")
    write_gold_dataset(session_metrics_df, settings.storage.gold_path, "session_metrics")
    write_gold_dataset(
        product_daily_metrics_df,
        settings.storage.gold_path,
        "product_daily_metrics",
    )

    if settings.warehouse.load_to_postgres:
        ensure_database_schemas(settings)
        write_postgres_table(
            silver_df,
            settings,
            f"{settings.warehouse.staging_schema}.user_events_silver",
        )
        write_postgres_table(
            daily_kpis_df,
            settings,
            f"{settings.warehouse.analytics_schema}.daily_kpis",
        )
        write_postgres_table(
            conversion_funnel_df,
            settings,
            f"{settings.warehouse.analytics_schema}.conversion_funnel",
        )
        write_postgres_table(
            session_metrics_df,
            settings,
            f"{settings.warehouse.analytics_schema}.session_metrics",
        )
        write_postgres_table(
            product_daily_metrics_df,
            settings,
            f"{settings.warehouse.analytics_schema}.product_daily_metrics",
        )

    LOGGER.info(
        "batch analytics build completed",
        extra={
            "silver_path": settings.storage.silver_path,
            "gold_path": settings.storage.gold_path,
            "warehouse_load": settings.warehouse.load_to_postgres,
        },
    )

    spark.stop()


if __name__ == "__main__":
    main()
