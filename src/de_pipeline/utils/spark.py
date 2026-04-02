from __future__ import annotations

from de_pipeline.config.settings import Settings


def build_spark_session(app_name: str, settings: Settings):
    try:
        from pyspark.sql import SparkSession
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "pyspark is not installed. Install the spark extra before running Spark jobs."
        ) from exc

    builder = SparkSession.builder.appName(app_name).master(settings.spark.master)
    if settings.spark.packages:
        builder = builder.config("spark.jars.packages", settings.spark.packages)
    return builder.getOrCreate()

