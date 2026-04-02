# Architecture

## Overview

The repository is organized as a data engineering mono-repo with one shared Python package
and thin entrypoints for event generation, streaming ingestion, batch analytics, and
orchestration.

```text
apps/event_generator
        |
        v
Kafka topic: user_events_v1
        |
        v
jobs/streaming/process_events.py
        |
        +--> Bronze Parquet: raw Kafka payloads
        +--> Silver Parquet: typed and deduplicated events
        +--> Quarantine Parquet: invalid records
        |
        v
jobs/batch/build_daily_analytics.py
        |
        +--> Gold Parquet analytics outputs
        +--> PostgreSQL staging and analytics schemas
        |
        v
airflow/dags/build_daily_analytics_marts.py
```

## Layer Responsibilities

- Bronze: append-only copy of Kafka payloads with offsets and Kafka timestamps.
- Silver: typed, validated, deduplicated event stream partitioned by `event_date`.
- Gold: batch-built business metrics and analytics marts.
- PostgreSQL: local analytics-serving layer for SQL exploration and quality checks.

## Design Principles

- Shared config, schema, and utilities live under `src/de_pipeline`.
- Spark jobs own transformation logic, not Airflow DAG code.
- Invalid records are quarantined with validation errors.
- Batch rebuilds are authoritative for daily analytics and late-event correction.

