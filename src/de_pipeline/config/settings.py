from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _env(key: str, default: str) -> str:
    return os.getenv(key, default).strip()


def _env_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    return default if raw is None else int(raw)


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    return default if raw is None else float(raw)


def _resolve_path(project_root: Path, raw_value: str | None, fallback: Path) -> Path:
    if not raw_value:
        path = fallback
    else:
        candidate = Path(raw_value)
        path = candidate if candidate.is_absolute() else project_root / candidate
    return path.resolve()


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    topic: str
    client_id: str


@dataclass(frozen=True)
class GeneratorSettings:
    interval_seconds: float
    batch_size: int
    seed: int


@dataclass(frozen=True)
class StorageSettings:
    root: Path
    data_lake_root: Path
    bronze_path: Path
    silver_path: Path
    gold_path: Path
    quarantine_path: Path
    checkpoint_root: Path


@dataclass(frozen=True)
class SparkSettings:
    master: str
    packages: str
    watermark_minutes: int


@dataclass(frozen=True)
class WarehouseSettings:
    host: str
    port: int
    database: str
    user: str
    password: str
    staging_schema: str
    analytics_schema: str
    load_to_postgres: bool
    product_seed_path: Path

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def psycopg_dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )


@dataclass(frozen=True)
class AirflowSettings:
    schedule: str


@dataclass(frozen=True)
class Settings:
    project_root: Path
    kafka: KafkaSettings
    generator: GeneratorSettings
    storage: StorageSettings
    spark: SparkSettings
    warehouse: WarehouseSettings
    airflow: AirflowSettings


@lru_cache(maxsize=1)
def load_settings() -> Settings:
    project_root = Path(_env("DE_PIPELINE_PROJECT_ROOT", ".")).resolve()
    storage_root = _resolve_path(
        project_root,
        os.getenv("DE_PIPELINE_STORAGE_ROOT"),
        project_root / "storage",
    )
    data_lake_root = _resolve_path(
        project_root,
        os.getenv("DE_PIPELINE_DATA_LAKE_ROOT"),
        storage_root / "data_lake",
    )
    checkpoint_root = _resolve_path(
        project_root,
        os.getenv("DE_PIPELINE_CHECKPOINT_ROOT"),
        storage_root / "checkpoints",
    )

    storage = StorageSettings(
        root=storage_root,
        data_lake_root=data_lake_root,
        bronze_path=data_lake_root / "bronze" / "user_events",
        silver_path=data_lake_root / "silver" / "user_events",
        gold_path=data_lake_root / "gold",
        quarantine_path=data_lake_root / "quarantine" / "user_events",
        checkpoint_root=checkpoint_root,
    )

    warehouse = WarehouseSettings(
        host=_env("DE_PIPELINE_WAREHOUSE_HOST", "localhost"),
        port=_env_int("DE_PIPELINE_WAREHOUSE_PORT", 5432),
        database=_env("DE_PIPELINE_WAREHOUSE_DATABASE", "analytics"),
        user=_env("DE_PIPELINE_WAREHOUSE_USER", "pipeline"),
        password=_env("DE_PIPELINE_WAREHOUSE_PASSWORD", "pipeline"),
        staging_schema=_env("DE_PIPELINE_WAREHOUSE_STAGING_SCHEMA", "staging"),
        analytics_schema=_env("DE_PIPELINE_WAREHOUSE_ANALYTICS_SCHEMA", "analytics"),
        load_to_postgres=_env_bool("DE_PIPELINE_WAREHOUSE_LOAD_TO_POSTGRES", True),
        product_seed_path=_resolve_path(
            project_root,
            os.getenv("DE_PIPELINE_PRODUCT_SEED_PATH"),
            project_root / "infra" / "postgres" / "products_seed.csv",
        ),
    )

    return Settings(
        project_root=project_root,
        kafka=KafkaSettings(
            bootstrap_servers=_env("DE_PIPELINE_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=_env("DE_PIPELINE_KAFKA_TOPIC", "user_events_v1"),
            client_id=_env(
                "DE_PIPELINE_KAFKA_CLIENT_ID",
                "realtime-ecommerce-funnel-analytics",
            ),
        ),
        generator=GeneratorSettings(
            interval_seconds=_env_float("DE_PIPELINE_GENERATOR_INTERVAL_SECONDS", 1.0),
            batch_size=_env_int("DE_PIPELINE_GENERATOR_BATCH_SIZE", 5),
            seed=_env_int("DE_PIPELINE_GENERATOR_SEED", 42),
        ),
        storage=storage,
        spark=SparkSettings(
            master=_env("DE_PIPELINE_SPARK_MASTER", "local[*]"),
            packages=_env("DE_PIPELINE_SPARK_PACKAGES", "org.postgresql:postgresql:42.7.3"),
            watermark_minutes=_env_int("DE_PIPELINE_WATERMARK_MINUTES", 15),
        ),
        warehouse=warehouse,
        airflow=AirflowSettings(
            schedule=_env("DE_PIPELINE_AIRFLOW_SCHEDULE", "0 2 * * *"),
        ),
    )
