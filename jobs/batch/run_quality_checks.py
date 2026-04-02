from __future__ import annotations

from pathlib import Path

from de_pipeline.config import load_settings
from de_pipeline.logging import configure_logging, get_logger

LOGGER = get_logger(__name__)


def _load_sql_files(sql_root: Path) -> list[Path]:
    return sorted(path for path in sql_root.glob("*.sql") if path.is_file())


def main() -> None:
    configure_logging()
    settings = load_settings()
    sql_root = settings.project_root / "sql" / "checks"
    sql_files = _load_sql_files(sql_root)

    try:
        import psycopg
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg is not installed. Install the postgres extra before running quality checks."
        ) from exc

    failures: list[str] = []
    with psycopg.connect(settings.warehouse.psycopg_dsn) as conn:
        with conn.cursor() as cursor:
            for sql_file in sql_files:
                query = sql_file.read_text(encoding="utf-8")
                cursor.execute(query)
                check_name, passed, details = cursor.fetchone()
                LOGGER.info(
                    "quality check executed",
                    extra={"check_name": check_name, "passed": passed, "details": details},
                )
                if not passed:
                    failures.append(f"{check_name}: {details}")

    if failures:
        raise RuntimeError("quality checks failed: " + "; ".join(failures))


if __name__ == "__main__":
    main()

