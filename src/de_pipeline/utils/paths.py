from __future__ import annotations

from pathlib import Path

from de_pipeline.config.settings import Settings


def ensure_runtime_directories(settings: Settings) -> None:
    directories: tuple[Path, ...] = (
        settings.storage.root,
        settings.storage.data_lake_root,
        settings.storage.bronze_path,
        settings.storage.silver_path,
        settings.storage.gold_path,
        settings.storage.quarantine_path,
        settings.storage.checkpoint_root,
    )
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)

