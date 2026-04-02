UV ?= uv

.PHONY: setup setup-jobs lint test integration up down generator-dry-run streaming-job batch-job quality-checks

setup:
	$(UV) sync --extra dev

setup-jobs:
	$(UV) sync --extra dev --extra kafka --extra spark --extra postgres

lint:
	$(UV) run --extra dev ruff check .

test:
	$(UV) run --extra dev pytest -m "not integration"

integration:
	$(UV) run --extra dev pytest -m integration

up:
	docker compose up -d --build

down:
	docker compose down -v

generator-dry-run:
	$(UV) run python -m apps.event_generator.main --dry-run --count 10 --seed 7

streaming-job:
	$(UV) run --extra spark python -m jobs.streaming.process_events

batch-job:
	$(UV) run --extra spark --extra postgres python -m jobs.batch.build_daily_analytics

quality-checks:
	$(UV) run --extra postgres python -m jobs.batch.run_quality_checks

