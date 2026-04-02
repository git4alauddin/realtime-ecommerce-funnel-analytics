# Runbook

## Local Setup

1. Install `uv`, Docker, and Docker Compose.
2. Copy `.env.example` to `.env` and adjust any local paths or credentials.
3. Run `uv sync --extra dev` for developer tooling.
4. Start infrastructure with `docker compose up -d --build`.

## Common Operations

- Dry-run the generator:
  - `uv run python -m apps.event_generator.main --dry-run --count 10 --seed 7`
- Publish real events to Kafka:
  - `uv sync --extra kafka`
  - `uv run python -m apps.event_generator.main --count 100`
- Start the streaming job:
  - `uv sync --extra spark`
  - `uv run python -m jobs.streaming.process_events`
- Build daily marts and warehouse tables:
  - `uv sync --extra spark --extra postgres`
  - `uv run python -m jobs.batch.build_daily_analytics`
- Run SQL quality checks:
  - `uv run python -m jobs.batch.run_quality_checks`

## Failure Handling

- If Kafka is unavailable, the generator will fail fast when creating the producer.
- If Spark checkpoints are corrupt, remove the affected checkpoint path before restarting.
- If PostgreSQL loads fail, verify the JDBC driver package and database connectivity.
- If quality checks fail, inspect the relevant table and rerun the batch load after fixing the input data.

