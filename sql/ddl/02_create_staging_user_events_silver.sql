CREATE TABLE IF NOT EXISTS staging.user_events_silver (
    event_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    ingest_time TIMESTAMPTZ NOT NULL,
    product_id TEXT NOT NULL,
    order_id TEXT,
    order_amount NUMERIC(12, 2),
    device_type TEXT NOT NULL,
    traffic_source TEXT NOT NULL,
    country_code TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    event_date DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_user_events_silver_event_date
    ON staging.user_events_silver (event_date);

CREATE INDEX IF NOT EXISTS idx_user_events_silver_event_type
    ON staging.user_events_silver (event_type);

