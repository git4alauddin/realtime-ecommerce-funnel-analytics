CREATE TABLE IF NOT EXISTS analytics.daily_kpis (
    event_date DATE PRIMARY KEY,
    daily_active_users INTEGER NOT NULL,
    total_events INTEGER NOT NULL,
    total_sessions INTEGER NOT NULL,
    total_orders INTEGER NOT NULL,
    total_revenue NUMERIC(12, 2) NOT NULL,
    purchase_users INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.conversion_funnel (
    event_date DATE PRIMARY KEY,
    home_view_users INTEGER NOT NULL,
    product_view_users INTEGER NOT NULL,
    add_to_cart_users INTEGER NOT NULL,
    checkout_start_users INTEGER NOT NULL,
    purchase_users INTEGER NOT NULL,
    product_view_to_purchase_rate NUMERIC(10, 4) NOT NULL,
    add_to_cart_to_purchase_rate NUMERIC(10, 4) NOT NULL,
    checkout_to_purchase_rate NUMERIC(10, 4) NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.session_metrics (
    event_date DATE NOT NULL,
    session_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_start_time TIMESTAMPTZ NOT NULL,
    session_end_time TIMESTAMPTZ NOT NULL,
    event_count INTEGER NOT NULL,
    purchase_count INTEGER NOT NULL,
    total_revenue NUMERIC(12, 2) NOT NULL,
    session_duration_seconds INTEGER NOT NULL,
    PRIMARY KEY (event_date, session_id)
);

CREATE TABLE IF NOT EXISTS analytics.product_daily_metrics (
    event_date DATE NOT NULL,
    product_id TEXT NOT NULL,
    category TEXT,
    price_band TEXT,
    views INTEGER NOT NULL,
    add_to_carts INTEGER NOT NULL,
    purchases INTEGER NOT NULL,
    revenue NUMERIC(12, 2) NOT NULL,
    PRIMARY KEY (event_date, product_id)
);

