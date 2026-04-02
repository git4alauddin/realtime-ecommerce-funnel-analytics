SELECT
    event_date,
    daily_active_users,
    total_events,
    total_sessions,
    total_orders,
    total_revenue,
    purchase_users
FROM analytics.daily_kpis
ORDER BY event_date DESC;

