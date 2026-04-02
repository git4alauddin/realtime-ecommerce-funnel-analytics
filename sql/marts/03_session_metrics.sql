SELECT
    event_date,
    session_id,
    user_id,
    event_count,
    purchase_count,
    total_revenue,
    session_duration_seconds
FROM analytics.session_metrics
ORDER BY event_date DESC, session_duration_seconds DESC;

