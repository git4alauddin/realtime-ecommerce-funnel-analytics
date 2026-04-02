SELECT
    'non_negative_revenue' AS check_name,
    NOT EXISTS (
        SELECT 1
        FROM analytics.daily_kpis
        WHERE total_revenue < 0
    ) AS passed,
    'analytics.daily_kpis.total_revenue must be non-negative' AS details;

