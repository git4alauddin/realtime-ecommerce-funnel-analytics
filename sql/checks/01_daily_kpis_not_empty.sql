SELECT
    'daily_kpis_not_empty' AS check_name,
    EXISTS (SELECT 1 FROM analytics.daily_kpis) AS passed,
    'analytics.daily_kpis must contain at least one row' AS details;

