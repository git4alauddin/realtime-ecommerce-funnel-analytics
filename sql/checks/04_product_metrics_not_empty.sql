SELECT
    'product_metrics_not_empty' AS check_name,
    EXISTS (SELECT 1 FROM analytics.product_daily_metrics) AS passed,
    'analytics.product_daily_metrics must contain at least one row' AS details;

