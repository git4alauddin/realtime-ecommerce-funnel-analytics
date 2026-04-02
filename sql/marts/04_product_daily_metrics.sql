SELECT
    event_date,
    product_id,
    category,
    price_band,
    views,
    add_to_carts,
    purchases,
    revenue
FROM analytics.product_daily_metrics
ORDER BY event_date DESC, revenue DESC;

