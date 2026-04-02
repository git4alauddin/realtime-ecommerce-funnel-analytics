SELECT
    event_date,
    home_view_users,
    product_view_users,
    add_to_cart_users,
    checkout_start_users,
    purchase_users,
    product_view_to_purchase_rate,
    add_to_cart_to_purchase_rate,
    checkout_to_purchase_rate
FROM analytics.conversion_funnel
ORDER BY event_date DESC;

