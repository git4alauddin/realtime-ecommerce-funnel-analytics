SELECT
    'no_duplicate_event_ids' AS check_name,
    NOT EXISTS (
        SELECT event_id
        FROM staging.user_events_silver
        GROUP BY event_id
        HAVING COUNT(*) > 1
    ) AS passed,
    'staging.user_events_silver must not contain duplicate event_id values' AS details;

