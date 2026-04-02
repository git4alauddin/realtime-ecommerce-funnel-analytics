# Data Contract

## Kafka Topic

- Topic name: `user_events_v1`
- Payload format: JSON
- Delivery semantics: at-least-once

## Event Schema

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `event_id` | string | yes | Global unique identifier for deduplication |
| `user_id` | string | yes | User identifier |
| `session_id` | string | yes | Session identifier |
| `event_type` | string | yes | One of `home_view`, `product_view`, `add_to_cart`, `checkout_start`, `purchase` |
| `event_time` | ISO-8601 timestamp | yes | Business event time |
| `ingest_time` | ISO-8601 timestamp | yes | Ingestion time emitted by generator |
| `product_id` | string | yes | Product dimension key |
| `order_id` | string | purchase only | Required for `purchase` |
| `order_amount` | number | purchase only | Must be positive for `purchase` |
| `device_type` | string | yes | Device category |
| `traffic_source` | string | yes | Acquisition source |
| `country_code` | string | yes | ISO-style country code |
| `schema_version` | integer | yes | Contract version |

## Validation Rules

- Required fields must be present and non-empty.
- `event_type` must be in the allowed enum.
- Timestamps must parse as valid ISO-8601 values.
- `schema_version` must be greater than or equal to `1`.
- Purchase events require both `order_id` and a positive `order_amount`.

