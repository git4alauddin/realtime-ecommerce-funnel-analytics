from __future__ import annotations

import argparse
import json
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from random import Random
from uuid import NAMESPACE_URL, uuid5

from de_pipeline.config import load_settings
from de_pipeline.logging import configure_logging, get_logger
from de_pipeline.schemas.event import EventRecord, event_to_payload
from de_pipeline.utils.paths import ensure_runtime_directories

LOGGER = get_logger(__name__)

PRODUCTS = (
    {"product_id": "sku-100", "category": "accessories", "price_band": "low", "price": 19.0},
    {"product_id": "sku-101", "category": "accessories", "price_band": "low", "price": 24.0},
    {"product_id": "sku-200", "category": "apparel", "price_band": "medium", "price": 59.0},
    {"product_id": "sku-201", "category": "apparel", "price_band": "medium", "price": 89.0},
    {"product_id": "sku-300", "category": "electronics", "price_band": "high", "price": 249.0},
    {"product_id": "sku-301", "category": "electronics", "price_band": "high", "price": 499.0},
)
DEVICE_TYPES = ("mobile", "desktop", "tablet")
TRAFFIC_SOURCES = ("organic", "paid_search", "email", "social")
COUNTRY_CODES = ("US", "IN", "GB", "DE", "SG")


@dataclass(frozen=True)
class GeneratorEvent:
    event_type: str
    offset_seconds: int


class EventFactory:
    def __init__(self, seed: int) -> None:
        self.seed = seed
        self.random = Random(seed)
        self.counter = 0

    def generate_events(self, count: int, start_time: datetime | None = None) -> list[EventRecord]:
        now = start_time or datetime.now(tz=UTC)
        emitted: list[EventRecord] = []

        while len(emitted) < count:
            emitted.extend(self._build_session(now + timedelta(seconds=len(emitted) * 3)))

        return emitted[:count]

    def _build_session(self, base_time: datetime) -> list[EventRecord]:
        user_id = f"user-{self.random.randint(1000, 9999)}"
        session_id = f"session-{self.random.randint(100000, 999999)}"
        product = self.random.choice(PRODUCTS)
        device_type = self.random.choice(DEVICE_TYPES)
        traffic_source = self.random.choice(TRAFFIC_SOURCES)
        country_code = self.random.choice(COUNTRY_CODES)

        flow = [
            GeneratorEvent("home_view", 0),
            GeneratorEvent("product_view", 5),
        ]
        if self.random.random() < 0.7:
            flow.append(GeneratorEvent("add_to_cart", 12))
        if self.random.random() < 0.5:
            flow.append(GeneratorEvent("checkout_start", 18))
        if self.random.random() < 0.35:
            flow.append(GeneratorEvent("purchase", 26))

        events: list[EventRecord] = []
        order_id = None
        order_amount = None
        if any(item.event_type == "purchase" for item in flow):
            order_id = f"order-{self.random.randint(1000000, 9999999)}"
            order_amount = round(float(product["price"]) * self.random.uniform(0.95, 1.05), 2)

        for item in flow:
            event_time = base_time + timedelta(seconds=item.offset_seconds)
            self.counter += 1
            event_key = f"{self.seed}:{self.counter}:{session_id}:{item.event_type}"
            event_id = str(uuid5(NAMESPACE_URL, event_key))
            events.append(
                EventRecord(
                    event_id=event_id,
                    user_id=user_id,
                    session_id=session_id,
                    event_type=item.event_type,
                    event_time=event_time,
                    ingest_time=event_time + timedelta(seconds=1),
                    product_id=str(product["product_id"]),
                    order_id=order_id if item.event_type == "purchase" else None,
                    order_amount=order_amount if item.event_type == "purchase" else None,
                    device_type=device_type,
                    traffic_source=traffic_source,
                    country_code=country_code,
                    schema_version=1,
                )
            )

        return events


def _build_producer(bootstrap_servers: str):
    try:
        from kafka import KafkaProducer
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "kafka-python is not installed. Install the kafka extra before publishing to Kafka."
        ) from exc

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def publish_events(topic: str, events: Iterable[EventRecord], bootstrap_servers: str) -> int:
    producer = _build_producer(bootstrap_servers)
    sent = 0
    for event in events:
        payload = event_to_payload(event)
        producer.send(topic=topic, key=event.event_id, value=payload)
        sent += 1
    producer.flush()
    producer.close()
    return sent


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate e-commerce funnel events.")
    parser.add_argument("--count", type=int, default=25, help="Total number of events to emit.")
    parser.add_argument("--batch-size", type=int, default=None, help="Events per interval.")
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=None,
        help="Pause between batches.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for deterministic output.",
    )
    parser.add_argument("--topic", default=None, help="Kafka topic override.")
    parser.add_argument(
        "--bootstrap-servers",
        default=None,
        help="Kafka bootstrap servers override.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print JSON lines instead of Kafka publish.",
    )
    parser.add_argument("--continuous", action="store_true", help="Run until interrupted.")
    return parser.parse_args()


def main() -> None:
    configure_logging()
    settings = load_settings()
    ensure_runtime_directories(settings)
    args = _parse_args()

    seed = settings.generator.seed if args.seed is None else args.seed
    batch_size = settings.generator.batch_size if args.batch_size is None else args.batch_size
    interval_seconds = (
        settings.generator.interval_seconds
        if args.interval_seconds is None
        else args.interval_seconds
    )
    topic = settings.kafka.topic if args.topic is None else args.topic
    bootstrap_servers = (
        settings.kafka.bootstrap_servers
        if args.bootstrap_servers is None
        else args.bootstrap_servers
    )

    factory = EventFactory(seed=seed)
    remaining = args.count
    total_sent = 0

    while args.continuous or remaining > 0:
        target = batch_size if args.continuous else min(batch_size, remaining)
        events = factory.generate_events(target)

        if args.dry_run:
            for event in events:
                print(json.dumps(event_to_payload(event)))
            sent = len(events)
        else:
            sent = publish_events(topic=topic, events=events, bootstrap_servers=bootstrap_servers)

        total_sent += sent
        LOGGER.info(
            "emitted event batch",
            extra={
                "sent": sent,
                "total_sent": total_sent,
                "dry_run": args.dry_run,
                "topic": topic,
            },
        )

        if not args.continuous:
            remaining -= sent
            if remaining <= 0:
                break

        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
