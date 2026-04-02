from __future__ import annotations

from apps.event_generator.main import EventFactory


def test_event_factory_is_deterministic_for_same_seed() -> None:
    first = EventFactory(seed=7).generate_events(5)
    second = EventFactory(seed=7).generate_events(5)

    assert [event.event_id for event in first] == [event.event_id for event in second]
    assert [event.event_type for event in first] == [event.event_type for event in second]


def test_event_factory_emits_purchase_with_order_fields_when_present() -> None:
    events = EventFactory(seed=3).generate_events(25)
    purchase_events = [event for event in events if event.event_type == "purchase"]

    assert purchase_events
    assert all(event.order_id for event in purchase_events)
    assert all((event.order_amount or 0) > 0 for event in purchase_events)

