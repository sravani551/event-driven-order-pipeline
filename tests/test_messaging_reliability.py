"""Outbox relay and Kafka consumer retry/DLQ behaviour, using in-memory fakes."""

import pytest
from sqlalchemy import select

from app.db import OutboxEvent
from app.kafka_utils import PoisonMessage, process_with_retries
from app.outbox_relay import PublishError, relay_batch


class FakeProducer:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.sent: list[tuple] = []

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self.sent.append((topic, key, value, headers))
        if on_delivery:
            on_delivery("broker unavailable" if self.fail else None, None)

    def flush(self, _timeout=None):
        return 0


class FakeMessage:
    def __init__(self, value=b"{}"):
        self._value = value

    def value(self):
        return self._value

    def key(self):
        return b"k"

    def headers(self):
        return []

    def topic(self):
        return "orders"

    def partition(self):
        return 0

    def offset(self):
        return 42


def seed_outbox(Session, n=3):
    with Session() as s:
        for i in range(n):
            s.add(OutboxEvent(topic="orders", key=f"order-{i}", event_type="OrderCreated", payload="{}"))
        s.commit()


def test_relay_publishes_in_order_and_marks_rows(Session):
    seed_outbox(Session)
    producer = FakeProducer()
    with Session() as s:
        assert relay_batch(s, producer) == 3
    assert [k for _, k, _, _ in producer.sent] == [b"order-0", b"order-1", b"order-2"]
    with Session() as s:
        assert all(r.published_at is not None for r in s.scalars(select(OutboxEvent)))
        assert relay_batch(s, producer) == 0  # nothing left


def test_relay_leaves_rows_unpublished_when_broker_fails(Session):
    seed_outbox(Session)
    with Session() as s, pytest.raises(PublishError):
        relay_batch(s, FakeProducer(fail=True))
    with Session() as s:
        assert all(r.published_at is None for r in s.scalars(select(OutboxEvent)))


def test_transient_failure_retried_in_place_then_succeeds():
    calls, sleeps = [], []

    def handle(msg):
        calls.append(msg)
        if len(calls) < 3:
            raise ConnectionError("db blip")

    dlq = FakeProducer()
    process_with_retries(FakeMessage(), handle, dlq, max_attempts=4, sleep=sleeps.append)
    assert len(calls) == 3
    assert sleeps == [0.5, 1.0]
    assert dlq.sent == []


def test_poison_message_goes_straight_to_dlq_with_diagnostics():
    def handle(_msg):
        raise PoisonMessage("bad schema")

    dlq = FakeProducer()
    process_with_retries(FakeMessage(b"garbage"), handle, dlq, max_attempts=4, sleep=lambda _: None)
    [(topic, _key, value, headers)] = dlq.sent
    assert topic == "orders.dlq" and value == b"garbage"
    h = dict(headers)
    assert h["dlq.error"] == b"bad schema"
    assert h["dlq.source.offset"] == b"42"


def test_exhausted_retries_dead_letter():
    def handle(_msg):
        raise TimeoutError("still down")

    dlq = FakeProducer()
    process_with_retries(FakeMessage(), handle, dlq, max_attempts=3, sleep=lambda _: None)
    assert len(dlq.sent) == 1
