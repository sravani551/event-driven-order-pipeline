"""Kafka producer/consumer factories and a reusable, reliable consumer loop.

Delivery semantics: at-least-once.
  * auto-commit is OFF; an offset is committed only after the handler succeeded
    (or the message was safely dead-lettered).
  * Transient failures are retried in place with backoff, which blocks the partition
    and therefore PRESERVES per-key ordering (important for order events).
  * Poison messages (cannot be parsed / violate the contract) go straight to a DLQ topic
    with diagnostic headers, so one bad message never stalls the partition forever.
"""

import logging
import threading
import time
from collections.abc import Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from app.config import settings
from app.retry import backoff_seconds

log = logging.getLogger(__name__)


class PoisonMessage(Exception):
    """The message can never be processed successfully; do not retry it."""


def make_producer(**overrides) -> Producer:
    conf = {
        "bootstrap.servers": settings.kafka_bootstrap,
        "enable.idempotence": True,  # no duplicates/reordering from producer retries
        "acks": "all",
        "linger.ms": 5,
        "compression.type": "lz4",
    }
    conf.update(overrides)
    return Producer(conf)


def make_consumer(group_id: str, **overrides) -> Consumer:
    conf = {
        "bootstrap.servers": settings.kafka_bootstrap,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",  # incremental rebalancing
    }
    conf.update(overrides)
    return Consumer(conf)


def ensure_topics() -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    topics = [
        NewTopic(settings.orders_topic, settings.topic_partitions, 1),
        NewTopic(settings.inventory_topic, settings.topic_partitions, 1),
        NewTopic(settings.dlq_topic, 1, 1),
    ]
    for name, future in admin.create_topics(topics).items():
        try:
            future.result()
            log.info("Created topic %s", name)
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                log.info("Topic %s already exists", name)
            else:
                raise


def dead_letter(producer: Producer, msg: Message, error: Exception) -> None:
    headers = list(msg.headers() or []) + [
        ("dlq.error", str(error)[:500].encode()),
        ("dlq.source.topic", msg.topic().encode()),
        ("dlq.source.partition", str(msg.partition()).encode()),
        ("dlq.source.offset", str(msg.offset()).encode()),
    ]
    producer.produce(settings.dlq_topic, key=msg.key(), value=msg.value(), headers=headers)
    if producer.flush(10) != 0:
        raise RuntimeError("Could not write to DLQ; refusing to commit offset")
    log.error("Dead-lettered %s[%s]@%s: %s", msg.topic(), msg.partition(), msg.offset(), error)


def run_consumer(
    group_id: str,
    topics: list[str],
    handle: Callable[[Message], None],
    stop: threading.Event,
    on_idle: Callable[[], None] | None = None,
) -> None:
    consumer = make_consumer(group_id)
    dlq_producer = make_producer()
    consumer.subscribe(
        topics,
        on_assign=lambda _c, parts: log.info("Assigned %s", [p.partition for p in parts]),
        on_revoke=lambda _c, parts: log.info("Revoked %s", [p.partition for p in parts]),
    )
    log.info("Consumer group %s subscribed to %s", group_id, topics)
    try:
        while not stop.is_set():
            msg = consumer.poll(1.0)
            if on_idle:
                on_idle()
            if msg is None:
                continue
            if msg.error():
                log.warning("Consumer error: %s", msg.error())
                continue
            process_with_retries(msg, handle, dlq_producer, stop)
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()  # commits nothing extra; leaves the group so partitions rebalance fast
        log.info("Consumer closed")


def process_with_retries(
    msg: Message,
    handle: Callable[[Message], None],
    dlq_producer: Producer,
    stop: threading.Event | None = None,
    max_attempts: int | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    max_attempts = max_attempts or settings.kafka_max_attempts
    for attempt in range(1, max_attempts + 1):
        try:
            handle(msg)
            return
        except PoisonMessage as exc:
            dead_letter(dlq_producer, msg, exc)
            return
        except Exception as exc:  # transient: DB hiccup, broker blip, ...
            if attempt == max_attempts or (stop is not None and stop.is_set()):
                if stop is not None and stop.is_set():
                    raise  # shutting down: don't commit, let the next owner retry it
                dead_letter(dlq_producer, msg, exc)
                return
            delay = backoff_seconds(attempt)
            log.warning("Attempt %d/%d failed (%s); retrying in %.1fs", attempt, max_attempts, exc, delay)
            sleep(delay)
