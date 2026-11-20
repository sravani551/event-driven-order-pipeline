"""Notification worker: competing consumers on a RabbitMQ work queue.

* Manual acks + `prefetch_count` for back-pressure and fair dispatch across replicas.
* Failures are retried via delay queues with escalating TTLs (5s -> 30s -> 2m) and then
  parked in a dead-letter queue for a human to inspect and replay.
* Idempotent: `task_id` is recorded with the "send", so redeliveries never double-send.

The email "provider" is simulated and fails at NOTIFICATION_FAIL_RATE so the retry path
is visible in a demo.
"""

import functools
import random
import threading

from pika.adapters.blocking_connection import BlockingChannel
from pydantic import ValidationError

from app.config import settings
from app.db import get_sessionmaker, mark_processed
from app.events import EmailTask
from app.logging_setup import setup_logging, stop_event_on_signals
from app.rabbit import (
    ATTEMPT_HEADER,
    DLX,
    EMAIL_QUEUE,
    RETRY_EXCHANGE,
    connect,
    declare_topology,
    persistent_props,
)
from app.retry import plan_retry

CONSUMER = "notification-worker"
log = setup_logging(CONSUMER)


class EmailProviderError(Exception):
    pass


def send_email(task: EmailTask, fail_rate: float | None = None) -> None:
    rate = settings.notification_fail_rate if fail_rate is None else fail_rate
    if random.random() < rate:
        raise EmailProviderError("simulated 503 from email provider")
    log.info("EMAIL sent to=%s template=%s order=%s", task.to, task.template, task.order_id)


def process_task(Session, task: EmailTask, sender=send_email) -> str:
    """Send the email once. Returns 'sent' or 'duplicate'. Raises on provider failure."""
    with Session() as session:
        if not mark_processed(session, CONSUMER, task.task_id):
            return "duplicate"
        sender(task)
        session.commit()  # if commit fails after sending, a retry may resend (at-least-once)
    return "sent"


def on_message(ch: BlockingChannel, method, props, body: bytes, Session) -> None:
    headers = props.headers or {}
    failed_attempts = int(headers.get(ATTEMPT_HEADER, 0))

    try:
        task = EmailTask.model_validate_json(body)
    except ValidationError:
        log.error("Unparseable task, dead-lettering")
        ch.basic_nack(method.delivery_tag, requeue=False)  # -> DLX -> DLQ
        return

    try:
        outcome = process_task(Session, task)
        log.info("task=%s %s (attempt %d)", task.task_id, outcome, failed_attempts + 1)
    except Exception as exc:
        decision = plan_retry(failed_attempts, settings.retry_delays_ms)
        new_props = persistent_props(str(task.task_id), failed_attempts + 1, {"x-last-error": str(exc)[:200]})
        if decision.action == "retry":
            ch.basic_publish(RETRY_EXCHANGE, decision.routing_key, body, new_props)
            log.warning("task=%s failed (%s); retry in %dms", task.task_id, exc, decision.delay_ms)
        else:
            ch.basic_publish(DLX, "", body, new_props)
            log.error("task=%s failed %d times; parked in DLQ", task.task_id, failed_attempts + 1)
    # Ack only after the retry/DLQ copy was confirmed by the broker.
    ch.basic_ack(method.delivery_tag)


def main() -> None:
    stop: threading.Event = stop_event_on_signals()
    Session = get_sessionmaker()
    conn = connect()
    ch = conn.channel()
    ch.confirm_delivery()
    declare_topology(ch)
    ch.basic_qos(prefetch_count=settings.notification_prefetch)
    ch.basic_consume(EMAIL_QUEUE, functools.partial(on_message, Session=Session))
    log.info("Consuming %s (prefetch=%d)", EMAIL_QUEUE, settings.notification_prefetch)
    try:
        while not stop.is_set():
            conn.process_data_events(time_limit=1)
    finally:
        if conn.is_open:
            ch.cancel()  # unacked messages are returned to the queue
            conn.close()
        log.info("Worker stopped")


if __name__ == "__main__":
    main()
