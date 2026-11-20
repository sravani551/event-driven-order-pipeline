"""RabbitMQ topology and a publisher with publisher confirms.

Topology (all durable):

  notifications (direct) --"email"--> notifications.email (quorum queue)  <-- worker consumes
        ^                                    | rejected / unparseable
        |                                    v
        |                         notifications.dlx (fanout) --> notifications.email.dlq
        |
  notifications.retry (direct)
        |--"delay.5000"--> notifications.email.retry.5000ms   (TTL 5s,  dead-letters back to main)
        |--"delay.30000"-> notifications.email.retry.30000ms  (TTL 30s, dead-letters back to main)
        '--"delay.120000"> notifications.email.retry.120000ms (TTL 2m,  dead-letters back to main)

One queue per delay (a queue-level TTL) avoids the head-of-line blocking you get with
per-message TTLs, where a long-delay message at the head holds back short-delay ones.
"""

import logging

import pika
from pika.adapters.blocking_connection import BlockingChannel

from app.config import settings
from app.events import EmailTask

log = logging.getLogger(__name__)

EXCHANGE = "notifications"
RETRY_EXCHANGE = "notifications.retry"
DLX = "notifications.dlx"
EMAIL_QUEUE = "notifications.email"
EMAIL_ROUTING_KEY = "email"
DLQ = "notifications.email.dlq"
ATTEMPT_HEADER = "x-attempt"


def retry_queue_name(delay_ms: int) -> str:
    return f"{EMAIL_QUEUE}.retry.{delay_ms}ms"


def connect() -> pika.BlockingConnection:
    params = pika.URLParameters(settings.rabbitmq_url)
    params.heartbeat = 30
    params.connection_attempts = 5
    params.retry_delay = 2
    return pika.BlockingConnection(params)


def declare_topology(ch: BlockingChannel, delays_ms=None) -> None:
    delays_ms = delays_ms or settings.retry_delays_ms
    ch.exchange_declare(EXCHANGE, "direct", durable=True)
    ch.exchange_declare(RETRY_EXCHANGE, "direct", durable=True)
    ch.exchange_declare(DLX, "fanout", durable=True)

    ch.queue_declare(DLQ, durable=True, arguments={"x-queue-type": "quorum"})
    ch.queue_bind(DLQ, DLX)

    ch.queue_declare(
        EMAIL_QUEUE,
        durable=True,
        arguments={"x-queue-type": "quorum", "x-dead-letter-exchange": DLX},
    )
    ch.queue_bind(EMAIL_QUEUE, EXCHANGE, routing_key=EMAIL_ROUTING_KEY)

    for delay in delays_ms:
        q = retry_queue_name(delay)
        ch.queue_declare(
            q,
            durable=True,
            arguments={
                "x-message-ttl": delay,
                "x-dead-letter-exchange": EXCHANGE,
                "x-dead-letter-routing-key": EMAIL_ROUTING_KEY,
            },
        )
        ch.queue_bind(q, RETRY_EXCHANGE, routing_key=f"delay.{delay}")
    log.info("RabbitMQ topology declared (retry tiers: %s ms)", list(delays_ms))


def persistent_props(task_id: str, attempt: int = 0, extra_headers: dict | None = None):
    headers = {ATTEMPT_HEADER: attempt, **(extra_headers or {})}
    return pika.BasicProperties(
        content_type="application/json",
        delivery_mode=pika.DeliveryMode.Persistent,
        message_id=task_id,
        headers=headers,
    )


class ConfirmedPublisher:
    """Publishes with publisher confirms + mandatory routing, reconnecting once on failure."""

    def __init__(self) -> None:
        self._conn: pika.BlockingConnection | None = None
        self._ch: BlockingChannel | None = None

    def _channel(self) -> BlockingChannel:
        if self._conn is None or self._conn.is_closed or self._ch is None or self._ch.is_closed:
            self._conn = connect()
            self._ch = self._conn.channel()
            self._ch.confirm_delivery()
        return self._ch

    def publish_email(self, task: EmailTask) -> None:
        body = task.model_dump_json().encode()
        props = persistent_props(str(task.task_id))
        for attempt in (1, 2):
            try:
                # Raises UnroutableError / NackError if the broker didn't take responsibility.
                self._channel().basic_publish(EXCHANGE, EMAIL_ROUTING_KEY, body, props, mandatory=True)
                return
            except (
                pika.exceptions.AMQPConnectionError,
                pika.exceptions.ChannelClosed,
                pika.exceptions.StreamLostError,
            ):
                self._conn = None
                if attempt == 2:
                    raise

    def keepalive(self) -> None:
        """Service heartbeats while the owning loop is busy doing something else."""
        if self._conn is not None and self._conn.is_open:
            try:
                self._conn.process_data_events(time_limit=0)
            except pika.exceptions.AMQPError:
                self._conn = None
