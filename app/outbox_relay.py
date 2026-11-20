"""Outbox relay: moves committed events from the `outbox` table to Kafka.

* `SELECT ... FOR UPDATE SKIP LOCKED` lets several relay replicas run without
  publishing the same rows at the same time.
* Rows are marked published only after Kafka acknowledged every message (acks=all).
  A crash in between means the batch is published again -> at-least-once, which the
  idempotent consumers downstream absorb.
* Messages are keyed by order_id, so all events of one order land on the same
  partition and are consumed in order.
"""

import time
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import OutboxEvent, get_sessionmaker
from app.logging_setup import setup_logging, stop_event_on_signals

log = setup_logging("outbox-relay")


class PublishError(RuntimeError):
    pass


def relay_batch(session: Session, producer, batch_size: int = 200) -> int:
    rows = session.scalars(
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
        .order_by(OutboxEvent.id)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    ).all()
    if not rows:
        session.rollback()
        return 0

    errors: list[str] = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(str(err))

    for row in rows:
        producer.produce(
            row.topic,
            key=row.key.encode(),
            value=row.payload.encode(),
            headers=[("event_type", row.event_type.encode())],
            on_delivery=on_delivery,
        )
    remaining = producer.flush(15)
    if remaining or errors:
        session.rollback()  # leave rows unpublished; they'll be retried
        raise PublishError(f"{remaining} undelivered, errors={errors[:3]}")

    now = datetime.now(UTC)
    for row in rows:
        row.published_at = now
    session.commit()
    return len(rows)


def main() -> None:
    from app.kafka_utils import make_producer

    stop = stop_event_on_signals()
    producer = make_producer()
    Session = get_sessionmaker()
    log.info("Outbox relay started")
    while not stop.is_set():
        try:
            with Session() as session:
                n = relay_batch(session, producer)
            if n:
                log.info("Published %d event(s)", n)
            else:
                time.sleep(0.25)
        except Exception:
            log.exception("Relay iteration failed; backing off")
            time.sleep(2)


if __name__ == "__main__":
    main()
