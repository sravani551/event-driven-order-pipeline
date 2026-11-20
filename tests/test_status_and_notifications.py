import uuid

import pytest

from app.db import Order
from app.events import EmailTask, StockRejected, StockReserved
from app.notification_worker import EmailProviderError, process_task
from app.status_updater import UnknownOrder, apply_inventory_event


@pytest.fixture
def order_id(Session):
    oid = uuid.uuid4()
    with Session() as s:
        s.add(Order(id=oid, customer_email="jo@example.com", items=[], status="PENDING"))
        s.commit()
    return oid


def apply(Session, event):
    with Session() as s:
        task = apply_inventory_event(s, event)
        s.commit()
        return task


def test_reserved_confirms_order_and_creates_email_task(Session, order_id):
    event = StockReserved(order_id=order_id)
    task = apply(Session, event)
    assert task.template == "order_confirmed" and task.to == "jo@example.com"
    with Session() as s:
        assert s.get(Order, order_id).status == "CONFIRMED"


def test_rejected_sets_reason(Session, order_id):
    task = apply(Session, StockRejected(order_id=order_id, reason="out of monitors"))
    assert task.template == "order_rejected"
    with Session() as s:
        o = s.get(Order, order_id)
        assert (o.status, o.status_reason) == ("REJECTED", "out of monitors")


def test_email_task_id_is_deterministic_and_duplicates_skipped(Session, order_id):
    event = StockReserved(order_id=order_id)
    # Crash before DB commit -> redelivery must produce the SAME task id.
    with Session() as s:
        first = apply_inventory_event(s, event)
        s.rollback()
    second = apply(Session, event)
    assert first.task_id == second.task_id
    assert apply(Session, event) is None  # already committed -> duplicate


def test_unknown_order_raises(Session):
    with pytest.raises(UnknownOrder):
        apply(Session, StockReserved(order_id=uuid.uuid4()))


def _task() -> EmailTask:
    return EmailTask(
        task_id=uuid.uuid4(), order_id=uuid.uuid4(), to="jo@example.com", template="order_confirmed"
    )


def test_notification_sent_once_even_if_redelivered(Session):
    sent = []
    task = _task()
    assert process_task(Session, task, sender=sent.append) == "sent"
    assert process_task(Session, task, sender=sent.append) == "duplicate"
    assert sent == [task]


def test_failed_send_is_not_marked_processed_so_retry_can_succeed(Session):
    task = _task()

    def flaky(_t):
        raise EmailProviderError("503")

    with pytest.raises(EmailProviderError):
        process_task(Session, task, sender=flaky)
    sent = []
    assert process_task(Session, task, sender=sent.append) == "sent"
    assert sent == [task]
