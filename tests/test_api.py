import json

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from app.api import app, get_session
from app.db import Order, OutboxEvent


@pytest.fixture
def client(Session):
    def _session():
        with Session() as s:
            yield s

    app.dependency_overrides[get_session] = _session
    yield TestClient(app)
    app.dependency_overrides.clear()


ORDER = {"customer_email": "jo@example.com", "items": [{"sku": "KB-001", "quantity": 2}]}


def test_create_order_writes_order_and_outbox_event_atomically(client, Session):
    resp = client.post("/orders", json=ORDER)
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "PENDING"

    with Session() as s:
        event = s.scalars(select(OutboxEvent)).one()
        assert event.topic == "orders"
        assert event.key == body["id"]  # keyed by order id -> per-order ordering
        assert event.event_type == "OrderCreated"
        assert event.published_at is None
        payload = json.loads(event.payload)
        assert payload["order_id"] == body["id"]
        assert payload["items"] == [{"sku": "KB-001", "quantity": 2}]


def test_idempotency_key_returns_same_order(client, Session):
    h = {"Idempotency-Key": "abc-123"}
    first = client.post("/orders", json=ORDER, headers=h).json()
    second = client.post("/orders", json=ORDER, headers=h).json()
    assert first["id"] == second["id"]
    with Session() as s:
        assert s.scalar(select(func.count()).select_from(Order)) == 1
        assert s.scalar(select(func.count()).select_from(OutboxEvent)) == 1


@pytest.mark.parametrize(
    "payload",
    [
        {"customer_email": "jo@example.com", "items": []},
        {"customer_email": "not-an-email", "items": [{"sku": "KB-001", "quantity": 1}]},
        {"customer_email": "jo@example.com", "items": [{"sku": "KB-001", "quantity": 0}]},
    ],
)
def test_invalid_orders_rejected_and_nothing_published(client, Session, payload):
    assert client.post("/orders", json=payload).status_code == 422
    with Session() as s:
        assert s.scalar(select(func.count()).select_from(OutboxEvent)) == 0


def test_get_order_and_stats(client):
    oid = client.post("/orders", json=ORDER).json()["id"]
    assert client.get(f"/orders/{oid}").json()["id"] == oid
    assert client.get("/orders/00000000-0000-0000-0000-000000000000").status_code == 404
    stats = client.get("/stats").json()
    assert stats["orders_by_status"] == {"PENDING": 1}
    assert stats["outbox_unpublished"] == 1
