"""Versioned event and task contracts shared by every service.

Every message carries an `event_id`, which consumers use as an idempotency key:
brokers deliver at-least-once, so the same message may arrive more than once.
"""

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, PositiveInt, TypeAdapter


def _now() -> datetime:
    return datetime.now(UTC)


class OrderItem(BaseModel):
    sku: str = Field(min_length=1, max_length=64)
    quantity: PositiveInt


class BaseEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    schema_version: int = 1
    occurred_at: datetime = Field(default_factory=_now)
    order_id: UUID


class OrderCreated(BaseEvent):
    event_type: Literal["OrderCreated"] = "OrderCreated"
    customer_email: str
    items: list[OrderItem]


class StockReserved(BaseEvent):
    event_type: Literal["StockReserved"] = "StockReserved"


class StockRejected(BaseEvent):
    event_type: Literal["StockRejected"] = "StockRejected"
    reason: str


InventoryEvent = Annotated[StockReserved | StockRejected, Field(discriminator="event_type")]
inventory_event_adapter: TypeAdapter[InventoryEvent] = TypeAdapter(InventoryEvent)


class EmailTask(BaseModel):
    """A unit of work for the notification worker (sent over RabbitMQ)."""

    task_id: UUID
    order_id: UUID
    to: str
    template: Literal["order_confirmed", "order_rejected"]
    context: dict[str, str] = Field(default_factory=dict)
