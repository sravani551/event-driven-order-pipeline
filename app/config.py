"""Runtime configuration, read from environment variables (12-factor style)."""

import os
from dataclasses import dataclass, field


def _delays() -> tuple[int, ...]:
    raw = os.getenv("RETRY_DELAYS_MS", "5000,30000,120000")
    return tuple(int(x) for x in raw.split(",") if x.strip())


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "postgresql+psycopg://app:app@localhost:5432/orders")
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
    rabbitmq_url: str = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")

    # Kafka topics
    orders_topic: str = "orders"
    inventory_topic: str = "inventory"
    dlq_topic: str = "orders.dlq"
    topic_partitions: int = int(os.getenv("TOPIC_PARTITIONS", "3"))

    # Kafka consumer in-place retries before dead-lettering a message
    kafka_max_attempts: int = int(os.getenv("KAFKA_MAX_ATTEMPTS", "4"))

    # RabbitMQ notification worker
    notification_fail_rate: float = float(os.getenv("NOTIFICATION_FAIL_RATE", "0.2"))
    notification_prefetch: int = int(os.getenv("NOTIFICATION_PREFETCH", "10"))
    retry_delays_ms: tuple[int, ...] = field(default_factory=_delays)


settings = Settings()
