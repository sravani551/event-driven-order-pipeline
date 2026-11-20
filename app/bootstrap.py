"""One-shot setup job: DB schema + seed data, Kafka topics, RabbitMQ topology."""

from sqlalchemy import select

from app.db import Base, Product, get_sessionmaker
from app.logging_setup import setup_logging

log = setup_logging("bootstrap")

SEED_PRODUCTS = [
    ("KB-001", "Mechanical keyboard", 200),
    ("MS-002", "Wireless mouse", 300),
    ("MN-003", "4K monitor", 15),  # deliberately low so some orders get rejected
    ("LT-004", "Laptop", 25),
]


def init_db() -> None:
    Session = get_sessionmaker()
    Base.metadata.create_all(Session.kw["bind"])
    with Session() as session:
        if session.scalar(select(Product).limit(1)) is None:
            session.add_all(Product(sku=s, name=n, stock=q) for s, n, q in SEED_PRODUCTS)
            session.commit()
            log.info("Seeded %d products", len(SEED_PRODUCTS))


def main() -> None:
    from app.kafka_utils import ensure_topics
    from app.rabbit import connect, declare_topology

    init_db()
    ensure_topics()
    conn = connect()
    declare_topology(conn.channel())
    conn.close()
    log.info("Bootstrap complete")


if __name__ == "__main__":
    main()
