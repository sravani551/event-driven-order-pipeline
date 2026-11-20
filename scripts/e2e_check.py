"""End-to-end check against a running stack (used in CI).

Asserts that:
  * every order reaches CONFIRMED or REJECTED,
  * an over-stock order is REJECTED with a reason,
  * stock never goes negative,
  * a retried request with the same Idempotency-Key does not create a second order,
  * every settled order's email is accounted for (sent or dead-lettered).
"""

import json
import subprocess
import sys
import time
import urllib.request
import uuid

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"


def call(method: str, path: str, body: dict | None = None, headers: dict | None = None):
    req = urllib.request.Request(
        BASE + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json", **(headers or {})},
        method=method,
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)


def dlq_count() -> int:
    """Messages parked in the email DLQ (read via rabbitmqctl inside the compose service)."""
    try:
        cmd = [
            "docker",
            "compose",
            "exec",
            "-T",
            "rabbitmq",
            "rabbitmqctl",
            "-q",
            "list_queues",
            "name",
            "messages",
        ]
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=True).stdout
    except (OSError, subprocess.SubprocessError):
        return 0
    for line in out.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] == "notifications.email.dlq":
            return int(parts[1])
    return 0


def wait_for_api(timeout: float = 240) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            call("GET", "/health")
            return
        except Exception:
            time.sleep(1)
    sys.exit("API never became healthy")


def wait_settled(order_ids: list[str], timeout: float = 120) -> dict[str, dict]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        orders = {oid: call("GET", f"/orders/{oid}") for oid in order_ids}
        if all(o["status"] != "PENDING" for o in orders.values()):
            return orders
        time.sleep(1)
    sys.exit(f"Orders did not settle within {timeout}s")


def main() -> None:
    wait_for_api()
    key = f"e2e-{uuid.uuid4()}"
    first = call(
        "POST",
        "/orders",
        {"customer_email": "a@example.com", "items": [{"sku": "KB-001", "quantity": 1}]},
        {"Idempotency-Key": key},
    )
    retry = call(
        "POST",
        "/orders",
        {"customer_email": "a@example.com", "items": [{"sku": "KB-001", "quantity": 1}]},
        {"Idempotency-Key": key},
    )
    assert first["id"] == retry["id"], "Idempotency-Key did not deduplicate"

    too_many = call(
        "POST",
        "/orders",
        {"customer_email": "b@example.com", "items": [{"sku": "MN-003", "quantity": 10_000}]},
    )
    others = [
        call(
            "POST",
            "/orders",
            {"customer_email": f"c{i}@example.com", "items": [{"sku": "MS-002", "quantity": 1}]},
        )["id"]
        for i in range(20)
    ]

    orders = wait_settled([first["id"], too_many["id"], *others])
    assert orders[first["id"]]["status"] == "CONFIRMED", orders[first["id"]]
    assert orders[too_many["id"]]["status"] == "REJECTED", orders[too_many["id"]]
    assert "insufficient stock" in orders[too_many["id"]]["status_reason"]
    assert all(p["stock"] >= 0 for p in call("GET", "/products"))

    # Every email must be accounted for: sent, or dead-lettered after exhausting its retries
    # (with a simulated failure rate, a few may legitimately end up in the DLQ).
    deadline = time.time() + 120
    while time.time() < deadline:
        stats = call("GET", "/stats")
        sent = stats["processed_by_consumer"].get("notification-worker", 0)
        parked = dlq_count()
        if sent + parked >= len(orders):
            break
        time.sleep(2)
    print(json.dumps(stats, indent=2))
    print(f"emails sent={sent} dead-lettered={parked} expected={len(orders)}")
    assert sent + parked >= len(orders), "emails missing"
    print("E2E OK")


if __name__ == "__main__":
    main()
