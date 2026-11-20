"""Fire N concurrent orders at the API, then report how fast they settled.

python scripts/load_test.py --orders 200 --concurrency 20
"""

import argparse
import json
import random
import time
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

SKUS = ["KB-001", "MS-002", "MN-003", "LT-004"]


def post_order(base: str, i: int) -> str:
    items = [
        {"sku": sku, "quantity": random.randint(1, 3)} for sku in random.sample(SKUS, k=random.randint(1, 3))
    ]
    body = json.dumps({"customer_email": f"customer{i}@example.com", "items": items}).encode()
    req = urllib.request.Request(
        f"{base}/orders",
        data=body,
        headers={"Content-Type": "application/json", "Idempotency-Key": f"load-{time.time_ns()}-{i}"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)["id"]


def get_status(base: str, order_id: str) -> str:
    with urllib.request.urlopen(f"{base}/orders/{order_id}", timeout=10) as resp:
        return json.load(resp)["status"]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="http://localhost:8000")
    p.add_argument("--orders", type=int, default=100)
    p.add_argument("--concurrency", type=int, default=20)
    p.add_argument("--timeout", type=float, default=120)
    args = p.parse_args()

    start = time.perf_counter()
    with ThreadPoolExecutor(args.concurrency) as pool:
        ids = list(pool.map(lambda i: post_order(args.base, i), range(args.orders)))
    accepted = time.perf_counter() - start
    print(f"Accepted {len(ids)} orders in {accepted:.2f}s ({len(ids) / accepted:.0f} req/s)")

    pending = set(ids)
    final: Counter = Counter()
    deadline = time.time() + args.timeout
    while pending and time.time() < deadline:
        with ThreadPoolExecutor(args.concurrency) as pool:
            batch = list(pending)
            statuses = dict(zip(batch, pool.map(lambda o: get_status(args.base, o), batch), strict=True))
        for oid, status in statuses.items():
            if status != "PENDING":
                pending.discard(oid)
                final[status] += 1
        if pending:
            time.sleep(0.5)

    settled = time.perf_counter() - start
    print(f"Settled {sum(final.values())}/{len(ids)} in {settled:.2f}s -> {dict(final)}")
    if pending:
        print(f"WARNING: {len(pending)} orders still PENDING after {args.timeout}s")


if __name__ == "__main__":
    main()
