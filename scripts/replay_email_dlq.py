"""Operational tool: inspect or replay messages parked in the email dead-letter queue.

    python scripts/replay_email_dlq.py               # list (dry run)
    python scripts/replay_email_dlq.py --replay      # move all back to the work queue
    python scripts/replay_email_dlq.py --replay --limit 10

Replayed messages get a fresh retry budget (x-attempt reset to 0). Each message is
re-published with a publisher confirm BEFORE it is acked from the DLQ, so a crash
mid-replay can duplicate a message (absorbed by the worker's idempotency) but never lose one.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.rabbit import (  # noqa: E402
    ATTEMPT_HEADER,
    DLQ,
    EMAIL_ROUTING_KEY,
    EXCHANGE,
    connect,
    persistent_props,
)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--replay", action="store_true", help="re-publish to the work queue (default: dry run)")
    p.add_argument("--limit", type=int, default=0, help="max messages to process (0 = all)")
    args = p.parse_args()

    conn = connect()
    ch = conn.channel()
    ch.confirm_delivery()
    total = ch.queue_declare(DLQ, passive=True).method.message_count
    limit = min(total, args.limit) if args.limit else total
    print(f"{total} message(s) in {DLQ}; processing {limit} ({'replay' if args.replay else 'dry run'})")

    seen = 0
    while seen < limit:
        method, props, body = ch.basic_get(DLQ, auto_ack=False)
        if method is None:
            break
        seen += 1
        headers = props.headers or {}
        print(
            f"- {props.message_id} attempts={headers.get(ATTEMPT_HEADER)} "
            f"last_error={headers.get('x-last-error', '-')}"
        )
        if args.replay:
            ch.basic_publish(
                EXCHANGE,
                EMAIL_ROUTING_KEY,
                body,
                persistent_props(props.message_id or "", 0, {"x-replayed": True}),
                mandatory=True,
            )
            ch.basic_ack(method.delivery_tag)
        # Dry run: leave unacked; closing the connection returns them all to the DLQ.
        # (Requeueing one by one would hand the same message straight back to basic_get.)
    conn.close()
    print(f"{'Replayed' if args.replay else 'Listed'} {seen} message(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
