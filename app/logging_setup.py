import logging
import os
import signal
import threading


def setup_logging(service: str) -> logging.Logger:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format=f"%(asctime)s [{service}] %(levelname)s %(message)s",
    )
    return logging.getLogger(service)


def stop_event_on_signals() -> threading.Event:
    """Return an Event that is set on SIGINT/SIGTERM so loops can shut down gracefully
    (finish the in-flight message, commit offsets, leave the consumer group cleanly)."""
    stop = threading.Event()

    def _handler(signum, _frame):
        logging.getLogger(__name__).info("Received signal %s, shutting down...", signum)
        stop.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    return stop
