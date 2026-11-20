"""Pure retry-policy logic, kept free of broker code so it is trivially unit-testable."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class RetryDecision:
    action: Literal["retry", "dead_letter"]
    delay_ms: int | None = None

    @property
    def routing_key(self) -> str | None:
        return f"delay.{self.delay_ms}" if self.delay_ms is not None else None


def plan_retry(failed_attempts: int, delays_ms: Sequence[int]) -> RetryDecision:
    """Decide what to do after a failure.

    `failed_attempts` is how many times the message had already failed BEFORE this
    failure (0 on the first failure). Each tier of `delays_ms` is used once, giving an
    escalating backoff (e.g. 5s -> 30s -> 2m); after that the message is dead-lettered.
    """
    if failed_attempts < 0:
        raise ValueError("failed_attempts must be >= 0")
    if failed_attempts < len(delays_ms):
        return RetryDecision("retry", delays_ms[failed_attempts])
    return RetryDecision("dead_letter")


def backoff_seconds(attempt: int, base: float = 0.5, cap: float = 8.0) -> float:
    """Exponential backoff for in-place Kafka retries: 0.5s, 1s, 2s, 4s, ... capped."""
    return min(cap, base * (2 ** max(0, attempt - 1)))
