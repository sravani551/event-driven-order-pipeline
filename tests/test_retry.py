import pytest

from app.retry import backoff_seconds, plan_retry

DELAYS = (5000, 30000, 120000)


def test_escalating_retry_tiers_then_dead_letter():
    decisions = [plan_retry(n, DELAYS) for n in range(4)]
    assert [d.action for d in decisions] == ["retry", "retry", "retry", "dead_letter"]
    assert [d.delay_ms for d in decisions[:3]] == [5000, 30000, 120000]
    assert decisions[0].routing_key == "delay.5000"
    assert decisions[3].routing_key is None


def test_no_retry_tiers_means_immediate_dead_letter():
    assert plan_retry(0, ()).action == "dead_letter"


def test_negative_attempts_rejected():
    with pytest.raises(ValueError):
        plan_retry(-1, DELAYS)


def test_backoff_is_exponential_and_capped():
    assert [backoff_seconds(a) for a in range(1, 7)] == [0.5, 1, 2, 4, 8, 8]
