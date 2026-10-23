"""Whether a pool may lease one more ticket now."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from wicket.limits import Concurrency, Rate
from wicket.tickets import Permit, Ticket, Waiting
from wicket.view import PoolView

EPOCH = datetime(2026, 1, 1, tzinfo=UTC)
MINUTE = timedelta(minutes=1)


def leased(count: int) -> tuple[Permit, ...]:
    return tuple(
        Permit("p", Waiting(Ticket(str(n), "a"), EPOCH, UUID(int=n)), EPOCH, n)
        for n in range(count)
    )


def test_concurrency_admits_fewer_than_its_number_of_leased_tickets():
    assert Concurrency(2).admits(PoolView(now=EPOCH, retention=MINUTE, leased=leased(1)))
    assert not Concurrency(2).admits(PoolView(now=EPOCH, retention=MINUTE, leased=leased(2)))


def test_rate_refuses_an_eleventh_grant_within_a_minute():
    now = EPOCH + timedelta(hours=1)
    ten = tuple(
        now - timedelta(seconds=s) for s in range(0, 59, 6)
    )  # 10 grants in (now - 60 s, now]
    assert not Rate(10, per=MINUTE).admits(PoolView(now=now, retention=MINUTE, grant_times=ten))
    assert Rate(10, per=MINUTE).admits(PoolView(now=now, retention=MINUTE, grant_times=ten[:9]))


def test_a_grant_exactly_one_window_ago_no_longer_counts():
    now = EPOCH + MINUTE
    assert Rate(1, per=MINUTE).admits(PoolView(now=now, retention=MINUTE, grant_times=(EPOCH,)))


def test_rate_refuses_a_store_that_keeps_less_history_than_its_window():
    with pytest.raises(ValueError, match="keeps grants for 0:00:30"):
        Rate(10, per=MINUTE).admits(PoolView(now=EPOCH, retention=timedelta(seconds=30)))


def test_limits_refuse_numbers_they_cannot_keep():
    with pytest.raises(ValueError):
        Concurrency(0)
    with pytest.raises(ValueError):
        Rate(0, per=MINUTE)
    with pytest.raises(ValueError):
        Rate(1, per=timedelta(0))
