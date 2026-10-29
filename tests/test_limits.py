"""Whether a pool may lease one more ticket now."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from wicket.limits import Concurrency, Lane, Rate
from wicket.tickets import Permit, Ticket, Waiting
from wicket.view import Grant, PoolView, Presence

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
    assert not Rate(10, per=MINUTE).admits(
        PoolView(now=now, retention=MINUTE, grants=tuple(Grant(at, "") for at in ten))
    )
    assert Rate(10, per=MINUTE).admits(
        PoolView(now=now, retention=MINUTE, grants=tuple(Grant(at, "") for at in ten[:9]))
    )


def test_a_grant_exactly_one_window_ago_no_longer_counts():
    now = EPOCH + MINUTE
    assert Rate(1, per=MINUTE).admits(
        PoolView(now=now, retention=MINUTE, grants=(Grant(EPOCH, ""),))
    )


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


def lane_view(leased_lanes: str = "", granted_lanes: str = "") -> PoolView:
    """A view whose leased tickets and grants are in the lanes named one letter each."""
    permits = tuple(
        Permit("p", Waiting(Ticket(str(n), "a", lane=lane), EPOCH, UUID(int=n)), EPOCH, n)
        for n, lane in enumerate(leased_lanes)
    )
    grants = tuple(Grant(EPOCH, lane) for lane in granted_lanes)
    return PoolView(now=EPOCH, retention=MINUTE, leased=permits, grants=grants)


def test_a_view_narrowed_to_lanes_keeps_only_their_tickets_grants_and_presences():
    m = Waiting(Ticket("m", "a", lane="m"), EPOCH, UUID(int=1))
    g = Waiting(Ticket("g", "a", lane="g"), EPOCH, UUID(int=2))
    view = PoolView(
        now=EPOCH,
        retention=MINUTE,
        waiting=(m, g),
        leased=(Permit("p", m, EPOCH, 1), Permit("p", g, EPOCH, 2)),
        grants=(Grant(EPOCH, "m"), Grant(EPOCH, "g")),
        present=(Presence("g", EPOCH), Presence("m", EPOCH)),
        version=7,
    )
    narrowed = view.in_lanes("m")
    assert narrowed.waiting == (m,) and [p.token for p in narrowed.leased] == [1]
    assert narrowed.grants == (Grant(EPOCH, "m"),) and narrowed.present == (Presence("m", EPOCH),)
    assert narrowed.version == 7 and view.in_lanes("m", "g") == view


def test_a_lane_counts_only_its_own_leased_tickets_and_grants():
    assert not Lane("m", Concurrency(2)).admits(lane_view(leased_lanes="mmg"))
    assert Lane("m", Concurrency(2)).admits(lane_view(leased_lanes="mgg"))
    assert not Lane("m", Rate(2, per=MINUTE)).admits(lane_view(granted_lanes="mgm"))
    assert Lane("g", Rate(2, per=MINUTE)).admits(lane_view(granted_lanes="mgm"))


def test_a_lane_admits_only_when_all_its_limits_do_and_without_limits_always():
    view = lane_view(leased_lanes="m", granted_lanes="mm")
    assert not Lane("m", Concurrency(5), Rate(2, per=MINUTE)).admits(view)
    assert Lane("m").admits(view)


def test_the_pool_limit_counts_every_lane_where_the_lane_limit_counts_its_own():
    view = lane_view(granted_lanes="mgg")
    assert not Rate(3, per=MINUTE).admits(view)
    assert Lane("m", Rate(2, per=MINUTE)).admits(view)
