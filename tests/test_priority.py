"""The decision as pure functions of a view: fairness between tenants, the factors, and the
choice of the highest priority."""

import random
from datetime import UTC, datetime, timedelta

import msgspec
import pytest

from wicket.fairness import Fairness
from wicket.priority import Age, FairShare, Priority, Trait
from wicket.results import NoTicket
from wicket.tickets import Ticket, Waiting
from wicket.view import PoolView

EPOCH = datetime(2026, 1, 1, tzinfo=UTC)
HOUR = timedelta(hours=1)
FAIR = Priority((FairShare(), 4), (Trait("tier"), 2), (Age(full_after=timedelta(minutes=30)), 1))


START = Fairness()


def ticket(
    id: str, tenant: str, at: float = 0, grants: int = 0, tier: float | None = None
) -> Waiting:
    """A waiting ticket submitted `at` seconds after EPOCH, with a tier if one is given."""
    traits = {} if tier is None else {"tier": tier}
    return Waiting(Ticket(id, tenant, traits), EPOCH + timedelta(seconds=at), grants)


def drain(
    priority: Priority, waiting: list[Waiting], fairness: Fairness = START
) -> tuple[list[str], Fairness]:
    """Grant every ticket one by one, as a pool would; the tenants in grant order, and the
    fairness after the last grant."""
    view = PoolView(now=EPOCH + HOUR, retention=HOUR, waiting=tuple(waiting), fairness=fairness)
    order = []
    while True:
        match priority.first(view):
            case NoTicket():
                return order, view.fairness
            case Waiting() as chosen:
                order.append(chosen.ticket.tenant)
                view = msgspec.structs.replace(
                    view.without(chosen.ticket.id), fairness=view.fairness.after(chosen)
                )


def tickets(tenant: str, count: int, start: float = 0, tier: float | None = None) -> list[Waiting]:
    return [ticket(f"{tenant}{n}", tenant, start + n, tier=tier) for n in range(count)]


def test_two_tenants_take_turns_from_the_first_grant():
    order, _ = drain(FAIR, tickets("a", 10) + tickets("b", 10, start=100))
    assert order == ["a", "b"] * 10


def test_three_tenants_take_turns():
    order, _ = drain(FAIR, tickets("a", 5) + tickets("b", 5, 10) + tickets("c", 5, 20))
    assert order == ["a", "b", "c"] * 5


def test_a_newcomer_after_an_hour_alone_takes_turns_at_once_with_no_burst_for_either():
    _, fairness = drain(FAIR, tickets("a", 100))
    order, _ = drain(FAIR, tickets("a", 5, 1000) + tickets("b", 5, 2000), fairness)
    assert order == ["b", "a"] * 5


def test_a_tenant_back_from_a_break_gets_no_extra_turns():
    _, fairness = drain(FAIR, tickets("a", 4) + tickets("b", 4, 10))
    _, fairness = drain(FAIR, tickets("a", 10, 100), fairness)  # b is away
    order, _ = drain(FAIR, tickets("a", 5, 1000) + tickets("b", 5, 2000), fairness)
    assert "bb" not in "".join(order) and "aa" not in "".join(order)


def test_a_redelivery_does_not_cost_its_tenant_a_turn():
    fairness = Fairness().after(ticket("a0", "a"))
    assert fairness.after(ticket("a1", "a", grants=1)) == fairness


def test_virtual_time_never_falls_and_no_tag_stays_at_or_below_it():
    rng = random.Random(7)
    fairness, last = Fairness(), 0
    for n in range(2000):
        fairness = fairness.after(ticket(str(n), rng.choice("abcdefgh")))
        assert fairness.v >= last
        assert all(finish > fairness.v for _, finish in fairness.tags)
        last = fairness.v


def test_within_a_tenant_the_oldest_goes_first():
    view = PoolView(
        now=EPOCH + HOUR, retention=HOUR, waiting=(ticket("new", "a", 5), ticket("old", "a", 1))
    )
    assert FAIR.first(view) == ticket("old", "a", 1)


def test_an_empty_priority_grants_in_submission_order():
    order, _ = drain(Priority(), [ticket("x", "b", 2), ticket("y", "a", 1), ticket("z", "b", 3)])
    assert order == ["a", "b", "b"]


def test_a_deciding_trait_puts_the_higher_tier_first():
    paid = Priority((Trait("tier"), 10), (FairShare(), 4), (Age(full_after=HOUR), 1))
    order, _ = drain(paid, tickets("free", 3, tier=0.0) + tickets("paid", 3, 100, tier=1.0))
    assert order == ["paid"] * 3 + ["free"] * 3


def test_a_deciding_trait_with_three_tiers_orders_them():
    paid = Priority((Trait("tier"), 20), (FairShare(), 4), (Age(full_after=HOUR), 1))
    waiting = (
        tickets("low", 2, tier=0.0)
        + tickets("mid", 2, 10, tier=0.5)
        + tickets("top", 2, 20, tier=1.0)
    )
    order, _ = drain(paid, waiting)
    assert order == ["top", "top", "mid", "mid", "low", "low"]


def test_the_tenants_of_the_top_tier_take_turns():
    paid = Priority((Trait("tier"), 10), (FairShare(), 4), (Age(full_after=HOUR), 1))
    waiting = tickets("p", 4, tier=1.0) + tickets("q", 4, 10, tier=1.0) + tickets("free", 2, 20)
    order, _ = drain(paid, waiting)
    assert order == ["p", "q"] * 4 + ["free", "free"]


def test_age_rises_to_one_and_stays():
    view = PoolView(
        now=EPOCH + HOUR,
        retention=HOUR,
        waiting=(ticket("new", "a", 3600), ticket("half", "a", 1800), ticket("old", "a")),
    )
    assert Age(full_after=timedelta(minutes=30)).scores(view) == {
        "new": 0.0,
        "half": 1.0,
        "old": 1.0,
    }
    assert Age(full_after=HOUR).scores(view)["half"] == 0.5


def test_a_ticket_without_the_trait_scores_zero():
    view = PoolView(
        now=EPOCH, retention=HOUR, waiting=(ticket("1", "a", tier=0.5), ticket("2", "a"))
    )
    assert Trait("tier").scores(view) == {"1": 0.5, "2": 0.0}


def test_fair_share_scores_one_for_the_tenants_whose_turn_it_is():
    fairness = Fairness().after(ticket("a0", "a"))
    view = PoolView(
        now=EPOCH,
        retention=HOUR,
        fairness=fairness,
        waiting=(ticket("a1", "a"), ticket("b1", "b"), ticket("c1", "c")),
    )
    assert FairShare().scores(view) == {"a1": 0.0, "b1": 1.0, "c1": 1.0}


def test_nothing_waiting_is_no_ticket():
    assert FAIR.first(PoolView(now=EPOCH, retention=HOUR)) == NoTicket()


def test_a_negative_weight_or_a_full_after_of_zero_is_refused():
    with pytest.raises(ValueError, match="weight"):
        Priority((FairShare(), -1))
    with pytest.raises(ValueError, match="full_after"):
        Age(full_after=timedelta(0))
