"""How important each waiting ticket is, and the choice of the most important."""

from collections.abc import Mapping
from datetime import timedelta
from typing import Protocol

import msgspec

from wicket.results import NoTicket
from wicket.tickets import Waiting
from wicket.view import PoolView


class Factor(Protocol):
    def scores(self, view: PoolView) -> Mapping[str, float]:
        """A number in 0..1 for every waiting ticket, by its id."""
        ...


class FairShare(msgspec.Struct, frozen=True):
    """1 for the tickets of the tenants whose turn it is, 0 for the rest."""

    def scores(self, view: PoolView) -> Mapping[str, float]:
        starts = view.fairness.starts({w.ticket.tenant for w in view.waiting})
        due = min(starts.values(), default=0)
        return {w.ticket.id: float(starts[w.ticket.tenant] == due) for w in view.waiting}


class Age(msgspec.Struct, frozen=True):
    """0 when submitted, rising evenly to 1 at `full_after`."""

    full_after: timedelta

    def __post_init__(self) -> None:
        if self.full_after <= timedelta(0):
            raise ValueError(f"full_after must be positive, not {self.full_after}")

    def scores(self, view: PoolView) -> Mapping[str, float]:
        return {
            w.ticket.id: min(1.0, max(0.0, (view.now - w.submitted_at) / self.full_after))
            for w in view.waiting
        }


class Trait(msgspec.Struct, frozen=True):
    """The ticket's trait of this name; 0 for a ticket without it."""

    name: str

    def scores(self, view: PoolView) -> Mapping[str, float]:
        return {w.ticket.id: w.ticket.traits.get(self.name, 0.0) for w in view.waiting}


class Priority:
    """Weighted factors, and the choice of the ticket with the highest weighted sum; ties go to the
    earlier submitted, then the smaller id. No factors is first in, first out. A plain class, since
    a Struct cannot take its factors as arguments; it never changes after construction."""

    __slots__ = ("_factors",)

    def __init__(self, *factors: tuple[Factor, float]) -> None:
        for factor, weight in factors:
            if weight < 0:
                raise ValueError(f"the weight of {factor!r} is {weight}; weights are not negative")
        self._factors = factors

    def first(self, view: PoolView) -> Waiting | NoTicket:
        if not view.waiting:
            return NoTicket()
        scored = [(factor.scores(view), weight) for factor, weight in self._factors]
        return min(
            view.waiting,
            key=lambda w: (
                -sum(weight * scores[w.ticket.id] for scores, weight in scored),
                w.submitted_at,
                w.ticket.id,
            ),
        )
