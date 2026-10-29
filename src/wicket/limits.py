"""Whether a pool may lease one more ticket now. At the same version a limit may turn from refuse
to admit, never the reverse; it reads only leased tickets, grants, now and retention."""

from datetime import timedelta
from typing import Protocol

import msgspec

from wicket.view import PoolView


class Limit(Protocol):
    def admits(self, view: PoolView) -> bool: ...


class Concurrency(msgspec.Struct, frozen=True):
    """Fewer than `n` leased tickets."""

    n: int

    def __post_init__(self) -> None:
        if self.n < 1:
            raise ValueError(f"Concurrency needs at least 1, not {self.n}")

    def admits(self, view: PoolView) -> bool:
        return len(view.leased) < self.n


class Rate(msgspec.Struct, frozen=True):
    """Fewer than `n` grants in `(now - per, now]`; a redelivery is a start and counts."""

    n: int
    per: timedelta

    def __post_init__(self) -> None:
        if self.n < 1 or self.per <= timedelta(0):
            raise ValueError(
                f"Rate needs at least 1 grant per a positive time, not {self.n} per {self.per}"
            )

    def admits(self, view: PoolView) -> bool:
        if self.per > view.retention:
            raise ValueError(
                f"Rate counts grants over {self.per}, but the store keeps grants for "
                f"{view.retention}; configure the store's retention to at least {self.per}"
            )
        since = view.now - self.per
        return sum(1 for grant in view.grants if grant.at > since) < self.n


class Lane:
    """One lane of a pool and the limits on it alone, which count only its leased tickets and
    grants. A pool serves one lane and grants only its tickets; it is not one of the pool's limits.
    A plain class, like Priority: a Struct cannot take its limits as arguments; it never changes
    after construction."""

    __slots__ = ("_limits", "_name")

    def __init__(self, name: str, *limits: Limit) -> None:
        self._name = name
        self._limits = limits

    @property
    def name(self) -> str:
        return self._name

    def admits(self, view: PoolView) -> bool:
        own = view.in_lanes(self._name)
        return all(limit.admits(own) for limit in self._limits)
