"""Whether a pool may lease one more ticket now. At the same version a limit may turn from refuse
to admit, never the reverse; it reads only leased tickets, grant times, now and retention."""

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
        return sum(1 for at in view.grant_times if at > since) < self.n
