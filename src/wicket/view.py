"""The snapshot of a pool that every decision reads."""

from datetime import datetime, timedelta

import msgspec

from wicket.fairness import Fairness
from wicket.tickets import Permit, Version, Waiting


class PoolView(msgspec.Struct, frozen=True, kw_only=True):
    """One consistent moment of a pool, by the store's clock: its waiting and leased tickets, the
    times of its grants within the store's retention, its fairness and its version."""

    now: datetime
    retention: timedelta
    waiting: tuple[Waiting, ...] = ()
    leased: tuple[Permit, ...] = ()
    grant_times: tuple[datetime, ...] = ()
    fairness: Fairness = msgspec.field(default_factory=Fairness)
    version: Version = 0

    def without(self, ticket_id: str) -> "PoolView":
        """The same moment without one waiting ticket: what is left to choose from when that one
        is missing."""
        return msgspec.structs.replace(
            self, waiting=tuple(w for w in self.waiting if w.ticket.id != ticket_id)
        )
