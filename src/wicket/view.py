"""The snapshot of a pool that every decision reads."""

from datetime import datetime, timedelta
from uuid import UUID

import msgspec

from wicket.fairness import Fairness
from wicket.tickets import Permit, Version, Waiting


class Grant(msgspec.Struct, frozen=True):
    """When a ticket of this lane was granted."""

    at: datetime
    lane: str


class Presence(msgspec.Struct, frozen=True):
    """A lane present until this moment, with the identity of its last attendance."""

    lane: str
    until: datetime
    token: UUID


class PoolView(msgspec.Struct, frozen=True, kw_only=True):
    """One consistent moment of a pool, by the store's clock: its waiting and leased tickets, its
    grants within the store's retention, the lanes present now, its fairness and its version."""

    now: datetime
    retention: timedelta
    waiting: tuple[Waiting, ...] = ()
    leased: tuple[Permit, ...] = ()
    grants: tuple[Grant, ...] = ()
    present: tuple[Presence, ...] = ()
    fairness: Fairness = msgspec.field(default_factory=Fairness)
    version: Version = 0

    def without(self, ticket_id: str) -> "PoolView":
        """The same moment without one waiting ticket: what is left to choose from when that one
        is missing."""
        return msgspec.structs.replace(
            self, waiting=tuple(w for w in self.waiting if w.ticket.id != ticket_id)
        )

    def in_lanes(self, *names: str) -> "PoolView":
        """The same moment with only the tickets, grants and presences of these lanes: what a
        lane's limits count, and what a lane chooses from."""
        return msgspec.structs.replace(
            self,
            waiting=tuple(w for w in self.waiting if w.ticket.lane in names),
            leased=tuple(p for p in self.leased if p.waiting.ticket.lane in names),
            grants=tuple(g for g in self.grants if g.lane in names),
            present=tuple(p for p in self.present if p.lane in names),
        )
