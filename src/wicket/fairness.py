"""Fairness between tenants: Start-time Fair Queuing with every ticket weighing one."""

from collections.abc import Iterable, Mapping

import msgspec

from wicket.tickets import Waiting


class Fairness(msgspec.Struct, frozen=True):
    """The pool's virtual time `v` and the finish tag of each recently served tenant. With unit
    weights this is round robin over the tenants with waiting tickets, a newcomer joining the
    current round."""

    v: int = 0
    tags: tuple[tuple[str, int], ...] = ()

    def start(self, tenant: str) -> int:
        """The tenant's start, max(v, its finish tag)."""
        return self.starts([tenant])[tenant]

    def starts(self, tenants: Iterable[str]) -> Mapping[str, int]:
        """Each tenant's start, max(v, its finish tag)."""
        tags = dict(self.tags)
        return {t: max(self.v, tags.get(t, self.v)) for t in tenants}

    def after(self, waiting: Waiting) -> "Fairness":
        """The state after granting this ticket; a redelivery leaves it as it is."""
        if waiting.grants > 0:
            return self
        v = self.start(waiting.ticket.tenant)
        tags = dict(self.tags) | {waiting.ticket.tenant: v + 1}
        return Fairness(v, tuple(sorted((t, f) for t, f in tags.items() if f > v)))
