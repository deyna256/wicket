"""Fairness between tenants: Start-time Fair Queuing with every ticket weighing one."""

import msgspec

from wicket.tickets import Waiting


class Fairness(msgspec.Struct, frozen=True):
    """The pool's virtual time `v` and the finish tag of each recently served tenant. With unit
    weights this is round robin over the tenants with waiting tickets, a newcomer joining the
    current round."""

    v: int = 0
    tags: tuple[tuple[str, int], ...] = ()

    def start(self, tenant: str) -> int:
        return max(self.v, dict(self.tags).get(tenant, self.v))

    def after(self, waiting: Waiting) -> "Fairness":
        """The state after granting this ticket; a redelivery leaves it as it is."""
        if waiting.grants > 0:
            return self
        v = self.start(waiting.ticket.tenant)
        tags = dict(self.tags) | {waiting.ticket.tenant: v + 1}
        return Fairness(v, tuple(sorted((t, f) for t, f in tags.items() if f > v)))
