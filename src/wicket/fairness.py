"""Fairness between tenants: Start-time Fair Queuing with every ticket weighing one."""

from collections.abc import Iterable, Mapping

import msgspec

from wicket.tickets import Waiting


class Fairness(msgspec.Struct, frozen=True):
    """The pool's virtual time `v` and a finish tag per tenant: those served recently, and those
    waiting in lanes the last grant could not see. With unit weights this is round robin over the
    tenants with waiting tickets, a newcomer joining the current round; a tenant of a lane served
    less often keeps its place."""

    v: int = 0
    tags: tuple[tuple[str, int], ...] = ()

    def tags_of(self, tenants: Iterable[str]) -> Mapping[str, int]:
        """Each tenant's finish tag, `v` for a tenant without one: the smallest was served longest
        ago."""
        tags = dict(self.tags)
        return {t: tags.get(t, self.v) for t in tenants}

    def after(self, waiting: Waiting, elsewhere: Iterable[str]) -> "Fairness":
        """The state after granting this ticket. `elsewhere` are the tenants waiting only outside
        the choice, in lanes it could not see: each keeps its tag, or gets the `v` it arrived at,
        so its turn waits for it. A redelivery leaves the state as it is."""
        if waiting.grants > 0:
            return self
        kept = set(elsewhere)
        tags = dict.fromkeys(kept, self.v) | dict(self.tags)
        v = max(self.v, tags.get(waiting.ticket.tenant, self.v))
        tags[waiting.ticket.tenant] = v + 1
        return Fairness(v, tuple(sorted((t, f) for t, f in tags.items() if f > v or t in kept)))
