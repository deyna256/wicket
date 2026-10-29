# 8. Lanes share a pool's capacity

**Status:** Proposed

## Context

Mentor and Generation share 10 grants a minute, and each may take at most 8. Each pool has its own
version and `Fairness`, so a limit shared by two pools would need one grant to compare-and-swap both,
and two pools cannot judge whose turn it is between them. Other queues give each kind of work its own
limits and skip a kind at its cap (River, Oban, sidekiq-limit_fetch), with no fairness across kinds;
[HTB](https://man7.org/linux/man-pages/man8/tc-htb.8.html) lets a class borrow its parent's spare rate
up to its own ceiling, but in one scheduler that sees every class.

## Decision

- **Kinds of work are lanes of one pool.** A ticket carries `lane`; work of one kind leaves it `""`.
  The pool keeps one version and one `Fairness` for every lane, so a grant stays one
  compare-and-swap ([decision 2](0002-pure-decision-over-a-store.md)) and counts in the pool's
  limits and its lane's.
- **A `Pool` serves one `Lane(name, *limits)`** and grants only its tickets. The lane's limits count
  only its leased tickets and grants; the pool's `limits` count every lane. `Pool` serves `Lane("")`
  unless given another.
- **Fairness is judged across every lane that can take work.** A worker makes its lane present in
  the store (`Ledger.attend`, for `2 × poll`) while the lane has a waiting ticket that its own limits
  let it take, and leaves once it has none. When another lane shows in its view (a waiting or leased
  ticket, a recent grant or a presence), it does so before it tries to grant, so the other lane sees
  it while its grant is on its way; when its lane is alone, it does so after a pass without a grant,
  so a lane waiting at the pool's limit is present before another lane appears. It chooses among its own lane's tickets and every present lane's, and grants only if the
  winner is its own; otherwise it yields, and the winner's lane takes it. Presences never change
  the version: they steer which ticket is chosen, never whether one may start, which the version
  check keeps deciding. As in HTB, only a lane with a
  backlog under its own ceiling competes; as in Temporal's pollers, a worker's presence expires
  rather than being withdrawn.
- **Every waiting tenant keeps its fairness tag, and ties go to the one served longest ago**
  ([decision 4](0004-start-time-fair-queuing.md)), so tenants of a lane served less often still take
  turns.
- **After `Missing`, a lane chooses again among its own tickets only,** so a ticket held by another
  transaction cannot make every lane yield to it.
- **Every process serving a pool configures the same pool limits and the same `Priority`, and every
  process of a lane the same lane limits.** The version does not catch a process configured with 12
  where the others say 8, and lanes ranking each other's tickets first would yield to each other
  forever. No worker with an old configuration may grant while one with the new does: to change
  `Priority` or the pool's limits, stop every worker of the pool first; to change a lane's limits,
  stop every worker of that lane first.
- **Ticket ids are unique per pool,** across lanes: a submit of a waiting id keeps its first lane.
- **One level.** A lane belongs to one pool; pools do not nest and lanes do not belong to several
  pools.

## Alternatives

- **A limit shared by several pools.** A grant would compare-and-swap several versions, and fairness
  between the pools would need a shared state anyway.
- **Choosing within one's own lane.** Simple, but after alice's grant in Mentor, with alice-m2 waiting
  there and bob-g1 in Generation, Mentor picks alice again: the shared fairness is only a shared
  history.
- **A lane's presence expiring without `leave`.** In simulation, a lane at its own `Concurrency(1)`
  kept the others yielding to tickets nobody took: idle time rose from 106–111 s to 664–690 s, and starts
  fell 19%.
- **Yielding only when the pool is short of capacity.** It would save the yield's latency when there
  is room for both, but the simple rule ("room for one more") let a lane with many workers fill a
  refilled `Rate` before the other polled. A correct rule must reserve room for every present lane.
- **Presence on the pool row, or written by `view` or `grant`.** The row is what the grant's
  compare-and-swap locks; a view is a read-only snapshot; a grant misses the lane that yields.

## Consequences

- About one presence write per lane every two passes while it has a candidate and either another
  lane shows in the pool or its workers wait at a limit; none for a lone lane that grants at once,
  nor for an idle one.
- A worker whose grant takes longer than `2 × poll` stops being seen until its next pass.
- Renewals in every lane change one version, so lanes conflict more often.
- A view reads every lane's tickets: merging pools multiplies the view size
  ([decision 2](0002-pure-decision-over-a-store.md)).
- A lost choice waits up to the winning lane's `poll`, even when the pool has room for both: give a
  pool's lanes similar polls.
- A lane whose last free worker just started its work stays present up to `2 × poll`, and others may
  yield to it meanwhile; a stale `leave` hides a lane for up to one `poll`.
- **An exception to "a busy lane does not hold back another":** a ticket held by another transaction
  (for example an uncommitted `withdraw`) stays the winner in the other lanes' views. If its lane has
  nothing else to grant, the other lanes yield to it until that transaction ends. If it commits a
  withdrawal, they go on within their next `poll`; if it rolls back, the ticket's own lane takes it
  first (up to its `poll`), then the others go on within theirs.
- A ticket in a lane nobody serves is never present: it waits forever and blocks no one.
