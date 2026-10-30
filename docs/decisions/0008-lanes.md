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
- **Fairness is judged across every lane that can take work.** Before every attempt to grant, a
  worker refreshes its lane's presence in the store (`Ledger.attend`, for `2 × poll`) while the
  lane has a waiting ticket that its own limits let it take, and leaves once it has none. Attendance
  carries the view's version and returns `Conflict` if a grant or renewal in that lane has changed
  the pool since the view; the worker reads again. A grant in another lane does not conflict; a
  version newer than the pool does. Each attendance gets a new token; a leave removes only the token
  in the worker's view. A stale leave
  therefore cannot hide a worker that has since attended. The worker checks the store rather than
  trusting the snapshot's time, which can be stale by the time it returns. So a lane is seen
  while it waits at the pool's limit and while its grant is on its way, by lanes already there and
  by lanes that appear meanwhile. It chooses among its own lane's tickets and every present lane's,
  and grants only if the winner is its own; otherwise it yields, and the winner's lane takes it.
  Presences never change the version: they steer which ticket is chosen, never whether one may
  start, which the version check keeps deciding. As in HTB, only a lane with a backlog under its own
  ceiling competes; as in Temporal's pollers, a worker's presence expires rather than being
  withdrawn.
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
- **A lane's presence expiring without `leave`.** A lane at its own cap would keep the others
  yielding to tickets nobody takes until its presence ends.
- **Yielding only when the pool is short of capacity.** It would save the yield's latency when there
  is room for both, but the simple rule ("room for one more") let a lane with many workers fill a
  refilled `Rate` before the other polled. A correct rule must reserve room for every present lane.
- **Writing a presence only when another lane is in view, or only after a pass without a grant.**
  Each left a window: a lane appearing while another's grant was on its way, or a lane waiting at
  the limit before its neighbour arrived, took every freed slot of a minute in tests.
- **Presence on the pool row, or written by `view` or `grant`.** The row is what the grant's
  compare-and-swap locks; a view is a read-only snapshot; a grant misses the lane that yields.

## Consequences

- Every pass with a candidate calls `attend`: PostgreSQL normally locks the row and then updates it,
  also reading the pool row under a shared lock, so every worker writes on every pass. Writes grow
  with the number of workers and their grant rate.
  An absent row needs an insert and, if that races, another lock. `leave` writes only a live presence
  whose token matches. A pool of one lane pays this too; an idle lane writes nothing.
- A presence lasts at least `2 × poll` from the store's clock at its refresh unless a matching leave
  ends it. Time spent returning from the store or granting uses part of that time; once it expires,
  the worker is unseen until its next pass.
- Renewals in every lane change one version, so lanes conflict more often.
- The pool keeps one last-change marker per lane ever granted or renewed. Its size grows with the
  number of distinct lane names.
- A delayed attendance holding the shared pool lock delays grants and renewals in every lane until
  it finishes; an unresponsive database or worker may therefore cause a lease to expire. Attendance
  waiting for its lane row does not yet hold the pool lock.
- A view reads every lane's tickets: merging pools multiplies the view size
  ([decision 2](0002-pure-decision-over-a-store.md)).
- A lost choice waits up to the winning lane's `poll`, even when the pool has room for both: give a
  pool's lanes similar polls.
- A lane whose last free worker just started its work stays present until the recorded end, and
  others may yield to it meanwhile. A longer poll from another worker can make that end later.
- **An exception to "a busy lane does not hold back another":** a ticket held by another transaction
  (for example an uncommitted `withdraw`) stays the winner in the other lanes' views. If its lane has
  nothing else to grant, the other lanes yield to it until that transaction ends. If it commits a
  withdrawal, they go on within their next `poll`; if it rolls back, the ticket's own lane takes it
  first (up to its `poll`), then the others go on within theirs.
- A ticket in a lane nobody serves is never present: it waits forever and blocks no one.
