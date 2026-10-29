# 4. Fairness between tenants is Start-time Fair Queuing

**Status:** Accepted

## Context

Two tenants who each submit ten tickets should get them in turns from the first grant. A tenant
returning after idle time should gain no burst of turns, and one that ran alone for an hour should
not be pushed to the back. The score must also let `FairShare` decide alone at a known weight
([decision 3](0003-priority-from-factors.md)).

## Decision

[Start-time Fair Queuing](https://www.cs.utexas.edu/ftp/techreports/tr96-02.pdf) (Goyal, Vin and
Cheng, 1996), with every ticket weighing one. [Kubernetes API Priority and
Fairness](https://github.com/kubernetes/enhancements/blob/master/keps/sig-api-machinery/1040-priority-and-fairness/README.md),
a fair queuing scheme of its own, also starts a newly active queue at the current virtual time.

- `Fairness` holds a virtual time `v` and a finish tag per tenant; a tenant starts at
  `max(v, finish[t])`, or at `v` with no tag.
- `Fairness.after(waiting, tenants)` for a first grant sets `v` to the tenant's start and its finish
  to `v + 1`. Every tenant in `tenants`, those with a waiting ticket, keeps its tag, or gets the `v`
  it arrived at; other tags are dropped once no greater than `v`. For a redelivery it returns the
  same `Fairness`, so a crashed worker costs its tenant no turn.
- `FairShare()` scores `1` for the waiting tickets of the tenants with the smallest start and `0` for
  the rest. Starts clamp a lagging tag to `v`, so a tenant passed over by a heavier factor cannot
  take `FairShare`'s point from the tenants of a higher tier.
- `Priority` breaks ties by the raw tag, served longest ago first. Among tenants passed over by a
  heavier factor, or waiting in a lane served less often ([decision 8](0008-lanes.md)), the turn
  order is kept: dropping their tags would leave every tie to the earlier submission.
- `Fairness` is written by the grant, so the version check covers it.

With every ticket weighing one this is round robin over the tenants with waiting tickets, in which a
tenant that becomes active joins the current round.

## Alternatives

Each was tried by review or simulation:

| attempt | what broke |
|---|---|
| `1 / (1 + grants / weight)` over a window | a grant's worth faded as counts grew; `Age` took over and FIFO came back |
| Slurm's `2^(-U/S)` | faded the same way |
| rank of tenants by grants | adjacent ranks differ by `1/(k−1)`, so a deciding weight stopped deciding past 3–4 tenants |
| `1` for the least-served tenant | a newcomer after a tenant ran alone for an hour got 25 turns in a row |

## Consequences

- Fairness counts starts, not running time: a tenant whose work takes ten minutes holds more slots
  than one whose work takes one.
- The state holds a tag for every tenant with a waiting ticket and every one served recently.
- The fairness properties hold while `FairShare` decides alone.
- Weighted shares, `finish = start + 1 / weight`, can come later without changing the contract;
  the tie by raw tag is shown for equal weights only.
