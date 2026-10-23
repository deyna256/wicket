# 3. Priority is a weighted sum of factors, and the highest wins

**Status:** Accepted

## Context

Different work needs different ways of judging what goes first: whose turn it is, a paid plan, a
nearer deadline, how long a ticket has waited. Two questions hide in "priority": how important a
ticket is, and how importance becomes a choice.

## Decision

- **A `Factor` scores every waiting ticket of a view in `0..1`** by one trait, all at once, so a
  factor that compares tickets does its work once per decision. Shipped: `FairShare()`,
  `Age(full_after)`, and `Trait(name)` for one of the ticket's `traits`.
- **`Priority` is weighted factors,** as in [Slurm's multifactor
  priority](https://slurm.schedmd.com/priority_multifactor.html), and `first(view)` returns the
  ticket with the highest weighted sum; ties go to the earlier `submitted_at`, then the smaller id.
  An empty `Priority` is FIFO. Weights are non-negative and their sum must fit in a finite float;
  `Priority` rejects an unsupported configuration with `ValueError` at construction, so valid factor
  scores cannot overflow the weighted sum.
- **No ordering seam.** Fairness and favouring a user are both factors, so a second discipline would
  solve the same problems less predictably.
- **What a weight means:** a factor decides alone when its weight times the smallest step between its
  scores exceeds the sum of the other weights; the others only break its ties.
- **A ticket's importance is named numbers, its `traits`,** such as `{"tier": 1.0}`, each in `0..1`,
  which `Ticket` refuses otherwise when it is built or decoded. The caller turns its own data into
  them where it submits, as Temporal takes a priority key and Slurm a QOS priority rather than the
  job's data. The worker reads the work from the caller's row, so nothing else needs to travel.
- **Pools do not nest.** Capacity is split between pools, as Kubernetes API Priority and Fairness
  gives each priority level its own seats.

## Alternatives

- **A pluggable ordering** (strict, weighted random). Dropped: with `Age` and `FairShare` in the
  score, the highest priority already gives fairness and no starvation, predictably.
- **Priority levels.** A `Trait` with a deciding weight gives the same precedence and composes with the
  other factors.

- **Typed facts,** a ticket generic in the caller's `msgspec.Struct` read by `Fact(lambda f: f.tier)`.
  The type parameter ran through every value, port and adapter for the sake of one lambda; the
  adapters needed the type to decode; ty and pyright disagreed on mixed factors; and a fact out of
  range was found only when a PostgreSQL view decoded it, failing every view of the pool.

## Consequences

- A deciding `Trait` gives precedence, not a share: under a steady flow of high-tier work lower tiers
  wait.
- `Age` caps at `1` and cannot outweigh a heavier factor.
- With `FairShare` after a deciding `Trait`, the tenants of the top tier still take turns: paid
  first, fair among the paid.
- A misspelt trait scores every ticket `0`; the caller keeps trait names in one constant.
