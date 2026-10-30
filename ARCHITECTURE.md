# Architecture

This document is for people who change wicket's code. It describes how the code is laid out and the
rules that hold everywhere in it. Why the main choices were made is in [docs/decisions](docs/decisions/).

The core, the in-memory store and the PostgreSQL store are built.

## Bird's eye view

wicket decides which waiting ticket of a pool may start now. Producers submit tickets to a `Backlog`;
workers ask a `Pool` for a permit. To grant one, the pool reads a snapshot of itself from its
`Ledger`, asks its limits whether one more may start, lets its priority choose the ticket, and writes
the grant only if the snapshot's version is still current and the chosen submission still exists.
The worker holds the permit as a lease that the pool renews until the work is done.

```text
producer ──▶ Backlog.submit ──▶ store ◀── Ledger.view / grant / renew / release ◀── Pool.permit() ◀── worker
                                                       │
                                     PoolView ──▶ Limit.admits, Priority.first (Factor.scores), Fairness.after
```

The decision is a pure function of a `PoolView`; only the store does I/O.

## Code map

All code is in `src/wicket/`.

| module | what is there |
|---|---|
| `results.py` | the result values `Withdrawn`, `AlreadyLeased`, `Gone`, `Attended`, `Conflict`, `Missing`, `Renewed`, `Released`, `LeaseLost`, `NoTicket`; the errors `LeaseLostError` and `StoreUnavailable` |
| `tickets.py` | `Version`, `Trait`, which checks its value, `Ticket`, `Waiting`, `Permit` |
| `fairness.py` | `Fairness`, Start-time Fair Queuing with every ticket weighing one |
| `view.py` | `PoolView`, the snapshot every decision reads, and its `Grant` and `Presence` records |
| `priority.py` | the contract `Factor`; `FairShare`, `Age`, `TraitFactor`; `Priority` |
| `limits.py` | the contract `Limit`; `Concurrency`, `Rate`; `Lane`, a lane and the limits on it alone |
| `store.py` | the ports `Backlog` and `Ledger` |
| `memory.py` | the in-memory store: `MemoryState` and its two adapters `MemoryBacklog` and `MemoryLedger` |
| `postgres.py` | the PostgreSQL store, in the extra `wicket[postgres]`: its tables on `metadata`, `PostgresBacklog` on the caller's session, `PostgresLedger` on an engine, and `_Reach`, where a driver's error becomes `StoreUnavailable` |
| `pool.py` | `Pool`, which serves one lane, keeps it present while it waits, chooses across present lanes and keeps a permit's lease while its block runs |

`tests/test_store_contract.py` is `StoreContract`, the suite every pair of store adapters passes;
it runs on the in-memory pair and on PostgreSQL through asyncpg and psycopg.

The public API is `wicket.__all__`, and `wicket.postgres` for the extra. Everything else may change
without notice.

## Invariants

wicket follows the ideas of *Elegant Objects* by Yegor Bugayenko, in idiomatic Python. These rules hold
in all of `src/wicket/`.

1. **A class stands for a thing in the problem, not an action:** a ticket, a pool, a limit, a ledger.
   There are no `*Manager`, `*Scheduler` or `*Service` classes, no static methods and no module-level
   functions with logic.
2. **Objects do not change after construction.** Classes are `msgspec.Struct` with `frozen=True`. A
   constructor stores its arguments and may reject a value the object cannot represent (ValueError);
   it never does work. A new state is a new object: `Fairness.after` returns one.
3. **An interface (`typing.Protocol`) exists only where there are two implementations or more:**
   `Factor`, `Limit`, `Backlog` and `Ledger`.
4. **New behaviour is a wrapper, not a flag or a subclass.** Observation, for one, is the caller's
   wrapper around its `Backlog` and `Ledger`. Only exceptions inherit, from `Exception`.
5. **"No value" is an object, not `None`.** An outcome is a result value — `Conflict`, `LeaseLost`,
   `NoTicket` and the rest — that the caller matches on; `None` is not a result.
6. **The decision is pure.** `Limit.admits`, `Factor.scores`, `Priority.first` and `Fairness.after`
   read a `PoolView` and return a value: no I/O, no clock, no randomness. A `Limit`'s answer may turn
   from refuse to admit without a version change, never the reverse, so it reads only `leased`,
   `grants`, `now` and `retention`, of the whole view or of its lane's: time passing, an expiry, a
   release or a withdrawal can only loosen it, and the version check keeps every limit.
7. **One clock, the store's.** Every moment in a view comes from the store. The only other clock is the
   event loop's (`loop.time()`), for when to renew a lease.
8. **Each port has its own adapter.** Producers and workers change for different reasons, so a store
   brings a `Backlog` class and a `Ledger` class that share only its data.
9. **The library does not print or log.** It returns values and raises errors; observing them is the
   caller's.
10. **Types are checked on data, never on behaviour.** `match` reads the closed unions of results;
    objects with behaviour are called through their interface.
11. **Nothing of the caller's type passes through.** A ticket carries its importance as named numbers
    in `0..1`: immutable `Trait` values in a tuple, with unique names. `Trait` checks its value and
    `Ticket` checks its collection where each is built or decoded; the work itself stays in the
    caller's row. So no class has a type parameter.

**Where wicket departs from *Elegant Objects*:**

- **Data has public fields.** `Trait`, `Ticket`, `Waiting`, `Permit` and `PoolView` are immutable records with
  public fields; accessor methods would be noise in Python.
- **A permit is held with `async with`**, Python's usual "take, use, give back".
- **Four plain classes.** `Priority` and `Lane` take their factors or limits as arguments, which a
  `Struct` cannot, and never change. `Pool` holds the lock that orders the `permit()` calls of one
  process, and its private `_Renewal` notes when a permit's lease was lost.
- **The in-memory store changes inside.** Its methods never await, so each runs whole in one event
  loop; its state leaves it only as views.

## Cross-cutting concerns

**Concurrency.** Grants and successful renewals change the pool's version. Every grant is a
compare-and-swap on it; submissions, withdrawals and releases keep the version unchanged.
The `permit()` calls of one `Pool` wait for a grant one at a time behind its lock, avoiding conflicts
between those grants; renewals and other processes can still make a view stale.
On a conflict the pool reads again at once; with nothing to grant it sleeps a random time
between `poll / 2` and `poll`
([decision 2](docs/decisions/0002-pure-decision-over-a-store.md)). Lane presences never change the
version ([decision 8](docs/decisions/0008-lanes.md)).

Each `Waiting` carries a store-generated UUIDv4 `submission_id`. A grant matches it with the pool
and ticket id, returning `Missing` if the ticket was removed and replaced. The identity and its
lifecycle are defined in [decision 2](docs/decisions/0002-pure-decision-over-a-store.md).

Each `Presence` carries the UUID of its last attendance. A leave matches it before removing the
presence, so a stale view cannot erase a later attendance. An attendance checks that no grant or
renewal in its lane has changed the pool since its view, so a stale view cannot make a capped lane
present ([decision 8](docs/decisions/0008-lanes.md)).

**Failure.** Delivery is at least once. Only a block that ends normally releases its ticket; an
error of the body, a cancellation or a lost lease (`LeaseLostError`) releases nothing, so the ticket
is delivered again once its lease ends, and the permit's token lets the caller reject the old
worker's late write ([decision 5](docs/decisions/0005-leases.md)). A store that cannot be reached
raises `StoreUnavailable`, the one store error `Pool` retries, and only while renewing a lease.

**Dependencies.** msgspec; why is in [decision 6](docs/decisions/0006-dependencies.md). The extra
`postgres` adds SQLAlchemy ([decision 7](docs/decisions/0007-postgres-store.md)). Adding a runtime
dependency needs an ADR.
