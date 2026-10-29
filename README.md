<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/deyna256/wicket/main/docs/assets/mark-dark.svg">
  <img alt="wicket" src="https://raw.githubusercontent.com/deyna256/wicket/main/docs/assets/mark-light.svg" height="72">
</picture>

<h3>Which waiting job may start now?</h3>

<p>Fair admission of work to shared capacity, for asyncio Python:<br>
priority, fairness between tenants, and limits that hold across every process.</p>

[![PyPI](https://img.shields.io/pypi/v/wicket)](https://pypi.org/project/wicket/)
[![Python](https://img.shields.io/pypi/pyversions/wicket)](https://pypi.org/project/wicket/)
[![CI](https://github.com/deyna256/wicket/actions/workflows/ci.yml/badge.svg)](https://github.com/deyna256/wicket/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](https://github.com/deyna256/wicket/blob/main/LICENSE)

[Quick start](https://github.com/deyna256/wicket#quick-start) ·
[PostgreSQL](https://github.com/deyna256/wicket#postgresql) ·
[Decisions](https://github.com/deyna256/wicket/tree/main/docs/decisions) ·
[Changelog](https://github.com/deyna256/wicket/blob/main/CHANGELOG.md)

</div>

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/deyna256/wicket/main/docs/assets/demo-dark.svg">
    <img alt="alice asks for 6 reports, bob for 2, carol for 1. In a plain queue carol starts last, 9th; with wicket she starts 3rd" src="https://raw.githubusercontent.com/deyna256/wicket/main/docs/assets/demo-light.svg" width="720">
  </picture>
</p>

## Why wicket

One tenant submits a thousand jobs, another submits one. A plain queue makes the second wait behind
the first thousand. wicket sits beside the queue you already have and tells your workers what to
take next.

- **Fair from the first grant.** Tenants get their turns by Start-time Fair Queuing; one who comes
  back after a break gets no burst of turns.
- **Priority you can read.** A weighted sum of factors: whose turn it is, a trait such as the plan
  tier, how long the job has waited.
- **Limits across processes.** Concurrency and rate hold for any number of workers: every grant is a
  compare-and-swap on the pool's version.
- **Safe when a worker dies.** Its lease ends and the job is delivered again; a fencing token rejects
  the old worker's late write.
- **Small.** One dependency, msgspec. An in-memory store, and a PostgreSQL one that commits a job
  in your own transaction.

wicket does not run the work, retry it or keep its status. It decides what starts.

## Quick start

```sh
pip install wicket
```

```python
import asyncio
from datetime import timedelta

from wicket import (
    Concurrency,
    FairShare,
    MemoryBacklog,
    MemoryLedger,
    MemoryState,
    Pool,
    Priority,
    Ticket,
)


async def main() -> None:
    state = MemoryState()
    backlog, ledger = MemoryBacklog(state), MemoryLedger(state)

    # alice floods the queue first, bob asks once
    for n in range(3):
        await backlog.submit("reports", Ticket(id=f"alice-{n}", tenant="alice"))
    await backlog.submit("reports", Ticket(id="bob-0", tenant="bob"))

    pool = Pool(
        ledger,
        "reports",
        priority=Priority((FairShare(), 1)),
        limits=(Concurrency(2),),
        lease=timedelta(seconds=30),
        poll=timedelta(milliseconds=50),
    )

    async def worker() -> None:
        async with pool.permit() as permit:  # waits for its turn, keeps the lease while it runs
            print("started", permit.waiting.ticket.id)
            await asyncio.sleep(0.1)  # the work

    await asyncio.gather(*(worker() for _ in range(4)))


asyncio.run(main())
```

```text
started alice-0
started bob-0
started alice-1
started alice-2
```

Bob submitted last and starts second. Only a block that ends normally releases its ticket; after an
error the ticket comes back once its lease ends.

## Priority and limits

Factors score tickets in `0..1` and are weighted; limits hold together:

```python
from wicket import Age, Rate, Trait, TraitFactor

pool = Pool(
    ledger,
    "materials",
    priority=Priority(
        (FairShare(), 4),
        (TraitFactor("tier"), 2),
        (Age(full_after=timedelta(minutes=30)), 1),
    ),
    limits=(Concurrency(4), Rate(10, per=timedelta(minutes=1))),
    lease=timedelta(minutes=2),
    poll=timedelta(seconds=1),
)

ticket = Ticket("task-1", "alice", traits=(Trait("tier", 1.0), Trait("urgent", 0.5)))
```

`Trait(name, value)` is an immutable number in `0..1` on a ticket. `TraitFactor(name)` scores
tickets by that named value, or `0` when absent. A ticket takes a tuple of traits with unique names;
neither the tuple nor its traits can change. Tickets encode and decode with msgspec without hooks.

## Lanes

Kinds of work that share one capacity are lanes of one pool: each lane has limits of its own, the
pool's limits count them all, and a freed slot goes to the tenant whose turn it is in any lane.

```python
from wicket import Lane

await backlog.submit("ai", Ticket(id="r1", tenant="alice", lane="mentor"))

mentor = Pool(
    ledger,
    "ai",
    lane=Lane("mentor", Rate(8, per=timedelta(minutes=1))),
    limits=(Rate(10, per=timedelta(minutes=1)),),
    priority=Priority((FairShare(), 1)),
    lease=timedelta(minutes=2),
    poll=timedelta(seconds=1),
)
```

Every process serving a pool must configure the same pool limits and priority; the rules and costs
are in [decision 8](https://github.com/deyna256/wicket/blob/main/docs/decisions/0008-lanes.md).

## PostgreSQL

The extra brings SQLAlchemy; bring your driver, asyncpg or psycopg.

```sh
pip install "wicket[postgres]"
```

```python
from wicket.postgres import PostgresBacklog, PostgresLedger, metadata

# in your Alembic env.py: target_metadata = [your_metadata, metadata]

async with session.begin():  # your transaction: the row and its ticket commit together
    session.add(row)
    await session.flush()  # the row gets its id
    await PostgresBacklog(session).submit("materials", Ticket(id=str(row.id), tenant=str(user_id)))

ledger = PostgresLedger(engine, retention=timedelta(hours=1))  # the same retention in every process
```

## Status

wicket is pre-alpha: the API may change before 1.0, and every change is in the
[changelog](https://github.com/deyna256/wicket/blob/main/CHANGELOG.md).

## Documentation

- [Architecture](https://github.com/deyna256/wicket/blob/main/ARCHITECTURE.md): how the code is laid
  out and the rules it keeps.
- [Decisions](https://github.com/deyna256/wicket/tree/main/docs/decisions): why it is shaped this way.

## Contributing

Issues and pull requests are welcome. Read
[CONTRIBUTING.md](https://github.com/deyna256/wicket/blob/main/CONTRIBUTING.md) first, and follow the
[Code of Conduct](https://github.com/deyna256/wicket/blob/main/CODE_OF_CONDUCT.md). Report
vulnerabilities privately, as the
[security policy](https://github.com/deyna256/wicket/blob/main/SECURITY.md) says.

## License

[MIT](https://github.com/deyna256/wicket/blob/main/LICENSE)
