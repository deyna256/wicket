# wicket

Fair admission of work to shared capacity, for Python.

wicket decides **which waiting piece of work may start now**: by priority, by fairness between the
parties that submit it, and within limits on how much runs at once and how fast it starts. It does
not run the work, retry it or keep its status. It sits beside the queue you already have and tells
your workers what to take next.

> **Status:** the core, the in-memory store and the PostgreSQL store are built and unreleased. The
> decisions behind the design are in
> [docs/decisions](https://github.com/deyna256/wicket/tree/main/docs/decisions).

## The idea

```python
await backlog.submit("materials", Ticket(id=row_id, tenant=str(user_id), traits={"tier": 1.0}))

pool = Pool(
    ledger,
    "materials",
    priority=Priority(
        (FairShare(), 4),
        (Trait("tier"), 2),
        (Age(full_after=timedelta(minutes=30)), 1),
    ),
    limits=(Concurrency(4), Rate(10, per=timedelta(minutes=1))),
    lease=timedelta(minutes=2),
    poll=timedelta(seconds=1),
)

async with pool.permit() as permit:  # waits for a grant, renews the lease while the block runs
    # only a block that ends normally releases; otherwise the ticket comes back after its lease
    await generate(permit.waiting.ticket.id)
```

- Two users who each submit ten tickets get them in turns from the first grant, by Start-time Fair
  Queuing; one who returns after a break gets no burst of turns.
- Priority is a weighted sum of factors, each scoring tickets in `0..1`: whose turn it is, a trait
  you give the ticket such as its plan tier, how long it has waited.
- Limits hold across any number of processes: every grant is a compare-and-swap on the pool's version.
- A worker that dies loses its lease, and the ticket is delivered again; a fencing token rejects the
  old worker's late write.
- State lives behind two small ports, `Backlog` and `Ledger`, with an in-memory store and a
  PostgreSQL one.

## PostgreSQL

`pip install "wicket[postgres]"` brings SQLAlchemy; bring your driver (asyncpg or psycopg).

```python
from wicket.postgres import PostgresBacklog, PostgresLedger, metadata

# in your Alembic env.py: target_metadata = [your_metadata, metadata]

async with session.begin():  # your transaction: the row and its ticket commit together
    session.add(row)
    await PostgresBacklog(session).submit("materials", Ticket(id=str(row.id), tenant=str(user_id)))

ledger = PostgresLedger(engine, retention=timedelta(hours=1))  # the same retention in every process
```

## Documentation

- [Architecture](https://github.com/deyna256/wicket/blob/main/ARCHITECTURE.md): how the code is laid out
  and the rules it keeps.
- [Decisions](https://github.com/deyna256/wicket/tree/main/docs/decisions): why it is shaped this way.
- [Contributing](https://github.com/deyna256/wicket/blob/main/CONTRIBUTING.md).

## License

[MIT](https://github.com/deyna256/wicket/blob/main/LICENSE).
