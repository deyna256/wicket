# 7. The PostgreSQL store is built on SQLAlchemy and ships as an extra

**Status:** Accepted

## Context

A service whose workers run in several processes needs one queue they all see, tickets that
survive a restart, and a ticket written in the same transaction as the caller's own row, so that
the two commit together or not at all. The caller owns that transaction; wicket must only be able
to write inside it, which needs it to speak the caller's database layer. For Python services that
is usually SQLAlchemy.

## Decision

- **SQLAlchemy's asyncio Core**, in the extra `wicket[postgres]` (`sqlalchemy[asyncio]>=2.1.6`, the
  first release with every asyncio cancellation fix up to
  [SQLAlchemy issue #13699](https://github.com/sqlalchemy/sqlalchemy/issues/13699)). The core keeps
  msgspec as its only dependency ([decision 6](0006-dependencies.md)).
- **`PostgresBacklog(session)`** works on the caller's `AsyncSession` and never commits.
  **`PostgresLedger(engine, retention)`** runs each call in a short transaction of its own on a
  connection from the engine. The driver is the caller's, through its URL: asyncpg or psycopg.
- **The tables are `MetaData` for the caller's migrations**, named `wicket_pools`,
  `wicket_tickets` and `wicket_grants`. wicket ships no migrations and creates no tables.
- **Traits are a JSONB array of name/value records**, preserving their tuple order. The reader also
  accepts the name-to-number objects written by 0.1.0 and validates both formats through `Ticket`.
- **The store's clock only**: a view reads `now()`, every other call `clock_timestamp()`, and times
  are read back as UTC.
- **Submission identity is a UUID column with `gen_random_uuid()` as its default.** A grant matches
  it while locking the chosen ticket with `SKIP LOCKED`; withdrawals need no pool-row lock
  ([decision 2](0002-pure-decision-over-a-store.md)).
- **Grant and renewal lock the pool before its ticket.** Renewal increments the existing pool
  version in `READ COMMITTED`, then checks the token and expiry against `clock_timestamp()`
  and updates the lease. It commits both changes together or rolls back if the lease is lost.
  A grant using a view that missed the renewal waits for it and returns `Conflict` if it commits.
  PostgreSQL [rechecks the update condition after a concurrent writer commits](https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED).

## Alternatives

- **psycopg or asyncpg directly.** One layer fewer, but the caller's SQLAlchemy transaction could
  not take the ticket, and the caller would need a second pool of connections.
- **wicket's own migrations.** Updates would apply themselves, but every caller would run two
  migration histories, and Alembic would become a dependency.
- **The caller's transaction in each call** (as River's `InsertTx`). The port would then carry a
  driver's transaction type; binding the session in the adapter's constructor keeps the port free
  of it, as repositories bound to a session do.

## Consequences

- A caller adds `wicket.postgres.metadata` to its migrations' target metadata, and reads the
  changelog for schema changes.
- A caller without SQLAlchemy writes its own adapter of the two ports and runs the store contract
  on it.
- Every process serving a pool passes the same `retention`.
- Upgrade all workers sharing a pool to enforce renewal/version atomicity, including custom
  ledgers. This change needs no schema migration; old renewers can still bypass the guarantee.
- Upgrade every process sharing a pool before writing the new trait format: 0.1.0 readers cannot
  read the arrays. Existing rows need no migration; the JSONB column's type stays the same.
- The PostgreSQL tests need a database: a server URL or Docker; CI runs them on Ubuntu.

## Adding submission IDs to an existing table

Stop producers and workers sharing the table. Add this to the caller's migrations before starting
the updated adapters:

```sql
ALTER TABLE wicket_tickets
    ADD COLUMN submission_id uuid NOT NULL DEFAULT gen_random_uuid();
```

PostgreSQL gives every existing row a UUID and generates one for each future insert. Ticket data,
leases, permit tokens and fairness are preserved. `metadata.create_all()` does not alter an existing
table, so it is not a substitute for this migration.

Upgrade every worker before resuming: old `grant` implementations do not check submission identity.
Custom ledgers and wrappers must now accept the chosen `Waiting` instead of a ticket id. A manually
constructed or decoded `Waiting` also requires `submission_id`; discard old cached snapshots and
read a fresh view after upgrading.
