# 6. msgspec is the only runtime dependency of the core

**Status:** Accepted

## Context

Values in the core are immutable records, and every store persists tickets, permits and the
pool's `Fairness`.

## Decision

- **msgspec** for every value: frozen `Struct`s whose `__post_init__` checks run on decoding too,
  so a ticket read from a store is checked as one built in code, and the encoding a store uses.
- **The PostgreSQL store is an optional extra,** `wicket[postgres]`, so the core installs with
  msgspec alone; it is built on SQLAlchemy ([decision 7](0007-postgres-store.md)).

## Alternatives

- **dataclasses.** No encoding or validation of their own; every store would write its own.
- **Pydantic.** Heavier, and mutable by default.

## Consequences

- No logging or metrics library: observation is the caller's wrapper.
