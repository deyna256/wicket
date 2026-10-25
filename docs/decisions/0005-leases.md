# 5. Work is leased, delivered at least once and fenced by a token

**Status:** Accepted

## Context

A worker can die, hang or lose the store while it holds work. The work must not be lost, and an old
worker that wakes up must not overwrite the result of the worker that took the ticket after it.

## Decision

- **A permit is a lease** that `pool.permit()` renews `lease / 3` after the grant was asked for,
  then `lease / 3` after each successful renewal was asked for, while the block runs. A
  renewal that raises `StoreUnavailable` is retried every `lease / 30` until the lease would end,
  measured on the event loop's clock.
- **Only a block that ends normally releases,** because a release means the work is done, as an
  [SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
  consumer deletes a message only after processing it. An error of the body, a cancellation from
  outside and a lost lease release nothing, and the ticket is delivered again once its lease ends.
- **A lost lease cancels the block** with `LeaseLostError`, even if the body caught the
  cancellation and ended normally.
- **A grant that arrives too late never enters the block.** If the grant's response comes after
  its lease could have ended, measured from when the grant was asked for on the event loop's clock,
  `permit()` raises `LeaseLostError` without running the block, and the ticket is delivered again.
  `LeaseLostError` means only a lost lease.
- **Expiry is the store's.** Once the store's clock passes the lease end the ticket is waiting again,
  with its `submitted_at` and grant count kept.
- **A late release succeeds** while no one has leased the ticket again; it removes a ticket that is
  already waiting again, which no limit counts. After a re-grant or a withdrawal it answers `LeaseLost` and
  nothing is raised.
- **The token is the pool's version after the grant,** increasing within the pool. The caller stores
  it with its row and rejects a write that carries an older one. Renewal keeps this token while
  advancing the pool's version ([decision 2](0002-pure-decision-over-a-store.md)), so tokens may
  have gaps and the original permit remains valid for renewal and release.
- **When to give up is the caller's,** from `permit.attempt` or the error: it records the failure in
  its own row and ends the block normally.

## Alternatives

- **Release on a lost lease.** In simulation it dropped work that was never finished.
- **Release on an error of the body.** The ticket is gone, a retry needs a resubmit in another
  transaction, which a crash between the two loses, and `attempt` never counts the errors.
- **Exactly once.** It needs the store and the work in one transaction, which wicket does not own.

## Consequences

- The caller's work must be idempotent.
- A failed ticket, or one whose worker shut down, waits up to `lease` before it is delivered again.
  A hand-back operation can come when that matters.
- A hung worker whose lease expired may still run, so real concurrency can exceed `Concurrency(n)`
  until it stops. A body that blocks the event loop cannot be cancelled and stops its own renewals.
