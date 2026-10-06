# Retries & Dead Letter

## Error types

What a handler returns decides what happens next:

```go
return nil, runnerq.NewRetryError("API temporarily down")   // retry with backoff
return nil, runnerq.NewNonRetryError("invalid payload")     // fail permanently
return nil, someError                                       // treated as retryable
return result, nil                                          // success
```

A panic in a handler is recovered and converted to a retryable error, so a
single bad input can't take down a worker.

## Retries and backoff

A retryable failure reschedules the activity with **exponential backoff**:
roughly `retry_delay * 2^attempt`, capped by `MaxRetryDelay` (default 1 hour).
`MaxRetries` (default 3; `0` means unlimited) bounds the attempts. `RetryCount`
on the `ActivityContext` tells the handler which attempt it's on.

```go
executor.ActivityNamed("call_api").
    Payload(p).
    MaxRetries(10).
    MaxRetryDelay(5 * time.Minute).
    Execute(ctx)
```

## Delivery semantics

RunnerQ is **at-least-once**: an activity's handler may run more than once (a
retry, or a crash-recovery after the work ran but the ack didn't commit).
Completion is **fenced** — only the worker that currently holds the lease can
ack — so exactly one completion record wins, but side effects can repeat.

This is why [durable steps](durable-execution.md) matter: wrap
non-idempotent side effects in `ctx.Run` (or a `.Step` child) so they're
recorded once and skipped on replay.

## Lease-based recovery

When a worker claims an activity it takes a **lease** (a deadline stored on the
row). If the worker crashes or wedges, the lease expires and the **reaper**
(every engine runs one) requeues the activity for another attempt — this is
what makes crash recovery automatic and cluster-wide. A lease expiry counts as
a failed attempt: `retry_count` increments, and an activity that exhausts its
retries this way is dead-lettered rather than looping forever.

The lease covers the activity's whole timeout (`max(engine lease, timeout +
10s)`), so a handler that legitimately runs near its `Timeout` won't be reaped
out from under itself. Tune the floor with `postgres.WithConfig` — see
[Configuration](configuration.md).

### When a live execution loses its lease

A lease can expire under a worker that is still alive: the process stalled
(a VM freeze, a long GC or CPU-throttling pause) or was cut off from the
database for longer than the lease. The reaper then hands the activity to
another worker while the first may still be running. Three mechanisms bound
what the superseded execution can do:

- **Fencing.** Every write an execution makes — completion, failure, park,
  checkpoint, dependency, and child spawn — is conditional on the claim token
  it was issued at dequeue. A superseded execution's writes are rejected with
  `ErrClaimLost`; it cannot complete the activity, publish a checkpoint, or add
  children to the tree.
- **Heartbeat.** While a handler runs, the engine renews its claim every 10s.
  A renewal that finds the claim gone cancels the handler's `Ctx`;
  `context.Cause(ctx.Ctx)` is then an `ErrClaimLost` error. The engine discards
  whatever the handler returns and records `activity_claim_lost`.
- **Checkpoint reads stop first.** `ctx.Run` does not start its function once
  the context is cancelled or the claim is known lost.

What the engine **cannot** do is interrupt a handler that ignores its context,
or undo an external side effect already in flight. Handlers must return
promptly when `ctx.Ctx` is done, and non-idempotent effects belong in `ctx.Run`
with an idempotency key passed to the external system.

## The dead-letter queue

When an activity exhausts its retries (or fails non-retryably) it moves to the
**dead-letter** state. Implement `OnDeadLetter` to react — alert, compensate,
log — or embed `DefaultDeadLetterHandler` for a no-op:

```go
func (h *Charge) OnDeadLetter(ctx runnerq.ActivityContext, payload json.RawMessage, errorMsg string) {
    alert("charge dead-lettered for %s: %s", ctx.ActivityID, errorMsg)
}
```

A dead-lettered activity stores an error result, so any parent awaiting it
resolves (with an error) instead of waiting forever. Dead-letter records are
listable from code (`storage.QueryStorage`).

> `OnDeadLetter` fires for activities dead-lettered by a handler failure. An
> activity dead-lettered by the **reaper** (repeated lease expiry — e.g. a
> handler that keeps crashing the process) has no live handler to call it on;
> watch the `activity_failed_non_retry` metric and query `status = dead_letter`
> through `storage.QueryStorage` for those.
