# Durable Execution

This is what separates RunnerQ from a plain task queue. An activity handler
that orchestrates work re-runs **from the top** whenever it's retried — after
a crash, a timeout, or a deploy. Durable primitives make that replay cheap and
safe: completed work is checkpointed in Postgres and skipped on replay, and a
workflow can pause for days without holding a worker.

The model is the same as Inngest and DBOS: **not** Temporal-style
deterministic event-sourced replay (which constrains what your code may do),
but **step memoization** — your handler is ordinary Go, and you wrap the parts
that must not repeat.

## The contract (read this once)

- A handler may run **many times**. Code *outside* a durable primitive
  re-executes on every replay. Keep bare code free of side effects, or move
  the side effect into a `ctx.RunStep`.
- Each durable primitive has a **name**. Names must be **stable across
  retries** and **unique within the handler**. The name is the checkpoint key.
- Delivery is **at-least-once**; checkpoints make recorded successes
  **at-most-once**.

## `ctx.RunStep` — checkpoint a side effect

```go
receipt, err := ctx.RunStep("charge-card", func(c context.Context) (Receipt, error) {
    return chargeCard(c, payload)   // runs at most once per recorded success
})
```

`fn` is a `runnerq.Step[R]`: it receives a context derived from the
activity's, so the activity timeout still bounds it, and returns any `R` that
round-trips through `encoding/json`. The result is encoded into the
checkpoint and decoded back into `R` on replay. If a deploy changes `R` so
that a stored result no longer decodes, the step fails with a `NonRetryError`
naming it — retrying cannot fix a shape mismatch.

`ctx.Run(name, func() (json.RawMessage, error))` is the untyped form for
callers already holding JSON. Both share the same checkpoint semantics.

On replay, a step whose result is stored returns it **without re-running
`fn`**. Semantics:

| `fn` returns | Stored? | On replay |
|---|---|---|
| success | yes | returns the stored result |
| `NonRetryError` | yes | returns the same error; `fn` never re-runs |
| retryable error | no | `fn` runs again next attempt |

The one edge: a crash *between* `fn` returning and the checkpoint committing
re-runs `fn`. Make `fn` as idempotent as the downstream allows (pass a
provider idempotency key to your payment API, etc.). See
[example 04](../examples/04-checkpoint-side-effects/).

## `.Step(name)` — memoize a child activity spawn

```go
fut, err := ctx.ActivityExecutor.
    Activity[ReserveInventory]().
    Step("reserve").
    Payload(payload).
    Execute(ctx.Ctx)
```

`.Step(name)` derives an idempotency key from `(root, parent, name)` with
`ReturnExisting`. On a parent replay, the same spawn **reattaches to the
existing child** instead of creating a duplicate, and `GetResult` returns its
memoized result. This is how a retried orchestrator fast-forwards through
children it already launched. `Step` is handler-only and incompatible with
`AsRoot`/`IdempotencyKeyOption`. See
[example 03](../examples/03-steps-and-retries/).

## `ctx.Sleep` — a durable timer

```go
ctx.Sleep("cooling-off", 24 * time.Hour)
```

The wake deadline is persisted, so a crash or redeploy mid-sleep resumes with
only the **remainder** — a 24h sleep doesn't restart. A sleep short enough to
fit the handler's timeout budget waits in-process; a longer one **yields**:
the activity is parked in Postgres (the worker is freed entirely) and
re-dispatched when the timer fires. A million sleeping workflows cost a million
rows, not a million goroutines. Always propagate `Sleep`'s error unchanged.
See [example 05](../examples/05-durable-sleep/).

## `ctx.WaitForSignal` — pause for an external event

```go
// In the workflow — parks until delivery (or 48h), holding no worker:
decision, err := ctx.WaitForSignal("approval", 48*time.Hour)
if runnerq.IsSignalTimeout(err) { return escalate(ctx) }

// From anywhere — deliver the signal:
engine.Signal(ctx, activityID, "approval", payload)            // with an engine
runnerq.SignalActivity(ctx, backend, activityID, "approval", payload)  // backend only
```

Signals are **buffered**: one delivered before the wait is reached — or before
the activity even starts — resolves instantly, including on replay. The wait
deadline persists across parks (the timeout measures from the first wait, not
per attempt). Per name, the last delivered payload wins. `timeout` of 0 waits
forever; a timeout surfaces as a non-retryable error (`IsSignalTimeout`). The
handle to signal is `future.ActivityID()`. See
[example 06](../examples/06-human-approval/).

## Awaiting children: parents park, they don't block

When a handler calls `GetResult` on a child, it waits in-process for a short
grace (~2s — fast children stay on the fast path), then **yield-parks** the
parent: no goroutine, no lease, no retry consumed. The child's completion
(success, failure, or dead-letter) wakes the parent, which replays,
fast-forwards through its memoized steps, and continues.

Consequences:

- **A workflow can outlive any number of handler invocations, restarts, and
  deploys.** A parent awaiting a child for hours holds nothing.
- Awaiting longer than the parent's `Timeout` no longer fails it — parking
  isn't a timeout.
- `WaitAll(ctx, futs...)` is already efficient under this model: the first
  unfinished child parks the parent, and each wake fast-forwards the finished
  ones.

## The `waiting` status

Parked activities (durable sleeps, signal waits, parents awaiting children)
sit in a dedicated `waiting` state — distinct from `scheduled` (a `Delay`d
activity) and `processing`. Queries show it with the reason; signal delivery wakes only
`waiting` rows, never `Delay`-scheduled ones.

## Putting it together

```go
func (h *FulfillOrder) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
    receipt, err := ctx.RunStep("charge", func(c context.Context) (Receipt, error) {
        return chargeCard(c, payload)
    })
    if err != nil { return nil, err }

    fut, err := ctx.ActivityExecutor.Activity[ShipOrder]().Step("ship").Payload(payload).Execute(ctx.Ctx)
    if err != nil { return nil, err }
    if _, err := fut.GetResult(ctx.Ctx); err != nil { return nil, err }

    if err := ctx.Sleep("delivery-window", 48*time.Hour); err != nil { return nil, err }

    feedback, err := ctx.WaitForSignal("delivered-confirmation", 7*24*time.Hour)
    if runnerq.IsSignalTimeout(err) { feedback = nil } else if err != nil { return nil, err }

    closed, err := ctx.RunStep("close-order", func(c context.Context) (Order, error) {
        return closeOrder(c, receipt, feedback)
    })
    if err != nil { return nil, err }
    return json.Marshal(closed)
}
```

Crash this anywhere and restart: `charge` and `ship` replay from checkpoints,
the sleep resumes its remainder, the signal wait re-parks — no double charge,
no duplicate shipment.

## Versioning across deploys

Step keys are **name-based, not sequence-based**, which makes deploys far more
forgiving than replay-deterministic engines:

- **Reordering** steps — safe.
- **Adding** a step — safe.
- **Removing** a step — safe (leaves a harmless orphaned checkpoint, cleaned by
  [retention](configuration.md#retention)).
- **Renaming** a step while workflows that used the old name are still in
  flight — **dangerous**: the new name is a new key, so the side effect runs
  again.

Rules of thumb:

- Never rename a live `Step`/`Run`/`Sleep`/`WaitForSignal` name.
- Gate behavioral changes on a version field in the payload, not on editing
  live step sequences.
- Remember code between steps re-runs on replay — keep it pure or wrap it.

## Quick reference

| Primitive | What it does | On replay |
|---|---|---|
| `ctx.Run(name, fn)` | checkpoint local work | stored success/permanent-failure returned without re-running `fn` |
| `.Step(name)` spawn | memoize a child spawn | same child reattached; memoized result instant |
| `ctx.Sleep(name, d)` | durable timer | waits only the remainder; long sleeps park (worker freed, no retry) |
| `ctx.WaitForSignal(name, t)` | pause for an event | buffered delivery resolves instantly; deadline persists |
| `fut.GetResult(ctx)` (in handler) | await a child | ~2s grace then parks; child completion wakes it |
