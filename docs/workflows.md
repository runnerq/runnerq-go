# Workflows & Activities

In RunnerQ an **activity** is a unit of work; a **workflow** is just an
activity handler that orchestrates other activities. There's no separate
"workflow" type to learn — orchestration is plain Go control flow inside a
handler.

## The handler interface

```go
type ActivityHandler interface {
    Handle(ctx ActivityContext, payload json.RawMessage) (json.RawMessage, error)
    OnDeadLetter(ctx ActivityContext, payload json.RawMessage, errorMsg string)
}
```

Embed `runnerq.DefaultDeadLetterHandler` for a no-op `OnDeadLetter`. Handlers
must be safe for concurrent use — the engine runs many at once.

A handler returns:

- `(result, nil)` — success; `result` may be nil.
- `(nil, runnerq.NewRetryError(msg))` — retry with backoff.
- `(nil, runnerq.NewNonRetryError(msg))` — fail permanently (no retry).
- any other non-nil `error` — treated as retryable.

See [Retries & Dead Letter](retries-and-dead-letter.md) for the failure model.

## Naming activities

Every activity carries a type string that routes it to a handler. Registration
decides that string, not the handler, and there are **two ways to say it**.
Both are first-class and can be mixed in one program.

| | Typed (recommended) | Named |
|---|---|---|
| Register | `engine.RegisterActivity(&ResizeImage{})` | `engine.RegisterActivityWithName("resize_image", &ResizeImage{})` |
| Spawn | `exec.Activity[ResizeImage]()` | `exec.ActivityNamed("resize_image")` |
| Type string | derived: `"ResizeImage"` | whatever you pass |

**Typed** derives the type from the handler's Go type name, so registration
and spawn can never drift and there is no string to keep in sync. Reach for
it by default.

**Named** takes the string from you. It is not a legacy path: it is how you
pin a type that must outlive a refactor, serve several types from one handler,
or enqueue from a producer that has no Go type in scope. It is also the shape
of the v0.5 API, so code written against v0.5 keeps the same structure
(`RegisterActivity(name, h)` there is `RegisterActivityWithName` here, and
`Activity(name)` is `ActivityNamed`). RunnerQ v0.6+ requires Go 1.27; on an
older toolchain stay on v0.5, where the named form is the only one.

```go
// Typed: the handler type is the name.
engine.RegisterActivity(&ResizeImage{})
exec.Activity[ResizeImage]().Payload(p).Execute(ctx)

// Named: you choose the string.
engine.RegisterActivityWithName("resize_image", &ResizeImage{})
exec.ActivityNamed("resize_image").Payload(p).Execute(ctx)
```

Derivation uses the bare type name — no package prefix, no case changes — and
panics for unnamed types (anonymous structs) and duplicate registrations.
`runnerq.NameOf[T]()` returns the derived string for APIs that take one —
`ActivityTypes` on the builder, `SignalByKey`.

Prefer a pinned name when:

- **The type is persisted.** Every enqueued row stores its activity type, so
  renaming a handler struct strands whatever is already queued, parked, or
  scheduled. A pinned name decouples the store from Go identifiers.
- **One handler type serves several activity types**, as in
  [workload isolation](../examples/12-workload-isolation).
- **Producers are outside this Go module** and enqueue by string.

`Activity[T]` and `NameOf` know nothing about registration: a handler pinned
under `"resize_image"` is spawned with `ActivityNamed("resize_image")`, not
`Activity[ResizeImage]()`.

### Migrating from explicit names (v0.4 and earlier)

Activities already in the store keep the type string they were enqueued
with. If a deploy switches `ChargeCard` from `"charge_card"` to the derived
`"ChargeCard"`, every queued, parked, or scheduled `"charge_card"` row is
dispatched with no matching handler and fails as `handler_not_found`.

Keep the old string until the store has drained it:

```go
// Before: engine.RegisterActivity("charge_card", &ChargeCard{})
engine.RegisterActivityWithName("charge_card", &ChargeCard{})
executor.ActivityNamed("charge_card").Payload(p).Execute(ctx)
```

Producers that enqueue by string — other services, other languages — must
keep sending the legacy type as well, since `NameOf` only reflects the Go
type name and never consults the store.

## The activity context

`ActivityContext` carries everything a handler needs:

| Field / method | Purpose |
|---|---|
| `ActivityID` | this activity's UUID |
| `ActivityType` | the registered type string |
| `RetryCount` | attempt number (0 on first run) |
| `Metadata` | `map[string]string` set at enqueue time |
| `Ctx` | a `context.Context` bounded by the activity timeout |
| `ActivityExecutor` | spawn child activities (tagged as children of this one) |
| `ParentActivityID`, `RootActivityID`, `Depth` | lineage |
| `RunStep`, `Run`, `Sleep`, `WaitForSignal` | durable primitives — see [Durable Execution](durable-execution.md) |

```go
func (h *MyHandler) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
    if id, ok := ctx.Metadata["correlation_id"]; ok {
        // ...
    }
    if ctx.Ctx.Err() != nil {
        return nil, runnerq.NewNonRetryError("cancelled")
    }
    // ...
}
```

## Enqueuing activities

Get an executor from the engine (`engine.GetActivityExecutor()`) or from
`ctx.ActivityExecutor` inside a handler. Both use the same fluent builder:

```go
future, err := executor.ActivityNamed("send_email").
    Payload(payload).
    Priority(runnerq.PriorityHigh).   // Critical > High > Normal (default) > Low
    MaxRetries(5).                     // default 3; 0 = unlimited
    Timeout(2 * time.Minute).          // default 300s
    MaxRetryDelay(10 * time.Minute).   // cap on backoff; default 1h
    Delay(30 * time.Second).           // run no earlier than now+delay
    Metadata("tenant", "acme").
    IdempotencyKeyOption("order-42", runnerq.ReturnExisting).
    Execute(ctx)
```

`Payload` is required. Everything else is optional. `Execute` returns an
`*ActivityFuture`.

### Idempotency keys

`IdempotencyKeyOption(key, behavior)` makes an enqueue exactly-once for that
key. Behaviors:

- `runnerq.ReturnExisting` — if the key exists, return a future for the
  existing activity (don't enqueue a duplicate). The common choice.
- `runnerq.AllowReuse` — repoint the key at a new activity each time.
- `runnerq.AllowReuseOnFailure` — reuse only if the prior activity failed.
- `runnerq.NoReuse` — error if the key already exists.

Keys are scoped per queue. The claim-and-enqueue is a single atomic operation,
so a crash can never leave a key pointing at an activity that was never
created.

## Orchestration: spawning children

Inside a handler, `ctx.ActivityExecutor` spawns child activities tagged with
this activity's lineage. This is how you build multi-step workflows:

```go
func (h *Order) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
    pay, err := ctx.ActivityExecutor.ActivityNamed("charge").Step("charge").Payload(payload).Execute(ctx.Ctx)
    if err != nil { return nil, err }
    if _, err := pay.GetResult(ctx.Ctx); err != nil { return nil, err }

    ship, err := ctx.ActivityExecutor.ActivityNamed("ship").Step("ship").Payload(payload).Execute(ctx.Ctx)
    if err != nil { return nil, err }
    return ship.GetResult(ctx.Ctx)
}
```

Use `.Step(name)` for spawns inside a handler — it makes the workflow
crash-safe (see [Durable Execution](durable-execution.md)). `MaxActivityDepth`
(default 32) caps recursion depth. `.AsRoot()` detaches a spawn from its
parent's lineage for fire-and-forget side jobs.

Lineage follows the context, not only the executor: the engine's executor
(`engine.GetActivityExecutor()`) spawning on a handler's context (`ctx.Ctx`, or
any context derived from it) spawns a child of that handler too. So services a
handler calls can keep an engine executor and still record their spawns under
the running activity, as long as they pass the context along. Spawns on any
other context, or with `.AsRoot()`, are roots.

The two differ in one way: `ctx.ActivityExecutor` fences each spawn on the
handler's claim (a superseded execution can't spawn, at the cost of locking the
parent row in the insert's transaction), while the engine's executor only
records the lineage and inserts as a root spawn does. Use `ctx.ActivityExecutor`
where a duplicate spawn from a superseded execution would matter and no
idempotency key or `.Step` already dedupes it.

## Getting results

`future.GetResult(ctx)` returns the activity's result, blocking until it's
available or `ctx` is done.

- **Outside a handler** (e.g. an HTTP server awaiting a workflow) it simply
  blocks — notification-driven, with a slow poll fallback, and works even if a
  different process produced the result.
- **Inside a handler**, awaiting a child parks the parent durably after a
  short grace — see [Durable Execution](durable-execution.md). "Inside" means
  on the handler's context, however deep in the call stack: a helper awaiting
  on that context returns the park sentinel too (`runnerq.IsYield` recognizes
  it), and must return it unchanged, or wrapped with `%w`, to the handler,
  which returns it. Don't await inside a
  `RunStep`/`Run` function; await between steps.

A failed activity surfaces as a `*WorkerError`:

```go
result, err := future.GetResult(ctx)
if err != nil {
    if we, ok := runnerq.IsWorkerError(err); ok {
        log.Printf("failed (retryable=%v): %v", we.IsRetryable(), we)
    }
    return
}
```

## Awaiting across processes

A future is just an activity ID plus a backend handle, so it's rehydratable —
enqueue a workflow in one process and await its result in another:

```go
// process A:
id := future.ActivityID()                    // persist this handle somewhere

// process B (only has a backend):
fut := runnerq.FutureFor(backend, id)
result, err := fut.GetResult(ctx)
```

`runnerq.WaitAll(ctx, futs...)` awaits several futures and returns their
results in order — handy for fan-out (see
[example 07](../examples/07-fan-out/)).
