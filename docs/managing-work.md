# Managing work

`runnerq.Client` submits, signals, awaits, inspects and manages activities.
It needs only a backend, not a running engine, so a web handler, a CLI or a
cron job can use it from any process that shares the database.

```go
backend, _ := postgres.New(ctx, dsn, "my_app")
client := runnerq.NewClient(backend)
```

An engine hands out a client bound to it with `engine.Client()`. That client
announces submissions like the engine does, spawns children when called on a
handler's context, and stops a handler the engine is running at once when it
cancels that handler's activity.

## Submitting, signalling, awaiting

```go
fut, err := runnerq.Activity[SendInvoice](client).Payload(p).Execute(ctx)
err = client.Signal(ctx, fut.ActivityID(), "approved", json.RawMessage(`{"by":"ops"}`))
result, err := client.Result(ctx, fut.ActivityID())
```

`client.Future(id)` rebuilds the future of an activity submitted elsewhere.
`SignalByKey` signals the activity that owns an idempotency key.

## Reading

```go
rec, err := client.Get(ctx, id) // payload, result and last error included
if runnerq.IsActivityNotFound(err) { ... }

page, err := client.List(ctx, runnerq.ListOptions{
    Statuses: []runnerq.ActivityStatus{runnerq.StatusFailed, runnerq.StatusDeadLetter},
    Type:     "SendInvoice",
    Limit:    100,
})
// page.Items, then page.NextCursor in ListOptions.Cursor for the next page
```

Statuses are the canonical ones: `pending`, `scheduled` (delayed, or waiting
out a retry backoff), `running`, `waiting` (parked on a sleep, signal or
child), `completed`, `failed`, `dead_letter` and `cancelled`. `Filter` takes
any `storage.QueryFilter` for conditions the options don't cover. Reads and
commands are scoped to the backend's queue.

## Commands

| Method | Acts on | Effect |
| --- | --- | --- |
| `Cancel` | unfinished activities | Cancelled, with their unfinished descendants (`NoCascade()` to keep them) |
| `Retry` | failed, dead-lettered, cancelled | Pending again; completed steps replay from checkpoints (`ResetAttempts()` restores the attempt budget) |
| `RunNow` | scheduled, waiting | Runnable now |
| `Reschedule` | scheduled | Runs at the new time |
| `SetPriority` | unfinished | New priority |
| `Delete` | finished roots | Removed with their whole tree |

Each takes a `Target`, either ids or a bounded filter:

```go
res, err := client.Cancel(ctx, runnerq.IDs(id), runnerq.WithReason("customer asked"))

// Retry up to 100 dead letters of a type; repeat while res.More.
res, err = client.Retry(ctx, runnerq.Matching(storage.QueryFilter{
    And: []storage.QueryFilter{
        {Field: "type", Op: storage.OpEq, Value: "SendInvoice"},
        {Field: "status", Op: storage.OpEq, Value: "dead_letter"},
    },
}, 100), runnerq.ResetAttempts())
```

A filter target only selects activities the command can act on, oldest
first, so repeating it makes progress.

### Results

A command doesn't fail because one target is in the wrong state. The result
has one item per target:

| `Outcome` | Meaning |
| --- | --- |
| `Applied` | Changed; `Status` is the status after |
| `NotEligible` | Its status doesn't allow the command; `Status` is current, `Reason` says why |
| `NotFound` | No such activity in this queue |
| `WouldApply` | A `DryRun()` would change it |

```go
if it, _ := res.Item(id); it.Outcome != runnerq.Applied {
    log.Printf("not cancelled: %s (%s)", it.Reason, it.Status)
}
```

`Applied` counts the changed activities and `Cascaded` the descendants a
cancel also cancelled. An error means nothing was applied.

### Idempotent retries

Every command has an id: a fresh one per call, or yours with
`WithCommandID`. Sending the same command again with the same id, say after a
timeout, returns the first result with `Replayed` set and applies nothing,
for at least 24 hours. The same id with different input fails with
`IsCommandConflict`.

```go
res, err := client.Cancel(ctx, runnerq.IDs(id), runnerq.WithCommandID("refund-"+orderID))
```

## Cancelling running work

Cancelling a running activity releases its claim in storage. The worker
running it, in any process, notices at once (the Postgres backend sends a
notification; otherwise the next heartbeat, within 10 seconds) and cancels
the handler's context: `ctx.Ctx.Done()` closes, and `context.Cause(ctx.Ctx)`
is a `storage.ErrClaimLost` error. Anything the handler returns afterwards
is discarded, and the activity is not retried.

```go
func (h *Export) Handle(ctx runnerq.ActivityContext, p json.RawMessage) (json.RawMessage, error) {
    for _, chunk := range chunks {
        if err := ctx.Ctx.Err(); err != nil {
            return nil, err // cancelled, timed out or reclaimed
        }
        ...
    }
}
```

A cancelled activity stores a cancellation error as its result. A parent
awaiting it, and `client.Result`, get an error for which
`runnerq.IsCancelled(err)` is true.
