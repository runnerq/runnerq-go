# Limits

A limit caps one activity type of a queue: how many run at once across every
worker, how many start per period, or both. Limits are stored with the queue
(`runnerq_limits`), so every engine of the queue, Go or TypeScript, enforces
the same value, and a worker that crashes leaves nothing to drift: the running
count is read from the activities themselves on every claim.

Use them where a handler talks to something with its own limits: a mail
provider, a payment API, a model endpoint.

## Declaring limits

Pass options where you register the handler:

```go
engine.RegisterActivity(&SendEmail{},
    runnerq.MaxConcurrent(10),          // at most 10 running, fleet-wide
    runnerq.RateLimit(5, time.Second),  // at most 5 started per second, fleet-wide
)
engine.RegisterActivity(&ResizeImage{},
    runnerq.MaxConcurrentPerWorker(2),  // at most 2 on this engine
)
```

| Option | Scope | Stored |
| --- | --- | --- |
| `MaxConcurrent(n)` | Every worker of the queue | Yes |
| `RateLimit(n, per)` | Every worker of the queue | Yes |
| `MaxConcurrentPerWorker(n)` | This engine, within `MaxWorkers` | No |

`Start` stores the declared `MaxConcurrent` and `RateLimit` of each type,
replacing what was stored. A type registered without them keeps whatever limit
is stored for it, so a limit set at runtime survives restarts of workers that
don't declare one. Two deployments declaring different limits for one type
overwrite each other on each start.

## What counts

`MaxConcurrent` counts **running** activities of the type: claimed and not yet
finished. An activity parked on `Sleep`, `WaitForSignal` or a child holds no
worker and doesn't count, nor does one waiting out a retry's backoff. A worker
that dies keeps its activities counted until their lease expires and the
reaper requeues them.

`RateLimit(n, per)` is a token bucket: a burst of up to `n`, refilled at `n`
per `per`, by the database clock. Every claim takes a token, retries and wakes
included. `RateLimit(5, time.Second)` starts at most 5 at once, then one every
200ms on average.

A limit only holds work back. Within a type, claims keep their order
(priority, then attempts, then age); other types are not held up. A delayed
activity is due at its scheduled time and starts when its type has room.

## Changing limits at runtime

Any client of the queue can set, read, list and delete limits:

```go
client := runnerq.NewClient(backend)
err := client.SetLimit(ctx, runnerq.Limit{ActivityType: "SendEmail", MaxConcurrent: 3})
limit, err := client.GetLimit(ctx, "SendEmail")
limits, err := client.ListLimits(ctx)
err = client.DeleteLimit(ctx, "SendEmail")
```

`SetLimit` replaces the whole limit: leave `MaxConcurrent` 0 for no cap and
`RateLimit` 0 for no rate. Restating the same rate keeps its bucket, so a
fleet restart doesn't refill it.

Lowering a limit never stops running work: activities already running finish,
and the type starts nothing until it is under the new limit. A changed or
deleted limit binds every worker's next claim; a limit added to a type that
had none binds every worker within a second.

## Cost

Claims of unlimited types run exactly as without limits. A claim that
includes a limited type takes a transaction instead of one statement: it locks
the type's limit row, counts its running activities and claims up to the room
left. Workers wait for each other on that lock only while there is work and
room; a type at its limit is skipped without it. When a limit holds work back,
the engine checks again within 200ms instead of waiting for new work.

Every worker serving a limited type must run a version with limits: older
ones ignore them.

## Storage

Limits need a backend implementing `storage.LimitStorage`; the Postgres
backend does. `Start` fails when a handler declares `MaxConcurrent` or
`RateLimit` on a backend without it. `MaxConcurrentPerWorker` works with any
backend. The rules every implementation follows are in runnerq-spec's
[limits.md](https://github.com/runnerq/runnerq-spec/blob/main/limits.md).
