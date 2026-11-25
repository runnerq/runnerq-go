# Configuration

## Builder options

```go
engine, err := runnerq.Builder().
    Backend(backend).                          // required
    QueueName("my_app").                       // default "default"
    MaxWorkers(8).                             // default 10
    SchedulePollInterval(30 * time.Second).    // only used by non-native backends
    ActivityTypes([]string{"send_email"}).     // workload isolation; default: all types
    Metrics(&MyMetrics{}).                     // default: no-op
    Retention(runnerq.RetentionConfig{...}).   // default: keep everything forever
    ShutdownGrace(30 * time.Second).           // default 30s
    Version("2026.11.2").                      // version pinning; default: none
    Build()
```

### Defaults

| Setting | Default |
|---|---|
| queue name | `"default"` |
| max workers | 10 |
| activity timeout | 300s (set per-activity via `.Timeout()`) |
| max retries | 3 (set per-activity via `.MaxRetries()`) |
| max retry delay | 1h |
| lease | 60s floor (see below) |
| reaper interval | 5s |
| schedule poll interval | 5s (Postgres ignores this — it schedules natively) |
| retention | off (keep forever) |
| shutdown grace | 30s |

## Lease and connection pool tuning

`postgres.New` uses a 60s lease floor and a 25-connection pool. To tune, use
`WithConfig`:

```go
backend, err := postgres.WithConfig(ctx, dsn, "my_app",
    /* defaultLeaseMS */ 30_000,
    /* poolSize       */ 50,
)
```

Sizing guidance:

- **Pool**: roughly `MaxWorkers + a few` per process (headroom for the reaper,
  scheduled processor, stats, and one shared listener connection). With many
  processes, put PgBouncer in front of Postgres.
- **Lease floor**: `WorkerConfig.LeaseMS`, when set, overrides the backend's
  `defaultLeaseMS`. The effective lease is `max(lease floor, (timeout+10s))`,
  so the per-activity `Timeout` usually dominates. Lower the floor only when
  you also use short activity timeouts and want fast crash recovery (see
  [example 02](../examples/02-crash-and-resume/)).

## Scheduling

`.Delay(d)` runs an activity no earlier than `now + d`:

```go
executor.ActivityNamed("send_reminder").Payload(p).Delay(1 * time.Hour).Execute(ctx)
```

For durable, in-workflow waits use `ctx.Sleep` instead — see
[Durable Execution](durable-execution.md).

To run an activity on a cron expression, declare a schedule:
`engine.RegisterSchedule(runnerq.Schedule{Name: "nightly", Cron: "0 2 * * *", Activity: "report"})`.
Every engine of the queue fires it, once per tick. See [Schedules](schedules.md).

## Workload isolation

By default every engine dequeues all activity types. Restrict an engine to
specific types with `ActivityTypes` to keep slow jobs from starving
latency-sensitive ones — multiple engines (even in separate processes) share
one queue:

```go
emailEngine, _ := runnerq.Builder().Backend(backend).
    ActivityTypes([]string{"send_email", "send_sms"}).MaxWorkers(4).Build()

reportEngine, _ := runnerq.Builder().Backend(backend).
    ActivityTypes([]string{"generate_report"}).MaxWorkers(8).Build()

// A no-filter engine is a catch-all: it dequeues EVERY type, so it must
// register a handler for each — otherwise it claims work it can't run and
// fails it as handler_not_found. Register all handlers here, or give every
// type a dedicated filtered fleet.
catchAll, _ := runnerq.Builder().Backend(backend).MaxWorkers(2).Build()
```

An engine whose `Dequeue` filter lists a type with no registered handler
panics at `Start()` with a clear message. See
[example 12](../examples/12-workload-isolation/).

## Retention

By default activities, events, results, and idempotency keys are kept forever.
Opt into cleanup:

```go
engine, _ := runnerq.Builder().Backend(backend).
    Retention(runnerq.RetentionConfig{
        Completed: 7 * 24 * time.Hour,    // completed trees kept 7 days
        Failed:    30 * 24 * time.Hour,   // failed/dead-letter kept 30 days
        Interval:  10 * time.Minute,      // sweep cadence (default 10m)
        BatchSize: 100,                   // roots per sweep (default 100)
    }).Build()
```

The deletion unit is a whole **workflow tree** — a root that's been terminal
past its TTL and has no still-running descendants. The tree's activities,
events, results (including `ctx.Run`/`ctx.Sleep` checkpoints), and idempotency
keys are deleted together, so a retried handler never finds a checkpoint
missing. `Completed` and `Failed` are separate clocks (keep failures longer for
inspection); zero means keep forever for that class. Safe to enable on every
engine — the backend elects one sweeper per queue via an advisory lock.
