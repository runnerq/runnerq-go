# Observability

Query the backend directly from code, read each worker's live state, and
forward counters to your metrics system. To connect workers to a console, see
the [conductor agent](conductor.md).

## Reading activities from code

Backends that implement `storage.QueryStorage` (the built-in Postgres backend
does) answer filtered and paged activity lists, counts, aggregates, events,
steps and trees, across every queue in the database. For example, how many
activities are pending, running, waiting or dead-lettered right now:

```go
qs, ok := backend.(storage.QueryStorage)
if !ok {
    return errors.New("this backend doesn't support queries")
}
rows, err := qs.AggregateActivities(ctx, storage.AggregateQuery{
    Filter:  &storage.QueryFilter{Field: "status", Op: "in", Value: []any{"pending", "running", "waiting", "dead_letter"}},
    GroupBy: []string{"status"},
    Count:   true,
})
if err != nil {
    return err
}
for _, r := range rows.Rows {
    fmt.Println(r.Key["status"], r.Count)
}
```

## Metrics

Provide a `MetricsSink` to forward counters into Prometheus/StatsD/etc.:

```go
type MetricsSink interface {
    IncCounter(name string, value uint64)
    ObserveDuration(name string, dur time.Duration)
}

engine, _ := runnerq.Builder().Backend(backend).Metrics(&MyMetrics{}).Build()
```

The default is `runnerq.NoopMetrics`. Counters currently emitted:

| Counter | Meaning |
|---|---|
| `activity_started` | claimed, and its handler started here |
| `activity_completed` | completed successfully |
| `activity_completion_error` | recording a completion failed for a reason other than a lost claim: a permanent storage error, or shutdown cancelled the retries |
| `activity_completion_pending_started` | started recording a completion |
| `activity_completion_pending_finished` | finished recording a completion, successfully or not; started minus finished is how many are in progress |
| `activity_retry` | requested a retry |
| `activity_failed_non_retry` | failed permanently |
| `activity_timeout` | exceeded its timeout |
| `activity_dead_lettered` | ran out of attempts (after a retry request or a timeout) |
| `activity_dead_letter_hook_panic` | a handler's `OnDeadLetter` hook panicked (the panic is recovered and logged) |
| `activity_claim_lost` | the execution lost its claim — cancelled mid-handler by the heartbeat, or its ack was rejected by the fence (the replacement execution owns the outcome) |
| `activity_heartbeat_failed` | a claim renewal failed and will be retried on the next beat |
| `activity_yielded` | parked for a durable wait (sleep/signal/await) |
| `activity_trees_swept` | workflow trees deleted by retention |
| `storage_retry` | a storage write (completion, failure, park, checkpoint, dependency registration) failed transiently and is being retried |

Durations go to `ObserveDuration`: `activity_claim_lag` is how long an
activity waited, from when it was due, until a worker started it, and
`activity_completion_persistence` how long recording a completion took.

> Gauges (queue depth) and per-type labels are not wired yet. A richer
> Prometheus-shaped surface is on the roadmap.

## Executor snapshots

`engine.Snapshot()` is the engine as it is now: its identity (id, queue,
activity types, concurrency, host, SDK version, labels), what it's running,
whether it's draining, and counters since it was built (claimed, succeeded,
retried, failed, timed out, dead-lettered, claims lost, heartbeat failures,
last claim lag). The counters come from the same metrics the sink sees.

Anything that reports a worker reads it. An `executor.Observer` attached
with `engine.Observe(o)` before `Start` is told when the engine starts and
stops, and reads its snapshots meanwhile.

The engine is also an `executor.Notifier`: `engine.Changed()` returns a
channel closed at its next change (an activity starting or finishing, or a
drain beginning). `executor.Report` reports on an interval and soon after
changes, spaced by a minimum gap so a busy worker doesn't flood its
destination; an `executor.Observer` can report through it.
