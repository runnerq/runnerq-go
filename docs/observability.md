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

The engine reports every metric of `runnerq.MetricCatalog` (the shared list
in runnerq-spec `observability/metrics.json`; the TypeScript SDK emits the
same names) to a `LabeledMetricsSink`:

```go
type LabeledMetricsSink interface {
    AddCounter(name string, value uint64, labels MetricLabels)
    RecordHistogram(name string, value float64, labels MetricLabels) // seconds for durations
    SetGauge(name string, value float64, labels MetricLabels)
}

engine, _ := runnerq.Builder().Backend(backend).LabeledMetrics(sink).Build()
```

`MetricLabels` carries `Queue`, `ActivityType` and, for `queue_depth`,
`Status`; the catalog says which labels each metric has. Use the `Metric*`
constants for names. Calls come from hot paths on many goroutines, so a sink
must be concurrency-safe and must not block. For OpenTelemetry, use
`otel.NewMetrics()` from `github.com/runnerq/runnerq-go/otel` (below).

The older `MetricsSink` (`IncCounter`, `ObserveDuration`) still works:
`Builder.Metrics(sink)` wraps it with `UnlabeledMetrics`, which drops the
labels, passes histograms to `ObserveDuration` as durations, and drops the
gauges. The default is `runnerq.NoopMetrics`.

| Metric | Kind | Meaning |
|---|---|---|
| `activity_started` | counter | claimed, and its handler started here |
| `activity_completed` | counter | completed successfully |
| `activity_completion_error` | counter | recording a completion failed for a reason other than a lost claim: a permanent storage error, or shutdown cancelled the retries |
| `activity_completion_pending_started` | counter | started recording a completion |
| `activity_completion_pending_finished` | counter | finished recording a completion, successfully or not; started minus finished is how many are in progress |
| `activity_retry` | counter | requested a retry |
| `activity_failed_non_retry` | counter | failed permanently |
| `activity_timeout` | counter | exceeded its timeout |
| `activity_dead_lettered` | counter | ran out of attempts (after a retry request or a timeout) |
| `activity_dead_letter_hook_panic` | counter | a handler's `OnDeadLetter` hook panicked (the panic is recovered and logged) |
| `activity_claim_lost` | counter | the execution lost its claim — cancelled mid-handler by the heartbeat, or its ack was rejected by the fence (the replacement execution owns the outcome) |
| `activity_heartbeat_failed` | counter | a claim renewal failed and will be retried on the next beat |
| `activity_yielded` | counter | parked for a durable wait (sleep/signal/await) |
| `activity_trees_swept` | counter | workflow trees deleted by retention (`queue` label only) |
| `storage_retry` | counter | a storage write (completion, failure, park, checkpoint, dependency registration) failed transiently and is being retried |
| `activity_claim_lag` | histogram (s) | how long an activity waited, from when it was due, until a worker started it |
| `activity_execution` | histogram (s) | how long an attempt took here, until its outcome was recorded |
| `activity_completion_persistence` | histogram (s) | how long recording a completion took |
| `queue_depth` | gauge | activities in the queue by type and status (`pending`, `scheduled`, `running`, `waiting`, `dead_letter`) |
| `queue_oldest_pending_age` | gauge (s) | how long the oldest pending activity has been due; 0 when none is |
| `activities_in_flight` | gauge | attempts running on this worker, for every type it serves |

### Gauges

Gauges are off by default. `Builder.GaugeInterval(30*time.Second)` (or
`WorkerConfig.GaugeInterval`) samples them about that often, jittered by up
to a fifth so a fleet's workers don't query in step. `activities_in_flight`
is the worker's own count. `queue_depth` and `queue_oldest_pending_age` need a
backend that implements `storage.QueryStorage` (the Postgres backend does):
each sample costs one `AggregateActivities` call and one single-row
`QueryActivities` call. A status that empties out is reported as 0. In a big
fleet, turning gauges on for a few workers is enough.

## Tracing

The core module depends on no tracing library. It calls a `runnerq.Tracer`
(`Start(ctx, name, parent, attrs...)` and `Traceparent(ctx)`), which
`Builder.Tracer` or `engine.SetTracer` sets (for a standalone `Client`,
`runnerq.NewClient(backend, runnerq.WithTracer(t))`), for spans named by the
`Span*` constants:

| Span | When |
|---|---|
| `runnerq.enqueue` | each enqueue from the engine's executors or a client, ended once the insert commits |
| `runnerq.attempt` | each attempt, from claim until its outcome is recorded |
| `runnerq.step` | each `Run`/`RunStep` that runs its function (replays start none) |
| `runnerq.sleep` | each `Sleep` reached; ends `elapsed` or `parked` |
| `runnerq.signal.wait` | each `WaitForSignal`; ends `received`, `parked` or `timeout` |
| `runnerq.signal.send` | `Signal` and `SignalByKey` on an engine or a client |

Every span ends with a `runnerq.outcome` attribute, and with its error when
there is one. Its other attributes are `runnerq.queue`, `runnerq.activity.type`,
`runnerq.activity.id`, `runnerq.activity.parent_id`, `runnerq.activity.root_id`,
`runnerq.attempt`, `runnerq.step.name`, `runnerq.signal.name` and
`runnerq.wake_at`.

**Trace context in storage.** An enqueue stores a W3C `traceparent` in the
activity's metadata (`runnerq.MetadataTraceparent`). It is the value you set
yourself with `Metadata("traceparent", …)` if you set one, otherwise the
enqueue span's, otherwise, inside a handler, the running activity's. Each
attempt span uses the stored value as its parent. So retries, and the
attempts that resume after a durable sleep or signal wait, all hang under the
enqueue span, and children join the same trace. Without a tracer, children
still copy their parent's traceparent, so a TypeScript or Go worker with
tracing on picks up the trace further down the tree.

### OpenTelemetry

`github.com/runnerq/runnerq-go/otel` is a separate module:

```sh
go get github.com/runnerq/runnerq-go/otel
```

```go
import rqotel "github.com/runnerq/runnerq-go/otel"

engine, err := runnerq.Builder().
    Backend(backend).
    Tracer(rqotel.NewTracer()).            // global TracerProvider; WithTracerProvider(tp) to choose
    LabeledMetrics(rqotel.MustNewMetrics()). // global MeterProvider; WithMeterProvider(mp)
    GaugeInterval(30 * time.Second).
    Build()
```

Spans are producer spans (enqueue, signal send), consumer spans (attempt) and
internal spans (the rest). Metrics become instruments named `runnerq.` plus
the catalog name (`runnerq.activity_started`, which a Prometheus exporter
shows as `runnerq_activity_started_total`), with the catalog's unit and the
labels `queue`, `activity_type` and `status` as attributes.

## Logs

The engine logs through `log/slog`'s default logger; set it with
`slog.SetDefault` (for example, a JSON handler). The lifecycle messages and
their fields (`activity_id`, `activity_type`, `reason`, `error`, `step`,
`wake_at`, `queue`) are the same as the TypeScript SDK's `logger` option uses
(runnerq-spec `observability/README.md`).

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
