# Changelog

All notable changes to runnerq-go. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
the policy in [RELEASING.md](RELEASING.md). GitHub release notes list every
merged PR by label; this file is the curated summary.

## Unreleased

Everything on `main` since v0.5.1. Breaking for v0.5.1 users: read
**Upgrading from v0.5.1** first.

### Upgrading from v0.5.1

- **Module path.** The module is `github.com/runnerq/runnerq-go` (was
  `github.com/alob-mtc/runnerq-go`). Rewrite imports with:

  ```bash
  grep -rl alob-mtc/runnerq-go --include='*.go' . | xargs sed -i 's#github.com/alob-mtc/runnerq-go#github.com/runnerq/runnerq-go#g'
  ```

  then `go get github.com/runnerq/runnerq-go@<version>` and `go mod tidy`.
- **Schema.** The Postgres schema now comes from
  [runnerq-spec](https://github.com/runnerq/runnerq-spec) and was reshaped
  (payloads in `runnerq_inputs`, fewer stored events and indexes, no
  `runnerq_worker_pools`). It is not upgrade-compatible with v0.5.1's tables:
  drain the queue and start on a fresh schema.
- **Activity names.** `RegisterActivity(name, h)` is
  `RegisterActivityWithName(name, h)` and `Activity(name)` is
  `ActivityNamed(name)`. `RegisterActivity(&H{})` and
  `runnerq.Activity[H](exec)` derive the name from the handler type.
- **Removed:** the inspector and its web console;
  `storage.EncodedStorage`. `storage.ManagedMaintenanceStorage` is
  `SelfMaintainingStorage`.

### Added

- Durable steps: `runnerq.RunStep(ctx, name, fn)` checkpoints a typed result,
  so a retried workflow skips steps that already succeeded.
- Typed spawns: `runnerq.Activity[T](exec)` spawns the activity served by
  handler type `T`. On Go 1.27+ both helpers are also methods
  (`ctx.RunStep`, `exec.Activity[T]()`).
- Claims are heartbeated while handlers run, and a handler that has lost its
  claim cannot spawn children (fencing).
- Storage contract: encodings and failure details, command storage
  (cancel, retry, reschedule, run now, priority, delete, signal), a durable
  backend conformance suite (`storage/storagetest`), and self-maintaining
  backends.
- Engine snapshots (`executor` package): one source of worker telemetry for
  observers and metrics.
- The `conductor` module (`github.com/runnerq/runnerq-go/conductor`), its own
  Go module so the SDK does not carry its dependencies.
- runnerq-spec's cross-language conformance scenarios run in CI, along with
  shared constants, vectors, and the retry decision and backoff.
- Observability: `LabeledMetricsSink` (labels `queue`, `activity_type`,
  `status`; the old `MetricsSink` works through `UnlabeledMetrics`), gauges
  for queue depth, oldest pending age and activities in flight (off unless
  `GaugeInterval` is set), and a generic `Tracer` that carries the W3C
  `traceparent` in activity metadata. Metric and span names come from
  runnerq-spec. OpenTelemetry is the separate `otel` module
  (`github.com/runnerq/runnerq-go/otel`); the core gains no dependency.
- Managing work from code: `runnerq.NewClient(backend)` (no engine needed)
  and `WorkerEngine.Client()` submit, signal and await activities, read them
  (`Get`, `List`) and manage them (`Cancel`, `Retry`, `Reschedule`, `RunNow`,
  `SetPriority`, `Delete`), idempotent by command id. Cancelling a running
  activity stops its handler on whichever worker holds it (`ErrClaimLost`),
  cascades to children by default, and an awaiting parent sees
  `IsCancelled(err)`. Command fingerprints follow runnerq-spec, so a mixed
  Go/TypeScript fleet replays each other's commands.
- `runnerq.Activity[T](client)`: the package function takes a `Spawner`
  (`*ActivityExecutor` or `*Client`).
- Cron schedules: `engine.RegisterSchedule` (upserted on start) and
  `runnerq.NewSchedules` / `engine.Schedules()` (upsert, get, list, pause,
  resume, trigger, delete). Each tick fires exactly once across a fleet,
  including a mixed Go/TypeScript one; the dialect, time zones, daylight
  saving and catch-up follow runnerq-spec's `schedules.md`. Needs schema
  migration `0003_schedules`.
- `MetricScheduleFires` (`schedule_fires`, label `queue`): activities
  enqueued by firing due schedules, now in runnerq-spec's metric catalog.

### Changed

- Go 1.25 is the minimum (was 1.27 on `main`); CI tests Go 1.25, 1.26 and
  1.27 and Postgres 14 to 18.
- Leaner storage: about half the writes and storage per activity; lease
  renewals are HOT updates.
- A backend's lease setting stands unless the worker config sets one.
- `max_attempts` in queries is the total attempts allowed.
- `ActivityStatus` holds the canonical statuses (lowercase, as in
  runnerq-spec): `StatusRetrying` is gone; `StatusScheduled`, `StatusWaiting`
  and `StatusCancelled` are new.
- Metric name constants are exported (`Metric*`); a claim lost while
  completing is logged as a warning, not an error.

### Fixed

- Result waits wake only active waiters.
- Children spawned on a handler's context get its lineage from the engine's
  executor.

### Security

- pgx 5.12.0 (fixes GO-2026-5004, SQL injection through placeholder
  confusion with dollar-quoted strings) and golang.org/x/text 0.41.0.
- govulncheck and gosec run in CI; Dependabot watches dependencies; see
  [SECURITY.md](SECURITY.md) to report a vulnerability.

## v0.5.1

- Poll for the schema lock instead of blocking.

Earlier releases: see the
[GitHub releases](https://github.com/runnerq/runnerq-go/releases).
