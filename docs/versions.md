# Version pinning

Step keys are names, so adding, removing and reordering steps is already
safe across a deploy ([Durable Execution](durable-execution.md#versioning-across-deploys)).
Version pinning covers the rest: a deploy that changes what happens *between*
steps, where an old workflow resumed by new code would go wrong. Pinned work
runs only on workers of its version, so old in-flight workflows finish on old
code while new ones start on new code.

It is opt-in. Without a version nothing is stamped and claims are exactly as
before. The rules are runnerq-spec's
[`versions.md`](https://github.com/runnerq/runnerq-spec/blob/main/versions.md),
which the TypeScript SDK follows too, so a mixed fleet pins the same way.

## Turning it on

Give each worker the version of the code it runs, a release tag or a commit:

```go
engine, err := runnerq.Builder().
    Backend(backend).
    Version(os.Getenv("RELEASE")). // e.g. "2026.11.2"
    Build()
```

That worker now:

- pins the roots it submits (`engine.Client()`, `engine.GetActivityExecutor()`)
  and the schedules it declares to its version;
- claims only unstamped work and work pinned to its version.

Children always take their **root's** version, whatever process spawns them,
so a workflow tree runs on one version. Retries, lease recovery, `RunNow` and
wakes (a sleep ending, a signal, a child's result) never change an activity's
version: parked work resumes only on a worker of its version.

To claim older versions too, add them with `Serves`:

```go
runnerq.Builder().Backend(backend).Version("B").Serves("A")
```

`WorkerConfig.Version` and `WorkerConfig.Serves` are the same settings. A
version is 1 to 256 bytes; a worker serves at most 64 versions, its own
included. `Serves` without `Version` is a configuration error.

A worker **without** a version still claims everything, pinned or not. The
guarantee holds once every worker of the queue has one.

## Producers

A standalone client stamps what it submits only when told to:

```go
client := runnerq.NewClient(backend, runnerq.ClientVersion("B"))
schedules, _ := runnerq.NewSchedules(backend, runnerq.ClientVersion("B"))
```

Without `ClientVersion`, its roots are unstamped and every worker may run
them. A schedule's fires carry the version stored with the schedule at
upsert (`ScheduleOptions.CodeVersion`), so a tick is pinned the same way
whichever worker fires it.

## A rolling deploy from A to B

1. Every worker of the queue runs with a version; today's run `A`.
2. Start workers at `B`. Roots submitted by `B` processes are pinned to `B`;
   `A` producers still pin to `A`, and the `A` workers run that work.
3. Move the producers to `B`.
4. Wait until no unfinished work is pinned to `A` (below), then stop the `A`
   workers.

If `A`'s work turns out to be safe on `B`, repin it, or run some `B` workers
with `Serves("A")` and stop the `A` workers.

## Draining: how much work is left on a version

Query unfinished work by version:

```go
rows, err := backend.AggregateActivities(ctx, storage.AggregateQuery{
    Filter: &storage.QueryFilter{Field: "status", Op: storage.OpIn,
        Value: []any{"pending", "scheduled", "running", "waiting"}},
    GroupBy: []string{"version"}, // unstamped work has no "version" key
    Count:   true,
})
```

`version` is also a filter field for `Client.List` and command targets, and
`ActivityRecord.Version` reports it. With gauges on
(`Builder.GaugeInterval`), a worker with a version reports the same counts
as `activities_by_version{queue, version}`.

## Stranded work

Work is stranded when no live worker serves its version. Each worker with a
version and gauges on reports `activities_unserved{queue, version}`: runnable
work pinned to versions it doesn't serve, and 0 for the versions it serves.
It also logs a warning, `Runnable work is pinned to a version this worker
does not serve`, the first time it sees such work for a version.

A version is stranded when every worker reports it unserved, so alert on the
minimum across the queue's workers:

```promql
min by (queue, version) (runnerq_activities_unserved) > 0
```

with a `for:` longer than a rollout's overlap (10 minutes, say).

## Moving work on purpose: Repin

When old work is known to be safe on new code, move it:

```go
// Everything still on A.
res, err := client.Repin(ctx, runnerq.Matching(storage.QueryFilter{
    Field: "version", Op: storage.OpEq, Value: "A",
}, 1000), "B", runnerq.WithReason("fix is compatible"))
```

`Repin` goes through the same command path as `Cancel` and `Retry`: it is
idempotent by `WithCommandID`, supports `DryRun`, and records a `Repinned`
event (`activity.repinned`) with the old and new version. It acts on
unfinished activities, a running one included (its current attempt finishes
where it runs); finished activities and ones already on the version are not
eligible. It is per activity: a child spawned after its root is repinned
takes the root's new version, so move a workflow by its tree
(`root_id`) or a whole version by filtering on `version`.

## Executors

A worker with a version reports it as its executor label `version`
(`executor.Info.Version`, and over the conductor), replacing any label of
that name, so a console reads one version concept.
