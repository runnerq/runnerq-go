# Conductor agent

The agent is its own module, so the SDK doesn't depend on it:

```sh
go get github.com/alob-mtc/runnerq-go/conductor
```

The `conductor` package connects worker processes to RunnerQ Cloud. The agent
dials out over a WebSocket and answers the Cloud's queries from your own
database, so the Cloud never connects into your network or holds database
credentials.

```go
import "github.com/alob-mtc/runnerq-go/conductor"

agent, err := conductor.Start(ctx, engine, conductor.Config{
    URL:          "wss://cloud.runnerq.dev",
    APIKey:       os.Getenv("RUNNERQ_CONDUCTOR_KEY"),
    AllowControl: true, // let operators run commands; omit for read-only
})
if err != nil {
    log.Fatal(err)
}
defer agent.Close(context.Background()) // before the engine stops
engine.Start(ctx)
```

- **Workers only.** Start one agent per engine. Each engine appears in the
  Cloud as an executor, identified by `engine.InstanceID()`, with its live
  in-flight activities and counters pushed as reports: on the interval the
  Cloud sets, and within about a second of a change (an activity starting or
  finishing, or a drain beginning).
- **Labels.** Tag the worker with `WorkerConfig.Labels` (region, deploy
  version); the Cloud shows them whichever way the worker reports.
  `conductor.Config.Labels` adds to them and wins on a clash.
- **Queries, not views.** The Cloud reads through the backend's
  `storage.QueryStorage` (filters, keyset paging, counts, aggregates, events,
  steps, trees). Backends without it serve only live executor state.
- **Off the execution path.** `Start` returns immediately. If the Cloud is
  unreachable the agent retries with backoff (1s → 30s, jittered) and your
  activities are unaffected.
- **Clean shutdowns.** `Close` tells the Cloud the executor is stopping, so a
  deploy isn't reported as a crash.
- **Commands are opt-in.** With `AllowControl: true` (and a backend
  implementing `storage.CommandStorage`) operators can cancel, retry, run
  now, reschedule, reprioritise, delete and signal activities from the Cloud.
  Every command is idempotent and audited. Cancelling a running activity
  stops its handler (immediately when it runs on the executor that received
  the command, otherwise at the next claim heartbeat); anything awaiting it
  receives a cancellation error. Without `AllowControl` the agent is
  read-only and advertises no commands.
- **Live events.** The Cloud subscribes to one executor per app and streams
  its event log (`events.subscribe`), resuming from the last cursor on
  another executor if that one goes away. Events whose transaction commits
  late are caught by rescanning just below the cursor.
- **Live notices.** Submissions, claims and successes store no event (an
  activity's own times record them). While someone is watching the app, each
  agent announces the ones its engine makes instead: what it claims and
  completes, and what its `ActivityExecutor`s submit (`engine.Announce` is the
  hook). A process without an agent announces nothing. Best effort and never
  stored.
- **Metadata-only mode.** Set per app in the Cloud (applied live), or forced
  locally with `MetadataOnly: true`, which the Cloud cannot relax. Payloads,
  results, errors and event details are never sent.
- **Bounded load.** At most `MaxConcurrentRequests` (default 16) requests run
  at once, each limited by `RequestTimeout` (default 30s) or the Cloud's
  deadline, whichever is sooner.
