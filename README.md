# RunnerQ for Go

**Durable Golang functions, with pluggable storage.**

[![Go Reference](https://pkg.go.dev/badge/github.com/alob-mtc/runnerq-go.svg)](https://pkg.go.dev/github.com/alob-mtc/runnerq-go)
[![CI](https://github.com/runnerq/runnerq-go/actions/workflows/ci.yml/badge.svg)](https://github.com/runnerq/runnerq-go/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/runnerq/runnerq-go?sort=semver)](https://github.com/runnerq/runnerq-go/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[Getting started](docs/getting-started.md) · [Docs](docs/) · [Examples](examples/)

Add crash-proof background jobs and multi-step workflows to your Go app in a
few lines — no separate orchestrator to run, no new infrastructure. RunnerQ is
a library: import it, point it at your storage (PostgreSQL is built in), and
write workflows as ordinary Go functions. When a process crashes mid-workflow, it
resumes from where it left off without redoing completed work.

```go
func (h *Checkout) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
    // Each step is checkpointed. Crash after the charge is recorded and
    // restart — it isn't repeated; the workflow resumes at shipping.
    receipt, err := ctx.RunStep("charge-card", func(c context.Context) (Receipt, error) {
        return chargeCard(c, payload)   // at most once per recorded success
    })
    if err != nil {
        return nil, err
    }

    ship, err := ctx.ActivityExecutor.Activity[Ship]().Step("ship").Payload(payload).Execute(ctx.Ctx)
    if err != nil {
        return nil, err
    }
    return ship.GetResult(ctx.Ctx)   // parent parks here — frees the worker, survives deploys
}
```

> **The guarantee:** steps run at least once, and a step whose result is
> recorded never runs again. A crash after `chargeCard` returns but before its
> checkpoint commits runs it again, so pass an idempotency key to your payment
> provider. See [the contract](docs/durable-execution.md#the-contract-read-this-once).

> See it for real: [`examples/02-crash-and-resume`](examples/02-crash-and-resume/)
> charges an order, lets you `Ctrl-C` it, and resumes on restart without
> double-charging. That demo is the whole pitch in 30 seconds.

## Install

```bash
go get github.com/alob-mtc/runnerq-go@main
```

Requires Go 1.27. The API in this README is on `main` and not in a tagged
release yet. The latest release, v0.5.1, has the older
[named-activity API](https://github.com/runnerq/runnerq-go/tree/v0.5.1#readme).

## Quick start

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"

    "github.com/alob-mtc/runnerq-go"
    "github.com/alob-mtc/runnerq-go/storage/postgres"
)

type Greeting struct{ runnerq.DefaultDeadLetterHandler }

func (h *Greeting) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
    greeting, err := ctx.RunStep("compose", func(context.Context) (string, error) {
        var name string
        if err := json.Unmarshal(payload, &name); err != nil {
            return "", runnerq.NewNonRetryError(err.Error())
        }
        return "hello, " + name, nil
    })
    if err != nil {
        return nil, err
    }
    return json.Marshal(greeting)
}

func main() {
    ctx := context.Background()

    backend, err := postgres.New(ctx, "postgres://postgres:runnerq@localhost:5432/runnerq", "my_app")
    if err != nil {
        log.Fatal(err)
    }
    defer backend.Close()

    engine, err := runnerq.Builder().Backend(backend).MaxWorkers(8).Build()
    if err != nil {
        log.Fatal(err)
    }
    engine.RegisterActivity(&Greeting{}) // serves activity type "Greeting"
    go engine.Start(ctx)

    future, _ := engine.GetActivityExecutor().
        Activity[Greeting]().
        Payload(json.RawMessage(`"world"`)).
        Execute(ctx)

    result, _ := future.GetResult(ctx)
    fmt.Println(string(result))   // "hello, world"
}
```

The **typed** form above derives the activity name from the handler type, so
nothing can drift. When a name must outlive a refactor, use the **named** form
(`RegisterActivityWithName`, `ActivityNamed`); see
[Naming activities](docs/workflows.md#naming-activities).

## Why RunnerQ

Background work that touches the real world — charging cards, sending email,
calling APIs, orchestrating multi-step jobs — has to survive crashes,
deploys, and retries without doing things twice. The usual options are a heavy
workflow server (a cluster to operate) or a plain task queue (no durability —
you hand-roll idempotency and recovery yourself).

RunnerQ gives you durable execution as a **library**:

- **A workflow is just a Go function.** Orchestration is normal control flow —
  loops, conditionals, error handling — not a DSL or a DAG.
- **Steps are checkpointed.** Completed work is skipped on replay, so retries
  and restarts are cheap and recorded steps don't run again.
- **Workflows pause for free.** Sleep for days or wait for a webhook while
  holding zero workers — paused workflows are just stored state.
- **Pluggable storage.** PostgreSQL is built in; implement the storage
  interface to use anything else. No orchestrator cluster, no broker, no
  control-plane bill. Scale by adding stateless worker processes.

**Good fits:** payment flows that must not redo completed steps, processing
each webhook once,
approval flows that wait days for a human, and fanning a batch out across
workers. The [examples](#examples) cover each of these.

## Features

<details open>
<summary><strong>Durable workflows that survive crashes</strong></summary>

A handler that calls `ctx.RunStep` is a durable workflow. Each step's result
is checkpointed; if the process dies, the handler replays and completed steps
return their stored results instead of re-executing.

```go
receipt, err := ctx.RunStep("charge-card", func(c context.Context) (Receipt, error) {
    return chargeCard(c, payload)   // at most once per recorded success
})
```

</details>

<details>
<summary><strong>Orchestration that fast-forwards on retry</strong></summary>

Spawn child activities with `.Step(name)`. A retried parent reattaches to the
children it already launched instead of duplicating them, and parents that
await children **park** in storage — no goroutine, no lease, no retry
burned while they wait.

```go
fut, _ := ctx.ActivityExecutor.Activity[Reserve]().Step("reserve").Payload(p).Execute(ctx.Ctx)
reserved, _ := fut.GetResult(ctx.Ctx)   // memoized on replay
```

</details>

<details>
<summary><strong>Durable timers and signals</strong></summary>

Sleep inside a workflow for seconds or weeks. The deadline is persisted — a
restart resumes the remainder — and long sleeps hold no worker.

```go
ctx.Sleep("cooling-off", 24 * time.Hour)   // survives restarts; frees the worker
```

Wait for an approval, a webhook, or a payment confirmation. The workflow parks
until a signal arrives, delivered from anywhere — even another process.

```go
decision, err := ctx.WaitForSignal("approval", 48*time.Hour)   // parks, holds no worker
// from an HTTP handler, a Slack action, a Kafka consumer:
engine.Signal(ctx, activityID, "approval", payload)
```

</details>

<details>
<summary><strong>Exactly-once enqueue</strong></summary>

Idempotency keys make enqueuing exactly-once — dedupe webhooks and event
handlers with one option.

```go
executor.ActivityNamed("process_event").
    Payload(p).
    IdempotencyKeyOption(eventID, runnerq.ReturnExisting).   // duplicate deliveries collapse to one
    Execute(ctx)
```

</details>

<details>
<summary><strong>A real queue underneath</strong></summary>

Priorities, exponential-backoff retries, a dead-letter queue, scheduling, and
worker-level type filtering so slow jobs can't starve latency-sensitive ones.

```go
executor.ActivityNamed("send_email").
    Payload(p).
    Priority(runnerq.PriorityHigh).
    MaxRetries(5).
    Timeout(2 * time.Minute).
    Delay(30 * time.Second).
    Execute(ctx)
```

</details>

<details>
<summary><strong>Pluggable storage</strong></summary>

PostgreSQL is built in. To use something else, implement `storage.Storage` and
run the conformance suite in `storage/storagetest` against it: a backend that
passes can replace Postgres without the engine noticing. See
[Storage backends](docs/storage-backends.md).

</details>

<details>
<summary><strong>A console for your workers</strong></summary>

The `conductor` agent connects your workers to a console that shows every
activity, its steps, events and results, read live from your own database
through the workers. Your data stays where it is. See
[Conductor agent](docs/conductor.md).

</details>

## Examples

Runnable, realistic, copy-pasteable — each in [`examples/`](examples/) with its
own README. `docker compose up -d` once, then `go run .` in any of them.

| # | Example | Shows |
|---|---------|-------|
| 01 | [hello-workflow](examples/01-hello-workflow/) | a durable two-step workflow |
| 02 | [crash-and-resume](examples/02-crash-and-resume/) | **kill it mid-flight; it resumes without redoing work** |
| 03 | [steps-and-retries](examples/03-steps-and-retries/) | child activities memoized across a retry |
| 04 | [checkpoint-side-effects](examples/04-checkpoint-side-effects/) | a recorded charge isn't repeated when the handler retries |
| 05 | [durable-sleep](examples/05-durable-sleep/) | timers that survive restarts |
| 06 | [human-approval](examples/06-human-approval/) | pause for an approval, delivered over HTTP |
| 07 | [fan-out](examples/07-fan-out/) | spawn many children, run them in parallel |
| 08 | [exactly-once-webhook](examples/08-exactly-once-webhook/) | idempotency keys collapse duplicate deliveries |
| 09 | [cross-process-futures](examples/09-cross-process-futures/) | enqueue in a web tier, await by ID anywhere |
| 10 | [retries-and-dead-letter](examples/10-retries-and-dead-letter/) | backoff, the dead-letter queue, `OnDeadLetter` |
| 11 | [retention](examples/11-retention/) | TTL cleanup of completed workflows |
| 12 | [workload-isolation](examples/12-workload-isolation/) | many worker fleets sharing one queue |

## How it compares

| | RunnerQ | Temporal / Cadence | Plain task queue (River, Asynq, …) |
|---|---|---|---|
| Durable workflows | ✅ | ✅ | ❌ |
| Deploy model | a Go library | a server cluster to operate | a library |
| Infrastructure | Postgres you already run, or your own storage backend | server + its own datastore | Postgres or Redis |
| Workflow code | plain Go, step-memoized | plain code, full replay-determinism | n/a |
| Durable timers / signals | ✅ | ✅ | ❌ |
| Scale | add stateless worker processes | scale the cluster | add workers |

- **Use RunnerQ** when you want durable, crash-safe workflows or queues with
  minimal new infrastructure, and you're already running Postgres.
- **Reach for a dedicated workflow server** (Temporal, Cadence) if you need
  many SDK languages or want workflow orchestration decoupled from your app
  and your database.
- **A plain queue** (River, Asynq, SQS) is enough if you only need
  fire-and-forget tasks and don't need workflows, steps, signals, or durable
  timers.

## Documentation

Full reference in [`docs/`](docs/):

- [Getting Started](docs/getting-started.md)
- [Workflows & Activities](docs/workflows.md)
- [Durable Execution](docs/durable-execution.md) — `Step`, `Run`, `Sleep`, signals, versioning
- [Retries & Dead Letter](docs/retries-and-dead-letter.md)
- [Configuration](docs/configuration.md) — tuning, scheduling, workload isolation, retention
- [Observability](docs/observability.md) — reading activities from code, worker snapshots, metrics
- [Conductor agent](docs/conductor.md) — connecting workers to a console
- [Storage Backends](docs/storage-backends.md)

## Status

RunnerQ is pre-1.0. The durable-execution core (steps, checkpoints, signals,
durable sleep, crash recovery) is built and integration-tested against
Postgres, but APIs may still change before 1.0. See [RELEASING.md](RELEASING.md)
for the stability policy.

## Development

Values and rules shared with the TypeScript SDK (key formats, notification
channels, error kinds) come from [runnerq-spec](https://github.com/runnerq/runnerq-spec),
checked out as the `spec` submodule:

```sh
git submodule update --init
go generate ./internal/spec   # after bumping spec: regenerate the constants
RUNNERQ_TEST_DSN=postgres://... go test ./... -race
# runnerq-spec's cross-language scenarios, through this SDK's conformance driver:
go build -o /tmp/conformancedriver ./internal/conformancedriver
go run -C spec/tools/conformance . -driver go=/tmp/conformancedriver
```

The vector tests (`TestSpec*`) need the submodule; the Postgres tests need
`RUNNERQ_TEST_DSN`.

## License

MIT — see [LICENSE](LICENSE).
