# RunnerQ Documentation

Reference docs for RunnerQ. New here? Start with the
[examples walkthrough](../examples/) — it's the fastest way in. These pages
are the deeper reference.

## Contents

- **[Getting Started](getting-started.md)** — install, connect to Postgres, write and run your first workflow.
- **[Workflows & Activities](workflows.md)** — handlers, the activity context, spawning child activities, getting results, awaiting across processes.
- **[Durable Execution](durable-execution.md)** — the core: `Step`, `ctx.RunStep`, `ctx.Sleep`, `WaitForSignal`, how parents park and resume, and versioning across deploys.
- **[Retries & Dead Letter](retries-and-dead-letter.md)** — error types, backoff, delivery semantics, lease recovery, the dead-letter queue.
- **[Configuration](configuration.md)** — builder options, defaults, lease/pool tuning, scheduling, workload isolation, retention.
- **[Observability](observability.md)** — reading activities from code, worker snapshots, metrics.
- **[Conductor agent](conductor.md)** — connecting workers to a console.
- **[Storage Backends](storage-backends.md)** — the Postgres backend, the schema, implementing your own.

## The one-paragraph mental model

A **workflow** is an ordinary Go function (an activity handler). Inside it you
call **steps** — `ctx.RunStep` for local work, `.Step(...)` spawns for child
activities — each of which is checkpointed in Postgres. If the process
crashes, the handler re-runs from the top, but completed steps return their
stored results instead of re-executing, so the workflow fast-forwards to where
it left off. `ctx.Sleep` and `ctx.WaitForSignal` let a workflow pause for
hours or days without holding any worker. That's the whole idea; everything
else is configuration.

## Two forms of every name

Activities are registered and spawned either **typed** — `RegisterActivity(&H{})`
and `Activity[H]()`, where the handler type is the name — or **named** —
`RegisterActivityWithName("h", &H{})` and `ActivityNamed("h")`, where you
choose the string. Typed is the recommended default; named is fully
supported and is the shape of the v0.5 API for anyone not yet on Go 1.27.
[Details](workflows.md#naming-activities).
