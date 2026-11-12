# RunnerQ Documentation

Reference docs for RunnerQ. New here? Start with the
[examples walkthrough](../examples/) — it's the fastest way in. These pages
are the deeper reference.

## Contents

- **[Getting Started](getting-started.md)** — install, connect to Postgres, write and run your first workflow.
- **[Workflows & Activities](workflows.md)** — handlers, the activity context, spawning child activities, getting results, awaiting across processes.
- **[Durable Execution](durable-execution.md)** — the core: `Step`, `runnerq.RunStep`, `ctx.Sleep`, `WaitForSignal`, how parents park and resume, and versioning across deploys.
- **[Retries & Dead Letter](retries-and-dead-letter.md)** — error types, backoff, delivery semantics, lease recovery, the dead-letter queue.
- **[Schedules](schedules.md)** — cron schedules: declaring, managing, time zones and daylight saving, catch-up.
- **[Configuration](configuration.md)** — builder options, defaults, lease/pool tuning, scheduling, workload isolation, retention.
- **[Managing work](managing-work.md)** — `runnerq.Client`: submit, signal and await without an engine; get and list activities; cancel (reaching running handlers), retry, run now, reschedule, set priority, delete.
- **[Observability](observability.md)** — reading activities from code, worker snapshots, metrics.
- **[Conductor agent](conductor.md)** — connecting workers to a console.
- **[Storage Backends](storage-backends.md)** — the Postgres backend, the schema, implementing your own.

## The one-paragraph mental model

A **workflow** is an ordinary Go function (an activity handler). Inside it you
call **steps** — `runnerq.RunStep` for local work, `.Step(...)` spawns for child
activities — each of which is checkpointed in Postgres. If the process
crashes, the handler re-runs from the top, but completed steps return their
stored results instead of re-executing, so the workflow fast-forwards to where
it left off. `ctx.Sleep` and `ctx.WaitForSignal` let a workflow pause for
hours or days without holding any worker. That's the whole idea; everything
else is configuration.

## Two forms of every name

Activities are registered and spawned either **typed** — `RegisterActivity(&H{})`
and `runnerq.Activity[H](exec)`, where the handler type is the name — or **named** —
`RegisterActivityWithName("h", &H{})` and `ActivityNamed("h")`, where you
choose the string. Typed is the recommended default; named is fully
supported and is the shape of the v0.5 API.
[Details](workflows.md#naming-activities).
