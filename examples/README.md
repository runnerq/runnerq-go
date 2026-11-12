# RunnerQ Examples

Each example is a standalone, runnable program with its own README. They build
on each other — start at `01` and walk up.

## Setup (once)

Every example talks to Postgres. Start one here:

```bash
docker compose up -d      # Postgres on localhost:5432
```

Each example reads `DATABASE_URL` and falls back to
`postgres://postgres:runnerq@localhost:5432/runnerq`, so once Postgres is up
you can just `cd` into any example and `go run .`.

When you're done: `docker compose down -v`.

## The walkthrough

Durable execution — start here:

| # | Example | What it teaches |
|---|---------|-----------------|
| 01 | [hello-workflow](01-hello-workflow/) | A durable two-step workflow — the smallest useful program |
| 02 | [crash-and-resume](02-crash-and-resume/) | **The headline.** Kill it mid-flight; it resumes without redoing work |
| 03 | [steps-and-retries](03-steps-and-retries/) | `.Step()` — child activities that survive a retry without re-running |
| 04 | [checkpoint-side-effects](04-checkpoint-side-effects/) | `ctx.Run` — charge once, even when the handler retries |
| 05 | [durable-sleep](05-durable-sleep/) | `ctx.Sleep` — timers that survive restarts (a 24h wait holds no worker) |
| 06 | [human-approval](06-human-approval/) | `ctx.WaitForSignal` — pause for an approval/webhook, delivered over HTTP |
| 07 | [fan-out](07-fan-out/) | `WaitAll` — spawn many children, run them in parallel, collect results |

Patterns & operations:

| # | Example | What it teaches |
|---|---------|-----------------|
| 08 | [exactly-once-webhook](08-exactly-once-webhook/) | Idempotency keys — duplicate webhook deliveries collapse to one |
| 09 | [cross-process-futures](09-cross-process-futures/) | `FutureFor` — enqueue in a web tier, await the result anywhere by ID |
| 10 | [retries-and-dead-letter](10-retries-and-dead-letter/) | Backoff, the dead-letter queue, and the `OnDeadLetter` callback |
| 11 | [retention](11-retention/) | TTL cleanup of completed workflow trees |
| 12 | [workload-isolation](12-workload-isolation/) | Many worker fleets on one queue, each handling its own types |
| 13 | [cron-schedules](13-cron-schedules/) | A cron schedule fired once per tick across every engine |

If you only run one, run **02** — it's the demo that shows what "durable" means.
