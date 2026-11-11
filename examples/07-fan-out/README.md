# 07 — Fan-Out and `WaitAll`

Spawn many children, run them in parallel across the worker pool, and wait for
all of them. While the parent waits it parks in Postgres, holding no worker.

## What this shows

A document is split into pages; each page is processed concurrently by a child
activity; the parent sums the word counts. The children run in parallel (bump
`MaxWorkers` and they spread across the pool). The parent awaits with
`WaitAll`, which returns results in spawn order.

Because awaiting is durable, the whole fan-out survives a restart: the parent
reattaches to its existing children (named steps), and any that already
finished return instantly.

## Run it

```bash
docker compose up -d        # from the examples/ directory, if not already up
go run .
```

## Expected output

```text
  spawned 3 page workers; parking until all finish
  ✓ page "intro": 100 words
  ✓ page "body": 200 words
  ✓ page "appendix": 300 words
✓ done: {"total_words":600}
```

(You may see "parking until all finish" print more than once — that's the
parent waking on a child completion, replaying, and re-parking until the rest
are done. Each replay fast-forwards through finished children.)

## The key idea

```go
var futures []*runnerq.ActivityFuture
for i, page := range pages {
    fut, _ := runnerq.Activity[ProcessPage](ctx.ActivityExecutor).
        Step(fmt.Sprintf("page-%d", i)).   // stable name → replay-safe
        Payload(...).Execute(ctx.Ctx)
    futures = append(futures, fut)
}
results, err := runnerq.WaitAll(ctx.Ctx, futures...)   // parks here, in order
```

Under replay semantics, sequential awaiting is already efficient: the first
unfinished child parks the parent, and on each wake every completed child
fast-forwards — so there's no separate "parallel wait" API to learn.

That's the walkthrough. For the full reference, see [docs/](../../docs/).
