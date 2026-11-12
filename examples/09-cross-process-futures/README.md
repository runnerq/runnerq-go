# 09 — Cross-Process Futures

A future is just an activity ID plus a backend handle, so it's rehydratable:
enqueue a job in your web tier, hand the caller an ID, and let any process —
a different replica, a polling endpoint, a separate worker fleet — await the
result later.

## What this shows

A web API: `POST /jobs` enqueues and returns an ID; `GET /jobs/{id}`
reconstructs the future from **just that ID** with
`runnerq.FutureFor(backend, id)` and reports `running` or the result. The GET
handler never touches the original `*ActivityFuture` — in a real system it
could be an entirely different process.

## Run it

```bash
docker compose up -d        # from the examples/ directory, if not already up
go run .
```

## Expected output

```text
enqueued job a6ac3c8a-...
  poll right away: {"status":"running"}
  poll after work: {"status":"done","result":{"width":800,"height":600}}
✓ the GET handler awaited the result from just the ID — no shared future.
```

## The key idea

```go
// Web tier: enqueue, return the handle.
fut, _ := runnerq.Activity[ResizeImage](executor).Payload(p).Execute(ctx)
respond(map[string]string{"id": fut.ActivityID().String()})

// Anywhere else, later, with only the ID and a backend:
result, err := runnerq.FutureFor(backend, id).GetResult(ctx)
```

`GetResult` is notification-driven across processes (with a polling fallback),
so the awaiter wakes promptly when whichever worker — in whatever process —
finishes the job. This is the clean handoff between a request/response API and
a durable worker fleet.

Next: [10 — Retries & Dead Letter](../10-retries-and-dead-letter/).
