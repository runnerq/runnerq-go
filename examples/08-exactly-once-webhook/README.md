# 08 — Exactly-Once Webhook Ingestion

Webhooks and event streams redeliver. An idempotency key makes enqueuing
exactly-once: duplicate deliveries of the same event collapse into a single
activity, so the work runs once no matter how many times the event arrives.

## What this shows

A `/webhook` endpoint enqueues a `ProcessEvent` activity keyed by the event
ID with `ReturnExisting`. The program delivers one event three times and a
second event once; the processor runs twice total.

## Run it

```bash
docker compose up -d        # from the examples/ directory, if not already up
go run .
```

## Expected output

```text
delivering webhooks: first event ×3, second event ×1
  ▶ processing evt-...-2 (handler run #1 for this event)
  ▶ processing evt-...-1 (handler run #1 for this event)
✓ first event processed 1 time(s); second event processed 1 time(s).
  → the three duplicate deliveries collapsed into a single activity.
```

## The key idea

```go
runnerq.Activity[ProcessEvent](engine.GetActivityExecutor()).
    IdempotencyKeyOption(eventID, runnerq.ReturnExisting).   // dedup on the event ID
    Payload(payload).
    Execute(ctx)
```

The claim-and-enqueue is one atomic step in Postgres, so concurrent duplicate
deliveries can't both create an activity — the second gets a future for the
first. Keys are scoped per queue. Combine this with
[durable steps](../../docs/durable-execution.md) and the whole event-handling
pipeline becomes exactly-once end to end.

Next: [09 — Cross-Process Futures](../09-cross-process-futures/).
