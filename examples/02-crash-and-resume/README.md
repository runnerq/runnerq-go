# 02 — Crash & Resume

**The demo that shows what "durable execution" actually means.** Kill the
process in the middle of a workflow, restart it, and watch it pick up exactly
where it left off — without repeating the work it already did.

## What this shows

An order workflow runs three steps: reserve inventory → charge the card →
ship. Each `ctx.Run` step prints `▶ EXECUTING` **only when its side effect
actually runs**. Kill the process after it charges, run it again, and the
reserve and charge lines do not reappear — they're replayed from their
Postgres checkpoints. Only shipping runs. The card isn't charged again.

## Run it

```bash
docker compose up -d        # from the examples/ directory, if not already up
go run .
```

You'll see it reserve, charge, then pause at:

```text
  ✓ reserve-inventory
  ✓ charge-card

  ⏸  CHARGED but not shipped. Press Ctrl-C NOW, then run `go run .` again.
```

**Press Ctrl-C now.** Then run it again:

```bash
go run .
```

## Expected output (the second run)

```text
  ✓ reserve-inventory          ← no "EXECUTING" line: replayed from checkpoint
  ✓ charge-card                ← no "EXECUTING" line: NOT re-charged
  ▶ EXECUTING ship-order          (real side effect)
  ✓ ship-order
✓ ORDER COMPLETE: {"status":"fulfilled"} — the card wasn't charged again.
```

The absence of `▶ EXECUTING charge-card` on the second run is the whole point:
the crashed workflow resumed without double-charging.

> Recovery kicks in within ~20s of the crash: the killed process's lease
> expires, the reaper requeues the workflow, and the next run picks it up. In
> production, *any* running worker recovers it — you don't need to restart the
> same process.

## The key idea

Two things make this work:

```go
// 1. A fixed idempotency key, so re-running reattaches to the SAME workflow.
runnerq.Activity[FulfillOrder](engine.GetActivityExecutor()).
    IdempotencyKeyOption(orderID, runnerq.ReturnExisting).
    Payload(...).Execute(ctx)

// 2. runnerq.RunStep checkpoints each step, so a replay skips completed ones.
chargeID, err := runnerq.RunStep(ctx, "charge-card", func(c context.Context) (string, error) {
    return chargeCard(c, ...)   // never runs twice for this workflow
})
```

Next: [03 — Steps & Retries](../03-steps-and-retries/) applies the same idea to
child activities.
