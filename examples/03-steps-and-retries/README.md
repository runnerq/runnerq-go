# 03 — Steps & Retries

Memoized child activities: a workflow that spawns a child with `.Step()` and
then fails will **not** re-run that child on retry — it reattaches to the one
already done and gets the result back instantly.

## What this shows

A payment workflow reserves inventory (a child activity), then hits a
transient error on its first attempt. The engine retries the whole handler.
On the retry, the reserve child is reused, not re-run — proven by a counter
that shows it executed exactly once across two parent attempts.

This is automatic (the retry happens on its own); no manual kill needed.

## Run it

```bash
docker compose up -d        # from the examples/ directory, if not already up
go run .
```

## Expected output

```text
process_order attempt #0
    ▶ reserve_inventory EXECUTING (run #1)
  ✓ reserved: {"qty":1,"sku":"WIDGET-1"}
  ✗ transient failure after reserve — will retry
process_order attempt #1
  ✓ reserved: {"qty":1,"sku":"WIDGET-1"}      ← child NOT re-run
  ✓ payment captured

✓ done: {"status":"paid"}
  reserve child executed 1 time(s) across 2 parent attempts (memoized).
```

## The key idea

```go
// .Step("reserve") derives an idempotency key from (root, parent, "reserve").
// On the parent's retry, this exact spawn reattaches to the existing child.
fut, _ := runnerq.Activity[ReserveInventory](ctx.ActivityExecutor).
    Step("reserve").
    Payload(payload).
    Execute(ctx.Ctx)
reserved, _ := fut.GetResult(ctx.Ctx)   // memoized result on retry
```

The rule: give each spawn a stable `Step` name. Reordering or adding steps
between deploys is safe; only *renaming* a step in a workflow that's still
in flight is hazardous (it would re-run). See
[docs/durable-execution.md](../../docs/durable-execution.md).

Next: [04 — Checkpoint Side Effects](../04-checkpoint-side-effects/) does the
same for local code, not just child activities.
