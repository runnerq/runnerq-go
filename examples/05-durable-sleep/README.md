# 05 — Durable Sleep

`ctx.Sleep(name, d)` is a timer that survives restarts. The wake deadline is
persisted, so a workflow that sleeps for 24 hours and is redeployed an hour in
resumes with 23 hours left — and holds no worker while it waits.

## What this shows

An onboarding workflow sends a welcome message, waits, then sends tips. The
wait is durable: it's not `time.Sleep`. For waits longer than the handler's
timeout budget the activity is **parked in Postgres** (the worker is freed
entirely) and re-dispatched when the timer fires — so a million sleeping
workflows cost a million rows, not a million goroutines.

The delay here is 5 seconds so the demo finishes quickly. Change it to
`24 * time.Hour` and it behaves identically.

## Run it

```bash
docker compose up -d        # from the examples/ directory, if not already up
go run .
```

## Expected output

```text
  ▶ 10:58:58  welcome email sent
  ⏳ waiting 5s before the follow-up (this is a durable timer)
  ▶ 10:59:03  tips email sent          ← ~5s later
✓ done: {"status":"onboarded"}
```

## The key idea

```go
runnerq.RunStep(ctx, "send-welcome", sendWelcome)   // sendWelcome is a runnerq.Step[string]
ctx.Sleep("drip-delay", 24*time.Hour)      // persisted deadline; resumes the remainder
runnerq.RunStep(ctx, "send-tips", sendTips)
```

If the process restarts mid-sleep, the workflow replays: `send-welcome` is
memoized, `Sleep` sees its stored deadline and waits only what's left, then
`send-tips` runs. The timer is as durable as the steps around it.

Next: [06 — Human Approval](../06-human-approval/) waits for an external
event instead of the clock.
