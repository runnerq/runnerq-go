# 10 — Retries and the Dead-Letter Queue

A retryable failure is retried with exponential backoff up to `MaxRetries`.
When the attempts run out, the activity is dead-lettered and its
`OnDeadLetter` callback fires — your hook for alerting, compensation, or
logging.

## What this shows

A charge against a down payment gateway fails every time. With `MaxRetries(2)`
it tries, backs off, tries again, then dead-letters. The handler implements
`OnDeadLetter`, and awaiting the dead-lettered activity returns an error
rather than hanging.

## Run it

```bash
docker compose up -d        # from the examples/ directory, if not already up
go run .
```

## Expected output

```text
  ▶ attempt #1 (RetryCount=0) — calling gateway...
  ▶ attempt #2 (RetryCount=1) — calling gateway...        ← after backoff
  ☠ dead-lettered <id> after exhausting retries: Retryable error: payment gateway 503
✓ workflow ended after 2 attempts; awaiting it returned: {"type":"dead_letter",...}
```

## The key idea

```go
type ChargeCard struct{}   // implements OnDeadLetter itself (no DefaultDeadLetterHandler)

func (h *ChargeCard) Handle(ctx runnerq.ActivityContext, p json.RawMessage) (json.RawMessage, error) {
    return nil, runnerq.NewRetryError("payment gateway 503")   // retryable
}

func (h *ChargeCard) OnDeadLetter(ctx runnerq.ActivityContext, p json.RawMessage, errorMsg string) {
    alertOnCall(ctx.ActivityID, errorMsg)   // compensate / refund / flag
}

executor.Activity[ChargeCard]().MaxRetries(2).Payload(p).Execute(ctx)
```

`NewNonRetryError` skips retries and dead-letters immediately. Embed
`runnerq.DefaultDeadLetterHandler` instead of writing `OnDeadLetter` if you
don't need the callback. Note: activities dead-lettered by repeated **lease
expiry** (a handler that keeps crashing the process) don't get an
`OnDeadLetter` call — there's no live handler to invoke; watch the metrics and
dead letters for those. See [docs/retries-and-dead-letter.md](../../docs/retries-and-dead-letter.md).

Next: [11 — Retention](../11-retention/).
