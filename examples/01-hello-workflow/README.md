# 01 — Hello, Workflow

The smallest useful RunnerQ program: a durable two-step workflow.

## What this shows

A workflow is just an activity handler. Inside it, each `runnerq.RunStep(ctx, name, fn)`
is a **step** whose typed result is checkpointed in Postgres. A step whose
result was recorded never re-runs — if the process restarted between them,
the first would replay from its checkpoint and the second would resume. A
crash *between* the side effect and the checkpoint commit does re-run `fn`,
so keep external effects idempotent. (Example 02 proves the replay; here we
just run it through.)

## Run it

```bash
docker compose up -d        # from the examples/ directory, if not already up
go run .
```

## Expected output

```text
starting signup workflow...
  ▶ creating account for ada@example.com
  ▶ sending welcome email to ada@example.com
✓ signup complete: {"email":"ada@example.com","user_id":"u_1001"}
```

## The key idea

```go
func (h *SignupWorkflow) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
    user, err := runnerq.RunStep(ctx, "create-account", func(c context.Context) (Account, error) {
        return createAccount(c, payload)   // checkpointed: runs at most once
    })
    if err != nil { return nil, err }

    _, err = runnerq.RunStep(ctx, "send-welcome", func(c context.Context) (bool, error) {
        return true, sendWelcome(c, user)  // only after the account is durable
    })
    if err != nil { return nil, err }
    return json.Marshal(user)
}
```

Next: [02 — Crash & Resume](../02-crash-and-resume/) shows why the checkpoints matter.
