# Getting Started

## Install

```bash
go get github.com/runnerq/runnerq-go
```

Requires Go 1.27+ and a PostgreSQL database (12+).

## Connect to Postgres

The Postgres backend creates its own schema on first connect (idempotently),
so there's no migration step:

```go
import "github.com/runnerq/runnerq-go/storage/postgres"

backend, err := postgres.New(ctx, "postgres://user:pass@localhost:5432/mydb", "my_app")
if err != nil {
    log.Fatal(err)
}
defer backend.Close()
```

The second argument is the **queue name** — a logical namespace stored as a
column, so many independent queues can share one database. For lease and pool
tuning, use `postgres.WithConfig` (see [Configuration](configuration.md)).

## Write a workflow

A workflow is an activity handler — a type implementing `ActivityHandler`.
Embed `DefaultDeadLetterHandler` unless you need a dead-letter callback. The
handler's type name doubles as its activity type (`Greeting` here). You can
also pick the name yourself; both forms are shown at the end of this page.

```go
type Greeting struct {
    runnerq.DefaultDeadLetterHandler
}

func (h *Greeting) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
    name := string(payload)
    return json.Marshal("hello, " + name)
}
```

## Build the engine and run it

```go
engine, err := runnerq.Builder().
    Backend(backend).
    MaxWorkers(8).
    Build()
if err != nil {
    log.Fatal(err)
}
engine.RegisterActivity(&Greeting{}) // serves activity type "Greeting"

// Start blocks until the context is cancelled or a SIGINT/SIGTERM arrives.
go engine.Start(ctx)
```

## Enqueue and await

```go
future, err := engine.GetActivityExecutor().
    Activity[Greeting]().
    Payload(json.RawMessage(`"world"`)).
    Execute(ctx)
if err != nil {
    log.Fatal(err)
}

result, err := future.GetResult(ctx)   // blocks until the activity completes
fmt.Println(string(result))            // "hello, world"
```

## Typed or named — both are supported

Everything above uses the **typed** API: the handler type is the activity
name, so there is no string to keep in sync. The **named** API does the same
job with a string you choose. Use it when the name must survive a refactor,
when one handler serves several types, or when the producer has no Go type
in scope. The two are interchangeable, line for line:

```go
// Typed (recommended)
engine.RegisterActivity(&Greeting{})
engine.GetActivityExecutor().Activity[Greeting]().Payload(p).Execute(ctx)

// Named
engine.RegisterActivityWithName("greeting", &Greeting{})
engine.GetActivityExecutor().ActivityNamed("greeting").Payload(p).Execute(ctx)
```

The named form is also the shape of the v0.5 API. RunnerQ v0.6+ requires
Go 1.27 for the typed methods; if you cannot move to 1.27 yet, stay on v0.5,
where the named form is the only one and your code carries over unchanged
apart from the method names. See
[Naming activities](workflows.md#naming-activities) for the full picture.

## Shutdown

Stop the engine and wait for the graceful drain before closing the backend:

```go
done := make(chan struct{})
go func() { defer close(done); engine.Start(ctx) }()
// ... later ...
engine.Stop()   // stop intake, let in-flight handlers finish and ack
<-done
backend.Close()
```

## Next

- The [examples](../examples/) — runnable, realistic, copy-pasteable.
- [Durable Execution](durable-execution.md) — make workflows survive crashes.
