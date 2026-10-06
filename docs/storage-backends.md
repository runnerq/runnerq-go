# Storage Backends

RunnerQ persists everything through a storage interface. PostgreSQL is the
built-in backend. The interface is exported so you *can* implement another,
and the conformance suite tells you when you have (see "Proving a backend
durable" below).

## PostgreSQL

```go
import "github.com/alob-mtc/runnerq-go/storage/postgres"

backend, err := postgres.New(ctx, "postgres://user:pass@localhost/runnerq", "my_queue")
// or, with lease/pool tuning:
backend, err := postgres.WithConfig(ctx, dsn, "my_queue", /*leaseMS*/ 30_000, /*pool*/ 50)
defer backend.Close()
```

What the backend gives you:

- **Crash-safe claiming** — `FOR UPDATE SKIP LOCKED` dequeue with leases;
  expired leases are reaped and requeued.
- **Atomic durability** — results are stored in the same transaction as
  completion; idempotency claim and enqueue are one atomic step.
- **Event-driven wakeups** — `LISTEN/NOTIFY` drives blocking dequeue and
  future resolution, with table re-checks as a lossless fallback. No
  busy-polling.
- **Self-managing schema** — created idempotently on connect (advisory-locked
  so concurrent boots don't race; hot indexes built `CONCURRENTLY`).

### Schema

On first connect the backend creates:

| Table | Holds |
|---|---|
| `runnerq_activities` | activities and their lifecycle state |
| `runnerq_inputs` | each activity's payload, written once at enqueue |
| `runnerq_events` | append-only lifecycle event timeline |
| `runnerq_results` | activity results and `Run`/`Sleep` checkpoints |
| `runnerq_idempotency` | idempotency-key → activity mapping |
| `runnerq_dependencies` | durable references from a waiting activity to the results it waits on |
| `runnerq_commands` | applied commands, so a repeated command replays its result |

Without [retention](configuration.md#retention) configured, these grow
forever — turn it on for production.

The schema is the one the TypeScript SDK uses, so either SDK can open a
database the other created.

#### Upgrading from an inline-payload schema

Earlier versions kept each payload in a `runnerq_activities.payload`
column. On first connect, this version moves every payload into
`runnerq_inputs` and drops that column, in one transaction under the
schema lock. Older versions can't use the database afterwards, so stop every
process on an older version before starting this one: a rolling deploy that
mixes the two will fail the old processes' enqueues and claims. The move
reads the whole activities table once; on a large table, expect the first
start to take a while.

## Implementing a custom backend

A custom backend must implement `storage.Storage`: `QueueStorage`
(enqueue/dequeue/ack, leases, idempotency, results, signals, yield,
retention) and `ResultStorage`; to be queried and commanded it also implements
`storage.QueryStorage` and `storage.CommandStorage`. It's a large surface — the durable
primitives in particular (`Yield`, `WakeWaiting`, `SignalActivity`,
`EnqueueIdempotent`, `CleanupExpired`, atomic `StoreResult` with an owner) each
carry correctness requirements documented on the interface methods in
[`storage/storage.go`](../storage/storage.go).

```go
type MyBackend struct{ /* ... */ }

func (b *MyBackend) Enqueue(ctx context.Context, a storage.QueuedActivity) error { /* ... */ }
func (b *MyBackend) Dequeue(ctx context.Context, workerID string, timeout time.Duration, types []string) (*storage.QueuedActivity, error) { /* ... */ }
func (b *MyBackend) AckSuccess(ctx context.Context, id uuid.UUID, result json.RawMessage, workerID string) error { /* ... */ }
// ... and the rest of QueueStorage + ResultStorage

engine, _ := runnerq.Builder().Backend(&MyBackend{}).Build()
```

If your backend can block efficiently until a result exists, also implement the
optional `storage.ResultWaiter` (`WaitForResult`) — futures use it instead of
polling, and it must work across processes. Without it, awaiting falls back to
a 100ms poll.

Two more optional interfaces bound what an execution that lost its lease can
do. `storage.AttemptLeaseStorage` (`ExtendLeaseForWorker`) lets the engine
heartbeat a running handler's claim and cancel the handler when the claim is
gone. `storage.SpawnStorage` (`EnqueueForWorker`, `EnqueueIdempotentForWorker`)
makes handler-issued spawns conditional on the claim, atomically with the
insert. Without them the engine falls back to lease sizing alone and unfenced
spawns.

`storage.QueryStorage` is optional too: a general query layer over a
canonical model of activities and events, for dashboards and tooling (the
`conductor` agent serves it too). Activity filters (`and`/`or`/`not` over fields such
as `status`, `type`, `queue`, `root_id`, `parent_id`, `metadata.<key>` and the
timestamps), one sort key with keyset cursors, heavy fields only on request,
counts, grouped aggregates with time buckets and duration percentiles, event
and step listings, and whole trees. Queries span every queue in the database.
A backend advertises what it evaluates in `QueryCapabilities` and must reject
anything else with `ErrUnsupported` rather than ignore it.

`storage.CommandStorage` (also optional) applies operator commands to the
backend's own queue: cancel (non-terminal work; a running claim is
fenced out, children are cancelled when asked, and a cancellation error
result wakes anything awaiting the activity), retry and redrive (from
checkpoints), run now, reschedule, set priority, whole-tree delete and
signal. Targets are ids, a filter bounded by a maximum (narrowed to what the
command can act on, so repeating it makes progress), or an idempotency key.
Commands are idempotent by id: the backend records each applied command and
its result, replays it on redelivery and rejects the id when reused with
different input.

### Proving a backend durable

`storage/storagetest` is the conformance suite: it states every behaviour the
engine relies on — claimed once under `SKIP LOCKED`-equivalent semantics,
fenced acknowledgements, lost-reply reconciliation, lease recovery, parked
parents woken by awaited results, checkpoints published by exactly one
owner, atomic idempotency, whole-tree retention — as tests driven only through the storage
interfaces. Run it from your backend's test file:

```go
type harness struct{}

func (harness) Open(t *testing.T, queue string) storage.Storage { /* fresh backend on queue */ }
func (harness) ExpireLease(ctx context.Context, b storage.Storage, id uuid.UUID) error { /* move the lease into the past */ }

func TestConformance(t *testing.T) { storagetest.Run(t, harness{}) }
```

The harness supplies only what the interface cannot express: opening a
backend on a named queue (two opens on one queue stand in for two processes)
and forcing a lease to expire. To check what a backend stored, the suite also
reads it back through `storagetest.Reader` (`GetActivity`,
`GetActivityEvents`, `GetActivitySteps`, `GetChildren`, `GetSubtree`,
`ListDeadLetter`), which the backend implements alongside `storage.Storage`;
the engine never calls these. A backend that passes the suite can replace
Postgres without the engine noticing. The PostgreSQL backend runs it in CI.

> **Caveat:** the interface — especially the parking/wakeup and atomicity
> contracts behind durable execution — is non-trivial to get right. Read the
> doc comments in `storage/storage.go` before starting, use the PostgreSQL
> backend as a reference implementation, and treat a green conformance
> run as the bar for "done".
