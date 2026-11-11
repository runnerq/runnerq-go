package runnerq

// Contract tests: the load-bearing invariants whose violation would cause
// production-devastating bugs (lost work, double execution, double charge).
// These deliberately exercise CONCURRENCY and CRASH RECOVERY — the scenarios
// least covered by the per-feature tests — through the public API, the way a
// real caller would. They are the regression guard for the engine's core
// guarantees; if one of these fails, do not ship.
//
// Skipped unless RUNNERQ_TEST_DSN is set (see storage/postgres tests).

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage/postgres"
)

func contractDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping contract test")
	}
	return dsn
}

func contractQueue() string {
	return "c_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
}

func contractBackend(t *testing.T, dsn, queue string) *postgres.PostgresBackend {
	t.Helper()
	b, err := postgres.New(context.Background(), dsn, queue)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(b.Close)
	return b
}

// startEngine starts an engine in the background and registers cleanup that
// stops it and waits for the drain.
func startEngine(t *testing.T, e *WorkerEngine) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); e.Start(context.Background()) }()
	t.Cleanup(func() {
		e.Stop()
		select {
		case <-done:
		case <-time.After(35 * time.Second):
			t.Error("engine did not stop")
		}
	})
}

// CONTRACT 1 — under concurrent workers across multiple engine "processes",
// every enqueued activity runs EXACTLY ONCE: none is lost, none is processed
// twice. This is the core queue guarantee (correct FOR UPDATE SKIP LOCKED
// claiming, no double-dispatch, no dropped work). A regression here means
// silently lost jobs or duplicated work in production.
func TestContract_NoLostNoDuplicateWork_ConcurrentFleets(t *testing.T) {
	dsn := contractDSN(t)
	queue := contractQueue()
	const (
		activities = 200
		fleets     = 3
		workers    = 8
	)

	var mu sync.Mutex
	runs := make(map[int]int, activities) // activity id -> times its handler ran
	var total atomic.Int64

	fn := func(_ ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
		var p struct {
			ID int `json:"id"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, NewNonRetryError("bad payload")
		}
		mu.Lock()
		runs[p.ID]++
		mu.Unlock()
		total.Add(1)
		return nil, nil
	}

	// Build the fleets (separate backends → distinct connection pools, like
	// separate processes), all on the same queue.
	var engines []*WorkerEngine
	for f := range fleets {
		b := contractBackend(t, dsn, queue)
		e, err := Builder().Backend(b).QueueName(queue).MaxWorkers(workers).Build()
		if err != nil {
			t.Fatalf("build engine %d: %v", f, err)
		}
		e.RegisterActivityWithName("unit", &funcHandler{fn: fn})
		engines = append(engines, e)
	}

	// Enqueue everything BEFORE starting any worker, so all 200 are pending
	// and the fleets race to claim them.
	exec := engines[0].GetActivityExecutor()
	for i := range activities {
		p, _ := json.Marshal(map[string]int{"id": i})
		if _, err := exec.ActivityNamed("unit").Payload(p).Execute(context.Background()); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	for _, e := range engines {
		startEngine(t, e)
	}

	// Wait until every distinct id has been seen, or fail with what's missing.
	deadline := time.After(60 * time.Second)
	for {
		mu.Lock()
		n := len(runs)
		mu.Unlock()
		if n == activities {
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			missing := activities - len(runs)
			mu.Unlock()
			t.Fatalf("timed out: %d/%d activities never ran (lost work)", missing, activities)
		case <-time.After(200 * time.Millisecond):
		}
	}

	mu.Lock()
	defer mu.Unlock()
	for id, c := range runs {
		if c != 1 {
			t.Errorf("activity %d ran %d times, want exactly 1 (duplicate execution)", id, c)
		}
	}
	if got := int(total.Load()); got != activities {
		t.Errorf("total executions = %d, want %d", got, activities)
	}
}

// CONTRACT 2 — when many callers enqueue the SAME idempotency key at the same
// instant, exactly one activity is created and its handler runs exactly once.
// This is the dedupe guarantee that makes webhook/event ingestion safe; a
// regression means duplicate processing of the same event under load.
func TestContract_ConcurrentIdempotentEnqueue(t *testing.T) {
	dsn := contractDSN(t)
	queue := contractQueue()

	var runs atomic.Int64
	b := contractBackend(t, dsn, queue)
	e, err := Builder().Backend(b).QueueName(queue).MaxWorkers(4).Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	e.RegisterActivityWithName("once", &funcHandler{fn: func(_ ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		runs.Add(1)
		return json.RawMessage(`{"ok":true}`), nil
	}})
	startEngine(t, e)

	const callers = 50
	exec := e.GetActivityExecutor()
	ids := make([]uuid.UUID, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fut, err := exec.ActivityNamed("once").
				IdempotencyKeyOption("same-event", ReturnExisting).
				Payload(json.RawMessage(`{}`)).
				Execute(context.Background())
			if err != nil {
				t.Errorf("caller %d enqueue: %v", i, err)
				return
			}
			ids[i] = fut.ActivityID()
		}(i)
	}
	wg.Wait()

	// Every concurrent enqueue must resolve to ONE activity.
	distinct := map[uuid.UUID]struct{}{}
	for _, id := range ids {
		distinct[id] = struct{}{}
	}
	if len(distinct) != 1 {
		t.Fatalf("concurrent idempotent enqueue created %d distinct activities, want 1", len(distinct))
	}

	// And that activity's handler must run exactly once. Await it, then check.
	var theID uuid.UUID
	for id := range distinct {
		theID = id
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := FutureFor(b, theID).GetResult(waitCtx); err != nil {
		t.Fatalf("await deduped activity: %v", err)
	}
	time.Sleep(time.Second) // catch any erroneous second execution
	if got := runs.Load(); got != 1 {
		t.Fatalf("handler ran %d times for one idempotency key, want 1", got)
	}
}
