package runnerq

// Engine-level integration tests. Skipped unless RUNNERQ_TEST_DSN is set —
// see storage/postgres/integration_test.go for how to run a test database.

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage/postgres"
)

type slowHandler struct {
	DefaultDeadLetterHandler
	dur   time.Duration
	calls atomic.Int32
}

func (h *slowHandler) Handle(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
	h.calls.Add(1)
	select {
	case <-time.After(h.dur):
		return json.RawMessage(`"done"`), nil
	case <-ctx.Ctx.Done():
		return nil, ctx.Ctx.Err()
	}
}

// Tier 0.6: a handler that is mid-flight when Stop is called must be allowed
// to finish and ack on a live context. Before the two-phase shutdown, Stop
// cancelled the engine context immediately, so the ack always failed and the
// activity was guaranteed to rerun on the next engine start.
func TestGracefulShutdownCompletesInFlightActivity(t *testing.T) {
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping integration test")
	}
	ctx := context.Background()
	queueName := "t_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]

	backend, err := postgres.New(ctx, dsn, queueName)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	defer backend.Close()

	engine, err := Builder().
		Backend(backend).
		QueueName(queueName).
		MaxWorkers(2).
		Build()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}
	handler := &slowHandler{dur: 1500 * time.Millisecond}
	engine.RegisterActivityWithName("slow", handler)

	startDone := make(chan error, 1)
	go func() { startDone <- engine.Start(ctx) }()

	fut, err := engine.GetActivityExecutor().
		ActivityNamed("slow").
		Payload(json.RawMessage(`{}`)).
		Execute(ctx)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// Wait until the handler is actually running, then stop the engine while
	// the activity is mid-flight.
	deadline := time.After(10 * time.Second)
	for handler.calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("activity never started")
		case <-time.After(20 * time.Millisecond):
		}
	}
	engine.Stop()

	select {
	case err := <-startDone:
		if err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
	case <-time.After(35 * time.Second):
		t.Fatal("Start did not return after Stop")
	}

	// The in-flight activity must have completed and stored its result — not
	// been requeued for another engine to redo.
	res, err := backend.GetResult(ctx, fut.activityID)
	if err != nil {
		t.Fatalf("get result: %v", err)
	}
	if res == nil {
		t.Fatal("no result stored — in-flight activity did not complete during drain")
	}
	snap, err := backend.GetActivity(ctx, fut.activityID)
	if err != nil {
		t.Fatalf("get activity: %v", err)
	}
	if snap == nil || snap.Status != "Completed" {
		t.Fatalf("activity status = %+v, want Completed", snap)
	}
	if got := handler.calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want exactly 1", got)
	}
}

// Concurrent Stop calls must not panic (the old select/default close idiom
// could double-close the shutdown channel).
func TestConcurrentStopDoesNotPanic(t *testing.T) {
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping integration test")
	}
	ctx := context.Background()
	queueName := "t_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]

	backend, err := postgres.New(ctx, dsn, queueName)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	defer backend.Close()

	engine, err := Builder().Backend(backend).QueueName(queueName).MaxWorkers(1).Build()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}
	engine.RegisterActivityWithName("slow", &slowHandler{dur: time.Millisecond})

	startDone := make(chan error, 1)
	go func() { startDone <- engine.Start(ctx) }()
	time.Sleep(200 * time.Millisecond)

	for range 8 {
		go engine.Stop()
	}
	engine.Stop()

	select {
	case <-startDone:
	case <-time.After(35 * time.Second):
		t.Fatal("Start did not return")
	}
}
