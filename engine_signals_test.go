package runnerq

// Integration tests for signals: WaitForSignal (buffered, parked, timeout)
// and cross-process delivery. Skipped unless RUNNERQ_TEST_DSN is set.
// Cross-process is simulated with a second backend instance sharing only the
// database, as in storage/postgres/signals_test.go.

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
)

// A signal delivered before the handler reaches WaitForSignal — here, before
// the engine even starts — is buffered and returned instantly on first pass.
func TestSignalBufferedBeforeWait(t *testing.T) {
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

	engine, err := Builder().Backend(backend).QueueName(queueName).MaxWorkers(2).Build()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}
	var invocations atomic.Int32
	engine.RegisterActivityWithName("approve_me", &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		invocations.Add(1)
		return ctx.WaitForSignal("approval", 0)
	}})

	// Enqueue and signal BEFORE the engine starts: the signal is buffered.
	fut, err := engine.GetActivityExecutor().
		ActivityNamed("approve_me").Payload(json.RawMessage(`{}`)).Execute(ctx)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if err := engine.Signal(ctx, fut.activityID, "approval", json.RawMessage(`{"approved":true}`)); err != nil {
		t.Fatalf("signal: %v", err)
	}

	startDone := make(chan error, 1)
	go func() { startDone <- engine.Start(ctx) }()
	t.Cleanup(func() {
		engine.Stop()
		select {
		case err := <-startDone:
			if err != nil {
				t.Errorf("engine.Start: %v", err)
			}
		case <-time.After(35 * time.Second):
			t.Error("engine did not stop")
		}
	})

	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	res, err := backend.WaitForResult(waitCtx, fut.activityID)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if res.State != storage.ResultOk || !strings.Contains(string(res.Data), "approved") {
		t.Fatalf("result = %v %s, want buffered signal payload", res.State, res.Data)
	}
	if got := invocations.Load(); got != 1 {
		t.Fatalf("handler invoked %d times, want 1 — buffered signal must resolve on first pass", got)
	}
}

// An unbounded WaitForSignal yields (no worker held, no retry consumed) and
// is woken by delivery from a DIFFERENT process.
func TestSignalWakesParkedWaiterAcrossProcesses(t *testing.T) {
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping integration test")
	}
	var invocations atomic.Int32
	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		invocations.Add(1)
		return ctx.WaitForSignal("human-approval", 0)
	}}
	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivityWithName("gate", h) })

	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("gate").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// Wait until the activity has actually yielded (parked as Waiting).
	deadline := time.After(15 * time.Second)
	for {
		snap, err := rig.backend.GetActivity(context.Background(), fut.activityID)
		if err == nil && snap != nil && snap.Status == "Waiting" {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("activity never parked; last snapshot: %+v", snap)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Deliver from a separate backend instance — a different "process".
	queueName := rig.engine.config.QueueName
	external, err := postgres.New(context.Background(), dsn, queueName)
	if err != nil {
		t.Fatalf("external backend: %v", err)
	}
	defer external.Close()
	if err := SignalActivity(context.Background(), external, fut.activityID, "human-approval", json.RawMessage(`{"by":"ops"}`)); err != nil {
		t.Fatalf("external signal: %v", err)
	}

	res := rig.await(t, fut.activityID, 20*time.Second)
	if res.State != storage.ResultOk || !strings.Contains(string(res.Data), "ops") {
		t.Fatalf("result = %v %s, want delivered payload", res.State, res.Data)
	}
	if got := invocations.Load(); got != 2 {
		t.Fatalf("handler invoked %d times, want 2 (park + resume)", got)
	}
	snap, err := rig.backend.GetActivity(context.Background(), fut.activityID)
	if err != nil || snap == nil {
		t.Fatalf("get activity: snap=%v err=%v", snap, err)
	}
	if snap.RetryCount != 0 {
		t.Fatalf("retry_count = %d, want 0 — a parked signal wait must not consume retries", snap.RetryCount)
	}
}

// A short timeout waits in-process — single invocation, no park — and
// surfaces a typed, non-retryable timeout error the handler can act on.
func TestSignalTimeoutInProcess(t *testing.T) {
	var invocations atomic.Int32
	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		invocations.Add(1)
		_, err := ctx.WaitForSignal("never-arrives", time.Second)
		if !IsSignalTimeout(err) {
			return nil, NewNonRetryError("expected a signal timeout, got: " + errString(err))
		}
		return json.RawMessage(`{"timed_out":true}`), nil
	}}
	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivityWithName("waiter", h) })

	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("waiter").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	res := rig.await(t, fut.activityID, 20*time.Second)
	if res.State != storage.ResultOk || !strings.Contains(string(res.Data), "timed_out") {
		t.Fatalf("result = %v %s, want timeout handled", res.State, res.Data)
	}
	// The contract under test is that a 1s wait inside the default 300s
	// budget stays in-process: exactly one invocation, no Yielded event.
	if got := invocations.Load(); got != 1 {
		t.Fatalf("handler invoked %d times, want 1 — a short wait must not park", got)
	}
	events, err := rig.backend.GetActivityEvents(context.Background(), fut.activityID, 100)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	for _, ev := range events {
		if ev.EventType == storage.EventYielded {
			t.Fatal("short in-budget signal wait recorded a Yielded event")
		}
	}
}

// A timeout longer than the handler budget parks; the wait deadline persists
// across the park, so the replay times out instead of restarting the clock.
func TestSignalTimeoutSurvivesPark(t *testing.T) {
	var invocations atomic.Int32
	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		invocations.Add(1)
		_, err := ctx.WaitForSignal("slow-approval", 4*time.Second)
		if err != nil {
			if IsSignalTimeout(err) {
				return json.RawMessage(`{"timed_out":true}`), nil
			}
			return nil, err // includes the yield sentinel on the first pass
		}
		return nil, NewNonRetryError("signal unexpectedly delivered")
	}}
	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivityWithName("slow_gate", h) })

	start := time.Now()
	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("slow_gate").
		Timeout(2 * time.Second). // 4s wait in a 2s budget → must park
		Payload(json.RawMessage(`{}`)).
		Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	res := rig.await(t, fut.activityID, 30*time.Second)
	elapsed := time.Since(start)
	if res.State != storage.ResultOk || !strings.Contains(string(res.Data), "timed_out") {
		t.Fatalf("result = %v %s, want persisted-deadline timeout", res.State, res.Data)
	}
	if elapsed < 4*time.Second {
		t.Fatalf("completed in %v — the 4s wait deadline was not honored across the park", elapsed)
	}
	if got := invocations.Load(); got != 2 {
		t.Fatalf("handler invoked %d times, want 2 (park + timed-out replay)", got)
	}
	snap, _ := rig.backend.GetActivity(context.Background(), fut.activityID)
	if snap == nil || snap.RetryCount != 0 {
		t.Fatalf("snapshot %+v, want retry_count 0", snap)
	}
}

// Signalling a nonexistent activity is rejected with a not-found error
// specifically — not just any backend failure.
func TestSignalNonexistentActivity(t *testing.T) {
	rig := newStepsRig(t, func(e *WorkerEngine) {})
	err := rig.engine.Signal(context.Background(), uuid.New(), "ghost", nil)
	if err == nil {
		t.Fatal("signal to nonexistent activity succeeded; it must be rejected")
	}
	we, ok := IsWorkerError(err)
	if !ok {
		t.Fatalf("error type = %T (%v), want *WorkerError", err, err)
	}
	se, ok := storage.IsStorageError(we.Cause)
	if !ok || se.Kind != storage.ErrNotFound {
		t.Fatalf("error = %v (cause %v), want a storage not-found rejection", err, we.Cause)
	}
}

func errString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// SignalByKey wakes the workflow that owns an idempotency key, without the
// caller knowing the internal activity ID — the library resolves
// key -> activity. This is what lets a webhook signal by business reference.
func TestSignalByKeyWakesWorkflowOwningKey(t *testing.T) {
	const key = "transfer-ref-9001"
	var invocations atomic.Int32
	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		invocations.Add(1)
		return ctx.WaitForSignal("settled", 0) // park until signalled
	}}
	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivityWithName("xfer", h) })

	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("xfer").
		IdempotencyKeyOption(key, ReturnExisting).
		Payload(json.RawMessage(`{}`)).
		Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// Deliver by KEY — we never reference fut.ActivityID() to signal. The
	// activity type plus the business key resolve the instance.
	if err := rig.engine.SignalByKey(context.Background(), "xfer", key, "settled", json.RawMessage(`{"status":"completed"}`)); err != nil {
		t.Fatalf("signal by key: %v", err)
	}

	res := rig.await(t, fut.activityID, 20*time.Second)
	if res.State != storage.ResultOk || !strings.Contains(string(res.Data), "completed") {
		t.Fatalf("result = %v %s, want the signalled payload", res.State, res.Data)
	}
}

// SignalByKey to an unclaimed key returns a typed not-found a caller can
// treat as "already settled / nothing to wake".
func TestSignalByKeyUnknownKeyIsNotFound(t *testing.T) {
	rig := newStepsRig(t, func(e *WorkerEngine) {})
	err := rig.engine.SignalByKey(context.Background(), "xfer", "no-such-key", "settled", nil)
	if err == nil {
		t.Fatal("signal to unclaimed key succeeded; want not-found")
	}
	if !IsActivityNotFound(err) {
		t.Fatalf("error = %v, want IsActivityNotFound", err)
	}
}

// A delivered signal is recorded with its human name — on the result row (so it
// shows in step listings) and on the Signaled event. (C.)
func TestSignalRecordedWithName(t *testing.T) {
	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		return ctx.WaitForSignal("approve", 0)
	}}
	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivityWithName("gate", h) })

	fut, err := rig.engine.GetActivityExecutor().ActivityNamed("gate").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	// Wait until parked, then deliver the named signal.
	deadline := time.After(15 * time.Second)
	for {
		snap, _ := rig.backend.GetActivity(context.Background(), fut.activityID)
		if snap != nil && snap.Status == "Waiting" {
			break
		}
		select {
		case <-deadline:
			t.Fatal("activity never parked")
		case <-time.After(100 * time.Millisecond):
		}
	}
	if err := SignalActivity(context.Background(), rig.backend, fut.activityID, "approve", json.RawMessage(`{"by":"ops"}`)); err != nil {
		t.Fatalf("signal: %v", err)
	}
	rig.await(t, fut.activityID, 20*time.Second)

	// The delivered signal appears as a named "signal" step.
	steps, err := rig.backend.GetActivitySteps(context.Background(), fut.activityID)
	if err != nil {
		t.Fatalf("steps: %v", err)
	}
	found := false
	for _, s := range steps {
		if s.Kind == "signal" && s.Name == "approve" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no signal step named 'approve' in %+v", steps)
	}

	// The Signaled event carries the name.
	events, err := rig.backend.GetActivityEvents(context.Background(), fut.activityID, 100)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	named := false
	for _, ev := range events {
		if ev.EventType != storage.EventSignaled {
			continue
		}
		var d map[string]any
		_ = json.Unmarshal(ev.Detail, &d)
		if d["name"] == "approve" {
			named = true
		}
	}
	if !named {
		t.Fatal("no Signaled event carrying name=approve")
	}
}
