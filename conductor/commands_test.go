package conductor

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go"
	"github.com/runnerq/runnerq-go/conductor/internal/wire"
	"github.com/runnerq/runnerq-go/storage"
)

// Blocker runs until its context ends, recording why it stopped.
type Blocker struct {
	runnerq.DefaultDeadLetterHandler
	started chan uuid.UUID
	stopped chan error
}

func (b *Blocker) Handle(ctx runnerq.ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
	b.started <- ctx.ActivityID
	<-ctx.Ctx.Done()
	b.stopped <- context.Cause(ctx.Ctx)
	return nil, ctx.Ctx.Err()
}

// Parent spawns a Blocker child and reports how awaiting it ended.
type Parent struct {
	runnerq.DefaultDeadLetterHandler
}

func (Parent) Handle(ctx runnerq.ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
	f, err := ctx.ActivityExecutor.ActivityNamed("Blocker").Payload(json.RawMessage(`{}`)).Step("child").Execute(ctx.Ctx)
	if err != nil {
		return nil, err
	}
	if _, err := f.GetResult(ctx.Ctx); err != nil {
		if _, failed := runnerq.IsWorkerError(err); !failed {
			return nil, err // parking: hand control back to the engine
		}
		out, _ := json.Marshal(map[string]string{"child_error": err.Error()})
		return out, nil
	}
	return json.RawMessage(`{"child":"ok"}`), nil
}

// Approval waits for an "approve" signal and returns its payload.
type Approval struct {
	runnerq.DefaultDeadLetterHandler
}

func (Approval) Handle(ctx runnerq.ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
	return ctx.WaitForSignal("approve", time.Hour)
}

func runEngine(t *testing.T, e *runnerq.WorkerEngine) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = e.Start(ctx); close(done) }()
	t.Cleanup(func() {
		e.Stop()
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
		}
	})
}

func controlEngine(t *testing.T) (*runnerq.WorkerEngine, *Blocker, string) {
	t.Helper()
	e, _, queue := pgEngine(t)
	blocker := &Blocker{started: make(chan uuid.UUID, 4), stopped: make(chan error, 4)}
	e.RegisterActivityWithName("Blocker", blocker)
	e.RegisterActivity(&Parent{})
	e.RegisterActivity(&Approval{})
	return e, blocker, queue
}

func spawn(t *testing.T, e *runnerq.WorkerEngine, activityType string, opts ...func(*runnerq.ActivityBuilder)) uuid.UUID {
	t.Helper()
	b := e.GetActivityExecutor().ActivityNamed(activityType).Payload(json.RawMessage(`{}`))
	for _, o := range opts {
		o(b)
	}
	f, err := b.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return f.ActivityID()
}

func (g *fakeGateway) status(id uuid.UUID) string {
	g.t.Helper()
	var a wire.Activity
	g.ok(wire.TypeActivitiesGet, wire.GetRequest{ID: id.String()}, &a)
	return string(a.Status)
}

func (g *fakeGateway) waitStatus(id uuid.UUID, want string) {
	g.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if got := g.status(id); got == want {
			return
		}
		if time.Now().After(deadline) {
			g.t.Fatalf("activity %s never reached %q (now %q)", id, want, g.status(id))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestCommandsNeedAllowControl(t *testing.T) {
	e, _, _ := pgEngine(t)
	g := newFakeGateway(t, wire.SessionConfig{})
	startAgent(t, e, g, Config{})
	h := g.waitHello()
	for msgType := range commandKinds {
		if _, ok := h.Capabilities[msgType]; ok {
			t.Fatalf("%s advertised without AllowControl", msgType)
		}
	}
	g.fails(wire.TypeActivitiesCancel, wire.CancelRequest{Target: wire.Target{IDs: []string{uuid.NewString()}}}, wire.CodeUnsupported)
}

func TestCancelStopsRunningHandler(t *testing.T) {
	e, blocker, _ := controlEngine(t)
	g := newFakeGateway(t, wire.SessionConfig{})
	startAgent(t, e, g, Config{AllowControl: true})
	h := g.waitHello()
	if c := h.Capabilities[wire.TypeActivitiesSignal]; len(c.Targets) != 3 {
		t.Fatalf("signal capability %+v", c)
	}
	runEngine(t, e)

	id := spawn(t, e, "Blocker")
	select {
	case <-blocker.started:
	case <-time.After(15 * time.Second):
		t.Fatal("blocker never started")
	}

	var res wire.CommandResult
	g.ok(wire.TypeActivitiesCancel, wire.CancelRequest{CommandID: "c-1", Reason: "stuck", Target: wire.Target{IDs: []string{id.String()}}}, &res)
	if res.Applied != 1 || res.Results[0].Status != storage.RecordStatusCancelled {
		t.Fatalf("cancel %+v", res)
	}
	select {
	case cause := <-blocker.stopped:
		if se, ok := storage.IsStorageError(cause); !ok || se.Kind != storage.ErrClaimLost {
			t.Fatalf("handler stopped with %v, want a claim-lost cause", cause)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("running handler was not stopped by the cancel")
	}
	time.Sleep(200 * time.Millisecond) // let the engine's fenced ack attempt land
	if st := g.status(id); st != storage.RecordStatusCancelled {
		t.Fatalf("status %q after the handler stopped", st)
	}

	var again wire.CommandResult
	g.ok(wire.TypeActivitiesCancel, wire.CancelRequest{CommandID: "c-1", Reason: "stuck", Target: wire.Target{IDs: []string{id.String()}}}, &again)
	if !again.Replayed || again.Applied != 1 {
		t.Fatalf("replay %+v", again)
	}
	g.fails(wire.TypeActivitiesCancel, wire.CancelRequest{CommandID: "c-1", Reason: "different", Target: wire.Target{IDs: []string{id.String()}}}, wire.CodeConflict)
}

func TestCancelledChildFailsAwaitingParent(t *testing.T) {
	e, blocker, _ := controlEngine(t)
	g := newFakeGateway(t, wire.SessionConfig{})
	startAgent(t, e, g, Config{AllowControl: true})
	g.waitHello()
	runEngine(t, e)

	parent := spawn(t, e, "Parent")
	var child uuid.UUID
	select {
	case child = <-blocker.started:
	case <-time.After(15 * time.Second):
		t.Fatal("child never started")
	}
	var res wire.CommandResult
	g.ok(wire.TypeActivitiesCancel, wire.CancelRequest{Cascade: wire.CascadeNone, Target: wire.Target{IDs: []string{child.String()}}}, &res)
	if res.Applied != 1 {
		t.Fatalf("cancel %+v", res)
	}
	g.waitStatus(parent, storage.RecordStatusCompleted)
	var r wire.Result
	g.ok(wire.TypeResultsGet, wire.ResultRequest{ActivityID: parent.String()}, &r)
	if !strings.Contains(string(r.Data), "cancelled") {
		t.Fatalf("parent did not see the cancellation: %s", r.Data)
	}
}

func TestSignalCommandByKey(t *testing.T) {
	e, _, _ := controlEngine(t)
	g := newFakeGateway(t, wire.SessionConfig{})
	startAgent(t, e, g, Config{AllowControl: true})
	g.waitHello()
	runEngine(t, e)

	id := spawn(t, e, "Approval", func(b *runnerq.ActivityBuilder) { b.IdempotencyKeyOption("order-9", runnerq.ReturnExisting) })
	g.waitStatus(id, storage.RecordStatusWaiting)

	var res wire.CommandResult
	g.ok(wire.TypeActivitiesSignal, wire.SignalRequest{Name: "approve", Payload: json.RawMessage(`{"by":"ops"}`),
		Target: wire.Target{IdempotencyKey: "order-9", Type: "Approval"}}, &res)
	if res.Applied != 1 {
		t.Fatalf("signal %+v", res)
	}
	g.waitStatus(id, storage.RecordStatusCompleted)
	var r wire.Result
	g.ok(wire.TypeResultsGet, wire.ResultRequest{ActivityID: id.String()}, &r)
	if !strings.Contains(string(r.Data), "ops") {
		t.Fatalf("workflow result %s", r.Data)
	}
}

func TestCommandValidation(t *testing.T) {
	e, _, queue := controlEngine(t)
	g := newFakeGateway(t, wire.SessionConfig{})
	startAgent(t, e, g, Config{AllowControl: true})
	g.waitHello()
	id := spawn(t, e, "Blocker") // engine not running: stays pending

	e1 := g.fails(wire.TypeActivitiesCancel, map[string]any{"priority": 3, "target": wire.Target{IDs: []string{id.String()}}}, wire.CodeInvalidArgument)
	if e1.Details["field"] != "priority" {
		t.Fatalf("details %+v", e1.Details)
	}
	g.fails(wire.TypeActivitiesCancel, wire.CancelRequest{Target: wire.Target{IDs: []string{id.String()}, Queue: queue + "-other"}}, wire.CodeFailedPrecondition)
	g.fails(wire.TypeActivitiesCancel, wire.CancelRequest{Cascade: "sideways", Target: wire.Target{IDs: []string{id.String()}}}, wire.CodeInvalidArgument)
	g.fails(wire.TypeActivitiesSignal, wire.SignalRequest{Name: "x", Target: wire.Target{IdempotencyKey: "k"}}, wire.CodeInvalidArgument)
	g.fails(wire.TypeActivitiesReschedule, wire.RescheduleRequest{At: "tomorrow", Target: wire.Target{IDs: []string{id.String()}}}, wire.CodeInvalidArgument)
	g.fails(wire.TypeActivitiesCancel, wire.CancelRequest{Target: wire.Target{Filter: mustFilter(map[string]any{"field": "type", "op": "eq", "value": "Blocker"})}}, wire.CodeInvalidArgument) // no max
	g.fails(wire.TypeActivitiesCancel, json.RawMessage(`{"target":{"ids":["x"]},"surprise":true}`), wire.CodeInvalidArgument)

	var res wire.CommandResult
	g.ok(wire.TypeActivitiesSetPriority, wire.SetPriorityRequest{Priority: 4, DryRun: true, Target: wire.Target{IDs: []string{id.String(), "not-an-id"}}}, &res)
	if len(res.Results) != 2 || res.Results[0].Outcome != storage.CommandWouldApply || res.Results[1].Error == nil || res.Results[1].Error.Code != wire.CodeNotFound {
		t.Fatalf("dry run with a foreign id %+v", res)
	}
	g.ok(wire.TypeActivitiesRetry, wire.RetryRequest{Target: wire.Target{IDs: []string{id.String()}}}, &res)
	if res.Results[0].Error == nil || res.Results[0].Error.Code != wire.CodeFailedPrecondition || res.Results[0].Error.Details["status"] != "pending" {
		t.Fatalf("retry of pending work %+v", res.Results[0])
	}
	g.ok(wire.TypeActivitiesCancel, wire.CancelRequest{Target: wire.Target{IDs: []string{"only-foreign"}}}, &res)
	if res.Matched != 0 || len(res.Results) != 1 || res.Results[0].Error.Code != wire.CodeNotFound {
		t.Fatalf("all-foreign ids %+v", res)
	}
}
