package runnerq

// Client: the management API over storage commands and queries, with no
// engine running, and a cancel reaching a handler on another engine.
// Skipped unless RUNNERQ_TEST_DSN is set.

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

func wantOutcome(t *testing.T, res *CommandResult, id uuid.UUID, outcome CommandOutcome, status ActivityStatus) {
	t.Helper()
	it, ok := res.Item(id)
	if !ok || it.Outcome != outcome || it.Status != status {
		t.Fatalf("item %s = %+v (found %v), want %s with status %q", id, it, ok, outcome, status)
	}
}

func TestClientManagesWorkWithoutAnEngine(t *testing.T) {
	dsn := contractDSN(t)
	ctx := context.Background()
	client := NewClient(contractBackend(t, dsn, contractQueue()))

	fut, err := client.ActivityNamed("report").Payload(json.RawMessage(`{"n":1}`)).Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id := fut.ActivityID()
	later, err := client.ActivityNamed("report").Payload(json.RawMessage(`{}`)).Delay(time.Hour).Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}

	rec, err := client.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var payload struct{ N int }
	if err := json.Unmarshal(rec.Payload, &payload); err != nil || rec.Status != storage.RecordStatusPending || rec.Type != "report" || payload.N != 1 {
		t.Fatalf("Get = %+v", rec)
	}
	page, err := client.List(ctx, ListOptions{Statuses: []ActivityStatus{StatusScheduled}, Type: "report"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != later.ActivityID() {
		t.Fatalf("List(scheduled) = %+v", page.Items)
	}

	res, err := client.SetPriority(ctx, IDs(id), PriorityCritical)
	if err != nil {
		t.Fatal(err)
	}
	wantOutcome(t, res, id, Applied, StatusPending)
	if rec, _ := client.Get(ctx, id); rec.Priority != int(PriorityCritical) {
		t.Fatalf("priority %d after SetPriority", rec.Priority)
	}

	at := time.Now().Add(2 * time.Hour)
	if res, err = client.Reschedule(ctx, IDs(later.ActivityID(), id), at); err != nil {
		t.Fatal(err)
	}
	wantOutcome(t, res, later.ActivityID(), Applied, StatusScheduled)
	wantOutcome(t, res, id, NotEligible, StatusPending)
	if it, _ := res.Item(id); it.Reason == "" {
		t.Fatal("a not-eligible item has no reason")
	}
	if res, err = client.RunNow(ctx, IDs(later.ActivityID())); err != nil {
		t.Fatal(err)
	}
	wantOutcome(t, res, later.ActivityID(), Applied, StatusScheduled)

	ghost := uuid.New()
	if res, err = client.Cancel(ctx, IDs(id, ghost), WithCommandID("cancel-report"), WithReason("not needed")); err != nil {
		t.Fatal(err)
	}
	wantOutcome(t, res, id, Applied, StatusCancelled)
	wantOutcome(t, res, ghost, NotFound, "")
	if res.CommandID != "cancel-report" || res.Replayed || res.Applied != 1 {
		t.Fatalf("Cancel = %+v", res)
	}
	if _, err := client.Result(ctx, id); !IsCancelled(err) {
		t.Fatalf("Result after Cancel = %v, want a cancellation", err)
	}

	// The same command id replays; with other input it conflicts.
	if res, err = client.Cancel(ctx, IDs(id, ghost), WithCommandID("cancel-report"), WithReason("not needed")); err != nil {
		t.Fatal(err)
	}
	if !res.Replayed {
		t.Fatalf("repeated command was not replayed: %+v", res)
	}
	wantOutcome(t, res, id, Applied, StatusCancelled)
	if _, err = client.Cancel(ctx, IDs(id), WithCommandID("cancel-report")); !IsCommandConflict(err) {
		t.Fatalf("reused command id with other input: %v, want a conflict", err)
	}

	// A dry run changes nothing.
	if res, err = client.Delete(ctx, IDs(id), DryRun()); err != nil {
		t.Fatal(err)
	}
	wantOutcome(t, res, id, WouldApply, "")
	if _, err := client.Get(ctx, id); err != nil {
		t.Fatalf("dry-run delete removed the activity: %v", err)
	}

	if res, err = client.Retry(ctx, IDs(id), ResetAttempts()); err != nil {
		t.Fatal(err)
	}
	wantOutcome(t, res, id, Applied, StatusPending)
	if res, err = client.Delete(ctx, IDs(id)); err != nil {
		t.Fatal(err)
	}
	wantOutcome(t, res, id, NotEligible, StatusPending)

	// Filter targets: cancel everything of the type, then delete it.
	if res, err = client.Cancel(ctx, Matching(storage.QueryFilter{Field: "type", Op: storage.OpEq, Value: "report"}, 10)); err != nil {
		t.Fatal(err)
	}
	if res.Applied != 2 || len(res.Items) != 2 || res.More {
		t.Fatalf("Cancel(filter) = %+v", res)
	}
	if res, err = client.Delete(ctx, IDs(id)); err != nil {
		t.Fatal(err)
	}
	wantOutcome(t, res, id, Applied, "")
	if _, err := client.Get(ctx, id); !IsActivityNotFound(err) {
		t.Fatalf("Get after Delete = %v, want not found", err)
	}

	if _, err := client.Retry(ctx, IDs(id), NoCascade()); err == nil {
		t.Fatal("NoCascade on Retry was accepted")
	}
	if _, err := client.Cancel(ctx, IDs(id), ResetAttempts()); err == nil {
		t.Fatal("ResetAttempts on Cancel was accepted")
	}
}

// A cancel issued through one engine's client stops the handler another
// engine (another process: its own backend and pool) is running, through the
// claim revocation the heartbeat uses, long before the next heartbeat.
func TestClientCancelStopsAHandlerOnAnotherEngine(t *testing.T) {
	dsn := contractDSN(t)
	queue := contractQueue()
	started := make(chan uuid.UUID, 1)
	stopped := make(chan error, 1)
	var runs atomic.Int32
	worker, err := Builder().Backend(contractBackend(t, dsn, queue)).QueueName(queue).MaxWorkers(2).Build()
	if err != nil {
		t.Fatal(err)
	}
	worker.heartbeatInterval = time.Minute // so only the cancel can stop it in time
	worker.RegisterActivityWithName("long", &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		runs.Add(1)
		started <- ctx.ActivityID
		<-ctx.Ctx.Done()
		stopped <- context.Cause(ctx.Ctx)
		return nil, ctx.Ctx.Err()
	}})
	startEngine(t, worker)

	other, err := Builder().Backend(contractBackend(t, dsn, queue)).QueueName(queue).Build()
	if err != nil {
		t.Fatal(err)
	}
	client := other.Client()
	ctx := context.Background()
	fut, err := client.ActivityNamed("long").Payload(json.RawMessage(`{}`)).Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("handler never started")
	}
	if rec, err := client.Get(ctx, fut.ActivityID()); err != nil || rec.Status != storage.RecordStatusRunning {
		t.Fatalf("Get = %+v, %v; want running", rec, err)
	}

	sent := time.Now()
	res, err := client.Cancel(ctx, IDs(fut.ActivityID()), WithReason("user abort"))
	if err != nil {
		t.Fatal(err)
	}
	wantOutcome(t, res, fut.ActivityID(), Applied, StatusCancelled)
	select {
	case cause := <-stopped:
		if se, ok := storage.IsStorageError(cause); !ok || se.Kind != storage.ErrClaimLost {
			t.Fatalf("handler context cause = %v, want a lost claim", cause)
		}
		if d := time.Since(sent); d > 5*time.Second {
			t.Fatalf("handler stopped %v after the cancel", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancel did not reach the running handler")
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := client.Result(waitCtx, fut.ActivityID()); !IsCancelled(err) {
		t.Fatalf("Result = %v, want a cancellation", err)
	}
	time.Sleep(500 * time.Millisecond) // a stray ack or retry would land here
	rec, err := client.Get(ctx, fut.ActivityID())
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != storage.RecordStatusCancelled || rec.LastError == nil || rec.LastError.Message != "activity cancelled: user abort" {
		t.Fatalf("after cancel: %+v (last error %+v)", rec, rec.LastError)
	}
	if n := runs.Load(); n != 1 {
		t.Fatalf("handler ran %d times", n)
	}
}

// A parent awaiting a child that is cancelled gets the cancellation from
// GetResult, and a cascading cancel of a parent stops its running child.
func TestClientCancelReachesTheTree(t *testing.T) {
	dsn := contractDSN(t)
	queue := contractQueue()
	childStopped := make(chan struct{}, 1)
	parentErr := make(chan error, 1)
	type spawned struct{ parent, child uuid.UUID }
	children := make(chan spawned, 16)
	engine, err := Builder().Backend(contractBackend(t, dsn, queue)).QueueName(queue).MaxWorkers(4).Build()
	if err != nil {
		t.Fatal(err)
	}
	engine.heartbeatInterval = time.Minute
	engine.RegisterActivityWithName("parent", &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		fut, err := ctx.ActivityExecutor.ActivityNamed("child").Payload(json.RawMessage(`{}`)).Step("child").Execute(ctx.Ctx)
		if err != nil {
			return nil, err
		}
		children <- spawned{ctx.ActivityID, fut.ActivityID()}
		_, err = fut.GetResult(ctx.Ctx)
		if IsYield(err) {
			return nil, err
		}
		parentErr <- err
		return nil, NewNonRetryError("child failed")
	}})
	engine.RegisterActivityWithName("child", &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		<-ctx.Ctx.Done()
		childStopped <- struct{}{}
		return nil, ctx.Ctx.Err()
	}})
	startEngine(t, engine)
	client := NewClient(contractBackend(t, dsn, queue))
	ctx := context.Background()

	childOf := func(parent uuid.UUID) uuid.UUID {
		for {
			select {
			case s := <-children:
				if s.parent == parent {
					return s.child
				}
			case <-time.After(15 * time.Second):
				t.Fatal("parent spawned no child")
			}
		}
	}
	first, err := client.ActivityNamed("parent").Payload(json.RawMessage(`{}`)).Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	kid := childOf(first.ActivityID())
	waitStatus(t, client, kid, StatusRunning)
	res, err := client.Cancel(ctx, IDs(kid), NoCascade())
	if err != nil {
		t.Fatal(err)
	}
	wantOutcome(t, res, kid, Applied, StatusCancelled)
	select {
	case <-childStopped:
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled child kept running")
	}
	select {
	case err := <-parentErr:
		if !IsCancelled(err) {
			t.Fatalf("parent's GetResult = %v, want a cancellation", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("parent never saw the child's cancellation")
	}

	// Cascade: cancelling a parent stops its running child.
	parent, err := client.ActivityNamed("parent").Payload(json.RawMessage(`{}`)).Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	kid = childOf(parent.ActivityID())
	waitStatus(t, client, kid, StatusRunning)
	if res, err = client.Cancel(ctx, IDs(parent.ActivityID())); err != nil {
		t.Fatal(err)
	}
	if res.Cascaded != 1 {
		t.Fatalf("Cancel = %+v, want one cascaded child", res)
	}
	select {
	case <-childStopped:
	case <-time.After(10 * time.Second):
		t.Fatal("cascaded cancel did not stop the running child")
	}
	waitStatus(t, client, kid, StatusCancelled)
}

func waitStatus(t *testing.T, c *Client, id uuid.UUID, want ActivityStatus) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		rec, err := c.Get(context.Background(), id)
		if err == nil && ActivityStatus(rec.Status) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("activity %s: %+v %v, want status %s", id, rec, err, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestClientWithoutCommandStorage(t *testing.T) {
	c := NewClient(queueOnly{})
	if _, err := c.Cancel(context.Background(), IDs(uuid.New())); err == nil {
		t.Fatal("Cancel on a backend without commands succeeded")
	} else if we, ok := IsWorkerError(err); !ok || we.Kind != ErrConfiguration {
		t.Fatalf("Cancel = %v, want a configuration error", err)
	}
	if _, err := c.List(context.Background(), ListOptions{}); err == nil {
		t.Fatal("List on a backend without queries succeeded")
	}
}

// queueOnly is a storage.Storage with no optional capabilities.
type queueOnly struct{ storage.Storage }

// A client given a tracer traces its submissions and signals as an engine
// does: an enqueue span under the caller's span, whose traceparent the
// activity stores, and a signal-send span per signal.
func TestClientTracer(t *testing.T) {
	dsn := contractDSN(t)
	queue := contractQueue()
	backend := contractBackend(t, dsn, queue)
	tracer := &fakeTracer{}
	client := NewClient(backend, WithTracer(tracer))

	ctx, endCaller := tracer.Start(context.Background(), "caller", "")
	defer endCaller(OutcomeOk, nil)
	caller := tracer.Traceparent(ctx)
	fut, err := client.ActivityNamed("report").Payload(json.RawMessage(`{}`)).
		IdempotencyKeyOption("order-1", ReturnExisting).Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	enqueues := tracer.named(SpanEnqueue)
	if len(enqueues) != 1 {
		t.Fatalf("enqueue spans %+v", enqueues)
	}
	enq := enqueues[0]
	if enq.parent != caller || enq.outcome != OutcomeEnqueued || enq.attrs[AttrQueue] != queue ||
		enq.attrs[AttrActivityType] != "report" || enq.attrs[AttrActivityID] != fut.ActivityID().String() {
		t.Fatalf("enqueue span %+v, caller %s", enq, caller)
	}
	snap, err := backend.GetActivity(ctx, fut.ActivityID())
	if err != nil || snap.Metadata[MetadataTraceparent] != enq.traceparent {
		t.Fatalf("stored traceparent %v, enqueue span %s (%v)", snap.Metadata, enq.traceparent, err)
	}

	if err := client.Signal(ctx, fut.ActivityID(), "approve", nil); err != nil {
		t.Fatal(err)
	}
	if err := client.SignalByKey(ctx, "report", "order-1", "ship", nil); err != nil {
		t.Fatal(err)
	}
	if err := client.Signal(ctx, uuid.New(), "approve", nil); !IsActivityNotFound(err) {
		t.Fatalf("signal to a missing activity: %v", err)
	}
	sends := tracer.named(SpanSignalSend)
	want := []struct{ name, outcome string }{{"approve", OutcomeSent}, {"ship", OutcomeSent}, {"approve", OutcomeError}}
	if len(sends) != len(want) {
		t.Fatalf("signal spans %+v", sends)
	}
	for i, w := range want {
		s := sends[i]
		if s.parent != caller || s.attrs[AttrSignalName] != w.name || s.outcome != w.outcome || !s.ended {
			t.Errorf("signal span %d = %+v, want %s %s under %s", i, s, w.name, w.outcome, caller)
		}
	}
	if sends[0].attrs[AttrActivityID] != fut.ActivityID().String() || sends[1].attrs[AttrActivityID] != fut.ActivityID().String() {
		t.Errorf("signal spans' activity ids %v, %v", sends[0].attrs[AttrActivityID], sends[1].attrs[AttrActivityID])
	}
}

// An engine's client signals through the engine's tracer.
func TestEngineClientSignalTraced(t *testing.T) {
	tracer := &fakeTracer{}
	rig := newStepsRig(t, func(e *WorkerEngine) { e.SetTracer(tracer) })
	client := rig.engine.Client()
	fut, err := client.ActivityNamed("report").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Signal(context.Background(), fut.ActivityID(), "approve", nil); err != nil {
		t.Fatal(err)
	}
	if enq, sends := tracer.named(SpanEnqueue), tracer.named(SpanSignalSend); len(enq) != 1 || len(sends) != 1 || sends[0].outcome != OutcomeSent {
		t.Fatalf("enqueue spans %+v, signal spans %+v", enq, sends)
	}
}
