package runnerq

// Unit tests for the batch intake path: the dispatcher that replaces the
// fixed worker pool when the backend implements storage.BatchQueueStorage,
// and the adapter that exposes that capability to the engine. No database:
// a scripted queue lets each test drive the dispatcher one claim at a time.

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

type batchRequest struct {
	limit  int
	prefix string
}

type batchReply struct {
	claims []claimedActivity
	err    error
}

type batchAck struct {
	leaseID string
	ctxErr  error
}

// scriptedBatchQueue reports every DequeueBatch call on requests and then
// blocks until the test answers on replies (or intake is cancelled), so the
// dispatcher never spins and each step of a test is deterministic.
type scriptedBatchQueue struct {
	activityQueue
	requests  chan batchRequest
	replies   chan batchReply
	completed chan batchAck
}

func newScriptedBatchQueue() *scriptedBatchQueue {
	return &scriptedBatchQueue{
		requests:  make(chan batchRequest, 16),
		replies:   make(chan batchReply, 16),
		completed: make(chan batchAck, 16),
	}
}

func (q *scriptedBatchQueue) DequeueBatch(ctx context.Context, limit int, _ time.Duration, prefix string) ([]claimedActivity, error) {
	select {
	case q.requests <- batchRequest{limit: limit, prefix: prefix}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case r := <-q.replies:
		return r.claims, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (q *scriptedBatchQueue) MarkCompleted(ctx context.Context, _ *activity, _ json.RawMessage, leaseID string) error {
	q.completed <- batchAck{leaseID: leaseID, ctxErr: ctx.Err()}
	return nil
}

// MarkFailed absorbs the failure a gated handler reports when test cleanup
// cancels its context; nothing observes it.
func (q *scriptedBatchQueue) MarkFailed(context.Context, *activity, string, bool, string) (bool, error) {
	return false, nil
}

// gatedHandler holds every activity until the test releases it and records
// the peak number of activities running at once.
type gatedHandler struct {
	DefaultDeadLetterHandler
	started  chan uuid.UUID
	release  chan struct{}
	inFlight atomic.Int32
	peak     atomic.Int32
}

func newGatedHandler() *gatedHandler {
	return &gatedHandler{started: make(chan uuid.UUID, 16), release: make(chan struct{}, 16)}
}

func (h *gatedHandler) Handle(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
	n := h.inFlight.Add(1)
	defer h.inFlight.Add(-1)
	for {
		peak := h.peak.Load()
		if n <= peak || h.peak.CompareAndSwap(peak, n) {
			break
		}
	}
	h.started <- ctx.ActivityID
	select {
	case <-h.release:
		return json.RawMessage(`"ok"`), nil
	case <-ctx.Ctx.Done():
		return nil, ctx.Ctx.Err()
	}
}

func (h *gatedHandler) releaseN(n int) {
	for range n {
		h.release <- struct{}{}
	}
}

func newClaims(n int) []claimedActivity {
	claims := make([]claimedActivity, 0, n)
	for range n {
		claims = append(claims, claimedActivity{
			activity: newActivity("test", nil, nil),
			leaseID:  uuid.NewString(),
		})
	}
	return claims
}

// startDispatcher runs the dispatcher with the given capacity and returns a
// channel closed when it exits plus the cancel for its intake context.
// Cleanup cancels intake and then the handler context, so a gated handler
// still blocked after a failed assertion unwinds instead of holding the
// dispatcher (and the test) until its activity timeout.
func startDispatcher(t *testing.T, q batchActivityQueue, capacity int, h ActivityHandler) (done <-chan struct{}, cancel context.CancelFunc) {
	t.Helper()
	cfg := DefaultWorkerConfig()
	cfg.MaxConcurrentActivities = capacity
	e := &WorkerEngine{queue: q, handlers: map[string]ActivityHandler{"test": h}, metrics: NoopMetrics{}, config: cfg}
	e.running.Store(true)
	ctx, cancelFn := context.WithCancel(context.Background())
	handlerCtx, cancelHandlers := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		e.runBatchDispatcher(ctx, handlerCtx, q)
	}()
	t.Cleanup(func() {
		e.running.Store(false)
		cancelFn()
		cancelHandlers()
		<-stopped
	})
	return stopped, cancelFn
}

func TestBatchDispatcherClaimsExactlyTheIdleSlots(t *testing.T) {
	const capacity = 3
	q := newScriptedBatchQueue()
	h := newGatedHandler()
	startDispatcher(t, q, capacity, h)

	// Cold start: every slot is idle, so the first claim asks for all of them.
	first := receive(t, q.requests)
	if first.limit != capacity {
		t.Fatalf("first claim limit = %d, want %d", first.limit, capacity)
	}
	issued := map[string]bool{}
	wave := newClaims(capacity)
	for _, c := range wave {
		issued[c.leaseID] = true
	}
	q.replies <- batchReply{claims: wave}
	for range capacity {
		receive(t, h.started)
	}

	// One activity finishes: exactly one slot is idle, so the next claim
	// asks for one, under a prefix the earlier claim did not use.
	h.releaseN(1)
	ack := receive(t, q.completed)
	if !issued[ack.leaseID] || ack.ctxErr != nil {
		t.Fatalf("ack = %+v, want an issued token acked on a live context", ack)
	}
	second := receive(t, q.requests)
	if second.limit != 1 {
		t.Fatalf("claim after one completion asked for %d slots, want 1", second.limit)
	}
	if second.prefix == first.prefix {
		t.Fatalf("claim prefix %q reused across calls; tokens would not be unique", second.prefix)
	}
	extra := newClaims(1)
	issued[extra[0].leaseID] = true
	q.replies <- batchReply{claims: extra}
	receive(t, h.started)

	// Everything finishes. The dispatcher may see the freed slots in more
	// than one sweep, but it must never ask for more than exist and must end
	// up asking for all of them once nothing is running.
	h.releaseN(capacity)
	acked := map[string]bool{ack.leaseID: true}
	for range capacity {
		a := receive(t, q.completed)
		if !issued[a.leaseID] || acked[a.leaseID] {
			t.Fatalf("ack token %q is not an issued, unacked token", a.leaseID)
		}
		acked[a.leaseID] = true
	}
	for {
		req := receive(t, q.requests)
		if req.limit < 1 || req.limit > capacity {
			t.Fatalf("claim limit %d outside 1..%d", req.limit, capacity)
		}
		if req.limit == capacity {
			break
		}
		q.replies <- batchReply{}
	}
	if got := h.peak.Load(); got != capacity {
		t.Fatalf("peak concurrency = %d, want %d (slots must be fully used, never exceeded)", got, capacity)
	}
}

func TestBatchDispatcherBacksOffAfterClaimError(t *testing.T) {
	q := newScriptedBatchQueue()
	done, _ := startDispatcher(t, q, 2, newGatedHandler())

	first := receive(t, q.requests)
	q.replies <- batchReply{err: errors.New("backend down")}

	again := receive(t, q.requests)
	if again.limit != first.limit {
		t.Fatalf("retry asked for %d slots, want the same %d (no slot may leak on error)", again.limit, first.limit)
	}
	select {
	case <-done:
		t.Fatal("dispatcher exited on a claim error; it must back off and keep claiming")
	default:
	}
}

func TestBatchDispatcherDrainsInFlightWorkAfterIntakeStops(t *testing.T) {
	q := newScriptedBatchQueue()
	h := newGatedHandler()
	done, cancelIntake := startDispatcher(t, q, 2, h)

	receive(t, q.requests)
	q.replies <- batchReply{claims: newClaims(1)}
	receive(t, h.started)

	cancelIntake()
	select {
	case <-done:
		t.Fatal("dispatcher returned while an activity was still running")
	case <-time.After(100 * time.Millisecond):
	}

	h.releaseN(1)
	if ack := receive(t, q.completed); ack.ctxErr != nil {
		t.Fatalf("activity acked on a cancelled context during drain: %v", ack.ctxErr)
	}
	receive(t, done)
}

// batchLifecycleBackend is lifecycleBackend plus storage.BatchQueueStorage,
// so Start must choose the dispatcher and never call Dequeue.
type batchLifecycleBackend struct {
	*lifecycleBackend
	batchCalls chan batchStorageCall
}

type batchStorageCall struct {
	prefix string
	limit  int
	types  []string
}

func (b *batchLifecycleBackend) DequeueBatch(ctx context.Context, prefix string, limit int, _ time.Duration, types, _ []string) ([]storage.DequeuedActivity, error) {
	b.batchCalls <- batchStorageCall{prefix: prefix, limit: limit, types: append([]string(nil), types...)}
	select {
	case a := <-b.claims:
		return []storage.DequeuedActivity{{Activity: a, LeaseID: prefix + ":" + a.ID.String()}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestStartUsesBatchIntakeWhenBackendClaimsInBulk(t *testing.T) {
	b := &batchLifecycleBackend{lifecycleBackend: newLifecycleBackend(), batchCalls: make(chan batchStorageCall, 4)}
	cfg := DefaultWorkerConfig()
	cfg.MaxConcurrentActivities = 3
	e := NewWorkerEngineWithBackend(b, cfg)
	e.RegisterActivityWithName("test", &funcHandler{fn: func(ActivityContext, json.RawMessage) (json.RawMessage, error) { return nil, nil }})
	b.claims <- storage.QueuedActivity{ID: uuid.New(), ActivityType: "test", TimeoutSeconds: 30}

	done := make(chan error, 1)
	go func() { done <- e.Start(context.Background()) }()
	defer e.Stop()

	call := receive(t, b.batchCalls)
	if call.limit != 3 || len(call.types) != 1 || call.types[0] != "test" || call.prefix == "" {
		t.Fatalf("batch claim = %+v, want limit 3, types [test], non-empty prefix", call)
	}
	if err := receive(t, b.ack); err != nil {
		t.Fatalf("ack failed: %v", err)
	}
	select {
	case w := <-b.dequeues:
		t.Fatalf("single-row Dequeue called (%q) although the backend claims in bulk", w)
	default:
	}
	e.Stop()
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
}

// batchStorageStub is a bare storage.BatchQueueStorage for adapter tests.
type batchStorageStub struct {
	storage.Storage
	claims []storage.DequeuedActivity
	got    batchStorageCall
}

func (s *batchStorageStub) DequeueBatch(_ context.Context, prefix string, limit int, _ time.Duration, types, _ []string) ([]storage.DequeuedActivity, error) {
	s.got = batchStorageCall{prefix: prefix, limit: limit, types: types}
	return s.claims, nil
}

func TestBatchAdapterCapabilityAndContract(t *testing.T) {
	if _, ok := newBackendQueueAdapter(struct{ storage.Storage }{}, nil).(batchActivityQueue); ok {
		t.Fatal("a backend without DequeueBatch must not be exposed as batch-capable")
	}

	claim := func(leaseID string) storage.DequeuedActivity {
		return storage.DequeuedActivity{Activity: storage.QueuedActivity{ID: uuid.New(), ActivityType: "test"}, LeaseID: leaseID}
	}
	cases := []struct {
		name    string
		limit   int
		claims  []storage.DequeuedActivity
		wantErr bool
	}{
		{name: "tokens pass through", limit: 2, claims: []storage.DequeuedActivity{claim("p:1"), claim("p:2")}},
		{name: "empty batch", limit: 2},
		{name: "blank token is a contract violation", limit: 2, claims: []storage.DequeuedActivity{claim("p:1"), claim("")}, wantErr: true},
		{name: "more than limit is a contract violation", limit: 1, claims: []storage.DequeuedActivity{claim("p:1"), claim("p:2")}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &batchStorageStub{claims: tc.claims}
			q, ok := newBackendQueueAdapter(stub, []string{"test"}).(batchActivityQueue)
			if !ok {
				t.Fatal("batch-capable backend not exposed as batchActivityQueue")
			}
			got, err := q.DequeueBatch(context.Background(), tc.limit, time.Second, "p")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("got %d claims and no error, want a contract error", len(got))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if stub.got.prefix != "p" || stub.got.limit != tc.limit || len(stub.got.types) != 1 || stub.got.types[0] != "test" {
				t.Fatalf("backend saw %+v, want prefix p, limit %d, types [test]", stub.got, tc.limit)
			}
			if len(got) != len(tc.claims) {
				t.Fatalf("got %d claims, want %d", len(got), len(tc.claims))
			}
			for i, c := range got {
				if c.leaseID != tc.claims[i].LeaseID || c.activity.ID != tc.claims[i].Activity.ID {
					t.Fatalf("claim %d = (%s, %s), want (%s, %s)", i, c.activity.ID, c.leaseID, tc.claims[i].Activity.ID, tc.claims[i].LeaseID)
				}
			}
		})
	}
}
