package runnerq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/google/uuid"
)

type recoveryQueue struct {
	activityQueue
	complete func(context.Context, *activity, json.RawMessage, string) error
	read     func(context.Context, uuid.UUID) (*activityResult, error)
	write    func(context.Context, uuid.UUID, uuid.UUID, activityResult, string) error
	fail     func(bool) (bool, error)
	park     func(time.Time) error
}

func (q *recoveryQueue) MarkCompleted(c context.Context, a *activity, r json.RawMessage, w string) error {
	return q.complete(c, a, r, w)
}
func (q *recoveryQueue) GetResult(c context.Context, id uuid.UUID) (*activityResult, error) {
	return q.read(c, id)
}
func (q *recoveryQueue) StoreResult(c context.Context, id, owner uuid.UUID, r activityResult, s string) error {
	return q.write(c, id, owner, r, s)
}
func (q *recoveryQueue) MarkFailed(_ context.Context, _ *activity, _ string, retry bool, _ string) (bool, error) {
	return q.fail(retry)
}
func (q *recoveryQueue) Yield(_ context.Context, _ *activity, w time.Time, _, _, _ string) error {
	return q.park(w)
}

type recoveryMetrics map[string]uint64

func (m recoveryMetrics) IncCounter(k string, n uint64)         { m[k] += n }
func (m recoveryMetrics) ObserveDuration(string, time.Duration) {}
func recoveryEngine(q activityQueue, h ActivityHandler) *WorkerEngine {
	return &WorkerEngine{queue: q, handlers: map[string]ActivityHandler{"test": h}, metrics: recoveryMetrics{}, config: DefaultWorkerConfig()}
}

type leaseProbe struct {
	storage.Storage
	renew func() (bool, error)
}

func (b *leaseProbe) ExtendLeaseForWorker(context.Context, uuid.UUID, string, time.Duration) (bool, error) {
	return b.renew()
}

func TestCompletionRecoveryRetainsResult(t *testing.T) {
	for _, lostReply := range []bool{false, true} {
		t.Run(fmt.Sprint("lost_reply=", lostReply), func(t *testing.T) {
			calls, acks, renewals := 0, 0, 0
			q := &recoveryQueue{complete: func(ctx context.Context, a *activity, r json.RawMessage, w string) error {
				acks++
				if string(r) != `{"receipt":"original"}` || w != "claim-token" {
					t.Fatalf("changed completion: %s %s", r, w)
				}
				if acks < 3 {
					return storage.NewUnavailableError("reply lost or temporary outage")
				}
				return nil
			}}
			h := &funcHandler{fn: func(ActivityContext, json.RawMessage) (json.RawMessage, error) {
				calls++
				return json.RawMessage(`{"receipt":"original"}`), nil
			}}
			e := recoveryEngine(q, h)
			e.backend = &leaseProbe{renew: func() (bool, error) { renewals++; return !lostReply, nil }}
			a := newActivity("test", nil, nil)
			a.MaxRetries = 1
			e.processActivity(context.Background(), a, "claim-token", 0)
			m := e.metrics.(recoveryMetrics)
			if calls != 1 || acks != 3 || renewals != 2 || m["activity_completed"] != 1 || m["storage_retry"] != 2 || a.RetryCount != 0 {
				t.Fatalf("calls=%d acks=%d renewals=%d metrics=%v", calls, acks, renewals, m)
			}
		})
	}
}

func TestCompletionRecoveryStopsOnTerminalError(t *testing.T) {
	for _, kind := range []storage.StorageErrorKind{storage.ErrSerialization, storage.ErrClaimLost} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			calls := 0
			q := &recoveryQueue{complete: func(context.Context, *activity, json.RawMessage, string) error {
				calls++
				return &storage.StorageError{Kind: kind}
			}}
			e := recoveryEngine(q, nil)
			a := newActivity("test", nil, nil)
			e.handleSuccess(context.Background(), a, nil, "claim", 0)
			m := e.metrics.(recoveryMetrics)
			if calls != 1 || m["activity_completed"] != 0 || m["activity_completion_pending_started"] != m["activity_completion_pending_finished"] {
				t.Fatalf("calls=%d metrics=%v", calls, m)
			}
		})
	}
}

func TestStorageRecoveryCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	err := retryStorage(ctx, "test", nil, nil, func(context.Context) error { calls++; cancel(); return storage.NewUnavailableError("outage") })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestRunRecoversReadsAndWritesWithoutRepeatingFunction(t *testing.T) {
	for _, permanent := range []bool{false, true} {
		t.Run(fmt.Sprint("permanent_step=", permanent), func(t *testing.T) {
			reads, writes, runs := 0, 0, 0
			var saved *activityResult
			q := &recoveryQueue{
				read: func(context.Context, uuid.UUID) (*activityResult, error) {
					reads++
					if reads == 1 {
						return nil, storage.NewUnavailableError("read outage")
					}
					return saved, nil
				},
				write: func(_ context.Context, _, _ uuid.UUID, r activityResult, _ string) error {
					writes++
					if writes < 3 {
						return storage.NewUnavailableError("write outage")
					}
					saved = &r
					return nil
				},
			}
			c := ActivityContext{ActivityID: uuid.New(), Ctx: context.Background(), queue: &attemptQueue{activityQueue: q, persistenceCtx: context.Background()}}
			fn := func() (json.RawMessage, error) {
				runs++
				if permanent {
					return nil, fmt.Errorf("wrapped: %w", NewNonRetryError("declined"))
				}
				return json.RawMessage(`"receipt"`), nil
			}
			for range 2 {
				out, err := c.Run("charge", fn)
				if permanent {
					if err == nil || retryableError(err) {
						t.Fatalf("permanent result: %v", err)
					}
				} else if err != nil || string(out) != `"receipt"` {
					t.Fatalf("out=%s err=%v", out, err)
				}
			}
			if runs != 1 || writes != 3 || saved == nil {
				t.Fatalf("runs=%d writes=%d saved=%v", runs, writes, saved)
			}
		})
	}
}

func TestEngineHonorsWrappedPermanentFailure(t *testing.T) {
	failed := false
	q := &recoveryQueue{fail: func(retry bool) (bool, error) {
		failed = true
		if retry {
			t.Error("wrapped permanent failure requested retry")
		}
		return false, nil
	}}
	e := recoveryEngine(q, &funcHandler{fn: func(ActivityContext, json.RawMessage) (json.RawMessage, error) {
		return nil, fmt.Errorf("wrapped: %w", NewNonRetryError("declined"))
	}})
	e.processActivity(context.Background(), newActivity("test", nil, nil), "claim", 0)
	if !failed {
		t.Fatal("failure was not recorded")
	}
}

type panicDeadLetterHandler struct{ funcHandler }

func (*panicDeadLetterHandler) OnDeadLetter(ActivityContext, json.RawMessage, string) {
	panic("hook failure")
}
func TestDeadLetterHookPanicIsContained(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprint("timeout=", timeout), func(t *testing.T) {
			q := &recoveryQueue{fail: func(bool) (bool, error) { return true, nil }}
			h := &panicDeadLetterHandler{funcHandler{fn: func(c ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
				if timeout {
					<-c.Ctx.Done()
				}
				return nil, NewRetryError("failure")
			}}}
			e := recoveryEngine(q, h)
			a := newActivity("test", nil, nil)
			if timeout {
				a.TimeoutSeconds = 0
			}
			e.processActivity(context.Background(), a, "claim", 0)
			if e.metrics.(recoveryMetrics)["activity_dead_letter_hook_panic"] != 1 {
				t.Fatal("panic was not reported")
			}
		})
	}
}

func TestPostParkFailureHasBoundedReplayDeadline(t *testing.T) {
	var parked time.Time
	q := &recoveryQueue{park: func(w time.Time) error { parked = w; return nil }, read: func(context.Context, uuid.UUID) (*activityResult, error) {
		return nil, storage.NewUnavailableError("repair unavailable")
	}}
	e := recoveryEngine(q, nil)
	a := newActivity("test", nil, nil)
	e.handleYield(context.Background(), a, &yieldPark{wakeAt: time.Now().Add(signalParkHorizon), recheck: uuid.New()}, "claim", 0)
	if remaining := time.Until(parked); remaining <= 0 || remaining > time.Minute {
		t.Fatalf("unbounded park: %s", remaining)
	}
}

// This fake models blocking dequeue and in-flight handler drain, without a DB.
type lifecycleBackend struct {
	storage.Storage
	claims   chan storage.QueuedActivity
	dequeues chan string
	filters  chan []string
	ack      chan error
}

func newLifecycleBackend() *lifecycleBackend {
	return &lifecycleBackend{claims: make(chan storage.QueuedActivity, 4), dequeues: make(chan string, 8), filters: make(chan []string, 8), ack: make(chan error, 4)}
}
func (b *lifecycleBackend) SchedulesNatively() bool { return true }
func (b *lifecycleBackend) Dequeue(ctx context.Context, w string, _ time.Duration, types []string) (*storage.QueuedActivity, error) {
	b.dequeues <- w
	b.filters <- append([]string(nil), types...)
	select {
	case a := <-b.claims:
		return &a, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (b *lifecycleBackend) AckSuccess(ctx context.Context, _ uuid.UUID, _ json.RawMessage, _ string) error {
	b.ack <- ctx.Err()
	return ctx.Err()
}
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for test synchronization")
		var zero T
		return zero
	}
}
func mustPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Error("expected active configuration mutation to panic")
		}
	}()
	fn()
}

func TestCallerCancellationDrainsAndRejectsOverlappingStart(t *testing.T) {
	b := newLifecycleBackend()
	cfg := DefaultWorkerConfig()
	cfg.MaxConcurrentActivities = 1
	e := NewWorkerEngineWithBackend(b, cfg)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	e.RegisterActivityWithName("test", &funcHandler{fn: func(c ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		close(entered)
		<-release
		return json.RawMessage(`true`), c.Ctx.Err()
	}})
	b.claims <- storage.QueuedActivity{ID: uuid.New(), ActivityType: "test", TimeoutSeconds: 30}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Start(parent) }()
	receive(t, entered)
	cancel()
	if err := e.Start(context.Background()); err == nil {
		t.Fatal("overlapping Start accepted")
	}
	mustPanic(t, func() { e.RegisterActivityWithName("other", &funcHandler{}) })
	mustPanic(t, func() { e.SetMetrics(nil) })
	unblock()
	if err := receive(t, b.ack); err != nil {
		t.Fatalf("completion context cancelled during drain: %v", err)
	}
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultRoutingAndFreshIdentityPerClaim(t *testing.T) {
	b := newLifecycleBackend()
	cfg := DefaultWorkerConfig()
	cfg.MaxConcurrentActivities = 1
	e := NewWorkerEngineWithBackend(b, cfg)
	e.RegisterActivityWithName("test", &funcHandler{fn: func(ActivityContext, json.RawMessage) (json.RawMessage, error) { return nil, nil }})
	for range 2 {
		b.claims <- storage.QueuedActivity{ID: uuid.New(), ActivityType: "test", TimeoutSeconds: 30}
	}
	done := make(chan error, 1)
	go func() { done <- e.Start(context.Background()) }()
	defer e.Stop()
	w1, w2 := receive(t, b.dequeues), receive(t, b.dequeues)
	types := receive(t, b.filters)
	if w1 == w2 || len(types) != 1 || types[0] != "test" {
		t.Fatalf("claims=%s,%s types=%v", w1, w2, types)
	}
	receive(t, b.ack)
	receive(t, b.ack)
	e.Stop()
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestStartRemainsBlockedAfterGraceUntilHandlerExits(t *testing.T) {
	b := newLifecycleBackend()
	cfg := DefaultWorkerConfig()
	cfg.MaxConcurrentActivities = 1
	grace := uint64(1)
	cfg.ShutdownGraceSeconds = &grace
	e := NewWorkerEngineWithBackend(b, cfg)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	e.RegisterActivityWithName("test", &funcHandler{fn: func(ActivityContext, json.RawMessage) (json.RawMessage, error) {
		close(entered)
		<-release
		return nil, nil
	}})
	b.claims <- storage.QueuedActivity{ID: uuid.New(), ActivityType: "test", TimeoutSeconds: 30}
	done := make(chan error, 1)
	go func() { done <- e.Start(context.Background()) }()
	receive(t, entered)
	e.Stop()
	receive(t, done)
	if err := e.Start(context.Background()); err == nil {
		t.Fatal("new generation overlapped stuck handler after grace")
	}
	unblock()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		e.mu.Lock()
		active := e.active
		e.mu.Unlock()
		if !active {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("engine did not leave draining state after last handler exited")
}

func TestReadOutageStopsBeforeEffectWhenClaimIsLost(t *testing.T) {
	reads, runs := 0, 0
	q := &recoveryQueue{read: func(context.Context, uuid.UUID) (*activityResult, error) {
		reads++
		if reads == 1 {
			return nil, storage.NewUnavailableError("read outage")
		}
		return nil, nil
	}}
	scoped := &attemptQueue{activityQueue: q, backend: &leaseProbe{renew: func() (bool, error) { return false, nil }}, persistenceCtx: context.Background()}
	c := ActivityContext{ActivityID: uuid.New(), Ctx: context.Background(), queue: scoped}
	_, err := c.Run("effect", func() (json.RawMessage, error) { runs++; return nil, nil })
	se, ok := storage.IsStorageError(err)
	if !ok || se.Kind != storage.ErrClaimLost || reads != 1 || runs != 0 {
		t.Fatalf("err=%v reads=%d runs=%d", err, reads, runs)
	}
}

func TestDurableWaitsRecoverTransientCheckpointRead(t *testing.T) {
	for _, kind := range []string{"sleep", "signal"} {
		t.Run(kind, func(t *testing.T) {
			reads := 0
			q := &recoveryQueue{read: func(context.Context, uuid.UUID) (*activityResult, error) {
				reads++
				if reads == 1 {
					return nil, storage.NewUnavailableError("checkpoint unavailable")
				}
				data := json.RawMessage(`"approved"`)
				if kind == "signal" && reads == 2 {
					data = json.RawMessage(`{"deadline":null}`)
				}
				if kind == "sleep" {
					data = json.RawMessage(`{"wake_at":"2020-01-01T00:00:00Z"}`)
				}
				return &activityResult{State: ResultOk, Data: data}, nil
			}}
			scoped := &attemptQueue{activityQueue: q, persistenceCtx: context.Background()}
			c := ActivityContext{ActivityID: uuid.New(), Ctx: context.Background(), queue: scoped}
			var err error
			if kind == "sleep" {
				err = c.Sleep("wait", time.Hour)
			} else {
				_, err = c.WaitForSignal("approval", 0)
			}
			expectedReads := 2
			if kind == "signal" {
				expectedReads = 3
			}
			if err != nil || reads != expectedReads {
				t.Fatalf("read recovery: reads=%d err=%v", reads, err)
			}
		})
	}
}

// The heartbeat renews the claim while the handler runs; when a renewal finds
// the claim gone the handler is cancelled with the cause and nothing is acked.
func TestHeartbeatRenewsAndRevokesHandlerOnClaimLoss(t *testing.T) {
	var renewals atomic.Int32
	q := &recoveryQueue{complete: func(context.Context, *activity, json.RawMessage, string) error {
		t.Error("superseded execution acknowledged")
		return nil
	}}
	var cause error
	e := recoveryEngine(q, &funcHandler{fn: func(c ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		<-c.Ctx.Done()
		cause = context.Cause(c.Ctx)
		return json.RawMessage(`"late"`), nil
	}})
	e.heartbeatInterval = 5 * time.Millisecond
	e.backend = &leaseProbe{renew: func() (bool, error) {
		switch renewals.Add(1) {
		case 1:
			return true, nil
		case 2:
			return false, storage.NewUnavailableError("blip")
		}
		return false, nil
	}}
	a := newActivity("test", nil, nil)
	a.TimeoutSeconds = 30
	e.processActivity(context.Background(), a, "claim", 0)
	se, ok := storage.IsStorageError(cause)
	m := e.metrics.(recoveryMetrics)
	if !ok || se.Kind != storage.ErrClaimLost || renewals.Load() != 3 || m["activity_claim_lost"] != 1 || m["activity_heartbeat_failed"] != 1 {
		t.Fatalf("cause=%v renewals=%d metrics=%v", cause, renewals.Load(), m)
	}
}

// No beat may land after the handler returns: the ack releases the claim, and
// a renewal racing it would misreport the execution as superseded.
func TestHeartbeatStopsBeforeAcknowledgement(t *testing.T) {
	var acking atomic.Bool
	var renewals atomic.Int32
	q := &recoveryQueue{complete: func(context.Context, *activity, json.RawMessage, string) error {
		acking.Store(true)
		time.Sleep(20 * time.Millisecond)
		return nil
	}}
	e := recoveryEngine(q, &funcHandler{fn: func(ActivityContext, json.RawMessage) (json.RawMessage, error) {
		time.Sleep(20 * time.Millisecond)
		return nil, nil
	}})
	e.heartbeatInterval = time.Millisecond
	e.backend = &leaseProbe{renew: func() (bool, error) {
		renewals.Add(1)
		if acking.Load() {
			t.Error("renewal raced the acknowledgement")
		}
		return true, nil
	}}
	a := newActivity("test", nil, nil)
	a.TimeoutSeconds = 30
	e.processActivity(context.Background(), a, "claim", 0)
	if !acking.Load() || renewals.Load() == 0 || e.metrics.(recoveryMetrics)["activity_completed"] != 1 {
		t.Fatalf("acked=%v renewals=%d metrics=%v", acking.Load(), renewals.Load(), e.metrics)
	}
}

type spawnProbe struct {
	storage.Storage
	owner  uuid.UUID
	worker string
	spawns int
}

func (b *spawnProbe) EnqueueForWorker(_ context.Context, _ storage.QueuedActivity, owner uuid.UUID, worker string) error {
	b.owner, b.worker = owner, worker
	b.spawns++
	return nil
}
func (b *spawnProbe) EnqueueIdempotentForWorker(_ context.Context, _ *storage.QueuedActivity, owner uuid.UUID, worker string) (*storage.IdempotencyResult, error) {
	b.owner, b.worker = owner, worker
	b.spawns++
	return nil, &storage.StorageError{Kind: storage.ErrClaimLost}
}

// Every spawn form a handler can issue carries the execution's claim, and a
// fenced rejection reaches the engine as a lost claim rather than a failure.
func TestHandlerSpawnsCarryTheExecutionClaim(t *testing.T) {
	b := &spawnProbe{}
	failed := false
	q := &recoveryQueue{fail: func(bool) (bool, error) { failed = true; return false, nil }}
	e := recoveryEngine(q, &funcHandler{fn: func(c ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		x := c.ActivityExecutor
		if _, err := x.ActivityNamed("child").Payload(json.RawMessage(`{}`)).Execute(c.Ctx); err != nil {
			return nil, err
		}
		if _, err := x.ActivityNamed("child").Payload(json.RawMessage(`{}`)).Delay(time.Hour).AsRoot().Execute(c.Ctx); err != nil {
			return nil, err
		}
		_, err := x.ActivityNamed("child").Payload(json.RawMessage(`{}`)).Step("s").Execute(c.Ctx)
		return nil, fmt.Errorf("spawn: %w", err)
	}})
	e.backend = b
	a := newActivity("test", nil, nil)
	a.TimeoutSeconds = 30
	e.processActivity(context.Background(), a, "claim", 0)
	if b.spawns != 3 || b.owner != a.ID || b.worker != "claim" || failed || e.metrics.(recoveryMetrics)["activity_claim_lost"] != 1 {
		t.Fatalf("spawns=%d owner=%s worker=%s failed=%v metrics=%v", b.spawns, b.owner, b.worker, failed, e.metrics)
	}
}

type lineageQueue struct {
	recoveryQueue
	spawned []*activity
}

func (q *lineageQueue) Enqueue(_ context.Context, a *activity) error {
	q.spawned = append(q.spawned, a)
	return nil
}
func (q *lineageQueue) EnqueueIdempotent(_ context.Context, a *activity) (*storage.IdempotencyResult, error) {
	q.spawned = append(q.spawned, a)
	return nil, nil
}

// The engine's executor spawning on a handler's context spawns children of
// that handler, so code the handler calls needn't be handed its executor, and
// inserts them unfenced, as cheaply as roots. AsRoot, an unrelated context or
// another queue's executor spawn roots.
func TestEngineExecutorOnHandlerContextSpawnsChildren(t *testing.T) {
	q := &lineageQueue{recoveryQueue: recoveryQueue{complete: func(context.Context, *activity, json.RawMessage, string) error { return nil }}}
	other := &lineageQueue{}
	var e *WorkerEngine
	e = recoveryEngine(q, &funcHandler{fn: func(c ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		derived, cancel := context.WithCancel(c.Ctx)
		defer cancel()
		root := e.GetActivityExecutor()
		for _, b := range []struct {
			ctx context.Context
			b   *ActivityBuilder
		}{
			{c.Ctx, root.ActivityNamed("child")},
			{derived, root.ActivityNamed("child")},
			{c.Ctx, root.ActivityNamed("child").Step("s")},
			{c.Ctx, root.ActivityNamed("root").AsRoot()},
			{context.Background(), root.ActivityNamed("root")},
			{c.Ctx, newActivityExecutor(other, 0, nil).ActivityNamed("root")},
		} {
			if _, err := b.b.Payload(json.RawMessage(`{}`)).Execute(b.ctx); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}})
	fenced := &spawnProbe{}
	e.backend = fenced
	parent := newActivity("test", nil, nil)
	parent.TimeoutSeconds = 30
	e.processActivity(context.Background(), parent, "claim", 0)

	if fenced.spawns != 0 {
		t.Fatalf("%d spawns took the fenced insert", fenced.spawns)
	}
	spawned := append(q.spawned, other.spawned...)
	if len(spawned) != 6 {
		t.Fatalf("spawned %d activities, want 6", len(spawned))
	}
	for i, a := range spawned {
		child := a.ActivityType == "child"
		if hasParent := a.ParentActivityID != nil; hasParent != child {
			t.Fatalf("spawn %d (%s): parent=%v", i, a.ActivityType, a.ParentActivityID)
		}
		if child && (*a.ParentActivityID != parent.ID || a.RootActivityID != parent.ID || a.Depth != 1) {
			t.Fatalf("spawn %d: parent=%s root=%s depth=%d, want parent=root=%s depth=1", i, a.ParentActivityID, a.RootActivityID, a.Depth, parent.ID)
		}
	}
}

func TestIsYield(t *testing.T) {
	y := &yieldPark{wakeAt: time.Now(), kind: "await", step: "await:x"}
	if !IsYield(y) || !IsYield(fmt.Errorf("charge: %w", y)) {
		t.Fatal("the park sentinel, bare or wrapped, is a yield")
	}
	if IsYield(NewRetryError("down")) || IsYield(nil) {
		t.Fatal("other errors are not yields")
	}
}
