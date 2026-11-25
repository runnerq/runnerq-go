// Package storagetest is the conformance suite for storage.Storage backends.
//
// RunnerQ's durable-execution guarantees (claimed once, fenced
// acknowledgements, lease recovery, checkpoints that survive replay, parents
// woken by awaited results, atomic idempotency) are kept by the backend; the
// engine only composes them. A backend that passes this suite can replace
// another without the engine noticing.
//
// A backend's own test file runs the suite through a Harness:
//
//	func TestConformance(t *testing.T) {
//		storagetest.Run(t, myHarness{})
//	}
//
// Each test opens a fresh backend on its own queue and drives it only through
// storage.Storage and the optional interfaces; the harness supplies what those
// can't express: opening a named queue and expiring a lease.
//
// CheckpointStorage, DependencyStorage, AttemptLeaseStorage and SpawnStorage
// are required: their tests fail without them. Tests for BatchQueueStorage,
// ResultWaiter, ResultNotifier, QueryStorage, CommandStorage and
// ScheduleStorage and LimitStorage are skipped when absent; schedule firing tests also need
// the harness to implement ScheduleClock.
package storagetest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// Harness adapts one backend implementation to the suite.
type Harness interface {
	// Open returns a backend serving queue, closed by t.Cleanup, that also
	// implements Reader. Two calls with the same queue must return independent
	// handles on the same store; the suite uses them as two processes.
	Open(t *testing.T, queue string) storage.Storage
	// ExpireLease moves a processing activity's lease into the past so the
	// backend's RequeueExpired recovers it.
	ExpireLease(ctx context.Context, b storage.Storage, activityID uuid.UUID) error
}

// wait bounds operations that block on another party: long enough for a slow
// CI database, short enough that a missed wake fails instead of hanging.
const wait = 10 * time.Second

// Run runs the whole suite against h.
func Run(t *testing.T, h Harness) {
	t.Helper()
	for _, g := range []struct {
		name  string
		tests []conformanceTest
	}{
		{"Claim", claimTests},
		{"Acknowledge", ackTests},
		{"LeaseRecovery", leaseTests},
		{"Durable", durableTests},
		{"Idempotency", idempotencyTests},
		{"Retention", retentionTests},
		{"Query", queryTests},
		{"Commands", commandTests},
		{"Schedules", scheduleTests},
		{"Limits", limitTests},
	} {
		t.Run(g.name, func(t *testing.T) {
			for _, tc := range g.tests {
				t.Run(tc.name, func(t *testing.T) { tc.fn(t, h) })
			}
		})
	}
}

type conformanceTest struct {
	name string
	fn   func(t *testing.T, h Harness)
}

// Reader lets the suite check what a backend stored. The engine never uses it,
// so it is not part of storage.Storage.
type Reader interface {
	GetActivity(ctx context.Context, activityID uuid.UUID) (*storage.ActivitySnapshot, error)
	GetActivityEvents(ctx context.Context, activityID uuid.UUID, limit int) ([]storage.ActivityEvent, error)
	GetActivitySteps(ctx context.Context, ownerActivityID uuid.UUID) ([]storage.StepRecord, error)
	GetChildren(ctx context.Context, parentID uuid.UUID, offset, limit int) ([]storage.ActivitySnapshot, error)
	GetSubtree(ctx context.Context, rootID uuid.UUID) ([]storage.ActivitySnapshot, error)
	ListDeadLetter(ctx context.Context, offset, limit int) ([]storage.DeadLetterRecord, error)
}

type suite struct {
	t     *testing.T
	h     Harness
	queue string
	b     storage.Storage
	r     Reader
	ctx   context.Context
}

func newSuite(t *testing.T, h Harness) *suite {
	t.Helper()
	queue := "ct_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	b := h.Open(t, queue)
	r, ok := b.(Reader)
	if !ok {
		t.Fatalf("backend %T doesn't implement storagetest.Reader, which the suite reads to check stored state", b)
	}
	return &suite{t: t, h: h, queue: queue, b: b, r: r, ctx: context.Background()}
}

// another opens a second handle on the queue, standing in for another process.
func (s *suite) another() storage.Storage {
	s.t.Helper()
	return s.h.Open(s.t, s.queue)
}

// activity builds a runnable activity; the 30s timeout keeps leases well in
// the future.
func activity(opts ...func(*storage.QueuedActivity)) storage.QueuedActivity {
	a := storage.QueuedActivity{
		ID:             uuid.New(),
		ActivityType:   "conformance",
		Payload:        json.RawMessage(`{"k":"v"}`),
		Priority:       storage.PriorityNormal,
		MaxRetries:     3,
		TimeoutSeconds: 30,
		CreatedAt:      time.Now().UTC(),
		Metadata:       map[string]string{},
	}
	for _, o := range opts {
		o(&a)
	}
	return a
}

func withType(typ string) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) { a.ActivityType = typ }
}
func withPriority(p storage.ActivityPriority) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) { a.Priority = p }
}
func withMaxRetries(n uint32) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) { a.MaxRetries = n }
}
func withRetryDelay(base, cap uint64) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) { a.RetryDelaySeconds, a.MaxRetryDelaySeconds = base, cap }
}
func withParent(parent storage.QueuedActivity) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) {
		p := parent.ID
		a.ParentActivityID = &p
		a.RootActivityID = parent.RootActivityID
		if a.RootActivityID == uuid.Nil {
			a.RootActivityID = parent.ID
		}
		a.Depth = parent.Depth + 1
	}
}
func withKey(key string, behavior storage.IdempotencyBehavior) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) {
		a.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: key, Behavior: behavior}
	}
}
func withScheduledAt(at time.Time) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) { a.ScheduledAt = &at }
}
func withCreatedAt(at time.Time) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) { a.CreatedAt = at }
}

func (s *suite) enqueue(a storage.QueuedActivity) storage.QueuedActivity {
	s.t.Helper()
	if err := s.b.Enqueue(s.ctx, a); err != nil {
		s.t.Fatalf("enqueue %s: %v", a.ID, err)
	}
	return a
}

// promote runs ProcessScheduled for backends that don't schedule natively.
func (s *suite) promote() {
	s.t.Helper()
	if s.b.SchedulesNatively() {
		return
	}
	if _, err := s.b.ProcessScheduled(s.ctx); err != nil {
		s.t.Fatalf("process scheduled: %v", err)
	}
}

// tryClaim makes one non-blocking claim; nil when nothing is claimable.
func (s *suite) tryClaim(worker string, types ...string) *storage.QueuedActivity {
	s.t.Helper()
	s.promote()
	got, err := s.b.Dequeue(s.ctx, worker, 0, types)
	if err != nil {
		s.t.Fatalf("dequeue as %s: %v", worker, err)
	}
	return got
}

func (s *suite) claim(worker string, want storage.QueuedActivity, types ...string) {
	s.t.Helper()
	got := s.tryClaim(worker, types...)
	if got == nil || got.ID != want.ID {
		s.t.Fatalf("dequeue as %s: got %v, want %s", worker, got, want.ID)
	}
}

func (s *suite) claimNothing(types ...string) {
	s.t.Helper()
	if got := s.tryClaim("nobody", types...); got != nil {
		s.t.Fatalf("claimed %s (%s), want nothing claimable", got.ID, got.ActivityType)
	}
}

func (s *suite) enqueueClaimed(worker string, a storage.QueuedActivity) storage.QueuedActivity {
	s.t.Helper()
	s.enqueue(a)
	s.claim(worker, a, a.ActivityType)
	return a
}

func (s *suite) snapshot(id uuid.UUID) storage.ActivitySnapshot {
	s.t.Helper()
	snap, err := s.r.GetActivity(s.ctx, id)
	if err != nil {
		s.t.Fatalf("get activity %s: %v", id, err)
	}
	if snap == nil {
		s.t.Fatalf("activity %s not found", id)
	}
	return *snap
}

// snapshotStatus maps each row status to the display name
// ActivitySnapshot.Status carries; both vocabularies are part of the contract.
var snapshotStatus = map[string]string{
	"pending":     "Pending",
	"processing":  "Running",
	"scheduled":   "Scheduled",
	"retrying":    "Retrying",
	"waiting":     "Waiting",
	"completed":   "Completed",
	"failed":      "Failed",
	"dead_letter": "DeadLetter",
	"cancelled":   "Cancelled",
}

// status returns the activity's row status, derived from its snapshot.
func (s *suite) status(id uuid.UUID) string {
	s.t.Helper()
	display := s.snapshot(id).Status
	for raw, d := range snapshotStatus {
		if d == display {
			return raw
		}
	}
	s.t.Fatalf("activity %s has unknown snapshot status %q", id, display)
	return ""
}

func (s *suite) wantStatus(id uuid.UUID, want string) {
	s.t.Helper()
	if got := s.status(id); got != want {
		s.t.Fatalf("activity %s status = %q, want %q", id, got, want)
	}
}

func (s *suite) result(id uuid.UUID) *storage.ActivityResult {
	s.t.Helper()
	res, err := s.b.GetResult(s.ctx, id)
	if err != nil {
		s.t.Fatalf("get result %s: %v", id, err)
	}
	return res
}

func (s *suite) wantResult(id uuid.UUID, state storage.ResultState) *storage.ActivityResult {
	s.t.Helper()
	res := s.result(id)
	if res == nil || res.State != state {
		s.t.Fatalf("result of %s = %+v, want state %v", id, res, state)
	}
	return res
}

func (s *suite) events(id uuid.UUID) []storage.ActivityEvent {
	s.t.Helper()
	evs, err := s.r.GetActivityEvents(s.ctx, id, 100)
	if err != nil {
		s.t.Fatalf("events of %s: %v", id, err)
	}
	return evs
}

func (s *suite) hasEvent(id uuid.UUID, typ storage.ActivityEventType) bool {
	s.t.Helper()
	for _, ev := range s.events(id) {
		if ev.EventType == typ {
			return true
		}
	}
	return false
}

func (s *suite) wantEvent(id uuid.UUID, typ storage.ActivityEventType) {
	s.t.Helper()
	if !s.hasEvent(id, typ) {
		s.t.Fatalf("activity %s has no %s event", id, typ)
	}
}

// wantNoEvents checks nothing was stored for id: submission, claims, success
// and results are the row's to say.
func (s *suite) wantNoEvents(id uuid.UUID) {
	s.t.Helper()
	if evs := s.events(id); len(evs) > 0 {
		s.t.Fatalf("activity %s has stored events %+v, want none", id, evs)
	}
}

func (s *suite) expire(id uuid.UUID) {
	s.t.Helper()
	if err := s.h.ExpireLease(s.ctx, s.b, id); err != nil {
		s.t.Fatalf("expire lease of %s: %v", id, err)
	}
}

func (s *suite) reap(want uint64) {
	s.t.Helper()
	n, err := s.b.RequeueExpired(s.ctx, 100)
	if err != nil || n != want {
		s.t.Fatalf("requeue expired: n=%d err=%v, want %d", n, err, want)
	}
}

// wantKind asserts err's storage error kind, which the engine branches on.
func wantKind(t *testing.T, err error, kind storage.StorageErrorKind, what string) {
	t.Helper()
	se, ok := storage.IsStorageError(err)
	if !ok || se.Kind != kind {
		t.Fatalf("%s: err = %v, want storage error of kind %v", what, err, kind)
	}
}

// need returns an interface durable execution relies on, failing the test
// when the backend lacks it.
func need[T any](t *testing.T, b storage.Storage) T {
	t.Helper()
	x, ok := b.(T)
	if !ok {
		var zero T
		t.Fatalf("backend does not implement %T, which durable execution relies on", &zero)
	}
	return x
}

// optional returns an interface the engine can do without, skipping the test
// when absent.
func optional[T any](t *testing.T, b storage.Storage) T {
	t.Helper()
	x, ok := b.(T)
	if !ok {
		var zero T
		t.Skipf("backend does not implement optional %T", &zero)
	}
	return x
}

func receive[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(wait):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func raw(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("storagetest: marshal %v: %v", v, err))
	}
	return data
}
