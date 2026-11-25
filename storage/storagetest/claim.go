package storagetest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// Claiming: what Dequeue and DequeueBatch hand out and in what order, and the
// promise everything rests on: no activity is claimed twice while its claim is
// live.
var claimTests = []conformanceTest{
	{"ClaimRecordsOwnership", testClaimRecordsOwnership},
	{"OrderIsPriorityThenRetriesThenAge", testClaimOrder},
	{"TypeFilterForms", testClaimTypeFilters},
	{"ScheduledRowsBecomeClaimableWhenDue", testClaimScheduled},
	{"ConcurrentClaimersNeverOverlap", testConcurrentClaimersNeverOverlap},
	{"BlockingClaimTimesOutEmpty", testBlockingClaimTimesOut},
	{"BlockingClaimWakesOnEnqueueFromAnotherProcess", testBlockingClaimWakesAcrossProcesses},
	{"BlockingClaimHonoursCancellation", testBlockingClaimCancel},
	{"BatchClaimsInDequeueOrderWithPerRowTokens", testBatchClaim},
	{"BatchClaimersNeverOverlap", testBatchClaimersNeverOverlap},
	{"BatchClaimBlocksUntilWork", testBatchClaimBlocks},
}

// A claim flips the row to processing under exactly the caller's token and
// starts a lease in the future; an empty queue claims nothing.
func testClaimRecordsOwnership(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.claimNothing()
	a := s.enqueue(activity())
	s.wantStatus(a.ID, "pending")

	s.claim("token-1", a)
	snap := s.snapshot(a.ID)
	if snap.Status != snapshotStatus["processing"] || snap.CurrentWorkerID == nil || *snap.CurrentWorkerID != "token-1" {
		t.Fatalf("after claim: status=%q worker=%v, want processing under token-1", snap.Status, snap.CurrentWorkerID)
	}
	if snap.LeaseDeadlineMS == nil || *snap.LeaseDeadlineMS <= time.Now().Add(-time.Minute).UnixMilli() {
		t.Fatalf("lease deadline %v is not in the future", snap.LeaseDeadlineMS)
	}
	if snap.StartedAt == nil {
		t.Fatal("claim did not record started_at")
	}
	s.wantNoEvents(a.ID)
	s.claimNothing() // a processing row is not claimable again
}

// Higher priority first; within a priority a retried attempt before a fresh
// one; then oldest first.
func testClaimOrder(t *testing.T, h Harness) {
	s := newSuite(t, h)
	base := time.Now().UTC().Add(-time.Minute)
	oldLow := s.enqueue(activity(withPriority(storage.PriorityLow), withCreatedAt(base)))
	oldNormal := s.enqueue(activity(withCreatedAt(base)))
	newNormal := s.enqueue(activity(withCreatedAt(base.Add(time.Second))))
	critical := s.enqueue(activity(withPriority(storage.PriorityCritical), withCreatedAt(base.Add(2*time.Second))))
	retried := s.enqueue(activity(withType("retried"), withCreatedAt(base.Add(3*time.Second)), withRetryDelay(0, 0)))

	// Fail retried once so it carries retry_count=1 and an immediate schedule.
	s.claim("first", retried, retried.ActivityType)
	if dead, err := s.b.AckFailure(s.ctx, retried.ID, storage.NewRetryableFailure("again"), "first"); err != nil || dead {
		t.Fatalf("retryable failure: dead=%v err=%v", dead, err)
	}

	for i, want := range []storage.QueuedActivity{critical, retried, oldNormal, newNormal, oldLow} {
		s.claim(fmt.Sprintf("w%d", i), want)
	}
	s.claimNothing()
}

// A nil filter claims every type; one and many types claim only those.
func testClaimTypeFilters(t *testing.T, h Harness) {
	s := newSuite(t, h)
	base := time.Now().UTC().Add(-time.Minute)
	a := s.enqueue(activity(withType("a"), withCreatedAt(base)))
	b := s.enqueue(activity(withType("b"), withCreatedAt(base.Add(time.Second))))
	c := s.enqueue(activity(withType("c"), withCreatedAt(base.Add(2*time.Second))))

	s.claimNothing("zzz")
	s.claim("one", b, "b")
	s.claim("many", a, "a", "c")
	s.claimNothing("a")
	s.claim("all", c)
	s.claimNothing()
}

// A future ScheduledAt is not claimable until it is due; a past one is
// claimable at once. Backends that do not schedule natively make due rows
// claimable through ProcessScheduled.
func testClaimScheduled(t *testing.T, h Harness) {
	s := newSuite(t, h)
	later := s.enqueue(activity(withScheduledAt(time.Now().UTC().Add(time.Hour))))
	s.wantStatus(later.ID, "scheduled")
	s.claimNothing()

	due := s.enqueue(activity(withScheduledAt(time.Now().UTC().Add(-time.Second))))
	s.claim("w", due)
	s.claimNothing()
}

// Many claimers on two handles drain a backlog with every activity claimed
// exactly once.
func testConcurrentClaimersNeverOverlap(t *testing.T, h Harness) {
	s := newSuite(t, h)
	const total = 60
	for range total {
		s.enqueue(activity())
	}
	handles := []storage.Storage{s.b, s.another()}
	var mu sync.Mutex
	seen := make(map[uuid.UUID]string, total)
	var wg sync.WaitGroup
	for i := range 6 {
		b := handles[i%len(handles)]
		wg.Go(func() {
			for n := 0; ; n++ {
				token := fmt.Sprintf("claimer-%d-%d", i, n)
				got, err := b.Dequeue(s.ctx, token, 0, nil, nil)
				if err != nil {
					t.Errorf("claimer %d: %v", i, err)
					return
				}
				if got == nil {
					return
				}
				mu.Lock()
				if prev, dup := seen[got.ID]; dup {
					t.Errorf("activity %s claimed twice: %s and %s", got.ID, prev, token)
				}
				seen[got.ID] = token
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if len(seen) != total {
		t.Fatalf("claimed %d distinct activities, want %d", len(seen), total)
	}
}

// A blocking claim on a queue that stays empty returns (nil, nil) once its
// window elapses — not earlier, and not much later.
func testBlockingClaimTimesOut(t *testing.T, h Harness) {
	s := newSuite(t, h)
	start := time.Now()
	got, err := s.b.Dequeue(s.ctx, "w", 500*time.Millisecond, nil, nil)
	took := time.Since(start)
	if err != nil || got != nil {
		t.Fatalf("empty blocking claim: got=%v err=%v", got, err)
	}
	if took < 400*time.Millisecond || took > 5*time.Second {
		t.Fatalf("empty blocking claim returned after %v, want about 500ms", took)
	}
}

// A claimer parked on an empty queue wakes when another process enqueues,
// and when a parked parent becomes runnable again — well inside its window.
func testBlockingClaimWakesAcrossProcesses(t *testing.T, h Harness) {
	s := newSuite(t, h)
	producer := s.another()
	type outcome struct {
		got  *storage.QueuedActivity
		err  error
		took time.Duration
	}
	block := func() <-chan outcome {
		ch := make(chan outcome, 1)
		start := time.Now()
		go func() {
			got, err := s.b.Dequeue(s.ctx, "parked-"+uuid.NewString(), wait, nil, nil)
			ch <- outcome{got, err, time.Since(start)}
		}()
		time.Sleep(300 * time.Millisecond) // let it park
		return ch
	}
	check := func(ch <-chan outcome, want uuid.UUID, what string) {
		t.Helper()
		o := receive(t, ch, what)
		if o.err != nil || o.got == nil || o.got.ID != want {
			t.Fatalf("%s: got=%v err=%v, want %s", what, o.got, o.err, want)
		}
		if o.took > 5*time.Second {
			t.Fatalf("%s: woke after %v, want a prompt wake rather than the window elapsing", what, o.took)
		}
	}

	ch := block()
	a := activity()
	if err := producer.Enqueue(s.ctx, a); err != nil {
		t.Fatal(err)
	}
	check(ch, a.ID, "wake on enqueue")

	// Park a parent on its child, then complete that child from the other process.
	parent := s.enqueueClaimed("parent", activity(withType("parent")))
	child := s.enqueueClaimed("child", activity(withType("child"), withParent(parent)))
	if err := need[storage.DependencyStorage](t, s.b).YieldForResult(s.ctx, parent.ID, child.ID, &child.ID, time.Now().UTC().Add(time.Hour), "parent", "await", "child"); err != nil {
		t.Fatal(err)
	}
	ch = block()
	if err := producer.AckSuccess(s.ctx, child.ID, nil, "child"); err != nil {
		t.Fatal(err)
	}
	check(ch, parent.ID, "wake on child completion")
}

// Cancelling the context ends a blocking claim with the context's error.
func testBlockingClaimCancel(t *testing.T, h Harness) {
	s := newSuite(t, h)
	ctx, cancel := context.WithCancel(s.ctx)
	done := make(chan error, 1)
	go func() {
		_, err := s.b.Dequeue(ctx, "w", wait, nil, nil)
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	if err := receive(t, done, "cancelled claim"); err == nil || ctx.Err() == nil {
		t.Fatalf("cancelled blocking claim returned %v", err)
	}
}

// Batch claims take rows in Dequeue's order, fence each with its own token
// derived from the caller's prefix, and respect the limit.
func testBatchClaim(t *testing.T, h Harness) {
	s := newSuite(t, h)
	bq := optional[storage.BatchQueueStorage](t, s.b)
	for _, p := range []storage.ActivityPriority{storage.PriorityLow, storage.PriorityCritical, storage.PriorityNormal, storage.PriorityHigh, storage.PriorityLow} {
		s.enqueue(activity(withPriority(p)))
	}
	claims, err := bq.DequeueBatch(s.ctx, "batch-1", 3, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []storage.ActivityPriority{storage.PriorityCritical, storage.PriorityHigh, storage.PriorityNormal}
	if len(claims) != len(want) {
		t.Fatalf("claimed %d, want %d", len(claims), len(want))
	}
	tokens := map[string]bool{}
	for i, c := range claims {
		if c.Activity.Priority != want[i] {
			t.Fatalf("claim %d priority %d, want %d", i, c.Activity.Priority, want[i])
		}
		if c.LeaseID == "" || tokens[c.LeaseID] {
			t.Fatalf("claim %d token %q is empty or repeated", i, c.LeaseID)
		}
		tokens[c.LeaseID] = true
		if c.Attempt != 1 || !c.LeaseDeadline.After(time.Now().Add(-time.Minute)) {
			t.Fatalf("claim %d attempt=%d deadline=%v", i, c.Attempt, c.LeaseDeadline)
		}
		s.wantStatus(c.Activity.ID, "processing")
	}
	// Siblings of one batch are fenced apart.
	wantKind(t, s.b.AckSuccess(s.ctx, claims[0].Activity.ID, nil, claims[1].LeaseID), storage.ErrClaimLost, "ack with sibling token")
	if err := s.b.AckSuccess(s.ctx, claims[0].Activity.ID, nil, claims[0].LeaseID); err != nil {
		t.Fatalf("ack with own token: %v", err)
	}
	rest, err := bq.DequeueBatch(s.ctx, "batch-2", 10, 0, nil, nil)
	if err != nil || len(rest) != 2 {
		t.Fatalf("remaining claim: n=%d err=%v, want the 2 low rows", len(rest), err)
	}
	if none, err := bq.DequeueBatch(s.ctx, "batch-3", 10, 0, nil, nil); err != nil || len(none) != 0 {
		t.Fatalf("empty claim: n=%d err=%v", len(none), err)
	}
	if none, err := bq.DequeueBatch(s.ctx, "batch-4", 0, time.Second, nil, nil); err != nil || len(none) != 0 {
		t.Fatalf("limit-0 claim: n=%d err=%v, want an immediate empty result", len(none), err)
	}
	// Type filter forms.
	for _, typ := range []string{"a", "a", "b", "b", "c"} {
		s.enqueue(activity(withType(typ)))
	}
	one, err := bq.DequeueBatch(s.ctx, "one", 10, 0, []string{"a"}, nil)
	if err != nil || len(one) != 2 || one[0].Activity.ActivityType != "a" || one[1].Activity.ActivityType != "a" {
		t.Fatalf("one-type batch: %v %v", one, err)
	}
	many, err := bq.DequeueBatch(s.ctx, "many", 2, 0, []string{"b", "c"}, nil)
	if err != nil || len(many) != 2 {
		t.Fatalf("many-type batch: n=%d err=%v", len(many), err)
	}
	for _, c := range many {
		if c.Activity.ActivityType == "a" {
			t.Fatal("many-type batch claimed a filtered-out type")
		}
	}
}

// Two processes claiming in bulk never receive the same row.
func testBatchClaimersNeverOverlap(t *testing.T, h Harness) {
	s := newSuite(t, h)
	optional[storage.BatchQueueStorage](t, s.b)
	const total = 50
	for range total {
		s.enqueue(activity())
	}
	handles := []storage.BatchQueueStorage{s.b.(storage.BatchQueueStorage), s.another().(storage.BatchQueueStorage)}
	var mu sync.Mutex
	seen := make(map[uuid.UUID]string, total)
	var wg sync.WaitGroup
	for i, b := range handles {
		wg.Go(func() {
			for round := 0; ; round++ {
				claims, err := b.DequeueBatch(s.ctx, fmt.Sprintf("claimer-%d-%d", i, round), 7, 0, nil, nil)
				if err != nil {
					t.Errorf("claimer %d: %v", i, err)
					return
				}
				if len(claims) == 0 {
					return
				}
				mu.Lock()
				for _, c := range claims {
					if prev, dup := seen[c.Activity.ID]; dup {
						t.Errorf("activity %s claimed twice: %s and %s", c.Activity.ID, prev, c.LeaseID)
					}
					seen[c.Activity.ID] = c.LeaseID
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if len(seen) != total {
		t.Fatalf("claimed %d distinct activities, want %d", len(seen), total)
	}
}

// A blocking batch claim parks like Dequeue and wakes when a row lands.
func testBatchClaimBlocks(t *testing.T, h Harness) {
	s := newSuite(t, h)
	bq := optional[storage.BatchQueueStorage](t, s.b)
	type outcome struct {
		claims []storage.DequeuedActivity
		err    error
		took   time.Duration
	}
	done := make(chan outcome, 1)
	start := time.Now()
	go func() {
		claims, err := bq.DequeueBatch(s.ctx, "blocker", 5, wait, nil, nil)
		done <- outcome{claims, err, time.Since(start)}
	}()
	time.Sleep(300 * time.Millisecond)
	a := activity()
	if err := s.another().Enqueue(s.ctx, a); err != nil {
		t.Fatal(err)
	}
	o := receive(t, done, "blocking batch claim")
	if o.err != nil || len(o.claims) != 1 || o.claims[0].Activity.ID != a.ID {
		t.Fatalf("blocking batch claim: n=%d err=%v", len(o.claims), o.err)
	}
	if o.took > 5*time.Second {
		t.Fatalf("blocking batch claim woke after %v", o.took)
	}
}
