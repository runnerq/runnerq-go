package storagetest

import (
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// Idempotency: a key is claimed and its activity enqueued as one atomic
// step, each duplicate behaviour resolves as documented, and concurrent
// claimers of one key produce exactly one activity.
var idempotencyTests = []conformanceTest{
	{"FreshKeyEnqueuesAndReturnExistingReattaches", testIdempotentReturnExisting},
	{"NoReuseRejectsDuplicates", testIdempotentNoReuse},
	{"AllowReuseRepointsTheKey", testIdempotentAllowReuse},
	{"AllowReuseOnFailureReclaimsOnlyTerminalFailures", testIdempotentAllowReuseOnFailure},
	{"ConcurrentClaimersProduceOneActivity", testIdempotentConcurrent},
	{"BusinessKeysAreNamespacedByType", testBusinessKeys},
}

func testIdempotentReturnExisting(t *testing.T, h Harness) {
	s := newSuite(t, h)
	parent := s.enqueue(activity(withType("parent")))
	first := activity(withParent(parent), withKey("job-42", storage.BehaviorReturnExisting))
	if existing, err := s.b.EnqueueIdempotent(s.ctx, &first); err != nil || existing != nil {
		t.Fatalf("fresh claim: %v %v", existing, err)
	}
	s.wantStatus(first.ID, "pending")
	if id, err := s.b.LookupIdempotencyActivityID(s.ctx, "job-42"); err != nil || id != first.ID {
		t.Fatalf("lookup: %s %v", id, err)
	}
	second := activity(withKey("job-42", storage.BehaviorReturnExisting))
	existing, err := s.b.EnqueueIdempotent(s.ctx, &second)
	if err != nil || existing == nil || existing.ExistingID != first.ID || existing.ExistingParentID == nil || *existing.ExistingParentID != parent.ID {
		t.Fatalf("duplicate: %+v %v", existing, err)
	}
	if snap, err := s.r.GetActivity(s.ctx, second.ID); err != nil || snap != nil {
		t.Fatalf("duplicate was enqueued: %+v %v", snap, err)
	}
	// A nil key is a plain enqueue.
	plain := activity()
	if existing, err := s.b.EnqueueIdempotent(s.ctx, &plain); err != nil || existing != nil {
		t.Fatalf("plain enqueue: %v %v", existing, err)
	}
	s.wantStatus(plain.ID, "pending")
	if _, err := s.b.LookupIdempotencyActivityID(s.ctx, "never-claimed"); err == nil {
		t.Fatal("lookup of an unclaimed key succeeded")
	} else {
		wantKind(t, err, storage.ErrNotFound, "unclaimed key")
	}
}

func testIdempotentNoReuse(t *testing.T, h Harness) {
	s := newSuite(t, h)
	first := activity(withKey("once", storage.BehaviorNoReuse))
	if _, err := s.b.EnqueueIdempotent(s.ctx, &first); err != nil {
		t.Fatal(err)
	}
	dup := activity(withKey("once", storage.BehaviorNoReuse))
	_, err := s.b.EnqueueIdempotent(s.ctx, &dup)
	wantKind(t, err, storage.ErrDuplicateActivity, "no-reuse duplicate")
	if snap, err := s.r.GetActivity(s.ctx, dup.ID); err != nil || snap != nil {
		t.Fatalf("rejected duplicate was enqueued: %+v %v", snap, err)
	}
}

func testIdempotentAllowReuse(t *testing.T, h Harness) {
	s := newSuite(t, h)
	first := activity(withKey("again", storage.BehaviorAllowReuse))
	if _, err := s.b.EnqueueIdempotent(s.ctx, &first); err != nil {
		t.Fatal(err)
	}
	second := activity(withKey("again", storage.BehaviorAllowReuse))
	if existing, err := s.b.EnqueueIdempotent(s.ctx, &second); err != nil || existing != nil {
		t.Fatalf("reuse: %v %v", existing, err)
	}
	s.wantStatus(second.ID, "pending")
	if id, err := s.b.LookupIdempotencyActivityID(s.ctx, "again"); err != nil || id != second.ID {
		t.Fatalf("key now points at %s (%v), want %s", id, err, second.ID)
	}
}

func testIdempotentAllowReuseOnFailure(t *testing.T, h Harness) {
	s := newSuite(t, h)
	first := activity(withKey("retry-me", storage.BehaviorAllowReuseOnFailure), withMaxRetries(1))
	if _, err := s.b.EnqueueIdempotent(s.ctx, &first); err != nil {
		t.Fatal(err)
	}
	blocked := activity(withKey("retry-me", storage.BehaviorAllowReuseOnFailure))
	_, err := s.b.EnqueueIdempotent(s.ctx, &blocked)
	wantKind(t, err, storage.ErrIdempotencyConflict, "reuse while live")
	s.claim("w", first)
	if dead, err := s.b.AckFailure(s.ctx, first.ID, storage.NewRetryableFailure("boom"), "w"); err != nil || !dead {
		t.Fatalf("dead=%v err=%v", dead, err)
	}
	replacement := activity(withKey("retry-me", storage.BehaviorAllowReuseOnFailure))
	if existing, err := s.b.EnqueueIdempotent(s.ctx, &replacement); err != nil || existing != nil {
		t.Fatalf("reuse after dead-letter: %v %v", existing, err)
	}
	s.wantStatus(replacement.ID, "pending")
	if id, err := s.b.LookupIdempotencyActivityID(s.ctx, "retry-me"); err != nil || id != replacement.ID {
		t.Fatalf("key points at %s (%v), want %s", id, err, replacement.ID)
	}
}

// Racing spawns of one key from two processes: exactly one enqueues, every
// other caller gets that activity back.
func testIdempotentConcurrent(t *testing.T, h Harness) {
	s := newSuite(t, h)
	handles := []storage.Storage{s.b, s.another()}
	const n = 10
	results := make(chan *storage.IdempotencyResult, n)
	ids := make(chan uuid.UUID, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		b := handles[i%len(handles)]
		wg.Go(func() {
			a := activity(withKey("race", storage.BehaviorReturnExisting))
			<-start
			existing, err := b.EnqueueIdempotent(s.ctx, &a)
			if err != nil {
				t.Errorf("claimer %d: %v", i, err)
				return
			}
			results <- existing
			ids <- a.ID
		})
	}
	close(start)
	wg.Wait()
	close(results)
	close(ids)
	var winner uuid.UUID
	fresh := 0
	for existing := range results {
		if existing == nil {
			fresh++
			continue
		}
		if winner != uuid.Nil && existing.ExistingID != winner {
			t.Fatalf("callers were told different owners: %s and %s", winner, existing.ExistingID)
		}
		winner = existing.ExistingID
	}
	if fresh != 1 {
		t.Fatalf("%d callers enqueued, want exactly 1", fresh)
	}
	enqueued := 0
	for id := range ids {
		if snap, err := s.r.GetActivity(s.ctx, id); err != nil {
			t.Fatal(err)
		} else if snap != nil {
			enqueued++
			if winner != uuid.Nil && id != winner {
				t.Fatalf("enqueued %s but callers were told %s", id, winner)
			}
		}
	}
	if enqueued != 1 {
		t.Fatalf("%d activities exist for one key", enqueued)
	}
}

// User keys encoded with BusinessIdempotencyKey are distinct per activity
// type and resolve through LookupIdempotencyActivityID.
func testBusinessKeys(t *testing.T, h Harness) {
	s := newSuite(t, h)
	charge := activity(withType("charge"), withKey(storage.BusinessIdempotencyKey("order-1", "charge"), storage.BehaviorReturnExisting))
	email := activity(withType("email"), withKey(storage.BusinessIdempotencyKey("order-1", "email"), storage.BehaviorReturnExisting))
	for _, a := range []*storage.QueuedActivity{&charge, &email} {
		if existing, err := s.b.EnqueueIdempotent(s.ctx, a); err != nil || existing != nil {
			t.Fatalf("%s: %v %v", a.ActivityType, existing, err)
		}
	}
	again := activity(withType("charge"), withKey(charge.IdempotencyKey.Key, storage.BehaviorReturnExisting))
	if existing, err := s.b.EnqueueIdempotent(s.ctx, &again); err != nil || existing == nil || existing.ExistingID != charge.ID {
		t.Fatalf("same key and type: %v %v", existing, err)
	}
	for _, a := range []storage.QueuedActivity{charge, email} {
		if id, err := s.b.LookupIdempotencyActivityID(s.ctx, a.IdempotencyKey.Key); err != nil || id != a.ID {
			t.Fatalf("lookup %s: %s %v", a.ActivityType, id, err)
		}
	}
}
