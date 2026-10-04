package postgres

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/executor"
	"github.com/alob-mtc/runnerq-go/storage"
)

type changes struct {
	mu  sync.Mutex
	got []executor.Change
}

func (c *changes) Announce(ch executor.Change) {
	c.mu.Lock()
	c.got = append(c.got, ch)
	c.mu.Unlock()
}

func (c *changes) take() []executor.Change {
	c.mu.Lock()
	defer c.mu.Unlock()
	got := c.got
	c.got = nil
	return got
}

// A backend announces each change it commits once: a new submission (not a
// duplicate key), each claim with the executor its token names, and a
// success (not a reconciled repeat), never a failure.
func TestBackendAnnouncesCommittedChanges(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	got := &changes{}
	b.SetAnnouncer(got)

	now := testActivity(3)
	later := testActivity(3)
	at := time.Now().UTC().Add(time.Hour)
	later.ScheduledAt = &at
	keyed := testActivity(3)
	keyed.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: "k-" + uuid.NewString(), Behavior: storage.BehaviorReturnExisting}
	for _, a := range []storage.QueuedActivity{now, later} {
		if err := b.Enqueue(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	if existing, err := b.EnqueueIdempotent(ctx, &keyed); err != nil || existing != nil {
		t.Fatal(existing, err)
	}
	dup := keyed
	dup.ID = uuid.New()
	if existing, err := b.EnqueueIdempotent(ctx, &dup); err != nil || existing == nil {
		t.Fatal(existing, err)
	}
	subs := got.take()
	if len(subs) != 3 || subs[0].Kind != executor.Created || subs[0].ActivityID != now.ID || subs[0].RootID != now.ID ||
		subs[1].Kind != executor.Scheduled || subs[2].ActivityID != keyed.ID || subs[0].ActivityType != "test_activity" {
		t.Fatalf("submissions %+v", subs)
	}

	claims, err := b.DequeueBatch(ctx, "exec-7:batch:1", 2, 0, nil)
	if err != nil || len(claims) != 2 {
		t.Fatal(claims, err)
	}
	started := got.take()
	if len(started) != 2 || started[0].Kind != executor.AttemptStarted || started[0].Attempt != 1 || started[0].ExecutorID != "exec-7" {
		t.Fatalf("claims %+v", started)
	}

	if err := b.AckSuccess(ctx, claims[0].Activity.ID, json.RawMessage(`1`), claims[0].LeaseID); err != nil {
		t.Fatal(err)
	}
	if err := b.AckSuccess(ctx, claims[0].Activity.ID, json.RawMessage(`1`), claims[0].LeaseID); err != nil {
		t.Fatal(err) // a lost reply's retry: reconciled, not done again
	}
	if _, err := b.AckFailure(ctx, claims[1].Activity.ID, storage.NewNonRetryableFailure("no"), claims[1].LeaseID); err != nil {
		t.Fatal(err)
	}
	done := got.take()
	if len(done) != 1 || done[0].Kind != executor.AttemptSucceeded || done[0].ActivityID != claims[0].Activity.ID ||
		done[0].Attempt != 1 || done[0].ExecutorID != "exec-7" || done[0].ActivityType != "test_activity" {
		t.Fatalf("completions %+v", done)
	}

	b.SetAnnouncer(nil)
	if err := b.Enqueue(ctx, testActivity(3)); err != nil {
		t.Fatal(err)
	}
	if extra := got.take(); len(extra) != 0 {
		t.Fatalf("announced after SetAnnouncer(nil): %+v", extra)
	}
}
