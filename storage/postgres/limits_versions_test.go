package postgres

import (
	"context"
	"testing"

	"github.com/runnerq/runnerq-go/storage"
)

// Wrong-version work must not make a saturated limit look like eligible work
// was held back (which would trigger needless limit-probe wakeups).
func TestLimitedVersionPrecheck(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	a := testActivity(0)
	a.CodeVersion = "B"
	if err := b.Enqueue(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := b.SetLimit(ctx, storage.Limit{ActivityType: a.ActivityType, MaxConcurrent: 1}); err != nil {
		t.Fatal(err)
	}
	if got, err := b.Dequeue(ctx, "B", 0, nil, []string{"B"}); err != nil || got == nil {
		t.Fatalf("claim B: %v, %v", got, err)
	}
	other := testActivity(0)
	other.CodeVersion = "B"
	if err := b.Enqueue(ctx, other); err != nil {
		t.Fatal(err)
	}
	if got, held, err := b.dequeueOnce(ctx, "A", nil, []string{"A"}); err != nil || got != nil || held {
		t.Fatalf("A precheck with only B work: got %v, held=%t, err=%v", got, held, err)
	}
	eligible := testActivity(0)
	eligible.CodeVersion = "A"
	if err := b.Enqueue(ctx, eligible); err != nil {
		t.Fatal(err)
	}
	if got, held, err := b.dequeueOnce(ctx, "A", nil, []string{"A"}); err != nil || got != nil || !held {
		t.Fatalf("A precheck with eligible work and B occupying the slot: got %v, held=%t, err=%v", got, held, err)
	}
}
