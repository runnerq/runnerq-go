package storagetest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// Limits (storage.LimitStorage, skipped when absent): stored limits and the
// claim's promise under them: a limited type never has more running than its
// max_concurrent, nor more claims than its rate's tokens, however many
// handles claim at once.
var limitTests = []conformanceTest{
	{"SetGetListDelete", testLimitCRUD},
	{"MaxConcurrentCapsRunning", testLimitMaxConcurrent},
	{"WaitingAndRetryingDontCount", testLimitWaitingDoesntCount},
	{"RateSpendsTokens", testLimitRate},
	{"BatchMixesLimitedAndUnlimited", testLimitMixedBatch},
	{"LoweringDoesNotPreempt", testLimitLowering},
	{"DeletingFreesTheType", testLimitDelete},
	{"UntypedClaimRespectsLimits", testLimitUntyped},
	{"FiftyClaimersNeverExceedTen", testLimitConcurrentClaimers},
}

func (s *suite) limits() storage.LimitStorage {
	s.t.Helper()
	ls, ok := s.b.(storage.LimitStorage)
	if !ok {
		s.t.Skip("backend does not implement storage.LimitStorage")
	}
	return ls
}

func (s *suite) setLimit(l storage.Limit) {
	s.t.Helper()
	if err := s.limits().SetLimit(s.ctx, l); err != nil {
		s.t.Fatalf("SetLimit(%+v): %v", l, err)
	}
}

func (s *suite) enqueueN(n int, typ string) []storage.QueuedActivity {
	s.t.Helper()
	base := time.Now().UTC().Add(-time.Minute)
	out := make([]storage.QueuedActivity, n)
	for i := range out {
		out[i] = s.enqueue(activity(withType(typ), withCreatedAt(base.Add(time.Duration(i)*time.Millisecond))))
	}
	return out
}

func (s *suite) batch(prefix string, limit int, types ...string) []storage.DequeuedActivity {
	s.t.Helper()
	bq, ok := s.b.(storage.BatchQueueStorage)
	if !ok {
		var out []storage.DequeuedActivity
		for i := range limit {
			a := s.tryClaim(fmt.Sprintf("%s:%d", prefix, i), types...)
			if a == nil {
				break
			}
			out = append(out, storage.DequeuedActivity{Activity: *a, LeaseID: fmt.Sprintf("%s:%d", prefix, i)})
		}
		return out
	}
	got, err := bq.DequeueBatch(s.ctx, prefix, limit, 0, types)
	if err != nil {
		s.t.Fatalf("DequeueBatch: %v", err)
	}
	return got
}

func claimedIDs(claims []storage.DequeuedActivity) []uuid.UUID {
	ids := make([]uuid.UUID, len(claims))
	for i, c := range claims {
		ids[i] = c.Activity.ID
	}
	return ids
}

func idsOf(as ...storage.QueuedActivity) []uuid.UUID {
	ids := make([]uuid.UUID, len(as))
	for i, a := range as {
		ids[i] = a.ID
	}
	return ids
}

func (s *suite) complete(c storage.DequeuedActivity) {
	s.t.Helper()
	if err := s.b.AckSuccess(s.ctx, c.Activity.ID, nil, c.LeaseID); err != nil {
		s.t.Fatalf("ack %s: %v", c.Activity.ID, err)
	}
}

func testLimitCRUD(t *testing.T, h Harness) {
	s := newSuite(t, h)
	ls := s.limits()
	s.setLimit(storage.Limit{ActivityType: "b", MaxConcurrent: 3})
	s.setLimit(storage.Limit{ActivityType: "a", RateLimit: 5, RatePeriod: 1500 * time.Millisecond})
	got, err := ls.GetLimit(s.ctx, "a")
	if err != nil || got.MaxConcurrent != 0 || got.RateLimit != 5 || got.RatePeriod != 1500*time.Millisecond || got.UpdatedAt.IsZero() {
		t.Fatalf("GetLimit(a) = %+v, %v", got, err)
	}
	list, err := ls.ListLimits(s.ctx)
	if err != nil || len(list) != 2 || list[0].ActivityType != "a" || list[1].ActivityType != "b" || list[1].MaxConcurrent != 3 {
		t.Fatalf("ListLimits = %+v, %v", list, err)
	}
	updated := got.UpdatedAt
	s.setLimit(storage.Limit{ActivityType: "a", RateLimit: 5, RatePeriod: 1500 * time.Millisecond})
	if again, _ := ls.GetLimit(s.ctx, "a"); !again.UpdatedAt.Equal(updated) {
		t.Fatalf("an unchanged definition moved updated_at from %v to %v", updated, again.UpdatedAt)
	}
	s.setLimit(storage.Limit{ActivityType: "a", MaxConcurrent: 1})
	if got, _ := ls.GetLimit(s.ctx, "a"); got.MaxConcurrent != 1 || got.RateLimit != 0 || got.RatePeriod != 0 {
		t.Fatalf("replaced limit = %+v, want max concurrent 1 and no rate", got)
	}
	for _, bad := range []storage.Limit{
		{MaxConcurrent: 1},
		{ActivityType: "x"},
		{ActivityType: "x", MaxConcurrent: -1},
		{ActivityType: "x", RateLimit: 1},
		{ActivityType: "x", RatePeriod: time.Second},
		{ActivityType: "x", RateLimit: -1, RatePeriod: time.Second},
	} {
		if err := ls.SetLimit(s.ctx, bad); !isKind(err, storage.ErrInvalidArgument) {
			t.Errorf("SetLimit(%+v) = %v, want invalid argument", bad, err)
		}
	}
	if err := ls.DeleteLimit(s.ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := ls.GetLimit(s.ctx, "b"); !isKind(err, storage.ErrNotFound) {
		t.Fatalf("GetLimit after delete = %v, want not found", err)
	}
	if err := ls.DeleteLimit(s.ctx, "b"); !isKind(err, storage.ErrNotFound) {
		t.Fatalf("DeleteLimit of a deleted limit = %v, want not found", err)
	}
	if list, _ := s.another().(storage.LimitStorage).ListLimits(s.ctx); len(list) != 1 || list[0].ActivityType != "a" {
		t.Fatalf("another handle lists %+v, want only a", list)
	}
}

// At most MaxConcurrent run at once, in the usual order; a completion makes
// room for one more. Another handle sees the same count.
func testLimitMaxConcurrent(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.setLimit(storage.Limit{ActivityType: "t", MaxConcurrent: 2})
	as := s.enqueueN(4, "t")
	first := s.batch("w1", 10, "t")
	if !slices.Equal(claimedIDs(first), idsOf(as[0], as[1])) {
		t.Fatalf("first batch = %v, want the two oldest", claimedIDs(first))
	}
	other := s.another()
	if got, err := other.Dequeue(s.ctx, "w2", 0, []string{"t"}); err != nil || got != nil {
		t.Fatalf("another handle claimed %v (%v) past the limit", got, err)
	}
	s.claimNothing("t")
	s.complete(first[0])
	s.claim("w3", as[2], "t")
	s.claimNothing("t")
}

func testLimitWaitingDoesntCount(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.setLimit(storage.Limit{ActivityType: "t", MaxConcurrent: 1})
	base := time.Now().UTC().Add(-time.Minute)
	var as []storage.QueuedActivity
	for i := range 3 {
		as = append(as, s.enqueue(activity(withType("t"), withRetryDelay(3600, 3600), withCreatedAt(base.Add(time.Duration(i)*time.Millisecond)))))
	}
	s.claim("w1", as[0], "t")
	s.claimNothing("t")
	if err := s.b.Yield(s.ctx, as[0].ID, time.Now().Add(time.Hour), "w1", "sleep", "sleep:nap"); err != nil {
		t.Fatalf("yield: %v", err)
	}
	s.claim("w2", as[1], "t")
	if dead, err := s.b.AckFailure(s.ctx, as[1].ID, storage.NewRetryableFailure("again"), "w2"); err != nil || dead {
		t.Fatalf("retryable failure: dead=%v err=%v", dead, err)
	}
	s.wantStatus(as[1].ID, "retrying")
	s.claim("w3", as[2], "t")
	s.claimNothing("t")
}

// Each claim takes a token; an hour-long period refills nothing meanwhile.
func testLimitRate(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.setLimit(storage.Limit{ActivityType: "t", RateLimit: 3, RatePeriod: time.Hour})
	as := s.enqueueN(5, "t")
	s.claim("w1", as[0], "t")
	got := s.batch("w2", 10, "t")
	if !slices.Equal(claimedIDs(got), idsOf(as[1], as[2])) {
		t.Fatalf("batch = %v, want the 2 tokens left", claimedIDs(got))
	}
	s.claimNothing("t")
	// Completing frees no tokens; restating the same rate keeps the bucket.
	s.complete(got[0])
	s.setLimit(storage.Limit{ActivityType: "t", RateLimit: 3, RatePeriod: time.Hour})
	s.claimNothing("t")
	// A cap added beside the rate keeps the bucket too.
	s.setLimit(storage.Limit{ActivityType: "t", MaxConcurrent: 10, RateLimit: 3, RatePeriod: time.Hour})
	s.claimNothing("t")
}

// The limited type takes its allowance and the unlimited ones fill the rest,
// all returned in claim order.
func testLimitMixedBatch(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.setLimit(storage.Limit{ActivityType: "x", MaxConcurrent: 1})
	xs := s.enqueueN(3, "x")
	ys := s.enqueueN(3, "y")
	urgent := s.enqueue(activity(withType("y"), withPriority(storage.PriorityCritical)))
	got := s.batch("w", 10, "x", "y")
	want := append(idsOf(urgent, xs[0]), idsOf(ys...)...)
	if !slices.Equal(claimedIDs(got), want) {
		t.Fatalf("mixed batch = %v, want %v", claimedIDs(got), want)
	}
	if more := s.batch("w2", 10, "x", "y"); len(more) != 0 {
		t.Fatalf("claimed %v past the limit", claimedIDs(more))
	}
	// A small claim limit bounds both parts.
	s.complete(got[1])
	s.enqueueN(2, "y")
	if got := s.batch("w3", 2, "x", "y"); len(got) != 2 || got[0].Activity.ID != xs[1].ID {
		t.Fatalf("limit-2 batch = %v, want x then one y", claimedIDs(got))
	}
}

func testLimitLowering(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.setLimit(storage.Limit{ActivityType: "t", MaxConcurrent: 3})
	as := s.enqueueN(4, "t")
	running := s.batch("w", 10, "t")
	if len(running) != 3 {
		t.Fatalf("claimed %d, want 3", len(running))
	}
	s.setLimit(storage.Limit{ActivityType: "t", MaxConcurrent: 1})
	for _, c := range running {
		s.wantStatus(c.Activity.ID, "processing")
	}
	s.claimNothing("t")
	s.complete(running[0])
	s.claimNothing("t")
	s.complete(running[1])
	s.claimNothing("t")
	s.complete(running[2])
	s.claim("w2", as[3], "t")
}

func testLimitDelete(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.setLimit(storage.Limit{ActivityType: "t", MaxConcurrent: 1})
	s.enqueueN(4, "t")
	s.batch("w", 10, "t")
	s.claimNothing("t")
	if err := s.another().(storage.LimitStorage).DeleteLimit(s.ctx, "t"); err != nil {
		t.Fatal(err)
	}
	if got := s.batch("w2", 10, "t"); len(got) != 3 {
		t.Fatalf("after delete claimed %d, want the 3 left", len(got))
	}
}

func testLimitUntyped(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.setLimit(storage.Limit{ActivityType: "x", MaxConcurrent: 1})
	s.enqueueN(2, "x")
	s.enqueueN(2, "y")
	got := s.batch("w", 10)
	types := map[string]int{}
	for _, c := range got {
		types[c.Activity.ActivityType]++
	}
	if types["x"] != 1 || types["y"] != 2 {
		t.Fatalf("untyped batch claimed %v, want 1 x and 2 y", types)
	}
	s.claimNothing()
}

// Fifty claimers on five handles claim and complete a type limited to 10 as
// fast as they can. The running count, sampled continuously from the store
// when the backend answers queries, never exceeds 10; neither does the number
// of claims the claimers hold, which the store's count bounds from above.
func testLimitConcurrentClaimers(t *testing.T, h Harness) {
	s := newSuite(t, h)
	const limit, claimers, total = 10, 50, 600
	s.setLimit(storage.Limit{ActivityType: "t", MaxConcurrent: limit})
	s.enqueueN(total, "t")
	handles := []storage.Storage{s.b}
	for range 4 {
		handles = append(handles, s.another())
	}

	ctx, cancel := context.WithTimeout(s.ctx, 2*time.Minute)
	defer cancel()
	var held, heldPeak, done atomic.Int64
	seen := sync.Map{}
	var dup atomic.Value
	var failure atomic.Value
	fail := func(err error) {
		failure.CompareAndSwap(nil, err)
		cancel()
	}

	var sampled, dbPeak atomic.Int64
	sampling, stopSampling := context.WithCancel(ctx)
	var wg sync.WaitGroup
	if qs, ok := s.b.(storage.QueryStorage); ok {
		filter := &storage.QueryFilter{And: []storage.QueryFilter{
			{Field: "queue", Op: storage.OpEq, Value: s.queue},
			{Field: "type", Op: storage.OpEq, Value: "t"},
			{Field: "status", Op: storage.OpEq, Value: storage.RecordStatusRunning},
		}}
		wg.Go(func() {
			for sampling.Err() == nil {
				n, _, err := qs.CountActivities(sampling, filter, 0)
				if err != nil {
					if sampling.Err() == nil {
						fail(fmt.Errorf("count running: %w", err))
					}
					return
				}
				sampled.Add(1)
				for p := dbPeak.Load(); n > p && !dbPeak.CompareAndSwap(p, n); p = dbPeak.Load() {
				}
			}
		})
	}

	var claimWG sync.WaitGroup
	for i := range claimers {
		b := handles[i%len(handles)]
		bq, batch := b.(storage.BatchQueueStorage)
		batch = batch && i%2 == 0
		claimWG.Go(func() {
			for done.Load() < total && ctx.Err() == nil {
				var claims []storage.DequeuedActivity
				if batch {
					got, err := bq.DequeueBatch(ctx, fmt.Sprintf("c%d:%s", i, uuid.NewString()), 3, 0, []string{"t"})
					if err != nil {
						if ctx.Err() == nil {
							fail(err)
						}
						return
					}
					claims = got
				} else {
					token := fmt.Sprintf("c%d:%s", i, uuid.NewString())
					a, err := b.Dequeue(ctx, token, 0, []string{"t"})
					if err != nil {
						if ctx.Err() == nil {
							fail(err)
						}
						return
					}
					if a != nil {
						claims = []storage.DequeuedActivity{{Activity: *a, LeaseID: token}}
					}
				}
				if len(claims) == 0 {
					time.Sleep(time.Millisecond)
					continue
				}
				n := held.Add(int64(len(claims)))
				for p := heldPeak.Load(); n > p && !heldPeak.CompareAndSwap(p, n); p = heldPeak.Load() {
				}
				time.Sleep(time.Duration(rand.IntN(3)) * time.Millisecond) // #nosec G404 -- jitter
				for _, c := range claims {
					if _, loaded := seen.LoadOrStore(c.Activity.ID, true); loaded {
						dup.CompareAndSwap(nil, c.Activity.ID)
					}
					held.Add(-1)
					if err := b.AckSuccess(context.WithoutCancel(ctx), c.Activity.ID, nil, c.LeaseID); err != nil {
						fail(fmt.Errorf("ack %s: %w", c.Activity.ID, err))
						return
					}
					done.Add(1)
				}
			}
		})
	}
	claimWG.Wait()
	stopSampling()
	wg.Wait()
	if err, _ := failure.Load().(error); err != nil {
		t.Fatal(err)
	}
	if id := dup.Load(); id != nil {
		t.Fatalf("activity %v claimed twice", id)
	}
	if done.Load() != total {
		t.Fatalf("completed %d of %d", done.Load(), total)
	}
	if p := heldPeak.Load(); p > limit {
		t.Fatalf("claimers held %d claims at once, over the limit of %d", p, limit)
	}
	if p := dbPeak.Load(); p > limit {
		t.Fatalf("%d running at once in %d samples, over the limit of %d", p, sampled.Load(), limit)
	}
	t.Logf("peak held %d, peak running %d over %d samples", heldPeak.Load(), dbPeak.Load(), sampled.Load())
}
