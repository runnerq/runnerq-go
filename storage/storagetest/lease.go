package storagetest

import (
	"fmt"
	"testing"
	"time"

	"github.com/runnerq/runnerq-go/storage"
)

// Lease recovery: an expired claim is presumed dead; the reaper requeues it as
// a failed attempt and the old execution can no longer act on it. A live
// execution can keep renewing its claim.
var leaseTests = []conformanceTest{
	{"ReaperRequeuesExpiredLeasesOnly", testReaperRequeuesExpired},
	{"ReaperCountsAttemptAndDeadLetters", testReaperAttemptAccounting},
	{"ReaperRespectsBatchSize", testReaperBatchSize},
	{"AttemptBudgetIsSharedByFailuresAndReaper", testAttemptBudget},
	{"RenewalIsFencedAndNeverShortens", testAttemptRenewal},
}

// Only rows whose lease has passed are reaped; a live claim is untouched.
func testReaperRequeuesExpired(t *testing.T, h Harness) {
	s := newSuite(t, h)
	live := s.enqueueClaimed("live", activity(withType("live")))
	dead := s.enqueueClaimed("dead", activity(withType("dead")))
	s.reap(0)
	s.expire(dead.ID)
	s.reap(1)
	s.reap(0)
	s.wantStatus(live.ID, "processing")
	snap := s.snapshot(dead.ID)
	if snap.Status != snapshotStatus["pending"] || snap.RetryCount != 1 || snap.CurrentWorkerID != nil || snap.LeaseDeadlineMS != nil || snap.StartedAt != nil || snap.LastError == nil {
		t.Fatalf("after reap: %+v", snap)
	}
	s.wantEvent(dead.ID, storage.EventRequeued)
	s.claim("next", dead)
	if err := s.b.AckSuccess(s.ctx, live.ID, nil, "live"); err != nil {
		t.Fatalf("live claim was disturbed: %v", err)
	}
}

// Each expiry consumes an attempt; the last one dead-letters with an Err
// result so a parent awaiting the activity resolves.
func testReaperAttemptAccounting(t *testing.T, h Harness) {
	s := newSuite(t, h)
	a := s.enqueueClaimed("w1", activity(withMaxRetries(2)))
	s.expire(a.ID)
	s.reap(1)
	if snap := s.snapshot(a.ID); snap.Status != snapshotStatus["pending"] || snap.RetryCount != 1 {
		t.Fatalf("after reap 1: %+v", snap)
	}
	s.claim("w2", a)
	s.expire(a.ID)
	s.reap(1)
	snap := s.snapshot(a.ID)
	// The dead-letter is terminal: it doesn't count another attempt.
	if snap.Status != snapshotStatus["dead_letter"] || snap.RetryCount != 1 || snap.CompletedAt == nil {
		t.Fatalf("after reap 2: %+v", snap)
	}
	s.wantEvent(a.ID, storage.EventDeadLetter)
	s.wantResult(a.ID, storage.ResultErr)
	s.claimNothing()
}

func testReaperBatchSize(t *testing.T, h Harness) {
	s := newSuite(t, h)
	for i := range 5 {
		a := s.enqueueClaimed(fmt.Sprint("w", i), activity())
		s.expire(a.ID)
	}
	if n, err := s.b.RequeueExpired(s.ctx, 2); err != nil || n != 2 {
		t.Fatalf("batch of 2: n=%d err=%v", n, err)
	}
	if n, err := s.b.RequeueExpired(s.ctx, 10); err != nil || n != 3 {
		t.Fatalf("remainder: n=%d err=%v", n, err)
	}
}

// Failures and lease expiries draw on the same attempt budget with the same
// boundary, for every MaxRetries setting.
func testAttemptBudget(t *testing.T, h Harness) {
	for _, reap := range []bool{false, true} {
		for _, max := range []uint32{0, 1, 3} {
			t.Run(fmt.Sprintf("reaper=%v/max=%d", reap, max), func(t *testing.T) {
				s := newSuite(t, h)
				a := s.enqueue(activity(withMaxRetries(max), withRetryDelay(0, 0)))
				attempts := 3
				if max == 1 {
					attempts = 1
				}
				for i := range attempts {
					worker := fmt.Sprintf("attempt-%d", i)
					s.claim(worker, a)
					if reap {
						s.expire(a.ID)
						s.reap(1)
					} else if _, err := s.b.AckFailure(s.ctx, a.ID, storage.NewRetryableFailure("failure"), worker); err != nil {
						t.Fatal(err)
					}
					dead := max > 0 && i+1 >= int(max)
					if got := s.status(a.ID); (got == "dead_letter") != dead {
						t.Fatalf("max=%d attempt=%d status=%s", max, i+1, got)
					}
				}
			})
		}
	}
}

// ExtendLeaseForWorker renews only the named execution's claim, never
// shortens a lease, and reports a lost claim as not owned.
func testAttemptRenewal(t *testing.T, h Harness) {
	s := newSuite(t, h)
	lease := need[storage.AttemptLeaseStorage](t, s.b)
	a := s.enqueueClaimed("owner", activity())
	if ok, err := lease.ExtendLeaseForWorker(s.ctx, a.ID, "other", time.Hour); err != nil || ok {
		t.Fatalf("renewed under a foreign token: %v %v", ok, err)
	}
	before := *s.snapshot(a.ID).LeaseDeadlineMS
	if ok, err := lease.ExtendLeaseForWorker(s.ctx, a.ID, "owner", time.Millisecond); err != nil || !ok {
		t.Fatalf("renew: %v %v", ok, err)
	}
	if after := *s.snapshot(a.ID).LeaseDeadlineMS; after < before {
		t.Fatalf("renewal shortened the lease %d -> %d", before, after)
	}
	if ok, err := lease.ExtendLeaseForWorker(s.ctx, a.ID, "owner", 10*time.Minute); err != nil || !ok {
		t.Fatalf("renew: %v %v", ok, err)
	}
	if after := *s.snapshot(a.ID).LeaseDeadlineMS; after <= before {
		t.Fatalf("renewal did not extend the lease %d -> %d", before, after)
	}
	s.expire(a.ID)
	s.reap(1)
	if ok, err := lease.ExtendLeaseForWorker(s.ctx, a.ID, "owner", time.Hour); err != nil || ok {
		t.Fatalf("renewed after the claim was lost: %v %v", ok, err)
	}
}
