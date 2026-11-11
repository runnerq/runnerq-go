package storagetest

import (
	"fmt"
	"testing"
	"time"

	"github.com/runnerq/runnerq-go/storage"
)

// Acknowledgements: every state transition an execution makes is fenced on
// its claim token, atomic with the result it publishes, and safe to retry
// after a lost reply.
var ackTests = []conformanceTest{
	{"SuccessStoresResultAtomically", testAckSuccess},
	{"SuccessRetryIsIdempotentAndConflictIsRejected", testAckSuccessRetry},
	{"StaleTokenCannotAcknowledge", testStaleTokenCannotAck},
	{"RetryableFailureSchedulesAnotherAttempt", testRetryableFailure},
	{"RetryBackoffIsCapped", testRetryBackoffCap},
	{"ExhaustedRetriesDeadLetter", testDeadLetter},
	{"UnlimitedRetriesNeverDeadLetter", testUnlimitedRetries},
	{"NonRetryableFailureIsTerminal", testNonRetryableFailure},
	{"FailureRetryDoesNotConsumeAnotherAttempt", testFailureAckRetry},
	{"YieldParksWithoutConsumingARetry", testYield},
	{"YieldIsFencedAndRetryable", testYieldFenced},
	{"WakeWaitingOnlyWakesParkedRows", testWakeWaiting},
}

// Completion and its result commit together, even for a nil result, so an
// awaiting parent can always resolve.
func testAckSuccess(t *testing.T, h Harness) {
	s := newSuite(t, h)
	a := s.enqueueClaimed("w", activity())
	if err := s.b.AckSuccess(s.ctx, a.ID, nil, "w"); err != nil {
		t.Fatal(err)
	}
	snap := s.snapshot(a.ID)
	if snap.Status != snapshotStatus["completed"] || snap.CompletedAt == nil || snap.CurrentWorkerID != nil || snap.LastWorkerID == nil || *snap.LastWorkerID != "w" {
		t.Fatalf("after success: %+v", snap)
	}
	s.wantResult(a.ID, storage.ResultOk)
	s.wantNoEvents(a.ID)
	s.claimNothing()

	b := s.enqueueClaimed("w2", activity())
	if err := s.b.AckSuccess(s.ctx, b.ID, raw(map[string]int{"answer": 42}), "w2"); err != nil {
		t.Fatal(err)
	}
	if res := s.wantResult(b.ID, storage.ResultOk); string(res.Data) != `{"answer": 42}` && string(res.Data) != `{"answer":42}` {
		t.Fatalf("result data = %s", res.Data)
	}
}

// Retrying a completion (lost reply) with the same token and result succeeds;
// a different result conflicts and leaves the row unchanged.
func testAckSuccessRetry(t *testing.T, h Harness) {
	s := newSuite(t, h)
	a := s.enqueueClaimed("w", activity())
	for range 2 {
		if err := s.b.AckSuccess(s.ctx, a.ID, raw(map[string]int{"a": 1, "b": 2}), "w"); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.b.AckSuccess(s.ctx, a.ID, raw(map[string]int{"b": 2, "a": 1}), "w"); err != nil {
		t.Fatalf("semantically equal retry rejected: %v", err)
	}
	wantKind(t, s.b.AckSuccess(s.ctx, a.ID, raw(false), "w"), storage.ErrCheckpointConflict, "different result, same token")
	if res := s.wantResult(a.ID, storage.ResultOk); string(res.Data) == "false" {
		t.Fatal("conflicting retry overwrote the result")
	}
}

// After the lease expires and the row is reclaimed, the old token cannot
// complete, fail, or park it; the new token can.
func testStaleTokenCannotAck(t *testing.T, h Harness) {
	s := newSuite(t, h)
	a := s.enqueueClaimed("stale", activity(withMaxRetries(5)))
	s.expire(a.ID)
	s.reap(1)
	s.claim("fresh", a)

	wantKind(t, s.b.AckSuccess(s.ctx, a.ID, raw("stale"), "stale"), storage.ErrClaimLost, "stale success")
	_, err := s.b.AckFailure(s.ctx, a.ID, storage.NewRetryableFailure("stale"), "stale")
	wantKind(t, err, storage.ErrClaimLost, "stale failure")
	wantKind(t, s.b.Yield(s.ctx, a.ID, time.Now().UTC().Add(time.Hour), "stale", "sleep", "nap"), storage.ErrClaimLost, "stale yield")
	snap := s.snapshot(a.ID)
	if snap.Status != snapshotStatus["processing"] || *snap.CurrentWorkerID != "fresh" {
		t.Fatalf("stale acks changed the row: %+v", snap)
	}
	if err := s.b.AckSuccess(s.ctx, a.ID, raw("fresh"), "fresh"); err != nil {
		t.Fatalf("owning token: %v", err)
	}
	if res := s.wantResult(a.ID, storage.ResultOk); string(res.Data) != `"fresh"` {
		t.Fatalf("result = %s, want the owning token's", res.Data)
	}
}

// A retryable failure counts the attempt, records the error, and makes the
// row claimable again after its backoff — immediately when the delay is 0.
func testRetryableFailure(t *testing.T, h Harness) {
	s := newSuite(t, h)
	a := s.enqueueClaimed("w1", activity(withRetryDelay(0, 0)))
	dead, err := s.b.AckFailure(s.ctx, a.ID, storage.NewRetryableFailure("boom"), "w1")
	if err != nil || dead {
		t.Fatalf("dead=%v err=%v", dead, err)
	}
	snap := s.snapshot(a.ID)
	if snap.Status != snapshotStatus["retrying"] || snap.RetryCount != 1 || snap.LastError == nil || *snap.LastError != "boom" || snap.CurrentWorkerID != nil {
		t.Fatalf("after retryable failure: %+v", snap)
	}
	s.wantEvent(a.ID, storage.EventRetrying)
	if s.result(a.ID) != nil {
		t.Fatal("a retrying activity must not have a result yet")
	}
	s.claim("w2", a)
	if s.snapshot(a.ID).RetryCount != 1 {
		t.Fatal("reclaim changed retry_count")
	}

	// A long backoff keeps the row off the queue.
	slow := s.enqueueClaimed("w3", activity(withRetryDelay(3600, 0)))
	if _, err := s.b.AckFailure(s.ctx, slow.ID, storage.NewRetryableFailure("later"), "w3"); err != nil {
		t.Fatal(err)
	}
	s.wantStatus(slow.ID, "retrying")
	s.claimNothing()
}

// MaxRetryDelaySeconds caps the exponential backoff.
func testRetryBackoffCap(t *testing.T, h Harness) {
	s := newSuite(t, h)
	a := s.enqueueClaimed("w", activity(withRetryDelay(3600, 1)))
	if _, err := s.b.AckFailure(s.ctx, a.ID, storage.NewRetryableFailure("boom"), "w"); err != nil {
		t.Fatal(err)
	}
	s.claimNothing()
	time.Sleep(1500 * time.Millisecond)
	s.claim("w2", a)
}

// Once retry_count + 1 reaches MaxRetries the failure dead-letters: terminal,
// an Err result so awaiting parents resolve, listed in the dead-letter queue.
func testDeadLetter(t *testing.T, h Harness) {
	s := newSuite(t, h)
	a := s.enqueueClaimed("w1", activity(withMaxRetries(2), withRetryDelay(0, 0)))
	if dead, err := s.b.AckFailure(s.ctx, a.ID, storage.NewRetryableFailure("first"), "w1"); err != nil || dead {
		t.Fatalf("attempt 1: dead=%v err=%v", dead, err)
	}
	s.claim("w2", a)
	dead, err := s.b.AckFailure(s.ctx, a.ID, storage.NewRetryableFailure("second"), "w2")
	if err != nil || !dead {
		t.Fatalf("attempt 2: dead=%v err=%v, want dead-letter", dead, err)
	}
	snap := s.snapshot(a.ID)
	if snap.Status != snapshotStatus["dead_letter"] || snap.CompletedAt == nil || snap.CurrentWorkerID != nil {
		t.Fatalf("after exhaustion: %+v", snap)
	}
	s.wantResult(a.ID, storage.ResultErr)
	s.wantEvent(a.ID, storage.EventDeadLetter)
	s.claimNothing()
	records, err := s.r.ListDeadLetter(s.ctx, 0, 10)
	if err != nil || len(records) != 1 || records[0].Activity.ID != a.ID || records[0].Error != "second" {
		t.Fatalf("dead-letter list = %+v err=%v", records, err)
	}
}

// MaxRetries 0 means unlimited: failures keep rescheduling.
func testUnlimitedRetries(t *testing.T, h Harness) {
	s := newSuite(t, h)
	a := s.enqueue(activity(withMaxRetries(0), withRetryDelay(0, 0)))
	for i := range 4 {
		w := fmt.Sprintf("w%d", i)
		s.claim(w, a)
		if dead, err := s.b.AckFailure(s.ctx, a.ID, storage.NewRetryableFailure("again"), w); err != nil || dead {
			t.Fatalf("attempt %d: dead=%v err=%v", i, dead, err)
		}
	}
	if snap := s.snapshot(a.ID); snap.Status != snapshotStatus["retrying"] || snap.RetryCount != 4 {
		t.Fatalf("after 4 failures: %+v", snap)
	}
}

// A non-retryable failure is terminal on the first attempt regardless of the
// retry budget, with an Err result.
func testNonRetryableFailure(t *testing.T, h Harness) {
	s := newSuite(t, h)
	a := s.enqueueClaimed("w", activity(withMaxRetries(5)))
	dead, err := s.b.AckFailure(s.ctx, a.ID, storage.NewNonRetryableFailure("declined"), "w")
	if err != nil || dead {
		t.Fatalf("dead=%v err=%v", dead, err)
	}
	snap := s.snapshot(a.ID)
	if snap.Status != snapshotStatus["failed"] || snap.RetryCount != 0 || snap.CompletedAt == nil || snap.LastError == nil || *snap.LastError != "declined" {
		t.Fatalf("after non-retryable failure: %+v", snap)
	}
	s.wantResult(a.ID, storage.ResultErr)
	s.wantEvent(a.ID, storage.EventFailed)
	s.claimNothing()
}

// Retrying a committed failure acknowledgement (lost reply) returns the
// original decision without consuming another attempt.
func testFailureAckRetry(t *testing.T, h Harness) {
	for _, max := range []uint32{0, 1, 3} {
		t.Run(fmt.Sprint("max=", max), func(t *testing.T) {
			s := newSuite(t, h)
			a := s.enqueueClaimed("w", activity(withMaxRetries(max)))
			for range 2 {
				dead, err := s.b.AckFailure(s.ctx, a.ID, storage.NewRetryableFailure("failed"), "w")
				if err != nil || dead != (max == 1) {
					t.Fatalf("dead=%v err=%v", dead, err)
				}
			}
			if snap := s.snapshot(a.ID); max != 1 && snap.RetryCount != 1 {
				t.Fatalf("retry_count = %d after one failure acked twice", snap.RetryCount)
			}
		})
	}
}

// Yield parks the row as waiting until wakeAt, keeps retry_count, releases
// the claim, and makes a due wake claimable.
func testYield(t *testing.T, h Harness) {
	s := newSuite(t, h)
	a := s.enqueueClaimed("w", activity())
	wakeAt := time.Now().UTC().Add(time.Hour)
	if err := s.b.Yield(s.ctx, a.ID, wakeAt, "w", "sleep", "nap"); err != nil {
		t.Fatal(err)
	}
	snap := s.snapshot(a.ID)
	if snap.Status != snapshotStatus["waiting"] || snap.RetryCount != 0 || snap.CurrentWorkerID != nil || snap.ScheduledAt == nil || snap.ScheduledAt.Sub(wakeAt).Abs() > time.Second {
		t.Fatalf("after yield: %+v", snap)
	}
	s.wantEvent(a.ID, storage.EventYielded)
	s.claimNothing()

	due := s.enqueueClaimed("w2", activity())
	if err := s.b.Yield(s.ctx, due.ID, time.Now().UTC().Add(-time.Second), "w2", "sleep", "nap"); err != nil {
		t.Fatal(err)
	}
	s.claim("w3", due)
	if s.snapshot(due.ID).RetryCount != 0 {
		t.Fatal("yield consumed a retry")
	}
}

// Yield is fenced like the acks, and a lost-reply retry of the same park
// succeeds without re-parking a row that has since been woken.
func testYieldFenced(t *testing.T, h Harness) {
	s := newSuite(t, h)
	a := s.enqueueClaimed("w", activity())
	wantKind(t, s.b.Yield(s.ctx, a.ID, time.Now().UTC().Add(time.Hour), "other", "sleep", "nap"), storage.ErrClaimLost, "yield with wrong token")
	s.wantStatus(a.ID, "processing")
	wakeAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	for range 2 {
		if err := s.b.Yield(s.ctx, a.ID, wakeAt, "w", "sleep", "nap"); err != nil {
			t.Fatalf("park retry: %v", err)
		}
	}
	if woke, err := s.b.WakeWaiting(s.ctx, a.ID); err != nil || !woke {
		t.Fatalf("wake: %v %v", woke, err)
	}
	if err := s.b.Yield(s.ctx, a.ID, wakeAt, "w", "sleep", "nap"); err != nil {
		t.Fatalf("park retry after wake: %v", err)
	}
	s.wantStatus(a.ID, "pending")
}

// WakeWaiting flips only a waiting row to pending.
func testWakeWaiting(t *testing.T, h Harness) {
	s := newSuite(t, h)
	a := s.enqueueClaimed("w", activity())
	if woke, err := s.b.WakeWaiting(s.ctx, a.ID); err != nil || woke {
		t.Fatalf("woke a processing row: %v %v", woke, err)
	}
	if err := s.b.Yield(s.ctx, a.ID, time.Now().UTC().Add(time.Hour), "w", "signal", "approval"); err != nil {
		t.Fatal(err)
	}
	if woke, err := s.b.WakeWaiting(s.ctx, a.ID); err != nil || !woke {
		t.Fatalf("wake: %v %v", woke, err)
	}
	s.wantStatus(a.ID, "pending")
	if woke, err := s.b.WakeWaiting(s.ctx, a.ID); err != nil || woke {
		t.Fatalf("woke a pending row: %v %v", woke, err)
	}
	s.claim("w2", a)
}
