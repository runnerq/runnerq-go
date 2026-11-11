package storagetest

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// Durable execution: checkpoints that outlive an attempt and are published by
// exactly one owner, parents that park and are woken by their children,
// signals delivered to parked waiters, spawns fenced on the spawner's claim,
// and result waits that work across processes.
var durableTests = []conformanceTest{
	{"ResultsRoundTripAndStepsAreListed", testResultsAndSteps},
	{"CheckpointIsImmutableAndFenced", testCheckpoint},
	{"ChildOutcomeWakesAwaitingParent", testChildWakesParent},
	{"DependencyRegistrationIsFencedAndCheckedAgainstProducer", testRegisterDependency},
	{"ParkOnResultWakesImmediatelyWhenReady", testYieldForResultReady},
	{"ParkAndPublishNeverLoseTheWake", testParkPublishRace},
	{"SharedResultWakesEveryConsumer", testSharedResultWakesAll},
	{"SignalIsStoredAndWakesParkedTarget", testSignal},
	{"SignalToUnknownActivityIsNotFound", testSignalUnknown},
	{"SpawnsAreFencedOnTheSpawnersClaim", testSpawnFence},
	{"WaitForResultAcrossProcesses", testWaitForResult},
	{"WaitForResultResolvesFailures", testWaitForResultFailure},
}

// StoreResult/GetResult round-trip checkpoints owned by an activity, and
// GetActivitySteps lists the named ones oldest first.
func testResultsAndSteps(t *testing.T, h Harness) {
	s := newSuite(t, h)
	a := s.enqueueClaimed("w", activity())
	if s.result(uuid.New()) != nil {
		t.Fatal("unknown result id returned a result")
	}
	run, sleep := uuid.New(), uuid.New()
	if err := s.b.StoreResult(s.ctx, run, a.ID, storage.ActivityResult{Data: raw("receipt"), State: storage.ResultOk}, "run:charge"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if err := s.b.StoreResult(s.ctx, sleep, a.ID, storage.ActivityResult{Data: raw(map[string]string{"wake_at": "2030-01-01T00:00:00Z"}), State: storage.ResultOk}, "sleep:cooldown"); err != nil {
		t.Fatal(err)
	}
	if err := s.b.StoreResult(s.ctx, uuid.New(), a.ID, storage.ActivityResult{Data: raw("err"), State: storage.ResultErr}, ""); err != nil {
		t.Fatal(err)
	}
	if res := s.wantResult(run, storage.ResultOk); string(res.Data) != `"receipt"` {
		t.Fatalf("checkpoint data = %s", res.Data)
	}
	steps, err := s.r.GetActivitySteps(s.ctx, a.ID)
	if err != nil || len(steps) != 2 {
		t.Fatalf("steps = %+v err=%v, want the two named checkpoints", steps, err)
	}
	if steps[0].Kind != "run" || steps[0].Name != "charge" || steps[1].Kind != "sleep" || steps[1].Name != "cooldown" || steps[0].State != storage.ResultOk {
		t.Fatalf("steps = %+v", steps)
	}
}

// A checkpoint can be re-stored with the same outcome (lost reply), never
// changed, and only by the execution that owns the activity.
func testCheckpoint(t *testing.T, h Harness) {
	s := newSuite(t, h)
	cp := need[storage.CheckpointStorage](t, s.b)
	a := s.enqueueClaimed("old", activity())
	id := uuid.New()
	ok := storage.ActivityResult{Data: raw("original"), State: storage.ResultOk}
	for range 2 {
		if err := cp.StoreCheckpoint(s.ctx, id, a.ID, "old", ok, "run:charge"); err != nil {
			t.Fatal(err)
		}
	}
	wantKind(t, cp.StoreCheckpoint(s.ctx, id, a.ID, "old", storage.ActivityResult{Data: raw("changed"), State: storage.ResultOk}, "run:charge"), storage.ErrCheckpointConflict, "changed checkpoint")
	wantKind(t, cp.StoreCheckpoint(s.ctx, uuid.New(), a.ID, "other", ok, "run:late"), storage.ErrClaimLost, "foreign token")
	s.expire(a.ID)
	s.reap(1)
	s.claim("new", a)
	wantKind(t, cp.StoreCheckpoint(s.ctx, uuid.New(), a.ID, "old", ok, "run:late"), storage.ErrClaimLost, "stale token")
	if err := cp.StoreCheckpoint(s.ctx, uuid.New(), a.ID, "new", ok, "run:late"); err != nil {
		t.Fatalf("owning token: %v", err)
	}
	if res := s.wantResult(id, storage.ResultOk); string(res.Data) != `"original"` {
		t.Fatalf("checkpoint changed: %s", res.Data)
	}
	s.wantNoEvents(id)
}

// A parent parked on a child's result is woken by its terminal outcome —
// success, permanent failure, exhaustion, or the reaper dead-lettering it —
// and left parked by a retryable child failure.
func testChildWakesParent(t *testing.T, h Harness) {
	for _, outcome := range []string{"success", "failed", "dead_letter", "reaper", "retrying"} {
		t.Run(outcome, func(t *testing.T) {
			s := newSuite(t, h)
			parent := s.enqueueClaimed("parent", activity(withType("parent")))
			maxRetries := uint32(1) // dead-letter on the first retryable failure
			if outcome == "retrying" {
				maxRetries = 3
			}
			child := s.enqueueClaimed("child", activity(withType("child"), withParent(parent), withMaxRetries(maxRetries), withRetryDelay(0, 0)))
			if err := need[storage.DependencyStorage](t, s.b).YieldForResult(s.ctx, parent.ID, child.ID, &child.ID, time.Now().UTC().Add(time.Hour), "parent", "await", "child"); err != nil {
				t.Fatal(err)
			}
			var err error
			switch outcome {
			case "success":
				err = s.b.AckSuccess(s.ctx, child.ID, nil, "child")
			case "failed":
				_, err = s.b.AckFailure(s.ctx, child.ID, storage.NewNonRetryableFailure("no"), "child")
			case "dead_letter":
				_, err = s.b.AckFailure(s.ctx, child.ID, storage.NewRetryableFailure("no"), "child")
			case "reaper":
				s.expire(child.ID)
				_, err = s.b.RequeueExpired(s.ctx, 10)
			case "retrying":
				_, err = s.b.AckFailure(s.ctx, child.ID, storage.NewRetryableFailure("again"), "child")
			}
			if err != nil {
				t.Fatal(err)
			}
			want := "pending"
			if outcome == "retrying" {
				want = "waiting"
			}
			s.wantStatus(parent.ID, want)
		})
	}
}

// RegisterDependency is fenced on the waiter's claim and rejects a producer
// that does not exist in the queue.
func testRegisterDependency(t *testing.T, h Harness) {
	s := newSuite(t, h)
	deps := need[storage.DependencyStorage](t, s.b)
	waiter := s.enqueueClaimed("w", activity(withType("waiter")))
	producer := s.enqueue(activity(withType("producer")))
	wantKind(t, deps.RegisterDependency(s.ctx, waiter.ID, producer.ID, "other"), storage.ErrClaimLost, "foreign token")
	wantKind(t, deps.RegisterDependency(s.ctx, waiter.ID, uuid.New(), "w"), storage.ErrNotFound, "unknown producer")
	for range 2 {
		if err := deps.RegisterDependency(s.ctx, waiter.ID, producer.ID, "w"); err != nil {
			t.Fatal(err)
		}
	}
}

// YieldForResult parks the waiter when the result is missing and leaves it
// runnable when the result already exists; a signal (nil producer) works
// the same way.
func testYieldForResultReady(t *testing.T, h Harness) {
	s := newSuite(t, h)
	deps := need[storage.DependencyStorage](t, s.b)
	producer := s.enqueueClaimed("p", activity(withType("producer")))
	wakeAt := time.Now().UTC().Add(time.Hour)

	parked := s.enqueueClaimed("w1", activity(withType("w1")))
	if err := deps.YieldForResult(s.ctx, parked.ID, producer.ID, &producer.ID, wakeAt, "w1", "await", "child"); err != nil {
		t.Fatal(err)
	}
	s.wantStatus(parked.ID, "waiting")
	s.wantEvent(parked.ID, storage.EventYielded)
	wantKind(t, deps.YieldForResult(s.ctx, parked.ID, producer.ID, &producer.ID, wakeAt, "other", "await", "child"), storage.ErrClaimLost, "foreign token")

	if err := s.b.AckSuccess(s.ctx, producer.ID, raw(1), "p"); err != nil {
		t.Fatal(err)
	}
	s.wantStatus(parked.ID, "pending")
	ready := s.enqueueClaimed("w2", activity(withType("w2")))
	if err := deps.YieldForResult(s.ctx, ready.ID, producer.ID, &producer.ID, wakeAt, "w2", "await", "child"); err != nil {
		t.Fatal(err)
	}
	s.wantStatus(ready.ID, "pending")

	sig := s.enqueueClaimed("w3", activity(withType("w3")))
	sigID := uuid.New()
	if err := deps.YieldForResult(s.ctx, sig.ID, sigID, nil, wakeAt, "w3", "signal", "approval"); err != nil {
		t.Fatal(err)
	}
	s.wantStatus(sig.ID, "waiting")
	if err := s.b.SignalActivity(s.ctx, sig.ID, sigID, "approval", raw("ok")); err != nil {
		t.Fatal(err)
	}
	s.wantStatus(sig.ID, "pending")
}

// A park racing publication of its result ends with the waiter runnable,
// never parked forever; a park retried after an early wake doesn't re-park.
func testParkPublishRace(t *testing.T, h Harness) {
	s := newSuite(t, h)
	deps := need[storage.DependencyStorage](t, s.b)
	for i := range 12 {
		producer := s.enqueueClaimed("p", activity(withType(fmt.Sprint("producer", i))))
		waiter := s.enqueueClaimed("w", activity(withType(fmt.Sprint("waiter", i))))
		wakeAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
		start := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Go(func() {
			<-start
			errs <- deps.YieldForResult(s.ctx, waiter.ID, producer.ID, &producer.ID, wakeAt, "w", "await", "child")
		})
		wg.Go(func() { <-start; errs <- s.b.AckSuccess(s.ctx, producer.ID, nil, "p") })
		close(start)
		wg.Wait()
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
		s.wantStatus(waiter.ID, "pending")
		if err := deps.YieldForResult(s.ctx, waiter.ID, producer.ID, &producer.ID, wakeAt, "w", "await", "child"); err != nil {
			t.Fatalf("park retry after wake: %v", err)
		}
		s.wantStatus(waiter.ID, "pending")
	}
}

// Every consumer parked on one result wakes when it is published, whatever
// the outcome — including a checkpoint stored under a synthetic id.
func testSharedResultWakesAll(t *testing.T, h Harness) {
	for _, outcome := range []string{"success", "failure", "reaper", "checkpoint"} {
		t.Run(outcome, func(t *testing.T) {
			s := newSuite(t, h)
			deps := need[storage.DependencyStorage](t, s.b)
			producer := s.enqueueClaimed("producer", activity(withType("producer"), withMaxRetries(1)))
			resultID := producer.ID
			if outcome == "checkpoint" {
				resultID = uuid.New()
			}
			var waiters []storage.QueuedActivity
			for i := range 2 {
				w := fmt.Sprint("waiter", i)
				a := s.enqueueClaimed(w, activity(withType(w)))
				if outcome != "checkpoint" {
					// A checkpoint id is not a producer; only the park registers it.
					if err := deps.RegisterDependency(s.ctx, a.ID, resultID, w); err != nil {
						t.Fatal(err)
					}
				}
				if err := deps.YieldForResult(s.ctx, a.ID, resultID, &producer.ID, time.Now().UTC().Add(time.Hour), w, "await", "child"); err != nil {
					t.Fatal(err)
				}
				s.wantStatus(a.ID, "waiting")
				waiters = append(waiters, a)
			}
			var err error
			switch outcome {
			case "success":
				err = s.b.AckSuccess(s.ctx, producer.ID, nil, "producer")
			case "failure":
				_, err = s.b.AckFailure(s.ctx, producer.ID, storage.NewNonRetryableFailure("failed"), "producer")
			case "reaper":
				s.expire(producer.ID)
				_, err = s.b.RequeueExpired(s.ctx, 10)
			case "checkpoint":
				err = need[storage.CheckpointStorage](t, s.b).StoreCheckpoint(s.ctx, resultID, producer.ID, "producer", storage.ActivityResult{Data: raw(1), State: storage.ResultOk}, "run:step")
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, a := range waiters {
				s.wantStatus(a.ID, "pending")
			}
		})
	}
}

// A signal stores its payload under the signal id (owned by the target),
// wakes a parked target, leaves other states alone, and last write wins.
func testSignal(t *testing.T, h Harness) {
	s := newSuite(t, h)
	a := s.enqueue(activity())
	sigID := uuid.New()
	if err := s.b.SignalActivity(s.ctx, a.ID, sigID, "approval", raw("first")); err != nil {
		t.Fatal(err)
	}
	s.wantStatus(a.ID, "pending")
	if err := s.b.SignalActivity(s.ctx, a.ID, sigID, "approval", raw("second")); err != nil {
		t.Fatal(err)
	}
	if res := s.wantResult(sigID, storage.ResultOk); string(res.Data) != `"second"` {
		t.Fatalf("signal payload = %s, want last write", res.Data)
	}
	s.wantEvent(a.ID, storage.EventSignaled)
	s.claim("w", a)
	if err := s.b.SignalActivity(s.ctx, a.ID, uuid.New(), "other", raw(1)); err != nil {
		t.Fatal(err)
	}
	s.wantStatus(a.ID, "processing")
	if err := s.b.Yield(s.ctx, a.ID, time.Now().UTC().Add(time.Hour), "w", "signal", "approval"); err != nil {
		t.Fatal(err)
	}
	if err := s.b.SignalActivity(s.ctx, a.ID, uuid.New(), "approval", raw(true)); err != nil {
		t.Fatal(err)
	}
	s.wantStatus(a.ID, "pending")
	s.claim("w2", a)
}

func testSignalUnknown(t *testing.T, h Harness) {
	s := newSuite(t, h)
	wantKind(t, s.b.SignalActivity(s.ctx, uuid.New(), uuid.New(), "approval", raw(1)), storage.ErrNotFound, "signal to unknown activity")
}

// Handler-issued spawns commit only while the spawner still owns its claim,
// for plain and idempotent spawns alike; a lost claim cannot grow the tree.
func testSpawnFence(t *testing.T, h Harness) {
	s := newSuite(t, h)
	spawn := need[storage.SpawnStorage](t, s.b)
	parent := s.enqueueClaimed("old", activity(withType("parent")))
	live := activity(withParent(parent))
	if err := spawn.EnqueueForWorker(s.ctx, live, parent.ID, "old"); err != nil {
		t.Fatal(err)
	}
	keyed := activity(withParent(parent), withKey("step-a", storage.BehaviorReturnExisting))
	if existing, err := spawn.EnqueueIdempotentForWorker(s.ctx, &keyed, parent.ID, "old"); err != nil || existing != nil {
		t.Fatalf("keyed spawn: %v %v", existing, err)
	}
	s.expire(parent.ID)
	s.reap(1)
	s.claim("new", parent, "parent")

	stale := activity(withParent(parent))
	wantKind(t, spawn.EnqueueForWorker(s.ctx, stale, parent.ID, "old"), storage.ErrClaimLost, "stale spawn")
	staleKeyed := activity(withParent(parent), withKey("step-b", storage.BehaviorReturnExisting))
	_, err := spawn.EnqueueIdempotentForWorker(s.ctx, &staleKeyed, parent.ID, "old")
	wantKind(t, err, storage.ErrClaimLost, "stale keyed spawn")
	reattach := activity(withParent(parent), withKey("step-a", storage.BehaviorReturnExisting))
	_, err = spawn.EnqueueIdempotentForWorker(s.ctx, &reattach, parent.ID, "old")
	wantKind(t, err, storage.ErrClaimLost, "stale reattach")
	if existing, err := spawn.EnqueueIdempotentForWorker(s.ctx, &reattach, parent.ID, "new"); err != nil || existing == nil || existing.ExistingID != keyed.ID {
		t.Fatalf("replacement reattach: %v %v", existing, err)
	}
	if _, err := s.b.LookupIdempotencyActivityID(s.ctx, "step-b"); err == nil {
		t.Fatal("stale keyed spawn claimed its key")
	}
	children, err := s.r.GetChildren(s.ctx, parent.ID, 0, 10)
	if err != nil || len(children) != 2 {
		t.Fatalf("children = %d err=%v, want the two live spawns", len(children), err)
	}
	if snap, err := s.r.GetActivity(s.ctx, stale.ID); err != nil || snap != nil {
		t.Fatalf("stale spawn was inserted: %+v %v", snap, err)
	}
}

// WaitForResult blocks until another process publishes the result, returns
// at once when it already exists, and returns the context's error when
// cancelled.
func testWaitForResult(t *testing.T, h Harness) {
	s := newSuite(t, h)
	awaiter := optional[storage.ResultWaiter](t, s.another())
	a := s.enqueueClaimed("w", activity())
	go func() {
		time.Sleep(400 * time.Millisecond)
		if err := s.b.AckSuccess(s.ctx, a.ID, raw(map[string]int{"answer": 42}), "w"); err != nil {
			t.Errorf("ack: %v", err)
		}
	}()
	waitCtx, cancel := context.WithTimeout(s.ctx, wait)
	defer cancel()
	start := time.Now()
	res, err := awaiter.WaitForResult(waitCtx, a.ID)
	if err != nil || res == nil || res.State != storage.ResultOk {
		t.Fatalf("wait: %+v %v", res, err)
	}
	var decoded map[string]int
	if json.Unmarshal(res.Data, &decoded) != nil || decoded["answer"] != 42 {
		t.Fatalf("result data = %s", res.Data)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("wait took %v, want a prompt wake", took)
	}
	if res, err := awaiter.WaitForResult(waitCtx, a.ID); err != nil || res == nil {
		t.Fatalf("wait for stored result: %+v %v", res, err)
	}
	ctx, cancelEarly := context.WithCancel(s.ctx)
	done := make(chan error, 1)
	go func() {
		_, err := awaiter.WaitForResult(ctx, uuid.New())
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)
	cancelEarly()
	if err := receive(t, done, "cancelled wait"); err == nil {
		t.Fatal("cancelled wait returned no error")
	}
}

// A failure outcome resolves waiters too: dead-letter (by ack and by reaper)
// and permanent failure all publish an Err result.
func testWaitForResultFailure(t *testing.T, h Harness) {
	s := newSuite(t, h)
	awaiter := optional[storage.ResultWaiter](t, s.another())
	a := s.enqueueClaimed("w", activity(withMaxRetries(1)))
	go func() {
		time.Sleep(300 * time.Millisecond)
		s.expire(a.ID)
		if _, err := s.b.RequeueExpired(s.ctx, 10); err != nil {
			t.Errorf("reap: %v", err)
		}
	}()
	waitCtx, cancel := context.WithTimeout(s.ctx, wait)
	defer cancel()
	res, err := awaiter.WaitForResult(waitCtx, a.ID)
	if err != nil || res == nil || res.State != storage.ResultErr {
		t.Fatalf("wait: %+v %v", res, err)
	}
}
