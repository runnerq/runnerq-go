package storagetest

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// Version pinning (runnerq-spec versions.md): claims filtered by version,
// children taking their root's version, versions surviving parks, retries
// and lease recovery, the repin command and versioned schedules.
var versionTests = []conformanceTest{
	{"ClaimsServeOnlyTheirVersions", testVersionedClaim},
	{"ChildrenTakeTheRootsVersion", testChildVersion},
	{"ParkedWorkWakesOnlyForItsVersion", testVersionedWake},
	{"VersionSurvivesRetriesAndRecovery", testVersionSurvives},
	{"RepinMovesWorkThroughTheLedger", testRepin},
	{"ScheduleFiresCarryTheirVersion", testScheduleVersion},
}

func withVersion(v string) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) { a.CodeVersion = v }
}

// claimAs makes one non-blocking claim as a worker serving versions; nil
// when nothing is claimable.
func (s *suite) claimAs(worker string, versions []string, types ...string) *storage.QueuedActivity {
	s.t.Helper()
	s.promote()
	got, err := s.b.Dequeue(s.ctx, worker, 0, types, versions)
	if err != nil {
		s.t.Fatalf("dequeue as %s serving %v: %v", worker, versions, err)
	}
	return got
}

func (s *suite) wantClaimAs(worker string, versions []string, want storage.QueuedActivity, types ...string) storage.QueuedActivity {
	s.t.Helper()
	got := s.claimAs(worker, versions, types...)
	if got == nil || got.ID != want.ID {
		s.t.Fatalf("dequeue as %s serving %v: got %v, want %s", worker, versions, got, want.ID)
	}
	return *got
}

func (s *suite) wantNothingFor(versions []string, types ...string) {
	s.t.Helper()
	if got := s.claimAs("nobody", versions, types...); got != nil {
		s.t.Fatalf("serving %v claimed %s (version %q), want nothing", versions, got.ID, got.CodeVersion)
	}
}

func (s *suite) wantVersion(id uuid.UUID, want string) {
	s.t.Helper()
	got := s.snapshot(id).CodeVersion
	if (want == "") != (got == nil) || (got != nil && *got != want) {
		s.t.Fatalf("activity %s code_version = %v, want %q", id, got, want)
	}
}

var (
	servesA = []string{"A"}
	servesB = []string{"B"}
)

// A worker serving versions claims unstamped work and work pinned to one of
// them, in the usual order, singly and in batches; no versions claims all.
func testVersionedClaim(t *testing.T, h Harness) {
	s := newSuite(t, h)
	base := time.Now().UTC().Add(-time.Minute)
	a1 := s.enqueue(activity(withVersion("A"), withCreatedAt(base)))
	b1 := s.enqueue(activity(withVersion("B"), withCreatedAt(base.Add(time.Second))))
	u := s.enqueue(activity(withCreatedAt(base.Add(2 * time.Second))))
	a2 := s.enqueue(activity(withVersion("A"), withCreatedAt(base.Add(3*time.Second))))
	s.wantVersion(a1.ID, "A")
	s.wantVersion(u.ID, "")

	if got := s.wantClaimAs("w1", servesA, a1); got.CodeVersion != "A" {
		t.Fatalf("claim returned CodeVersion %q, want A", got.CodeVersion)
	}
	if bq, ok := s.b.(storage.BatchQueueStorage); ok {
		claims, err := bq.DequeueBatch(s.ctx, "batch", 10, 0, nil, []string{"A", "C"})
		if err != nil {
			t.Fatal(err)
		}
		if len(claims) != 2 || claims[0].Activity.ID != u.ID || claims[1].Activity.ID != a2.ID {
			t.Fatalf("batch serving A and C claimed %v, want %s then %s", claims, u.ID, a2.ID)
		}
	} else {
		s.wantClaimAs("w2", servesA, u)
		s.wantClaimAs("w3", servesA, a2)
	}
	s.wantNothingFor(servesA)
	s.wantClaimAs("w4", servesB, b1)

	x := s.enqueue(activity(withVersion("X")))
	s.claim("unversioned", x)
}

// A child takes its root's version at insert, whatever the spawner passed,
// down the tree; a child of an unstamped root is unstamped.
func testChildVersion(t *testing.T, h Harness) {
	s := newSuite(t, h)
	root := s.enqueue(activity(withType("root"), withVersion("A")))
	s.wantClaimAs("root", servesA, root, "root")
	child := s.enqueue(activity(withType("child"), withParent(root), withVersion("B")))
	s.wantVersion(child.ID, "A")
	s.wantNothingFor(servesB, "child")
	if got := s.wantClaimAs("child", servesA, child, "child"); got.CodeVersion != "A" {
		t.Fatalf("child claimed with CodeVersion %q, want A", got.CodeVersion)
	}
	spawned := activity(withType("grandchild"), withParent(child), withVersion("C"))
	if err := need[storage.SpawnStorage](t, s.b).EnqueueForWorker(s.ctx, spawned, child.ID, "child"); err != nil {
		t.Fatal(err)
	}
	s.wantVersion(spawned.ID, "A")

	plain := s.enqueue(activity(withType("root")))
	s.claim("plain", plain, "root")
	kid := s.enqueue(activity(withType("child"), withParent(plain), withVersion("B")))
	s.wantVersion(kid.ID, "")
}

// Parked work wakes into the claim, so only a worker serving its version
// takes it again: after a sleep, a signal and a child's result.
func testVersionedWake(t *testing.T, h Harness) {
	s := newSuite(t, h)
	sleeper := s.enqueue(activity(withVersion("A")))
	s.wantClaimAs("w", servesA, sleeper)
	if err := s.b.Yield(s.ctx, sleeper.ID, time.Now().UTC().Add(-time.Second), "w", "sleep", "nap"); err != nil {
		t.Fatal(err)
	}
	s.wantNothingFor(servesB)
	s.wantClaimAs("w2", servesA, sleeper)

	waiter := s.enqueue(activity(withType("waiter"), withVersion("A")))
	s.wantClaimAs("w", servesA, waiter, "waiter")
	if err := s.b.Yield(s.ctx, waiter.ID, time.Now().UTC().Add(time.Hour), "w", "signal", "go"); err != nil {
		t.Fatal(err)
	}
	if err := s.b.SignalActivity(s.ctx, waiter.ID, storage.CheckpointID(waiter.ID, "signal", "go"), "go", json.RawMessage(`true`)); err != nil {
		t.Fatal(err)
	}
	s.wantStatus(waiter.ID, "pending")
	s.wantNothingFor(servesB, "waiter")
	s.wantClaimAs("w2", servesA, waiter, "waiter")

	parent := s.enqueue(activity(withType("parent"), withVersion("A")))
	s.wantClaimAs("p", servesA, parent, "parent")
	child := s.enqueue(activity(withType("child"), withParent(parent)))
	if err := need[storage.DependencyStorage](t, s.b).YieldForResult(s.ctx, parent.ID, child.ID, &child.ID, time.Now().UTC().Add(time.Hour), "p", "await", "child"); err != nil {
		t.Fatal(err)
	}
	s.wantClaimAs("c", servesA, child, "child")
	if err := s.b.AckSuccess(s.ctx, child.ID, nil, "c"); err != nil {
		t.Fatal(err)
	}
	s.wantStatus(parent.ID, "pending")
	s.wantNothingFor(servesB, "parent")
	s.wantClaimAs("p2", servesA, parent, "parent")
}

// A retryable failure, lease recovery and a retry command keep the version.
func testVersionSurvives(t *testing.T, h Harness) {
	s := newSuite(t, h)
	a := s.enqueue(activity(withVersion("A"), withMaxRetries(5), withRetryDelay(0, 0)))
	s.wantClaimAs("w1", servesA, a)
	if _, err := s.b.AckFailure(s.ctx, a.ID, storage.NewRetryableFailure("again"), "w1"); err != nil {
		t.Fatal(err)
	}
	s.wantNothingFor(servesB)
	s.wantClaimAs("w2", servesA, a)
	s.expire(a.ID)
	if _, err := s.b.RequeueExpired(s.ctx, 10); err != nil {
		t.Fatal(err)
	}
	s.wantVersion(a.ID, "A")
	s.wantNothingFor(servesB)
	s.wantClaimAs("w3", servesA, a)
	if _, err := s.b.AckFailure(s.ctx, a.ID, storage.NewNonRetryableFailure("no"), "w3"); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.b.(storage.CommandStorage); ok {
		wantApplied(t, s.apply(onIDs(storage.CommandRetry, a.ID)), a.ID, storage.RecordStatusPending)
		s.wantVersion(a.ID, "A")
		s.wantNothingFor(servesB)
	}
}

// Repin moves unfinished work to a version: it records Repinned, replays by
// command id, skips finished work and work already there, and a filter on
// version selects only what it can move.
func testRepin(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.commands()
	base := time.Now().UTC().Add(-time.Minute)
	a := s.enqueue(activity(withVersion("A"), withCreatedAt(base)))
	b := s.enqueue(activity(withVersion("A"), withCreatedAt(base.Add(time.Second))))
	done := s.enqueueClaimed("w", activity(withType("done"), withVersion("A")))
	if err := s.b.AckSuccess(s.ctx, done.ID, nil, "w"); err != nil {
		t.Fatal(err)
	}

	repin := onIDs(storage.CommandRepin, a.ID, done.ID)
	repin.ID, repin.Version = "repin-1", "B"
	repin.Fingerprint = storage.CommandFingerprint(repin)
	res := s.apply(repin)
	wantApplied(t, res, a.ID, storage.RecordStatusPending)
	wantSkipped(t, res, done.ID, storage.ErrConflict)
	s.wantVersion(a.ID, "B")
	s.wantVersion(done.ID, "A")
	evs := s.events(a.ID)
	if len(evs) != 1 || evs[0].EventType != storage.EventRepinned {
		t.Fatalf("events after repin: %+v, want one Repinned", evs)
	}
	var detail struct {
		From *string `json:"from"`
		To   string  `json:"to"`
	}
	if err := json.Unmarshal(evs[0].Detail, &detail); err != nil || detail.From == nil || *detail.From != "A" || detail.To != "B" {
		t.Fatalf("Repinned detail %s, want from A to B", evs[0].Detail)
	}
	if again := s.apply(repin); !again.Replayed || again.Applied != 1 {
		t.Fatalf("repeated repin: %+v, want the recorded result replayed", again)
	}
	same := onIDs(storage.CommandRepin, a.ID)
	same.Version = "B"
	wantSkipped(t, s.apply(same), a.ID, storage.ErrConflict)

	byVersion := storage.Command{Kind: storage.CommandRepin, Version: "B",
		Target: storage.CommandTarget{Filter: &storage.QueryFilter{Field: "version", Op: storage.OpEq, Value: "A"}, Max: 10}}
	res = s.apply(byVersion)
	if res.Applied != 1 || len(res.Items) != 1 || res.Items[0].ID != b.ID {
		t.Fatalf("repin by version: %+v, want only %s", res, b.ID)
	}
	s.wantNothingFor(servesA)
	s.wantClaimAs("w1", servesB, a)
	s.wantClaimAs("w2", servesB, b)

	_, err := s.commands().ApplyCommand(s.ctx, onIDs(storage.CommandRepin, a.ID))
	wantKind(t, err, storage.ErrInvalidArgument, "repin without a version")
}

// A schedule's CodeVersion pins what it fires and triggers.
func testScheduleVersion(t *testing.T, h Harness) {
	s := newSuite(t, h)
	ss := s.schedules()
	pinned := schedule("pinned", "@hourly")
	pinned.Options.CodeVersion = "A"
	if err := ss.UpsertSchedule(s.ctx, pinned); err != nil {
		t.Fatal(err)
	}
	if got := s.getSchedule("pinned"); got.Options.CodeVersion != "A" {
		t.Fatalf("stored CodeVersion %q, want A", got.Options.CodeVersion)
	}
	id, err := ss.TriggerSchedule(s.ctx, "pinned")
	if err != nil {
		t.Fatal(err)
	}
	s.wantVersion(id, "A")
	if err := ss.UpsertSchedule(s.ctx, schedule("plain", "@hourly")); err != nil {
		t.Fatal(err)
	}
	plain, err := ss.TriggerSchedule(s.ctx, "plain")
	if err != nil {
		t.Fatal(err)
	}
	s.wantVersion(plain, "")
}
