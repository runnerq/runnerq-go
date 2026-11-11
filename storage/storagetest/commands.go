package storagetest

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// Commands (storage.CommandStorage, skipped when absent): cancel (fencing a
// running claim, cascading to children, waking awaiting parents), retry and
// redrive, run now, reschedule, set priority, whole-tree delete, signals,
// filter targets, dry runs and the idempotency ledger.
var commandTests = []conformanceTest{
	{"CancelFinishesNonTerminalWork", testCancel},
	{"CancelFencesTheRunningClaim", testCancelFencesClaim},
	{"CancelCascadesAndWakesAwaitingParent", testCancelCascade},
	{"RetryAndRedriveReplayFromCheckpoints", testRetry},
	{"RunNowRescheduleAndPriority", testScheduling},
	{"DeleteRemovesFinishedTrees", testDelete},
	{"SignalDeliversAndWakes", testCommandSignal},
	{"FilterTargetsAreBounded", testFilterTarget},
	{"LedgerReplaysAndRejectsReuse", testLedger},
	{"CancelledTreesAreRetained", testCancelledRetention},
}

func (s *suite) commands() storage.CommandStorage {
	s.t.Helper()
	cs, ok := s.b.(storage.CommandStorage)
	if !ok {
		s.t.Skip("backend does not implement storage.CommandStorage")
	}
	return cs
}

func (s *suite) apply(cmd storage.Command) *storage.CommandResult {
	s.t.Helper()
	res, err := s.commands().ApplyCommand(s.ctx, cmd)
	if err != nil {
		s.t.Fatalf("%s: %v", cmd.Kind, err)
	}
	return res
}

func onIDs(kind storage.CommandKind, ids ...uuid.UUID) storage.Command {
	return storage.Command{Kind: kind, Target: storage.CommandTarget{IDs: ids}}
}

func itemFor(t *testing.T, res *storage.CommandResult, id uuid.UUID) storage.CommandItem {
	t.Helper()
	for _, it := range res.Items {
		if it.ID == id {
			return it
		}
	}
	t.Fatalf("no result item for %s in %+v", id, res.Items)
	return storage.CommandItem{}
}

func wantApplied(t *testing.T, res *storage.CommandResult, id uuid.UUID, status string) {
	t.Helper()
	it := itemFor(t, res, id)
	if it.Outcome != storage.CommandApplied || (status != "" && it.Status != status) {
		t.Fatalf("activity %s: %+v, want applied with status %q", id, it, status)
	}
}

func wantSkipped(t *testing.T, res *storage.CommandResult, id uuid.UUID, kind storage.StorageErrorKind) {
	t.Helper()
	it := itemFor(t, res, id)
	if it.Outcome != storage.CommandSkipped || it.ErrKind != kind || it.ErrMessage == "" {
		t.Fatalf("activity %s: %+v, want skipped with kind %d", id, it, kind)
	}
}

func testCancel(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.commands()
	waiting := s.enqueueClaimed("w", activity())
	if err := s.b.Yield(s.ctx, waiting.ID, time.Now().UTC().Add(time.Hour), "w", "sleep", "nap"); err != nil {
		t.Fatal(err)
	}
	done := s.enqueueClaimed("c", activity())
	if err := s.b.AckSuccess(s.ctx, done.ID, nil, "c"); err != nil {
		t.Fatal(err)
	}
	pending := s.enqueue(activity())
	scheduled := s.enqueue(activity(withScheduledAt(time.Now().UTC().Add(time.Hour))))
	missing := uuid.New()

	res := s.apply(storage.Command{Kind: storage.CommandCancel, Reason: "bad deploy",
		Target: storage.CommandTarget{IDs: []uuid.UUID{pending.ID, scheduled.ID, waiting.ID, done.ID, missing}}})
	if res.Matched != 4 || res.Applied != 3 || len(res.Items) != 5 {
		t.Fatalf("result %+v", res)
	}
	for _, id := range []uuid.UUID{pending.ID, scheduled.ID, waiting.ID} {
		wantApplied(t, res, id, storage.RecordStatusCancelled)
		if st := s.status(id); st != "cancelled" {
			t.Fatalf("activity %s: status %q after cancel", id, st)
		}
		r, err := s.b.GetResult(s.ctx, id)
		if err != nil || r == nil || r.State != storage.ResultErr {
			t.Fatalf("activity %s: no error result after cancel: %+v %v", id, r, err)
		}
	}
	wantSkipped(t, res, done.ID, storage.ErrConflict)
	wantSkipped(t, res, missing, storage.ErrNotFound)
	s.claimNothing() // cancelled work is never claimed
}

func testCancelFencesClaim(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.commands()
	running := s.enqueueClaimed("exec:w1", activity())
	wantApplied(t, s.apply(onIDs(storage.CommandCancel, running.ID)), running.ID, storage.RecordStatusCancelled)

	if lease, ok := s.b.(storage.AttemptLeaseStorage); ok {
		if still, err := lease.ExtendLeaseForWorker(s.ctx, running.ID, "exec:w1", time.Minute); still && err == nil {
			t.Fatal("the cancelled claim can still be renewed")
		}
	}
	if err := s.b.AckSuccess(s.ctx, running.ID, json.RawMessage(`{}`), "exec:w1"); err == nil && s.status(running.ID) != "cancelled" {
		t.Fatal("a late acknowledgement overwrote the cancellation")
	}
	if st := s.status(running.ID); st != "cancelled" {
		t.Fatalf("status %q after late ack", st)
	}
	if _, err := s.b.RequeueExpired(s.ctx, 100); err != nil {
		t.Fatal(err)
	}
	s.claimNothing()
}

func testCancelCascade(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.commands()
	dep, ok := s.b.(storage.DependencyStorage)
	if !ok {
		t.Skip("backend does not implement DependencyStorage")
	}
	parent := s.enqueueClaimed("p", activity())
	child := s.enqueue(activity(withParent(parent)))
	grandchild := s.enqueue(activity(withParent(child)))
	// The parent parks awaiting its child's result.
	if err := dep.YieldForResult(s.ctx, parent.ID, child.ID, &child.ID, time.Now().UTC().Add(time.Hour), "p", "await", "await:child"); err != nil {
		t.Fatal(err)
	}
	if st := s.status(parent.ID); st != "waiting" {
		t.Fatalf("parent status %q before cancel", st)
	}

	res := s.apply(storage.Command{Kind: storage.CommandCancel, CascadeChildren: true, Target: storage.CommandTarget{IDs: []uuid.UUID{child.ID}}})
	if res.Applied != 1 || res.Cascaded != 1 {
		t.Fatalf("result %+v", res)
	}
	if s.status(grandchild.ID) != "cancelled" {
		t.Fatal("cascade did not reach the grandchild")
	}
	if st := s.status(parent.ID); st != "pending" {
		t.Fatalf("awaiting parent was not woken by the cancellation: %q", st)
	}

	// Without cascade, children keep running.
	other := s.enqueue(activity())
	kid := s.enqueue(activity(withParent(other)))
	if res := s.apply(onIDs(storage.CommandCancel, other.ID)); res.Cascaded != 0 {
		t.Fatalf("cascaded without asking: %+v", res)
	}
	if s.status(kid.ID) != "pending" {
		t.Fatal("child cancelled without cascade")
	}
}

func testRetry(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.commands()
	failed := s.enqueueClaimed("f", activity(withMaxRetries(0)))
	if err := s.b.StoreResult(s.ctx, uuid.New(), failed.ID, storage.ActivityResult{Data: json.RawMessage(`1`)}, "run:step-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.b.AckFailure(s.ctx, failed.ID, storage.NewNonRetryableFailure("boom"), "f"); err != nil {
		t.Fatal(err)
	}
	dead := s.enqueueClaimed("d", activity(withMaxRetries(1)))
	if _, err := s.b.AckFailure(s.ctx, dead.ID, storage.NewRetryableFailure("again"), "d"); err != nil {
		t.Fatal(err)
	}
	pending := s.enqueue(activity())

	res := s.apply(storage.Command{Kind: storage.CommandRetry, ResetAttempts: true,
		Target: storage.CommandTarget{IDs: []uuid.UUID{failed.ID, dead.ID, pending.ID}}})
	wantApplied(t, res, failed.ID, storage.RecordStatusPending)
	wantApplied(t, res, dead.ID, storage.RecordStatusPending)
	wantSkipped(t, res, pending.ID, storage.ErrConflict)
	if r, err := s.b.GetResult(s.ctx, failed.ID); err != nil || r != nil {
		t.Fatalf("stale failure result survived the retry: %+v %v", r, err)
	}
	if qs, ok := s.b.(storage.QueryStorage); ok {
		steps, err := qs.ListStepEntries(s.ctx, failed.ID, false, 0, "")
		if err != nil || len(steps.Items) != 1 {
			t.Fatalf("checkpoints must survive a retry: %+v %v", steps, err)
		}
	}
	if sn := s.snapshot(dead.ID); sn.RetryCount != 0 {
		t.Fatalf("reset_attempts left retry count %d", sn.RetryCount)
	}
	s.promote()
	got := map[uuid.UUID]bool{}
	for range 3 {
		if a := s.tryClaim("r"); a != nil {
			got[a.ID] = true
		}
	}
	if !got[failed.ID] || !got[dead.ID] {
		t.Fatalf("retried activities were not claimable: %v", got)
	}
}

func testScheduling(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.commands()
	later := s.enqueue(activity(withScheduledAt(time.Now().UTC().Add(time.Hour))))
	pending := s.enqueue(activity())

	res := s.apply(onIDs(storage.CommandRunNow, later.ID, pending.ID))
	wantApplied(t, res, later.ID, storage.RecordStatusScheduled)
	wantSkipped(t, res, pending.ID, storage.ErrConflict)
	// Both are claimable now; their order depends on timestamps from two
	// clocks (the app's created_at, the database's run-now time).
	claimed := map[uuid.UUID]bool{}
	for range 2 {
		if a := s.tryClaim("x"); a != nil {
			claimed[a.ID] = true
		}
	}
	if !claimed[pending.ID] || !claimed[later.ID] {
		t.Fatalf("claimed %v, want the pending and the run-now activity", claimed)
	}

	moved := s.enqueue(activity(withScheduledAt(time.Now().UTC().Add(time.Minute))))
	s.apply(storage.Command{Kind: storage.CommandReschedule, At: time.Now().UTC().Add(24 * time.Hour), Target: storage.CommandTarget{IDs: []uuid.UUID{moved.ID}}})
	if sn := s.snapshot(moved.ID); sn.ScheduledAt == nil || time.Until(*sn.ScheduledAt) < 23*time.Hour {
		t.Fatalf("reschedule did not move the time: %+v", sn.ScheduledAt)
	}

	a := s.enqueue(activity(withPriority(storage.PriorityLow)))
	s.apply(storage.Command{Kind: storage.CommandSetPriority, Priority: storage.PriorityCritical, Target: storage.CommandTarget{IDs: []uuid.UUID{a.ID}}})
	if sn := s.snapshot(a.ID); sn.Priority != storage.PriorityCritical {
		t.Fatalf("priority %d after set_priority", sn.Priority)
	}
	_, err := s.commands().ApplyCommand(s.ctx, storage.Command{Kind: storage.CommandSetPriority, Priority: 9, Target: storage.CommandTarget{IDs: []uuid.UUID{a.ID}}})
	wantStorageErr(t, "bad priority", err, storage.ErrInvalidArgument, "priority")
}

func testDelete(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.commands()
	root := s.enqueueClaimed("r", activity())
	child := s.enqueue(activity(withParent(root)))
	s.claim("c", child)
	if err := s.b.AckSuccess(s.ctx, child.ID, nil, "c"); err != nil {
		t.Fatal(err)
	}

	res := s.apply(onIDs(storage.CommandDelete, root.ID, child.ID))
	wantSkipped(t, res, root.ID, storage.ErrConflict)  // still running
	wantSkipped(t, res, child.ID, storage.ErrConflict) // not a root

	if err := s.b.AckSuccess(s.ctx, root.ID, nil, "r"); err != nil {
		t.Fatal(err)
	}
	dry := s.apply(storage.Command{Kind: storage.CommandDelete, DryRun: true, Target: storage.CommandTarget{IDs: []uuid.UUID{root.ID}}})
	if itemFor(t, dry, root.ID).Outcome != storage.CommandWouldApply {
		t.Fatalf("dry run %+v", dry)
	}
	if sn, _ := s.r.GetActivity(s.ctx, root.ID); sn == nil {
		t.Fatal("dry run deleted the tree")
	}
	wantApplied(t, s.apply(onIDs(storage.CommandDelete, root.ID)), root.ID, "")
	for _, id := range []uuid.UUID{root.ID, child.ID} {
		if sn, err := s.r.GetActivity(s.ctx, id); err != nil || sn != nil {
			t.Fatalf("activity %s survived delete: %+v %v", id, sn, err)
		}
	}
}

func testCommandSignal(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.commands()
	waiter := activity(withKey("order-7", storage.BehaviorReturnExisting))
	if existing, err := s.b.EnqueueIdempotent(s.ctx, &waiter); err != nil || existing != nil {
		t.Fatalf("enqueue keyed: %+v %v", existing, err)
	}
	s.claim("w", waiter)
	if err := s.b.Yield(s.ctx, waiter.ID, time.Now().UTC().Add(time.Hour), "w", "signal", "approve"); err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"ok":true}`)
	res := s.apply(storage.Command{Kind: storage.CommandSignal, SignalName: "approve", SignalPayload: payload,
		Target: storage.CommandTarget{IdempotencyKey: "order-7"}})
	wantApplied(t, res, waiter.ID, storage.RecordStatusPending)
	r, err := s.b.GetResult(s.ctx, storage.CheckpointID(waiter.ID, "signal", "approve"))
	if err != nil || r == nil || r.State != storage.ResultOk {
		t.Fatalf("signal payload not stored: %+v %v", r, err)
	}
	var got map[string]bool
	if err := json.Unmarshal(r.Data, &got); err != nil || !got["ok"] {
		t.Fatalf("signal payload %s", r.Data)
	}
	none := s.apply(storage.Command{Kind: storage.CommandSignal, SignalName: "approve", Target: storage.CommandTarget{IdempotencyKey: "nobody"}})
	if none.Matched != 0 {
		t.Fatalf("unknown key matched: %+v", none)
	}
}

func testFilterTarget(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.commands()
	for range 5 {
		s.enqueue(activity(withType("batch")))
	}
	keep := s.enqueue(activity(withType("keep")))
	filter := &storage.QueryFilter{Field: "type", Op: storage.OpEq, Value: "batch"}

	res := s.apply(storage.Command{Kind: storage.CommandCancel, Target: storage.CommandTarget{Filter: filter, Max: 3}})
	if res.Matched != 3 || res.Applied != 3 || !res.More {
		t.Fatalf("first batch %+v", res)
	}
	// Filter targets select only what the command can act on, so repeating
	// it works through the rest.
	res = s.apply(storage.Command{Kind: storage.CommandCancel, Target: storage.CommandTarget{Filter: filter, Max: 3}})
	if res.Matched != 2 || res.Applied != 2 || res.More {
		t.Fatalf("second batch %+v", res)
	}
	if s.status(keep.ID) != "pending" {
		t.Fatal("filter reached an activity it does not match")
	}
	_, err := s.commands().ApplyCommand(s.ctx, storage.Command{Kind: storage.CommandCancel, Target: storage.CommandTarget{Filter: filter}})
	wantStorageErr(t, "filter without max", err, storage.ErrInvalidArgument, "target.max")
}

func testLedger(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.commands()
	a := s.enqueue(activity())
	cmd := storage.Command{ID: "cmd-" + uuid.NewString(), Fingerprint: "fp-1", Kind: storage.CommandSetPriority,
		Priority: storage.PriorityHigh, Target: storage.CommandTarget{IDs: []uuid.UUID{a.ID}}}

	dry := cmd
	dry.DryRun = true
	if res := s.apply(dry); res.Replayed || itemFor(t, res, a.ID).Outcome != storage.CommandWouldApply {
		t.Fatalf("dry run %+v", res)
	}
	if s.snapshot(a.ID).Priority != storage.PriorityNormal {
		t.Fatal("dry run changed state")
	}

	first := s.apply(cmd)
	if first.Replayed || first.Applied != 1 {
		t.Fatalf("first delivery %+v", first)
	}
	// An operator changes the priority again; replaying the old command must
	// not undo it.
	s.apply(storage.Command{Kind: storage.CommandSetPriority, Priority: storage.PriorityLow, Target: storage.CommandTarget{IDs: []uuid.UUID{a.ID}}})
	again := s.apply(cmd)
	if !again.Replayed || again.Applied != 1 || len(again.Items) != 1 || again.Items[0].ID != a.ID {
		t.Fatalf("replay %+v", again)
	}
	if s.snapshot(a.ID).Priority != storage.PriorityLow {
		t.Fatal("a replayed command was applied again")
	}

	reused := cmd
	reused.Fingerprint = "fp-2"
	_, err := s.commands().ApplyCommand(s.ctx, reused)
	var se *storage.StorageError
	if !errors.As(err, &se) || se.Kind != storage.ErrConflict {
		t.Fatalf("reused command id: got %v, want a conflict", err)
	}
}

func testCancelledRetention(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.commands()
	root := s.enqueue(activity())
	s.enqueue(activity(withParent(root)))
	if res := s.apply(storage.Command{Kind: storage.CommandCancel, CascadeChildren: true, Target: storage.CommandTarget{IDs: []uuid.UUID{root.ID}}}); res.Cascaded != 1 {
		t.Fatalf("cancel %+v", res)
	}
	time.Sleep(1100 * time.Millisecond)
	n, err := s.b.CleanupExpired(s.ctx, storage.RetentionPolicy{Failed: time.Second}, 10)
	if err != nil || n != 1 {
		t.Fatalf("retention swept %d cancelled trees (%v), want 1", n, err)
	}
	if sn, _ := s.r.GetActivity(s.ctx, root.ID); sn != nil {
		t.Fatal("cancelled tree survived retention")
	}
}
