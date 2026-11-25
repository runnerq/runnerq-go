package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/runnerq/runnerq-go/storage"
)

func TestDatabaseErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		err   error
		retry bool
	}{
		{io.EOF, true}, {context.DeadlineExceeded, true}, {&pgconn.PgError{Code: "40001"}, true}, {&pgconn.PgError{Code: "40P01"}, true}, {&pgconn.PgError{Code: "08006"}, true}, {&pgconn.PgError{Code: "57P01"}, true},
		{&pgconn.PgError{Code: "22P02"}, false}, {&pgconn.PgError{Code: "23505"}, false}, {&pgconn.PgError{Code: "42601"}, false},
	} {
		t.Run(fmt.Sprint(tc.err), func(t *testing.T) {
			err := databaseError(fmt.Errorf("wrapped: %w", tc.err), "operation")
			se, ok := storage.IsStorageError(err)
			if !ok || se.IsRetryable() != tc.retry || !errors.Is(err, tc.err) {
				t.Fatalf("classified %v as %v", tc.err, err)
			}
		})
	}
}
func TestRejectPoolWithoutQueryCapacity(t *testing.T) {
	for _, size := range []int32{0, 1} {
		if _, err := WithConfig(context.Background(), "", "test", 30000, size); err == nil {
			t.Fatalf("accepted pool size %d", size)
		}
	}
}
func claimTestActivity(t *testing.T, b *PostgresBackend, a storage.QueuedActivity, worker string) {
	t.Helper()
	if err := b.Enqueue(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	got, err := b.Dequeue(context.Background(), worker, 0, []string{a.ActivityType}, nil)
	if err != nil || got == nil || got.ID != a.ID {
		t.Fatalf("claim: %v %v", got, err)
	}
}
func TestCompletionReconcilesDuplicateAndRejectsConflict(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	a := testActivity(3)
	claimTestActivity(t, b, a, "attempt1")
	for range 2 {
		if err := b.AckSuccess(ctx, a.ID, json.RawMessage(`{"a":1,"b":2}`), "attempt1"); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.AckSuccess(ctx, a.ID, json.RawMessage(`{ "b":2, "a":1 }`), "attempt1"); err != nil {
		t.Fatalf("semantic JSON duplicate: %v", err)
	}
	for _, tc := range []struct{ w, r string }{{"attempt1", `false`}, {"stale", `{"a":1,"b":2}`}} {
		if err := b.AckSuccess(ctx, a.ID, json.RawMessage(tc.r), tc.w); err == nil {
			t.Fatal("conflicting completion accepted")
		}
	}
}

func TestChildCompletionWakesOnlyItsActiveWaiters(t *testing.T) {
	newChild := func(t *testing.T, b *PostgresBackend, parent storage.QueuedActivity, name string) storage.QueuedActivity {
		t.Helper()
		child := testActivity(3)
		child.ActivityType = name
		child.ParentActivityID = &parent.ID
		child.RootActivityID = parent.ID
		child.Depth = 1
		claimTestActivity(t, b, child, name)
		return child
	}
	t.Run("parent sleeping", func(t *testing.T) {
		b := testBackend(t)
		ctx := context.Background()
		parent := testActivity(3)
		parent.ActivityType = "parent"
		claimTestActivity(t, b, parent, "parent")
		child := newChild(t, b, parent, "child")
		if err := b.Yield(ctx, parent.ID, time.Now().Add(time.Hour), "parent", "sleep", "later"); err != nil {
			t.Fatal(err)
		}
		if err := b.AckSuccess(ctx, child.ID, nil, "child"); err != nil {
			t.Fatal(err)
		}
		if status, _ := activityStatus(t, b, parent.ID); status != "waiting" {
			t.Fatalf("unawaited child woke sleeping parent: %s", status)
		}
	})
	t.Run("different child", func(t *testing.T) {
		b := testBackend(t)
		ctx := context.Background()
		parent := testActivity(3)
		parent.ActivityType = "parent"
		claimTestActivity(t, b, parent, "parent")
		awaited := newChild(t, b, parent, "awaited")
		other := newChild(t, b, parent, "other")
		if err := b.YieldForResult(ctx, parent.ID, awaited.ID, &awaited.ID, time.Now().Add(time.Hour), "parent", "await", "awaited"); err != nil {
			t.Fatal(err)
		}
		if err := b.AckSuccess(ctx, other.ID, nil, "other"); err != nil {
			t.Fatal(err)
		}
		if status, _ := activityStatus(t, b, parent.ID); status != "waiting" {
			t.Fatalf("unawaited sibling woke parent: %s", status)
		}
		if err := b.AckSuccess(ctx, awaited.ID, nil, "awaited"); err != nil {
			t.Fatal(err)
		}
		if status, _ := activityStatus(t, b, parent.ID); status != "pending" {
			t.Fatalf("awaited child did not wake parent: %s", status)
		}
	})
}

func TestRetentionPinsSharedProducerUntilConsumerTreeFinishes(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	producer := plantTree(t, b, "completed", "completed", 2*time.Hour)
	consumer := plantTree(t, b, "processing", "completed", 2*time.Hour)
	// The consumer activity itself is terminal; its live root can replay.
	if _, err := b.pool.Exec(ctx, `INSERT INTO runnerq_dependencies(queue_name,waiter_activity_id,result_id,producer_activity_id) VALUES($1,$2,$3,$3)`, b.queueName, consumer.child, producer.child); err != nil {
		t.Fatal(err)
	}
	policy := storage.RetentionPolicy{Completed: time.Hour}
	if n, err := b.CleanupExpired(ctx, policy, 100); err != nil || n != 0 {
		t.Fatalf("deleted pinned tree: %d %v", n, err)
	}
	assertTreePresent(t, b, producer, "shared producer")
	if _, err := b.pool.Exec(ctx, `UPDATE runnerq_activities SET status='completed',completed_at=NOW()-INTERVAL '2 hours' WHERE id=$1`, consumer.root); err != nil {
		t.Fatal(err)
	}
	if n, err := b.CleanupExpired(ctx, policy, 100); err != nil || n != 2 {
		t.Fatalf("cleanup terminal trees: %d %v", n, err)
	}
	var refs int
	if err := b.pool.QueryRow(ctx, `SELECT count(*) FROM runnerq_dependencies WHERE queue_name=$1`, b.queueName).Scan(&refs); err != nil || refs != 0 {
		t.Fatalf("orphan refs=%d err=%v", refs, err)
	}
}
func TestReusedChildReferenceIsCommittedBeforeReturn(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	parent := testActivity(3)
	claimTestActivity(t, b, parent, "parent")
	child := testActivity(3)
	child.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: "shared", Behavior: storage.BehaviorReturnExisting}
	if _, err := b.EnqueueIdempotent(ctx, &child); err != nil {
		t.Fatal(err)
	}
	other := testActivity(3)
	other.ParentActivityID = &parent.ID
	other.IdempotencyKey = child.IdempotencyKey
	got, err := b.EnqueueIdempotent(ctx, &other)
	if err != nil || got == nil || got.ExistingID != child.ID {
		t.Fatalf("reuse %v %v", got, err)
	}
	var refs int
	if err := b.pool.QueryRow(ctx, `SELECT count(*) FROM runnerq_dependencies WHERE queue_name=$1 AND waiter_activity_id=$2 AND producer_activity_id=$3`, b.queueName, parent.ID, child.ID).Scan(&refs); err != nil || refs != 1 {
		t.Fatalf("reference was not committed: %d %v", refs, err)
	}
	if err := b.RegisterDependency(ctx, parent.ID, uuid.New(), "parent"); err == nil {
		t.Fatal("nonexistent future accepted")
	}
}

func TestBusinessKeyEncodingPreservesLegacyWithoutAliasing(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	old := testActivity(3)
	old.ActivityType = "c"
	old.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: "a-b-c", Behavior: storage.BehaviorReturnExisting}
	if _, err := b.EnqueueIdempotent(ctx, &old); err != nil {
		t.Fatal(err)
	}
	replay := testActivity(3)
	replay.ActivityType = "c"
	replay.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: storage.BusinessIdempotencyKey("a-b", "c"), Behavior: storage.BehaviorReturnExisting}
	existing, err := b.EnqueueIdempotent(ctx, &replay)
	if err != nil || existing == nil || existing.ExistingID != old.ID {
		t.Fatalf("legacy replay %v %v", existing, err)
	}
	distinct := testActivity(3)
	distinct.ActivityType = "b-c"
	distinct.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: storage.BusinessIdempotencyKey("a", "b-c"), Behavior: storage.BehaviorReturnExisting}
	existing, err = b.EnqueueIdempotent(ctx, &distinct)
	if err != nil || existing != nil {
		t.Fatalf("aliased distinct pair %v %v", existing, err)
	}
	for _, a := range []storage.QueuedActivity{replay, distinct} {
		id, err := b.LookupIdempotencyActivityID(ctx, a.IdempotencyKey.Key)
		want := distinct.ID
		if a.ActivityType == "c" {
			want = old.ID
		}
		if err != nil || id != want {
			t.Fatalf("signal lookup id=%s want=%s err=%v", id, want, err)
		}
	}
}

func TestRetentionSkipsReferenceRegistrationLock(t *testing.T) {
	b := testBackend(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	producer := plantTree(t, b, "completed", "completed", 2*time.Hour)
	consumer := testActivity(3)
	claimTestActivity(t, b, consumer, "consumer")
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if locked, err := b.lockProducerRootTx(ctx, tx, producer.child); err != nil || !locked {
		t.Fatalf("lock producer root: locked=%v err=%v", locked, err)
	}
	if err := b.addDependencyTx(ctx, tx, consumer.ID, producer.child, &producer.child); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		n, err := b.CleanupExpired(ctx, storage.RetentionPolicy{Completed: time.Hour}, 100)
		if err == nil && n != 0 {
			err = fmt.Errorf("deleted %d pinned trees", n)
		}
		done <- err
	}()
	// SKIP LOCKED may let cleanup return immediately; either way it must not
	// delete the producer while the registration transaction owns its root row.
	finished := false
	select {
	case err := <-done:
		finished = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(40 * time.Millisecond):
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if !finished {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if n, err := b.CleanupExpired(ctx, storage.RetentionPolicy{Completed: time.Hour}, 100); err != nil || n != 0 {
		t.Fatalf("cleanup crossed committed reference: n=%d err=%v", n, err)
	}
	assertTreePresent(t, b, producer, "producer pinned by concurrent registration")
}

// pinnedTx opens a transaction the test drives by hand to stand in for one
// side of the park/publish ordering. All of its statements are issued before
// the other side is started, and it then only commits, so it never requests a
// new lock while another session (a concurrent test package running schema
// DDL on the shared tables) may be queued behind it.
func pinnedTx(t *testing.T, b *PostgresBackend) (tx pgx.Tx, commit func()) {
	t.Helper()
	ctx := context.Background()
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(ctx) })
	return tx, func() {
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

// retryConflict runs op the way the engine's persistence retry does: a
// storage conflict (deadlock / serialization failure) is transient and is
// simply re-attempted. Concurrent test packages run schema DDL on the shared
// tables whenever they construct a backend, and a call queued behind that
// DDL can be picked as the deadlock victim.
func retryConflict(op func() error) error {
	var err error
	for range 5 {
		if err = op(); err == nil {
			return nil
		}
		if se, ok := storage.IsStorageError(err); !ok || se.Kind != storage.ErrConflict {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return err
}

// mustStayBlocked starts op and fails the test if it returns before the
// pinned side commits; it then commits and returns op's result.
func mustStayBlocked(t *testing.T, commit func(), op func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- op() }()
	select {
	case err := <-done:
		t.Fatalf("returned while the owner row was still held by the other side: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	commit()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("still blocked after the other side committed")
		return nil
	}
}

// The park/publish ordering rests on the owner's row lock, not on timing.
// Each subtest pins one side mid-transaction by hand, proves the other side
// waits on the row, and checks that once the pinned side commits the waiting
// side observes it: the waiter always ends up runnable.
func TestParkAndPublishOrderOnOwnerRowLock(t *testing.T) {
	ctx := context.Background()
	newPair := func(t *testing.T) (*PostgresBackend, storage.QueuedActivity, storage.QueuedActivity) {
		b := testBackend(t)
		producer := testActivity(1)
		producer.ActivityType = "producer"
		claimTestActivity(t, b, producer, "p")
		waiter := testActivity(3)
		waiter.ActivityType = "waiter"
		claimTestActivity(t, b, waiter, "w")
		return b, producer, waiter
	}

	t.Run("consumer commits first, publisher waits", func(t *testing.T) {
		b, producer, waiter := newPair(t)
		// Hand-driven park: owner FOR SHARE, dependency, active result wait.
		tx, commit := pinnedTx(t, b)
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_activities WHERE id = $1 AND queue_name = $2 FOR SHARE`, producer.ID, b.queueName).Scan(&one); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO runnerq_dependencies (queue_name, waiter_activity_id, result_id, producer_activity_id) VALUES ($1, $2, $3, $3)`, b.queueName, waiter.ID, producer.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE runnerq_activities SET status = 'waiting', waiting_result_id = $3, current_worker_id = NULL, lease_deadline_ms = NULL WHERE id = $1 AND queue_name = $2`, waiter.ID, b.queueName, producer.ID); err != nil {
			t.Fatal(err)
		}
		err := mustStayBlocked(t, commit, func() error {
			return retryConflict(func() error { return b.AckSuccess(ctx, producer.ID, nil, "p") })
		})
		if err != nil {
			t.Fatal(err)
		}
		if status, _ := activityStatus(t, b, waiter.ID); status != "pending" {
			t.Fatalf("waiter status=%s, want pending (publisher must see the committed dependency)", status)
		}
	})

	t.Run("publisher commits first, consumer waits", func(t *testing.T) {
		b, producer, waiter := newPair(t)
		// Hand-driven publication: owner exclusively locked, result stored.
		tx, commit := pinnedTx(t, b)
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_activities WHERE id = $1 AND queue_name = $2 FOR NO KEY UPDATE`, producer.ID, b.queueName).Scan(&one); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO runnerq_results (activity_id, queue_name, state, created_at, owner_activity_id) VALUES ($1, $2, 'Ok', NOW(), $1)`, producer.ID, b.queueName); err != nil {
			t.Fatal(err)
		}
		err := mustStayBlocked(t, commit, func() error {
			return retryConflict(func() error {
				return b.YieldForResult(ctx, waiter.ID, producer.ID, &producer.ID, time.Now().Add(time.Hour), "w", "await", "child")
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		if status, _ := activityStatus(t, b, waiter.ID); status != "pending" {
			t.Fatalf("waiter status=%s, want pending (park must see the committed result)", status)
		}
	})
}

// A signal's owner is its target, so SignalActivity and a target parking for
// that signal order on the target's own row.
func TestSignalAndParkOrderOnTargetRow(t *testing.T) {
	ctx := context.Background()

	t.Run("park commits first, signal waits", func(t *testing.T) {
		b := testBackend(t)
		sigID := uuid.New()
		target := testActivity(3)
		claimTestActivity(t, b, target, "w")
		tx, commit := pinnedTx(t, b)
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_activities WHERE id = $1 AND queue_name = $2 FOR UPDATE`, target.ID, b.queueName).Scan(&one); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO runnerq_dependencies (queue_name, waiter_activity_id, result_id) VALUES ($1, $2, $3)`, b.queueName, target.ID, sigID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE runnerq_activities SET status = 'waiting', current_worker_id = NULL, lease_deadline_ms = NULL WHERE id = $1 AND queue_name = $2`, target.ID, b.queueName); err != nil {
			t.Fatal(err)
		}
		err := mustStayBlocked(t, commit, func() error {
			return retryConflict(func() error { return b.SignalActivity(ctx, target.ID, sigID, "approve", json.RawMessage(`true`)) })
		})
		if err != nil {
			t.Fatal(err)
		}
		if status, _ := activityStatus(t, b, target.ID); status != "pending" {
			t.Fatalf("target status=%s, want pending (signal must see the committed park)", status)
		}
	})

	t.Run("signal commits first, park waits", func(t *testing.T) {
		b := testBackend(t)
		sigID := uuid.New()
		target := testActivity(3)
		claimTestActivity(t, b, target, "w")
		tx, commit := pinnedTx(t, b)
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_activities WHERE id = $1 AND queue_name = $2 FOR NO KEY UPDATE`, target.ID, b.queueName).Scan(&one); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO runnerq_results (activity_id, queue_name, state, created_at, owner_activity_id, step) VALUES ($1, $2, 'Ok', NOW(), $3, 'signal:approve')`, sigID, b.queueName, target.ID); err != nil {
			t.Fatal(err)
		}
		err := mustStayBlocked(t, commit, func() error {
			return retryConflict(func() error {
				return b.YieldForResult(ctx, target.ID, sigID, nil, time.Now().Add(time.Hour), "w", "signal", "approve")
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		if status, _ := activityStatus(t, b, target.ID); status != "pending" {
			t.Fatalf("target status=%s, want pending (park must see the committed signal)", status)
		}
	})
}

// Key reuse (ReturnExisting) registers a dependency on an existing child that
// may be a retention candidate at that very moment. The child's idempotency
// row is the ordering point: cleanup locks it before its dependency check,
// reuse holds it while linking. Either order leaves no future pointing at a
// deleted activity.
func TestCleanupAndKeyReuseOrderOnIdempotencyRow(t *testing.T) {
	ctx := context.Background()
	policy := storage.RetentionPolicy{Completed: time.Hour}

	// An expired, completed child owning a ReturnExisting key, plus a live
	// parent that will reuse it. Returns the stored (encoded) key string.
	plant := func(t *testing.T) (b *PostgresBackend, parent, child storage.QueuedActivity, storedKey string) {
		b = testBackend(t)
		parent = testActivity(3)
		parent.ActivityType = "parent"
		claimTestActivity(t, b, parent, "parent")
		child = testActivity(3)
		child.ActivityType = "child"
		child.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: "shared", Behavior: storage.BehaviorReturnExisting}
		if got, err := b.EnqueueIdempotent(ctx, &child); err != nil || got != nil {
			t.Fatalf("first claim: %v %v", got, err)
		}
		if c, err := b.Dequeue(ctx, "c", 0, []string{"child"}, nil); err != nil || c == nil {
			t.Fatalf("claim child: %v %v", c, err)
		}
		if err := b.AckSuccess(ctx, child.ID, json.RawMessage(`42`), "c"); err != nil {
			t.Fatal(err)
		}
		if _, err := b.pool.Exec(ctx, `UPDATE runnerq_activities SET completed_at = NOW() - INTERVAL '2 hours' WHERE id = $1`, child.ID); err != nil {
			t.Fatal(err)
		}
		if err := b.pool.QueryRow(ctx, `SELECT idempotency_key FROM runnerq_idempotency WHERE queue_name = $1 AND activity_id = $2`, b.queueName, child.ID).Scan(&storedKey); err != nil {
			t.Fatal(err)
		}
		return b, parent, child, storedKey
	}
	childExists := func(t *testing.T, b *PostgresBackend, id uuid.UUID) bool {
		t.Helper()
		var n int
		if err := b.pool.QueryRow(ctx, `SELECT count(*) FROM runnerq_activities WHERE id = $1`, id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n > 0
	}

	t.Run("reuse commits first, cleanup waits and keeps the tree", func(t *testing.T) {
		b, parent, child, storedKey := plant(t)
		// Hand-driven reuse mid-flight: key row held, dependency written.
		tx, commit := pinnedTx(t, b)
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_idempotency WHERE queue_name = $1 AND idempotency_key = $2 FOR UPDATE`, b.queueName, storedKey).Scan(&one); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO runnerq_dependencies (queue_name, waiter_activity_id, result_id, producer_activity_id) VALUES ($1, $2, $3, $3)`, b.queueName, parent.ID, child.ID); err != nil {
			t.Fatal(err)
		}
		var swept uint64
		err := mustStayBlocked(t, commit, func() error {
			return retryConflict(func() error {
				var err error
				swept, err = b.CleanupExpired(ctx, policy, 100)
				return err
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		if swept != 0 || !childExists(t, b, child.ID) {
			t.Fatalf("cleanup swept %d trees and child present=%v; a tree the reuse just pinned must survive", swept, childExists(t, b, child.ID))
		}
		if res, err := b.GetResult(ctx, child.ID); err != nil || res == nil {
			t.Fatalf("reused child's result must still resolve: %v %v", res, err)
		}
	})

	t.Run("cleanup commits first, reuse waits and claims fresh", func(t *testing.T) {
		b, parent, child, storedKey := plant(t)
		// Hand-driven cleanup mid-flight: root and key row held, tree deleted.
		tx, commit := pinnedTx(t, b)
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_activities WHERE id = $1 AND queue_name = $2 FOR UPDATE`, child.ID, b.queueName).Scan(&one); err != nil {
			t.Fatal(err)
		}
		if err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_idempotency WHERE queue_name = $1 AND idempotency_key = $2 FOR UPDATE`, b.queueName, storedKey).Scan(&one); err != nil {
			t.Fatal(err)
		}
		for _, stmt := range []string{
			`DELETE FROM runnerq_results WHERE activity_id = $1`,
			`DELETE FROM runnerq_idempotency WHERE activity_id = $1`,
			`DELETE FROM runnerq_activities WHERE id = $1`,
		} {
			if _, err := tx.Exec(ctx, stmt, child.ID); err != nil {
				t.Fatal(err)
			}
		}
		other := testActivity(3)
		other.ActivityType = "child"
		other.ParentActivityID = &parent.ID
		other.IdempotencyKey = child.IdempotencyKey
		var got *storage.IdempotencyResult
		err := mustStayBlocked(t, commit, func() error {
			return retryConflict(func() error {
				var err error
				got, err = b.EnqueueIdempotent(ctx, &other)
				return err
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Fatalf("reuse returned existing %s after cleanup deleted it; it must claim the key fresh", got.ExistingID)
		}
		var owner uuid.UUID
		if err := b.pool.QueryRow(ctx, `SELECT activity_id FROM runnerq_idempotency WHERE queue_name = $1 AND idempotency_key = $2`, b.queueName, storedKey).Scan(&owner); err != nil || owner != other.ID {
			t.Fatalf("key owner = %s (%v), want the fresh activity %s", owner, err, other.ID)
		}
		if !childExists(t, b, other.ID) {
			t.Fatal("fresh activity was not enqueued")
		}
	})
}
