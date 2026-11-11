package postgres

// Integration tests for the retention sweeper (CleanupExpired). Skipped
// unless RUNNERQ_TEST_DSN is set — see integration_test.go.

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// retentionTree is the set of IDs a planted workflow tree owns, for asserting
// presence/absence across every table after a sweep.
type retentionTree struct {
	root, child  uuid.UUID
	checkpointID uuid.UUID // Run/Sleep-style result row owned by the root
	idemKey      string
}

// plantTree inserts a two-activity workflow tree directly via SQL: root +
// child with the given statuses, completed terminalAge ago, plus events for
// both, a result row for the child, a synthetic checkpoint result owned by
// the root (no matching activity row — like ctx.Run/Sleep writes), and an
// idempotency key pointing at the child.
func plantTree(t *testing.T, b *PostgresBackend, rootStatus, childStatus string, terminalAge time.Duration) retentionTree {
	t.Helper()
	ctx := context.Background()
	tree := retentionTree{
		root:         uuid.New(),
		child:        uuid.New(),
		checkpointID: uuid.New(),
	}
	tree.idemKey = "rq:step:" + tree.root.String() + ":" + tree.root.String() + ":reserve"
	completedAt := time.Now().UTC().Add(-terminalAge)

	insert := func(id uuid.UUID, status string, parent *uuid.UUID, key *string) {
		var cAt *time.Time
		if status == "completed" || status == "failed" || status == "dead_letter" {
			cAt = &completedAt
		}
		if _, err := b.pool.Exec(ctx, `
			INSERT INTO runnerq_activities (id, queue_name, activity_type, priority, status,
				created_at, completed_at, parent_activity_id, root_activity_id, depth, idempotency_key)
			VALUES ($1, $2, 'tree_member', 2, $3, $4, $5, $6, $7, $8, $9)`,
			id, b.queueName, status, completedAt.Add(-time.Minute), cAt, parent, tree.root, 0, key); err != nil {
			t.Fatalf("insert activity: %v", err)
		}
		if _, err := b.pool.Exec(ctx, `
			INSERT INTO runnerq_inputs (activity_id, queue_name, payload) VALUES ($1, $2, '{}'::jsonb)`,
			id, b.queueName); err != nil {
			t.Fatalf("insert input: %v", err)
		}
		if _, err := b.pool.Exec(ctx, `
			INSERT INTO runnerq_events (activity_id, queue_name, event_type, created_at)
			VALUES ($1, $2, 'Yielded', $3)`, id, b.queueName, completedAt.Add(-time.Minute)); err != nil {
			t.Fatalf("insert event: %v", err)
		}
	}
	insert(tree.root, rootStatus, nil, nil)
	insert(tree.child, childStatus, &tree.root, &tree.idemKey)

	if _, err := b.pool.Exec(ctx, `
		INSERT INTO runnerq_results (activity_id, queue_name, state, data, created_at, owner_activity_id)
		VALUES ($1, $2, 'Ok', '{}'::jsonb, $3, $1), ($4, $2, 'Ok', '{}'::jsonb, $3, $5)`,
		tree.child, b.queueName, completedAt, tree.checkpointID, tree.root); err != nil {
		t.Fatalf("insert results: %v", err)
	}
	// An event under the checkpoint's synthetic ID goes with its result.
	if _, err := b.pool.Exec(ctx, `
		INSERT INTO runnerq_events (activity_id, queue_name, event_type, created_at)
		VALUES ($1, $2, 'Signaled', $3)`, tree.checkpointID, b.queueName, completedAt); err != nil {
		t.Fatalf("insert checkpoint event: %v", err)
	}
	if _, err := b.pool.Exec(ctx, `
		INSERT INTO runnerq_idempotency (queue_name, idempotency_key, activity_id, created_at, updated_at)
		VALUES ($1, $2, $3, NOW(), NOW())`, b.queueName, tree.idemKey, tree.child); err != nil {
		t.Fatalf("insert idempotency: %v", err)
	}
	return tree
}

// rowCounts returns how many rows each table still holds for the tree.
func rowCounts(t *testing.T, b *PostgresBackend, tree retentionTree) (activities, events, results, idem int) {
	t.Helper()
	ctx := context.Background()
	ids := []uuid.UUID{tree.root, tree.child, tree.checkpointID}
	queries := []struct {
		dst *int
		sql string
	}{
		{&activities, `SELECT COUNT(*) FROM runnerq_activities WHERE queue_name=$1 AND id = ANY($2)`},
		{&events, `SELECT COUNT(*) FROM runnerq_events WHERE queue_name=$1 AND activity_id = ANY($2)`},
		{&results, `SELECT COUNT(*) FROM runnerq_results WHERE queue_name=$1 AND activity_id = ANY($2)`},
	}
	for _, q := range queries {
		if err := b.pool.QueryRow(ctx, q.sql, b.queueName, ids).Scan(q.dst); err != nil {
			t.Fatalf("count: %v", err)
		}
	}
	if err := b.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM runnerq_idempotency WHERE queue_name=$1 AND idempotency_key=$2`,
		b.queueName, tree.idemKey).Scan(&idem); err != nil {
		t.Fatalf("count idem: %v", err)
	}
	// Inputs go with their activities.
	var inputs int
	if err := b.pool.QueryRow(ctx, `SELECT COUNT(*) FROM runnerq_inputs WHERE queue_name=$1 AND activity_id = ANY($2)`,
		b.queueName, ids).Scan(&inputs); err != nil {
		t.Fatalf("count inputs: %v", err)
	}
	if inputs != activities {
		t.Fatalf("%d inputs for %d activities", inputs, activities)
	}
	return
}

func assertTreePresent(t *testing.T, b *PostgresBackend, tree retentionTree, label string) {
	t.Helper()
	if a, e, r, i := rowCounts(t, b, tree); a != 2 || e != 3 || r != 2 || i != 1 {
		t.Fatalf("%s: counts activities=%d events=%d results=%d idem=%d, want 2/3/2/1", label, a, e, r, i)
	}
}

func assertTreeGone(t *testing.T, b *PostgresBackend, tree retentionTree, label string) {
	t.Helper()
	if a, e, r, i := rowCounts(t, b, tree); a != 0 || e != 0 || r != 0 || i != 0 {
		t.Fatalf("%s: counts activities=%d events=%d results=%d idem=%d, want all 0", label, a, e, r, i)
	}
}

// Only one sweeper runs per queue: while another session holds the advisory
// lock, CleanupExpired no-ops instead of contending.
// The sweep after release also checks every table a tree spans, including
// runnerq_inputs, which the conformance suite cannot see.
func TestCleanupExpiredLeadership(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	tree := plantTree(t, b, "completed", "completed", 2*time.Hour)

	holder, err := b.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin holder tx: %v", err)
	}
	var got bool
	if err := holder.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1::int4, hashtext($2))`,
		cleanupLockClass, b.queueName).Scan(&got); err != nil || !got {
		t.Fatalf("holder lock: got=%v err=%v", got, err)
	}

	n, err := b.CleanupExpired(ctx, storage.RetentionPolicy{Completed: time.Hour}, 100)
	if err != nil {
		t.Fatalf("cleanup under contention: %v", err)
	}
	if n != 0 {
		t.Fatalf("swept %d trees while another sweeper held the lock, want 0", n)
	}
	assertTreePresent(t, b, tree, "tree during contention")

	if err := holder.Rollback(ctx); err != nil {
		t.Fatalf("release holder: %v", err)
	}
	n, err = b.CleanupExpired(ctx, storage.RetentionPolicy{Completed: time.Hour}, 100)
	if err != nil || n != 1 {
		t.Fatalf("cleanup after release: n=%d err=%v, want 1", n, err)
	}
	assertTreeGone(t, b, tree, "tree after release")
}

// Legacy roots from before lineage columns existed have a NULL
// root_activity_id. They must be deleted by their own id — matching only on
// lineage would count them as swept without removing them, and the sweeper
// would re-select the same roots on every pass forever.
func TestCleanupExpiredHandlesLegacyNullRootID(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	legacyID := uuid.New()
	completedAt := time.Now().UTC().Add(-2 * time.Hour)
	if _, err := b.pool.Exec(ctx, `
		INSERT INTO runnerq_activities (id, queue_name, activity_type, priority, status,
			created_at, completed_at, parent_activity_id, root_activity_id, depth)
		VALUES ($1, $2, 'legacy', 2, 'completed', $3, $3, NULL, NULL, 0)`,
		legacyID, b.queueName, completedAt); err != nil {
		t.Fatalf("insert legacy root: %v", err)
	}

	n, err := b.CleanupExpired(ctx, storage.RetentionPolicy{Completed: time.Hour}, 100)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if n != 1 {
		t.Fatalf("swept %d roots, want 1", n)
	}
	var remaining int
	if err := b.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM runnerq_activities WHERE id = $1`, legacyID).Scan(&remaining); err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining != 0 {
		t.Fatal("legacy NULL-root row was counted as swept but not deleted — the sweeper would loop on it forever")
	}

	// And the pass after it must find nothing — no perpetual re-selection.
	n, err = b.CleanupExpired(ctx, storage.RetentionPolicy{Completed: time.Hour}, 100)
	if err != nil || n != 0 {
		t.Fatalf("second pass: n=%d err=%v, want 0", n, err)
	}
}
