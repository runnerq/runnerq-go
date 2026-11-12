package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The fast path checks for every object in the spec's catalog, so it must
// include the ones the hot paths depend on.
func TestSchemaExpectationCoversCatalog(t *testing.T) {
	e := expectedSchema()
	for _, want := range []string{"runnerq_activities", "runnerq_results", "runnerq_dependencies", "runnerq_commands"} {
		if !slices.Contains(e.tables, want) {
			t.Fatalf("tables %v missing %s", e.tables, want)
		}
	}
	for _, want := range []string{"runnerq_activities.root_activity_id", "runnerq_results.owner_activity_id", "runnerq_results.step"} {
		if !slices.Contains(e.columns, want) {
			t.Fatalf("columns %v missing %s", e.columns, want)
		}
	}
	for _, want := range []string{"idx_runnerq_parent_id", "idx_runnerq_root_terminal", "idx_runnerq_dequeue_order_v2", "idx_runnerq_dequeue_effective_v2", "idx_runnerq_query_created"} {
		if !slices.Contains(e.indexes, want) {
			t.Fatalf("indexes %v missing %s", e.indexes, want)
		}
	}
	for _, want := range []string{"idx_runnerq_dequeue_order", "idx_runnerq_root_status"} {
		if !slices.Contains(e.retired, want) {
			t.Fatalf("retired %v missing %s", e.retired, want)
		}
	}
}

// A retired index left by an older version disables the fast path, and init
// drops it.
func TestSchemaInitDropsRetiredIndex(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	if _, err := b.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_runnerq_root_status
		ON runnerq_activities(queue_name, status) WHERE parent_activity_id IS NULL`); err != nil {
		t.Fatal(err)
	}
	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	current, err := schemaCurrent(ctx, conn)
	conn.Release()
	if err != nil || current {
		t.Fatalf("schema reported current with a retired index: %v, %v", current, err)
	}

	testBackendNamed(t, "t_retired").Close()

	var lingering bool
	if err := b.pool.QueryRow(ctx, `SELECT to_regclass('idx_runnerq_root_status') IS NOT NULL`).Scan(&lingering); err != nil {
		t.Fatal(err)
	}
	if lingering {
		t.Fatal("init kept a retired index")
	}
}

// On an initialized database a new backend must take no table locks: with a
// row on runnerq_activities locked by an open transaction, the DDL's ACCESS
// EXCLUSIVE would block, so a fast connect proves the DDL did not run.
func TestSchemaInitSkipsDDLWhenCurrent(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if current, err := schemaCurrent(ctx, conn); err != nil || !current {
		t.Fatalf("schema current after init = %v, %v", current, err)
	}

	a := testActivity(1)
	if err := b.Enqueue(ctx, a); err != nil {
		t.Fatal(err)
	}
	tx, _ := pinnedTx(t, b)
	var one int
	if err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_activities WHERE id = $1 FOR UPDATE`, a.ID).Scan(&one); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	other := testBackendNamedCtx(t, connectCtx, "t_fastpath")
	other.Close()
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("connect took %v with a row lock held; schema init ran DDL on a current schema", took)
	}
}

// A missing object disables the fast path and the next init restores it.
func TestSchemaInitMigratesWhenObjectMissing(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	if _, err := b.pool.Exec(ctx, `DROP INDEX IF EXISTS idx_runnerq_parent_id`); err != nil {
		t.Fatal(err)
	}
	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	current, err := schemaCurrent(ctx, conn)
	conn.Release()
	if err != nil || current {
		t.Fatalf("schema reported current with an index missing: %v, %v", current, err)
	}

	testBackendNamed(t, "t_migrate").Close()

	conn, err = b.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if current, err := schemaCurrent(ctx, conn); err != nil || !current {
		t.Fatalf("schema not restored by a fresh init: %v, %v", current, err)
	}
}

// Index names are unique per schema only. A same-named valid index in another
// schema must not satisfy the dequeue-index inspection, or init would skip
// the build and the fast path would never report the schema current.
func TestSchemaInitIgnoresSameNamedIndexInOtherSchema(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	const decoy = "rq_decoy_schema"
	for _, stmt := range []string{
		`CREATE SCHEMA IF NOT EXISTS ` + decoy,
		`CREATE TABLE IF NOT EXISTS ` + decoy + `.t (x INT)`,
		`CREATE INDEX IF NOT EXISTS idx_runnerq_dequeue_order_v2 ON ` + decoy + `.t (x)`,
		`DROP INDEX IF EXISTS public.idx_runnerq_dequeue_order_v2`,
	} {
		if _, err := b.pool.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = b.pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+decoy+` CASCADE`) })

	testBackendNamed(t, "t_decoy").Close()

	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if current, err := schemaCurrent(ctx, conn); err != nil || !current {
		t.Fatalf("dequeue index not rebuilt in the active schema behind a same-named decoy: current=%v err=%v", current, err)
	}
	var decoyStillThere bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = $1 AND indexname = 'idx_runnerq_dequeue_order_v2')`, decoy).Scan(&decoyStillThere); err != nil {
		t.Fatal(err)
	}
	if !decoyStillThere {
		t.Fatal("init dropped an index that belongs to another schema")
	}
}

// A waiter for the schema lock must not sit inside a blocked
// pg_advisory_lock: that statement pins a snapshot, and the holder's CREATE
// INDEX CONCURRENTLY waits for all older snapshots — a cycle Postgres breaks
// with 40P01. With the lock held externally and the fast path disabled, a
// booting backend must wait without any session blocked on the advisory
// lock, then complete once the lock is released.
func TestSchemaInitWaitsForLockWithoutPinningSnapshot(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	if _, err := b.pool.Exec(ctx, `DROP INDEX IF EXISTS idx_runnerq_parent_id`); err != nil {
		t.Fatal(err)
	}

	holder, err := b.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Release()
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_lock($1)`, schemaAdvisoryLockKey); err != nil {
		t.Fatal(err)
	}
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		if _, err := holder.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, schemaAdvisoryLockKey); err != nil {
			t.Errorf("unlock: %v", err)
		}
	}
	defer release()

	connectCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	done := make(chan *PostgresBackend, 1)
	go func() { done <- testBackendNamedCtx(t, connectCtx, "t_lockpoll") }()

	select {
	case other := <-done:
		other.Close()
		t.Fatal("backend initialized while another session held the schema lock")
	case <-time.After(500 * time.Millisecond):
	}
	var blocked int
	if err := holder.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND wait_event = 'advisory'`).Scan(&blocked); err != nil {
		t.Fatal(err)
	}
	if blocked != 0 {
		t.Fatalf("%d session(s) blocked inside pg_advisory_lock while waiting for the schema lock; a blocked waiter pins a snapshot and can deadlock the holder's CREATE INDEX CONCURRENTLY", blocked)
	}

	release()
	select {
	case other := <-done:
		other.Close()
	case <-time.After(15 * time.Second):
		t.Fatal("backend did not initialize after the schema lock was released")
	}

	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if current, err := schemaCurrent(ctx, conn); err != nil || !current {
		t.Fatalf("schema not restored after the waiter initialized: %v, %v", current, err)
	}
}

// A database from before runnerq_inputs (payload inline on the activity row)
// is migrated on connect: payloads move to runnerq_inputs, the column is
// dropped, and queued work is still claimed with its payload.
func TestSchemaMovesInlinePayloadsToInputs(t *testing.T) {
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping integration test")
	}
	ctx := context.Background()
	schema := "rq_inline_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`) })
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	scoped := dsn + sep + "search_path=" + schema

	// Today's schema, taken back to the inline-payload layout.
	b, err := WithConfig(ctx, scoped, "inline", 30_000, 5)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	a := testActivity(3)
	a.Payload = json.RawMessage(`{"order":42,"items":["a","b"]}`)
	for _, stmt := range []string{
		`DROP TABLE ` + schema + `.runnerq_inputs`,
		`ALTER TABLE ` + schema + `.runnerq_results DROP COLUMN serialization`,
		`ALTER TABLE ` + schema + `.runnerq_activities ADD COLUMN payload JSONB NOT NULL`,
		`ALTER TABLE ` + schema + `.runnerq_activities ALTER COLUMN max_retries SET DEFAULT 3`,
	} {
		if _, err := admin.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if _, err := admin.Exec(ctx, `INSERT INTO `+schema+`.runnerq_activities
		(id, queue_name, activity_type, payload, priority, status, created_at, max_retries, root_activity_id)
		VALUES ($1, 'inline', $2, $3, 2, 'pending', now(), 3, $1)`, a.ID, a.ActivityType, a.Payload); err != nil {
		t.Fatal(err)
	}

	b, err = WithConfig(ctx, scoped, "inline", 30_000, 5)
	if err != nil {
		t.Fatalf("connect to an inline-payload database: %v", err)
	}
	defer b.Close()

	var payloadColumn bool
	var input json.RawMessage
	var serialization, maxRetriesDefault string
	if err := admin.QueryRow(ctx, `SELECT
		EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = $1 AND table_name = 'runnerq_activities' AND column_name = 'payload'),
		(SELECT payload FROM `+schema+`.runnerq_inputs WHERE activity_id = $2),
		(SELECT serialization FROM `+schema+`.runnerq_inputs WHERE activity_id = $2),
		(SELECT column_default FROM information_schema.columns WHERE table_schema = $1 AND table_name = 'runnerq_activities' AND column_name = 'max_retries')`,
		schema, a.ID).Scan(&payloadColumn, &input, &serialization, &maxRetriesDefault); err != nil {
		t.Fatal(err)
	}
	if payloadColumn || serialization != "json-v1" || maxRetriesDefault != "0" {
		t.Fatalf("after migration: payload column %v, serialization %q, max_retries default %q", payloadColumn, serialization, maxRetriesDefault)
	}
	var got, want any
	_ = json.Unmarshal(input, &got)
	_ = json.Unmarshal(a.Payload, &want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("input %s, want %s", input, a.Payload)
	}

	claimed, err := b.Dequeue(ctx, "w1", 0, nil)
	if err != nil || claimed == nil || claimed.ID != a.ID {
		t.Fatalf("claim after migration: %+v, %v", claimed, err)
	}
	if err := json.Unmarshal(claimed.Payload, &got); err != nil || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("claimed payload %s", claimed.Payload)
	}

	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if current, err := schemaCurrent(ctx, conn); err != nil || !current {
		t.Fatalf("schema current after migration = %v, %v", current, err)
	}
}
