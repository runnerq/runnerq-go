package postgres

// Integration tests against a real PostgreSQL instance. They are skipped
// unless RUNNERQ_TEST_DSN is set, e.g.:
//
//	docker run -d --rm -e POSTGRES_PASSWORD=test -e POSTGRES_DB=runnerq_test -p 55432:5432 postgres:16-alpine
//	RUNNERQ_TEST_DSN='postgres://postgres:test@localhost:55432/runnerq_test' go test ./...
//
// Each test uses a fresh random queue name, so tests are isolated and can run
// in parallel against one database.

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

func testBackend(t *testing.T) *PostgresBackend {
	t.Helper()
	return testBackendNamed(t, "t_"+strings.ReplaceAll(uuid.New().String(), "-", "")[:16])
}

// testBackendNamed connects a backend to a specific queue. Two backends on
// the same queue name simulate separate processes sharing a database.
func testBackendNamed(t *testing.T, queueName string) *PostgresBackend {
	t.Helper()
	return testBackendNamedCtx(t, context.Background(), queueName)
}

func testBackendNamedCtx(t *testing.T, ctx context.Context, queueName string) *PostgresBackend {
	t.Helper()
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping integration test")
	}
	b, err := WithConfig(ctx, dsn, queueName, 30_000, 5)
	if err != nil {
		t.Fatalf("connect backend: %v", err)
	}
	t.Cleanup(b.Close)
	return b
}

func testActivity(maxRetries uint32) storage.QueuedActivity {
	return storage.QueuedActivity{
		ID:             uuid.New(),
		ActivityType:   "test_activity",
		Payload:        json.RawMessage(`{"k":"v"}`),
		Priority:       storage.PriorityNormal,
		MaxRetries:     maxRetries,
		TimeoutSeconds: 30,
		CreatedAt:      time.Now().UTC(),
		Metadata:       map[string]string{},
	}
}

// expireLease force-expires an activity's lease so the reaper sees it.
func expireLease(t *testing.T, b *PostgresBackend, id uuid.UUID) {
	t.Helper()
	_, err := b.pool.Exec(context.Background(), `
		UPDATE runnerq_activities
		SET lease_deadline_ms = (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint - 10000
		WHERE id = $1`, id)
	if err != nil {
		t.Fatalf("expire lease: %v", err)
	}
}

func activityStatus(t *testing.T, b *PostgresBackend, id uuid.UUID) (status string, retryCount int32) {
	t.Helper()
	err := b.pool.QueryRow(context.Background(),
		`SELECT status, retry_count FROM runnerq_activities WHERE id = $1`, id).
		Scan(&status, &retryCount)
	if err != nil {
		t.Fatalf("read activity row: %v", err)
	}
	return status, retryCount
}

func hasEvent(t *testing.T, b *PostgresBackend, id uuid.UUID, eventType string) bool {
	t.Helper()
	events, err := b.GetActivityEvents(context.Background(), id, 100)
	if err != nil {
		t.Fatalf("get events: %v", err)
	}
	for _, e := range events {
		if e.EventType == eventType {
			return true
		}
	}
	return false
}

// Go claims only plain-JSON inputs: an activity the TypeScript SDK enqueued
// in its native encoding stays queued for a TypeScript worker, however high
// its priority.
func TestClaimsSkipInputsGoCannotRead(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	native, plain := testActivity(3), testActivity(3)
	native.Priority, plain.Priority = storage.PriorityCritical, storage.PriorityLow
	for _, a := range []storage.QueuedActivity{native, plain} {
		if err := b.Enqueue(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.pool.Exec(ctx, `UPDATE runnerq_inputs SET serialization = 'superjson-v1',
		payload = '{"json":{"k":"v"},"meta":{"values":{}}}' WHERE activity_id = $1`, native.ID); err != nil {
		t.Fatal(err)
	}

	claimed, err := b.Dequeue(ctx, "w1", 0, nil)
	if err != nil || claimed == nil || claimed.ID != plain.ID {
		t.Fatalf("claimed %+v, %v; want the plain-JSON activity", claimed, err)
	}
	if again, err := b.Dequeue(ctx, "w2", 0, nil); err != nil || again != nil {
		t.Fatalf("claimed %+v, %v; want nothing", again, err)
	}
	if batch, err := b.DequeueBatch(ctx, "engine:batch:1", 10, 0, nil); err != nil || len(batch) != 0 {
		t.Fatalf("batch claimed %d, %v; want nothing", len(batch), err)
	}
	if status, _ := activityStatus(t, b, native.ID); status != "pending" {
		t.Fatalf("native activity status %q, want pending", status)
	}
}

// storage.BatchQueueStorage: one round trip claims up to limit rows in the
// order Dequeue would have taken them, each with its own fenced token. Beyond
// the conformance suite's batch test, this pins the token format
// (prefix:activityID) this backend derives.
func TestDequeueBatchClaimsInDequeueOrderWithFencedTokens(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	for _, p := range []storage.ActivityPriority{storage.PriorityLow, storage.PriorityCritical, storage.PriorityNormal, storage.PriorityHigh, storage.PriorityLow} {
		a := testActivity(3)
		a.Priority = p
		if err := b.Enqueue(ctx, a); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	claims, err := b.DequeueBatch(ctx, "engine:batch:1", 3, 0, nil)
	if err != nil {
		t.Fatalf("batch dequeue: %v", err)
	}
	wantOrder := []storage.ActivityPriority{storage.PriorityCritical, storage.PriorityHigh, storage.PriorityNormal}
	if len(claims) != len(wantOrder) {
		t.Fatalf("claimed %d activities, want %d", len(claims), len(wantOrder))
	}
	for i, c := range claims {
		if c.Activity.Priority != wantOrder[i] {
			t.Fatalf("claim %d priority = %d, want %d (dequeue order)", i, c.Activity.Priority, wantOrder[i])
		}
		if want := "engine:batch:1:" + c.Activity.ID.String(); c.LeaseID != want {
			t.Fatalf("lease token = %q, want %q", c.LeaseID, want)
		}
		if c.Attempt != 1 || !c.LeaseDeadline.After(time.Now()) {
			t.Fatalf("claim %d attempt=%d deadline=%v, want attempt 1 and a future deadline", i, c.Attempt, c.LeaseDeadline)
		}
		if status, _ := activityStatus(t, b, c.Activity.ID); status != "processing" {
			t.Fatalf("status = %q, want processing", status)
		}
	}

	// Rows claimed together are still fenced apart: a sibling's token is
	// rejected, the row's own token is accepted.
	if err := b.AckSuccess(ctx, claims[0].Activity.ID, nil, claims[1].LeaseID); err == nil {
		t.Fatal("ack with a sibling claim's token succeeded; tokens must be per activity")
	}
	if err := b.AckSuccess(ctx, claims[0].Activity.ID, nil, claims[0].LeaseID); err != nil {
		t.Fatalf("ack with own token: %v", err)
	}

	rest, err := b.DequeueBatch(ctx, "engine:batch:2", 10, 0, nil)
	if err != nil || len(rest) != 2 {
		t.Fatalf("remaining claim: n=%d err=%v, want the 2 low-priority rows", len(rest), err)
	}
	for _, c := range rest {
		if c.Activity.Priority != storage.PriorityLow {
			t.Fatalf("remaining row priority = %d, want low", c.Activity.Priority)
		}
	}
	if none, err := b.DequeueBatch(ctx, "engine:batch:3", 10, 0, nil); err != nil || len(none) != 0 {
		t.Fatalf("empty queue claim: n=%d err=%v, want none", len(none), err)
	}
	if none, err := b.DequeueBatch(ctx, "engine:batch:4", 0, time.Second, nil); err != nil || len(none) != 0 {
		t.Fatalf("limit 0 claim: n=%d err=%v, want an immediate empty result", len(none), err)
	}
}

// A batch token is fenced exactly like a single-claim token: once the lease
// expires and the row is reclaimed, the old token can no longer ack.
func TestStaleBatchTokenCannotAck(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	a := testActivity(5)
	if err := b.Enqueue(ctx, a); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	stale, err := b.DequeueBatch(ctx, "stale", 1, 0, nil)
	if err != nil || len(stale) != 1 {
		t.Fatalf("first claim: n=%d err=%v", len(stale), err)
	}

	expireLease(t, b, a.ID)
	if n, err := b.RequeueExpired(ctx, 10); err != nil || n != 1 {
		t.Fatalf("requeue expired: n=%d err=%v", n, err)
	}
	fresh, err := b.DequeueBatch(ctx, "fresh", 1, 0, nil)
	if err != nil || len(fresh) != 1 || fresh[0].Activity.ID != a.ID {
		t.Fatalf("second claim: n=%d err=%v", len(fresh), err)
	}
	if fresh[0].LeaseID == stale[0].LeaseID || fresh[0].Attempt != 2 {
		t.Fatalf("reclaim token=%q attempt=%d; want a new token and attempt 2", fresh[0].LeaseID, fresh[0].Attempt)
	}

	if err := b.AckSuccess(ctx, a.ID, json.RawMessage(`"stale"`), stale[0].LeaseID); err == nil {
		t.Fatal("stale batch token ack succeeded; it must be fenced out")
	}
	if status, _ := activityStatus(t, b, a.ID); status != "processing" {
		t.Fatalf("status after stale ack = %q, want processing", status)
	}
	if err := b.AckSuccess(ctx, a.ID, json.RawMessage(`"fresh"`), fresh[0].LeaseID); err != nil {
		t.Fatalf("owning token ack failed: %v", err)
	}
}

// Tier 0.3: a key orphaned by the old non-atomic claim/enqueue split (key row
// pointing at an activity that doesn't exist) is repaired instead of bricking
// every future spawn with that key.
func TestEnqueueIdempotentRepairsOrphanedKey(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	// Simulate the legacy crash window: claimed key, no activity row.
	if _, err := b.pool.Exec(ctx, `
		INSERT INTO runnerq_idempotency (queue_name, idempotency_key, activity_id, created_at, updated_at)
		VALUES ($1, $2, $3, NOW(), NOW())`,
		b.queueName, "orphan-key", uuid.New()); err != nil {
		t.Fatalf("plant orphan: %v", err)
	}

	a := testActivity(3)
	a.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: "orphan-key", Behavior: storage.BehaviorReturnExisting}
	existing, err := b.EnqueueIdempotent(ctx, &a)
	if err != nil {
		t.Fatalf("enqueue over orphan: %v", err)
	}
	if existing != nil {
		t.Fatalf("orphaned key returned existing %v; it points at nothing and must be reclaimed", existing.ExistingID)
	}
	if status, _ := activityStatus(t, b, a.ID); status != "pending" {
		t.Fatalf("status = %q, want pending", status)
	}
	var pointsAt uuid.UUID
	if err := b.pool.QueryRow(ctx, `
		SELECT activity_id FROM runnerq_idempotency
		WHERE queue_name = $1 AND idempotency_key = $2`,
		b.queueName, "orphan-key").Scan(&pointsAt); err != nil {
		t.Fatalf("read key: %v", err)
	}
	if pointsAt != a.ID {
		t.Fatalf("key points at %s, want %s", pointsAt, a.ID)
	}
}

// Tier 0.5: lease deadlines come from the database clock and a renewal
// pushes them forward; a non-processing activity cannot have its lease
// extended.
func TestLeaseUsesDBClockAndExtends(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	a := testActivity(3)
	if err := b.Enqueue(ctx, a); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if claimed, err := b.Dequeue(ctx, "w1", time.Second, nil); err != nil || claimed == nil {
		t.Fatalf("dequeue: claimed=%v err=%v", claimed, err)
	}

	var leaseMS int64
	var dbNowMS int64
	if err := b.pool.QueryRow(ctx, `
		SELECT lease_deadline_ms, (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint
		FROM runnerq_activities WHERE id = $1`, a.ID).Scan(&leaseMS, &dbNowMS); err != nil {
		t.Fatalf("read lease: %v", err)
	}
	if leaseMS <= dbNowMS {
		t.Fatalf("fresh lease %d is not in the database's future (db now %d)", leaseMS, dbNowMS)
	}

	ok, err := b.ExtendLeaseForWorker(ctx, a.ID, "w1", 10*time.Minute)
	if err != nil || !ok {
		t.Fatalf("extend lease: ok=%v err=%v", ok, err)
	}
	var extendedMS int64
	if err := b.pool.QueryRow(ctx,
		`SELECT lease_deadline_ms FROM runnerq_activities WHERE id = $1`, a.ID).Scan(&extendedMS); err != nil {
		t.Fatalf("read extended lease: %v", err)
	}
	if extendedMS <= leaseMS {
		t.Fatalf("extended lease %d not after original %d", extendedMS, leaseMS)
	}

	if err := b.AckSuccess(ctx, a.ID, nil, "w1"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	ok, err = b.ExtendLeaseForWorker(ctx, a.ID, "w1", time.Minute)
	if err != nil {
		t.Fatalf("extend after complete: %v", err)
	}
	if ok {
		t.Fatal("extended the lease of a completed activity")
	}
}

// A failure's details are kept in its error result and events, for SDKs
// that record them.
func TestFailureDetailsKept(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	a := testActivity(1)
	if err := b.Enqueue(ctx, a); err != nil {
		t.Fatal(err)
	}
	c, err := b.Dequeue(ctx, "w1", 0, nil)
	if err != nil || c == nil {
		t.Fatal(err)
	}
	if _, err := b.AckFailure(ctx, a.ID, storage.FailureKind{Reason: "x", Details: json.RawMessage(`{not json`)}, "w1"); !isKind(err, storage.ErrInvalidArgument) {
		t.Fatalf("malformed details: %v", err)
	}
	details := json.RawMessage(`{"name":"TypeError","message":"bad input","stack":"at handler"}`)
	if _, err := b.AckFailure(ctx, a.ID, storage.FailureKind{Reason: "bad input", Details: details}, "w1"); err != nil {
		t.Fatal(err)
	}
	res, err := b.GetResult(ctx, a.ID)
	if err != nil || res == nil || res.State != storage.ResultErr {
		t.Fatalf("result %+v, %v", res, err)
	}
	var stored struct {
		Error   string          `json:"error"`
		Type    string          `json:"type"`
		Failure json.RawMessage `json:"failure"`
	}
	if err := json.Unmarshal(res.Data, &stored); err != nil || stored.Error != "bad input" || stored.Type != "non_retryable" {
		t.Fatalf("stored %s: %v", res.Data, err)
	}
	var failure map[string]string
	if err := json.Unmarshal(stored.Failure, &failure); err != nil || failure["name"] != "TypeError" || failure["stack"] != "at handler" {
		t.Fatalf("failure %s: %v", stored.Failure, err)
	}
}

func isKind(err error, kind storage.StorageErrorKind) bool {
	se, ok := storage.IsStorageError(err)
	return ok && se.Kind == kind
}

// A claim commits on its own, so once sent its reply must reach the caller
// even if the caller's context ends meanwhile; otherwise the row would sit
// claimed until lease recovery.
func TestClaimSentBeforeCancelIsDelivered(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	a := testActivity(3)
	if err := b.Enqueue(ctx, a); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Hold the claim statement mid-flight: its input read waits on this lock.
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `LOCK TABLE runnerq_inputs IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	claimCtx, cancel := context.WithCancel(ctx)
	type result struct {
		claims []storage.DequeuedActivity
		err    error
	}
	done := make(chan result, 1)
	go func() {
		claims, err := b.DequeueBatch(claimCtx, "w", 1, 0, []string{a.ActivityType})
		done <- result{claims, err}
	}()
	waitUntil(t, func() bool {
		var n int
		_ = b.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND query LIKE '%SET status = ''processing''%'`).Scan(&n)
		return n > 0
	})
	cancel()
	time.Sleep(50 * time.Millisecond)
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	r := <-done
	if r.err != nil || len(r.claims) != 1 || r.claims[0].Activity.ID != a.ID {
		t.Fatalf("claim sent before cancel: got %d claims, err %v; want the activity", len(r.claims), r.err)
	}
	if status, _ := activityStatus(t, b, a.ID); status != "processing" {
		t.Fatalf("status = %s, want processing", status)
	}

	// A context that has already ended sends nothing.
	if _, err := b.DequeueBatch(claimCtx, "w", 1, 0, nil); err == nil {
		t.Fatal("claim on an ended context: want its error")
	}
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}
	}
}
