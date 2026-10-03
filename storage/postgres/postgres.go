package postgres

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alob-mtc/runnerq-go/internal/spec"
	"github.com/alob-mtc/runnerq-go/storage"
)

// PostgresBackend is RunnerQ's PostgreSQL storage backend for one queue.
type PostgresBackend struct {
	pool           *pgxpool.Pool
	queueName      string
	defaultLeaseMS atomic.Int64

	sig       *signaler
	watcherMu sync.Mutex
	watch     *watcher
}

// New creates a backend with pool size 25 and a 60s lease, creating or
// migrating the schema if needed. Use WithConfig to size the pool to the worker
// count (rule of thumb: maxWorkers + ~5 for reaper and scheduler headroom).
func New(ctx context.Context, databaseURL, queueName string) (*PostgresBackend, error) {
	return WithConfig(ctx, databaseURL, queueName, 60_000, 25)
}

// WithConfig is New with an explicit default lease (ms, > 0) and pool size
// (>= 2: one connection is reserved for LISTEN).
func WithConfig(ctx context.Context, databaseURL, queueName string, defaultLeaseMS int64, poolSize int32) (*PostgresBackend, error) {
	if err := validateQueueName(queueName); err != nil {
		return nil, err
	}

	if poolSize < 2 {
		return nil, storage.NewConfigurationError("PostgreSQL pool requires at least 2 connections: one listener and one query connection")
	}
	if defaultLeaseMS <= 0 {
		return nil, storage.NewConfigurationError("default lease must be positive")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, storage.NewUnavailableError(fmt.Sprintf("Failed to parse PostgreSQL URL: %v", err))
	}
	config.MaxConns = poolSize

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, storage.NewUnavailableError(fmt.Sprintf("Failed to connect to PostgreSQL: %v", err))
	}

	backend := &PostgresBackend{
		pool:      pool,
		queueName: queueName,
	}

	backend.defaultLeaseMS.Store(defaultLeaseMS)
	if err := backend.initSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	backend.sig = newSignaler(backend)

	return backend, nil
}

// Close stops the signal machinery and closes the connection pool.
func (b *PostgresBackend) Close() {
	b.watcherMu.Lock()
	w := b.watch
	b.watch = nil
	b.watcherMu.Unlock()
	if w != nil {
		w.stop()
	}
	if b.sig != nil {
		b.sig.stop()
	}
	b.pool.Close()
}

// QueueName is the queue this backend claims from and commands act on.
func (b *PostgresBackend) QueueName() string { return b.queueName }

func (b *PostgresBackend) SetLeaseMS(leaseMS int64) {
	b.defaultLeaseMS.Store(leaseMS)
}

func validateQueueName(name string) error {
	if name == "" {
		return storage.NewConfigurationError("queue_name cannot be empty")
	}
	if len(name) > 48 {
		return storage.NewConfigurationError(fmt.Sprintf("queue_name '%s' is too long (max 48 characters)", name))
	}
	runes := []rune(name)
	first := runes[0]
	if !unicode.IsLetter(first) && first != '_' {
		return storage.NewConfigurationError(fmt.Sprintf("queue_name '%s' must start with a letter or underscore", name))
	}
	for _, c := range runes[1:] {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_' {
			return storage.NewConfigurationError(fmt.Sprintf("queue_name '%s' contains invalid character '%c'; only letters, digits, and underscores are allowed", name, c))
		}
	}
	return nil
}

// schemaAdvisoryLockKey is arbitrary but must stay stable forever: every
// RunnerQ instance on the database must agree on it.
const schemaAdvisoryLockKey = spec.SchemaAdvisoryLockKey

func (b *PostgresBackend) initSchema(ctx context.Context) error {
	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to acquire connection for schema init: %v", err))
	}
	defer conn.Release()

	// Fast path: no DDL, advisory lock or table locks (see schema_check.go).
	if current, err := schemaCurrent(ctx, conn); err != nil {
		return err
	} else if current {
		return nil
	}

	// ALTER TABLE takes ACCESS EXCLUSIVE before evaluating IF NOT EXISTS, so
	// concurrent inits deadlock (40P01). A session-level advisory lock
	// serializes them and auto-releases if the session dies.
	if err := acquireSchemaLock(ctx, conn); err != nil {
		return databaseError(err, fmt.Sprintf("Failed to acquire schema lock: %v", err))
	}
	defer func() {
		// Cancellation-proof: the lock must be released on this connection or
		// it blocks every other process's startup until the connection closes.
		if _, err := conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, schemaAdvisoryLockKey); err != nil {
			slog.Warn("runnerq: failed to release schema advisory lock; it releases when the connection closes", "error", err)
		}
	}()

	// Another process may have migrated while we waited; skipping avoids the
	// DDL's locks.
	if current, err := schemaCurrent(ctx, conn); err != nil {
		return err
	} else if current {
		return nil
	}

	err = retryDeadlock(ctx, "schema", func() error {
		_, err := conn.Exec(ctx, schemaSql)
		return err
	})
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to initialize schema: %v", err))
	}

	return b.ensureDequeueIndexes(ctx, conn)
}

const schemaLockPoll = 50 * time.Millisecond

// acquireSchemaLock polls pg_try_advisory_lock rather than blocking in
// pg_advisory_lock: a blocked call holds its snapshot while waiting, and the
// holder's CREATE INDEX CONCURRENTLY waits for older snapshots to end, a cycle
// Postgres breaks with 40P01 (the second process booting on a fresh database
// failed). Each poll's snapshot ends immediately.
func acquireSchemaLock(ctx context.Context, conn *pgxpool.Conn) error {
	for {
		var locked bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, schemaAdvisoryLockKey).Scan(&locked); err != nil {
			return err
		}
		if locked {
			return nil
		}
		select {
		case <-time.After(schemaLockPoll):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ensureDequeueIndexes builds runnerq-spec's concurrent indexes (the dequeue
// and query indexes) on the schema-locked connection, each statement
// autocommitting: CONCURRENTLY can't run in a transaction, and a plain CREATE
// INDEX would block writes for the whole build. INVALID leftovers of failed
// builds are dropped and rebuilt; a predecessor is dropped only after its
// replacement is valid, so the claim path is never unindexed.
func (b *PostgresBackend) ensureDequeueIndexes(ctx context.Context, conn *pgxpool.Conn) error {
	// Scope to the schema unqualified DDL resolves to: index names are unique
	// only per schema, so an unscoped lookup could see another schema's index
	// and skip the build, and an unqualified DROP could follow search_path.
	var schema string
	if err := conn.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		return databaseError(err, fmt.Sprintf("Failed to resolve current schema: %v", err))
	}
	for _, idx := range spec.PostgresConcurrentIndexes {
		// Safe to rerun: a build killed mid-way leaves an INVALID index that
		// ensureDequeueIndex drops before rebuilding.
		if err := retryDeadlock(ctx, idx.Name, func() error { return b.ensureDequeueIndex(ctx, conn, schema, idx) }); err != nil {
			return err
		}
	}
	return nil
}

func (b *PostgresBackend) ensureDequeueIndex(ctx context.Context, conn *pgxpool.Conn, schema string, idx spec.PostgresConcurrentIndex) error {
	dropIndex := func(name string) error {
		if name == "" {
			return nil
		}
		_, err := conn.Exec(ctx, "DROP INDEX IF EXISTS "+pgx.Identifier{schema, name}.Sanitize())
		return err
	}
	{
		var valid *bool
		err := conn.QueryRow(ctx, `
			SELECT i.indisvalid
			FROM pg_index i
			JOIN pg_class c ON c.oid = i.indexrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2`, schema, idx.Name).Scan(&valid)
		switch {
		case err == pgx.ErrNoRows:
		case err != nil:
			return databaseError(err, fmt.Sprintf("Failed to inspect index %s: %v", idx.Name, err))
		case valid != nil && !*valid:
			if err := dropIndex(idx.Name); err != nil {
				return databaseError(err, fmt.Sprintf("Failed to drop invalid index %s: %v", idx.Name, err))
			}
		default:
			if err := dropIndex(idx.Replaces); err != nil {
				return databaseError(err, fmt.Sprintf("Failed to drop superseded index %s: %v", idx.Replaces, err))
			}
			return nil
		}

		if _, err := conn.Exec(ctx, idx.SQL); err != nil {
			return databaseError(err, fmt.Sprintf("Failed to build index %s: %v", idx.Name, err))
		}
		if err := dropIndex(idx.Replaces); err != nil {
			return databaseError(err, fmt.Sprintf("Failed to drop superseded index %s: %v", idx.Replaces, err))
		}
	}
	return nil
}

func intToPriority(val int32) storage.ActivityPriority {
	switch {
	case val <= int32(storage.PriorityLow):
		return storage.PriorityLow
	case val <= int32(storage.PriorityCritical):
		return storage.ActivityPriority(val)
	}
	return storage.PriorityNormal
}

type stmt struct {
	sql  string
	args []any
	what string
}

// execAll pipelines stmts in one round trip. On a pgx.Tx they join it; on the
// pool they form one implicit transaction, so a failure rolls back the rest.
func execAll(ctx context.Context, q interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
}, stmts ...stmt) error {
	if len(stmts) == 1 {
		if _, err := q.Exec(ctx, stmts[0].sql, stmts[0].args...); err != nil {
			return databaseError(err, fmt.Sprintf("%s: %v", stmts[0].what, err))
		}
		return nil
	}
	batch := &pgx.Batch{}
	for _, s := range stmts {
		batch.Queue(s.sql, s.args...)
	}
	br := q.SendBatch(ctx, batch)
	for _, s := range stmts {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return databaseError(err, fmt.Sprintf("%s: %v", s.what, err))
		}
	}
	if err := br.Close(); err != nil {
		return databaseError(err, fmt.Sprintf("%s: %v", stmts[len(stmts)-1].what, err))
	}
	return nil
}

// eventStmt must never NOTIFY: it runs inside hot-path transactions, and
// Postgres serializes every NOTIFY-carrying commit on one global lock.
func (b *PostgresBackend) eventStmt(activityID uuid.UUID, eventType string, workerID *string, detail json.RawMessage) stmt {
	return stmt{`
		INSERT INTO runnerq_events (activity_id, queue_name, event_type, worker_id, detail, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		[]any{activityID, b.queueName, eventType, workerID, detail, time.Now().UTC()},
		"Failed to record event"}
}

func (b *PostgresBackend) recordEvent(ctx context.Context, tx pgx.Tx, activityID uuid.UUID, eventType string, workerID *string, detail json.RawMessage) error {
	return execAll(ctx, tx, b.eventStmt(activityID, eventType, workerID, detail))
}

// resultStmts store a result and wake its parked consumers. owner's tree
// governs the row's lifetime (activityID for normal results, the handler's
// activity for Run/Sleep checkpoints).
//
// Precondition: the transaction already holds an exclusive lock (UPDATE, FOR
// UPDATE or FOR NO KEY UPDATE) on owner's row, ordering this publication
// against a consumer parking on the result (see dependencies.go).
func (b *PostgresBackend) resultStmts(activityID, owner uuid.UUID, result *storage.ActivityResult, now time.Time, step string) []stmt {
	state := "Ok"
	if result.State == storage.ResultErr {
		state = "Err"
	}
	// An activity's own result has no checkpoint identity: NULL step.
	var stepArg any
	if step != "" {
		stepArg = step
	}
	return []stmt{{`
		INSERT INTO runnerq_results (activity_id, queue_name, state, data, created_at, owner_activity_id, step, serialization)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (activity_id) DO UPDATE
		SET state = $3, data = $4, created_at = $5, owner_activity_id = $6, step = $7, serialization = $8`,
		[]any{activityID, b.queueName, state, result.Data, now, owner, stepArg, storedSerialization(result.Serialization)},
		"Failed to store result"}, b.wakeStmt(activityID)}
}

func (b *PostgresBackend) storeResultTx(ctx context.Context, tx pgx.Tx, activityID, owner uuid.UUID, result *storage.ActivityResult, now time.Time, step string) error {
	return execAll(ctx, tx, b.resultStmts(activityID, owner, result, now, step)...)
}

func storedSerialization(s string) string {
	if s == "" {
		return storage.SerializationJSON
	}
	return s
}

// reportedSerialization reports plain JSON as "", as Go callers always saw it.
func reportedSerialization(s string) string {
	if s == storage.SerializationJSON {
		return ""
	}
	return s
}

func withFailure(kv map[string]any, details json.RawMessage) map[string]any {
	if len(details) > 0 {
		kv["failure"] = details
	}
	return kv
}

func toDetail(kv map[string]any) json.RawMessage {
	data, _ := json.Marshal(kv)
	return data
}

func (b *PostgresBackend) Enqueue(ctx context.Context, a storage.QueuedActivity) error {
	return b.enqueue(ctx, a, nil)
}

// A non-nil fence makes the insert conditional on the spawner's claim.
func (b *PostgresBackend) enqueue(ctx context.Context, a storage.QueuedActivity, fence *spawnFence) error {
	stmts, err := b.enqueueStmts(&a)
	if err != nil {
		return err
	}
	if fence == nil {
		if err := execAll(ctx, b.pool, stmts...); err != nil {
			return err
		}
		b.signalEnqueued(&a)
		return nil
	}

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)
	if err := b.verifySpawnFenceTx(ctx, tx, fence); err != nil {
		return err
	}
	if err := execAll(ctx, tx, stmts...); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return databaseError(err, fmt.Sprintf("Failed to commit enqueue: %v", err))
	}
	b.signalEnqueued(&a)
	return nil
}

// Future-scheduled rows are found by blocked dequeuers' periodic probes.
func (b *PostgresBackend) signalEnqueued(a *storage.QueuedActivity) {
	if a.ScheduledAt == nil || !a.ScheduledAt.After(time.Now().UTC()) {
		b.signalWork()
	}
}

// enqueueStmts are returned rather than run so callers can make the enqueue
// atomic with other writes (e.g. the idempotency-key claim).
func (b *PostgresBackend) enqueueStmts(a *storage.QueuedActivity) ([]stmt, error) {
	status := "pending"
	if a.ScheduledAt != nil {
		status = "scheduled"
	}

	metadataJSON, err := json.Marshal(a.Metadata)
	if err != nil {
		return nil, storage.NewSerializationError(err.Error())
	}

	var idempotencyKey *string
	if a.IdempotencyKey != nil {
		idempotencyKey = &a.IdempotencyKey.Key
	}

	rootID := a.RootActivityID
	if rootID == (uuid.UUID{}) {
		rootID = a.ID
	}

	stmts := []stmt{{`
		WITH activity AS (
			INSERT INTO runnerq_activities (
				id, queue_name, activity_type, priority, status,
				created_at, scheduled_at, retry_count, max_retries,
				timeout_seconds, retry_delay_seconds, max_retry_delay_seconds,
				metadata, idempotency_key,
				parent_activity_id, root_activity_id, depth
			) VALUES ($1, $2, $3, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
		)
		INSERT INTO runnerq_inputs (activity_id, queue_name, payload, serialization)
		VALUES ($1, $2, COALESCE($4::jsonb, 'null'::jsonb), $19)`,
		[]any{a.ID, b.queueName, a.ActivityType, a.Payload,
			int32(a.Priority), status, a.CreatedAt, a.ScheduledAt,
			int32(a.RetryCount), int32(a.MaxRetries),
			int64(a.TimeoutSeconds), int64(a.RetryDelaySeconds),
			int64(a.MaxRetryDelaySeconds),
			metadataJSON, idempotencyKey,
			a.ParentActivityID, rootID, int16(a.Depth), storedSerialization(a.Serialization)},
		"Failed to enqueue activity"}}

	if a.ParentActivityID != nil {
		stmts = append(stmts, b.linkStmt(*a.ParentActivityID, a.ID))
	}

	var event stmt
	if a.ScheduledAt != nil {
		event = b.eventStmt(a.ID, storage.EventScheduled, nil, toDetail(map[string]any{
			"activity_type": a.ActivityType, "priority": int(a.Priority), "scheduled_at": a.ScheduledAt,
		}))
	} else {
		event = b.eventStmt(a.ID, storage.EventEnqueued, nil, toDetail(map[string]any{
			"activity_type": a.ActivityType, "priority": int(a.Priority),
		}))
	}
	return append(stmts, event), nil
}

// Single-row and batch claims share eligibility, ordering and RETURNING list;
// they differ in how many rows the SKIP LOCKED walk takes and how the token is
// formed. Each has three static forms so the planner picks the right index
// instead of one compromise plan for `($x IS NULL OR activity_type = ANY($x))`:
//
//   - no type filter → idx_runnerq_dequeue_order_v2 (key order == ORDER BY)
//   - one type       → idx_runnerq_dequeue_effective_v2 (type pinned by
//     equality, remaining key order == ORDER BY)
//   - many types     → idx_runnerq_dequeue_order_v2 with an in-scan type filter
//
// Every form is an ordered index walk stopping at the first unlocked row(s),
// never a sort over the backlog.
//
// Leases use the DATABASE clock: RequeueExpired compares the deadline against
// NOW(), so with app clocks a fast-clocked reaper would reclaim other nodes'
// live leases and double-execute. Deadline = max(default lease, timeout + 10s).
const (
	// `status IN (...) AND (status = 'pending' OR scheduled_at <= NOW())`, not
	// the equivalent OR-only form, deliberately: the first conjunct textually
	// matches the partial-index predicate, which the predicate prover needs to
	// offer an ORDERED scan. The OR-only form gets bitmap scans only, forcing a
	// top-1 sort of the whole backlog per claim (EXPLAIN ANALYZE, 200k rows:
	// external merge sort vs a 0.2ms index walk).
	// Go handlers take plain JSON only; other encodings (the TypeScript SDK's
	// superjson-v1) are left for a worker that can read them. One primary-key
	// probe per candidate row.
	claimEligibleSQL = `
			WHERE queue_name = $3
			  AND status IN ('pending', 'scheduled', 'retrying', 'waiting')
			  AND (status = 'pending' OR scheduled_at <= NOW())
			  AND NOT EXISTS (SELECT 1 FROM runnerq_inputs i
			      WHERE i.activity_id = runnerq_activities.id AND i.serialization <> 'json-v1')`
	claimOrderSQL = `
			ORDER BY
				priority DESC,
				retry_count DESC,
				COALESCE(scheduled_at, created_at) ASC`
	claimLeaseSQL = `(EXTRACT(EPOCH FROM NOW()) * 1000)::bigint + GREATEST($2::bigint, (timeout_seconds + 10) * 1000)`
	// Order must match scanClaim.
	claimColumnsSQL = `id, activity_type,
			(SELECT i.payload FROM runnerq_inputs i WHERE i.activity_id = a.id) AS payload,
			(SELECT i.serialization FROM runnerq_inputs i WHERE i.activity_id = a.id) AS serialization,
			priority, retry_count, max_retries,
			timeout_seconds, retry_delay_seconds, max_retry_delay_seconds,
			scheduled_at, metadata, idempotency_key, created_at,
			parent_activity_id, root_activity_id, depth, current_worker_id, lease_deadline_ms`
	// Records the Dequeued events in the claim statement itself ($4 is the
	// event time): one round trip, no explicit transaction.
	claimEventsSQL = `
		), dequeued AS (
			INSERT INTO runnerq_events (activity_id, queue_name, event_type, worker_id, detail, created_at)
			SELECT id, $3::text, '` + storage.EventDequeued + `', current_worker_id,
				jsonb_build_object('activity_type', activity_type, 'lease_deadline_ms', lease_deadline_ms), $4::timestamptz
			FROM claimed
		)
		SELECT * FROM claimed`

	// Single claim: $1 worker token, $2 default lease ms, $3 queue, $4 event
	// time, $5 type filter.
	dequeueSQLHead = `
		WITH claimed AS (
		UPDATE runnerq_activities AS a
		SET status = 'processing',
			current_worker_id = $1,
			started_at = NOW(),
			lease_deadline_ms = ` + claimLeaseSQL + `
		WHERE id = (
			SELECT id FROM runnerq_activities` + claimEligibleSQL
	dequeueSQLTail = claimOrderSQL + `
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING ` + claimColumnsSQL + claimEventsSQL

	dequeueSQLAllTypes  = dequeueSQLHead + dequeueSQLTail
	dequeueSQLOneType   = dequeueSQLHead + ` AND activity_type = $5` + dequeueSQLTail
	dequeueSQLManyTypes = dequeueSQLHead + ` AND activity_type = ANY($5)` + dequeueSQLTail

	// Batch claim: $1 token prefix, $2 default lease ms, $3 queue, $4 event
	// time, $5 limit, $6 type filter. Each row's token is prefix:id, so rows
	// are fenced independently. The subquery aliases its id to keep unqualified
	// columns in SET and RETURNING unambiguous.
	dequeueBatchSQLHead = `
		WITH claimed AS (
		UPDATE runnerq_activities AS a
		SET status = 'processing',
			current_worker_id = $1 || ':' || a.id::text,
			started_at = NOW(),
			lease_deadline_ms = ` + claimLeaseSQL + `
		FROM (
			SELECT id AS claim_id FROM runnerq_activities` + claimEligibleSQL
	dequeueBatchSQLTail = claimOrderSQL + `
			LIMIT $5
			FOR UPDATE SKIP LOCKED
		) AS c
		WHERE a.id = c.claim_id
		RETURNING ` + claimColumnsSQL + claimEventsSQL

	dequeueBatchSQLAllTypes  = dequeueBatchSQLHead + dequeueBatchSQLTail
	dequeueBatchSQLOneType   = dequeueBatchSQLHead + ` AND activity_type = $6` + dequeueBatchSQLTail
	dequeueBatchSQLManyTypes = dequeueBatchSQLHead + ` AND activity_type = ANY($6)` + dequeueBatchSQLTail

	// Must match claimEligibleSQL's text: the encoded claims replace it.
	claimPlainJSON = `i.serialization <> 'json-v1'`
)

// EncodedStorage batch claims take the caller's readable encodings as the
// last parameter.
var (
	dequeueBatchEncodedSQLAllTypes  = strings.Replace(dequeueBatchSQLAllTypes, claimPlainJSON, `i.serialization <> ALL($6::text[])`, 1)
	dequeueBatchEncodedSQLOneType   = strings.Replace(dequeueBatchSQLOneType, claimPlainJSON, `i.serialization <> ALL($7::text[])`, 1)
	dequeueBatchEncodedSQLManyTypes = strings.Replace(dequeueBatchSQLManyTypes, claimPlainJSON, `i.serialization <> ALL($7::text[])`, 1)
)

func scanClaim(row pgx.Row) (storage.DequeuedActivity, error) {
	var ar activityRow
	var leaseID string
	var leaseDeadlineMS int64
	if err := row.Scan(
		&ar.id, &ar.activityType, &ar.payload, &ar.serialization, &ar.priority,
		&ar.retryCount, &ar.maxRetries, &ar.timeoutSeconds,
		&ar.retryDelaySeconds, &ar.maxRetryDelaySeconds,
		&ar.scheduledAt, &ar.metadata,
		&ar.idempotencyKey, &ar.createdAt,
		&ar.parentActivityID, &ar.rootActivityID, &ar.depth,
		&leaseID, &leaseDeadlineMS); err != nil {
		return storage.DequeuedActivity{}, err
	}
	return storage.DequeuedActivity{
		Activity:      *ar.toQueuedActivity(),
		LeaseID:       leaseID,
		Attempt:       uint32(ar.retryCount) + 1,
		LeaseDeadline: time.UnixMilli(leaseDeadlineMS).UTC(),
	}, nil
}

// waitForClaim re-probes on each work signal and at least every
// workWaitProbe until probe claims, fails, or maxBlock expires (nil error).
func (b *PostgresBackend) waitForClaim(ctx context.Context, maxBlock time.Duration, probe func() (claimed bool, err error)) error {
	deadline := time.Now().Add(maxBlock)

	claimed, err := probe()
	if claimed || err != nil || maxBlock <= 0 {
		return err
	}

	w := b.getWatcher()
	ch := w.registerWork()
	defer w.unregisterWork(ch)
	probeTimer := time.NewTimer(workWaitProbe)
	defer probeTimer.Stop()

	for {
		// Re-probe after registering so a signal sent before it isn't missed.
		if claimed, err := probe(); claimed || err != nil {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		probeTimer.Reset(min(remaining, workWaitProbe))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		case <-probeTimer.C:
		}
	}
}

func (b *PostgresBackend) Dequeue(ctx context.Context, workerID string, maxBlock time.Duration, activityTypes []string) (*storage.QueuedActivity, error) {
	var claimed *storage.QueuedActivity
	err := b.waitForClaim(ctx, maxBlock, func() (bool, error) {
		var err error
		claimed, err = b.dequeueOnce(ctx, workerID, activityTypes)
		return claimed != nil, err
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

// claimContext is ctx for sending a claim statement, which commits on its own:
// once sent, its reply is read even if ctx ends, so rows it claimed reach the
// engine instead of waiting for lease recovery.
func claimContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	c, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimStatementTimeout)
	return c, cancel, nil
}

func (b *PostgresBackend) dequeueOnce(ctx context.Context, workerID string, activityTypes []string) (*storage.QueuedActivity, error) {
	ctx, cancel, err := claimContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	lease, now := b.defaultLeaseMS.Load(), time.Now().UTC()
	var row pgx.Row
	switch len(activityTypes) {
	case 0:
		row = b.pool.QueryRow(ctx, dequeueSQLAllTypes, workerID, lease, b.queueName, now)
	case 1:
		row = b.pool.QueryRow(ctx, dequeueSQLOneType, workerID, lease, b.queueName, now, activityTypes[0])
	default:
		row = b.pool.QueryRow(ctx, dequeueSQLManyTypes, workerID, lease, b.queueName, now, activityTypes)
	}

	claim, err := scanClaim(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, databaseError(err, fmt.Sprintf("Failed to dequeue: %v", err))
	}
	a := &claim.Activity
	slog.Debug("Activity claimed",
		"activity_id", a.ID,
		"activity_type", a.ActivityType,
		"priority", a.Priority)
	return a, nil
}

// DequeueBatch claims up to limit rows in one UPDATE, each with the token
// "<workerIDPrefix>:<activity id>".
func (b *PostgresBackend) DequeueBatch(ctx context.Context, workerIDPrefix string, limit int, maxBlock time.Duration, activityTypes []string) ([]storage.DequeuedActivity, error) {
	if limit <= 0 {
		return nil, nil
	}
	return b.dequeueBatch(ctx, workerIDPrefix, limit, maxBlock, activityTypes, nil)
}

func (b *PostgresBackend) DequeueBatchEncoded(ctx context.Context, workerIDPrefix string, limit int, maxBlock time.Duration, activityTypes []string, serializations []string) ([]storage.DequeuedActivity, error) {
	// An empty entry is plain JSON; no encodings claims nothing.
	if len(serializations) == 0 {
		return nil, nil
	}
	reads := make([]string, 0, len(serializations))
	for _, s := range serializations {
		reads = append(reads, storedSerialization(s))
	}
	return b.dequeueBatch(ctx, workerIDPrefix, limit, maxBlock, activityTypes, reads)
}

func (b *PostgresBackend) dequeueBatch(ctx context.Context, workerIDPrefix string, limit int, maxBlock time.Duration, activityTypes, reads []string) ([]storage.DequeuedActivity, error) {
	if limit <= 0 {
		return nil, nil
	}
	var claims []storage.DequeuedActivity
	err := b.waitForClaim(ctx, maxBlock, func() (bool, error) {
		var err error
		claims, err = b.dequeueBatchOnce(ctx, workerIDPrefix, limit, activityTypes, reads)
		return len(claims) > 0, err
	})
	if err != nil {
		return nil, err
	}
	return claims, nil
}

// reads nil means plain JSON only.
func (b *PostgresBackend) dequeueBatchOnce(ctx context.Context, workerIDPrefix string, limit int, activityTypes, reads []string) ([]storage.DequeuedActivity, error) {
	ctx, cancel, err := claimContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	var rows pgx.Rows
	lease, now := b.defaultLeaseMS.Load(), time.Now().UTC()
	switch {
	case reads == nil && len(activityTypes) == 0:
		rows, err = b.pool.Query(ctx, dequeueBatchSQLAllTypes, workerIDPrefix, lease, b.queueName, now, limit)
	case reads == nil && len(activityTypes) == 1:
		rows, err = b.pool.Query(ctx, dequeueBatchSQLOneType, workerIDPrefix, lease, b.queueName, now, limit, activityTypes[0])
	case reads == nil:
		rows, err = b.pool.Query(ctx, dequeueBatchSQLManyTypes, workerIDPrefix, lease, b.queueName, now, limit, activityTypes)
	case len(activityTypes) == 0:
		rows, err = b.pool.Query(ctx, dequeueBatchEncodedSQLAllTypes, workerIDPrefix, lease, b.queueName, now, limit, reads)
	case len(activityTypes) == 1:
		rows, err = b.pool.Query(ctx, dequeueBatchEncodedSQLOneType, workerIDPrefix, lease, b.queueName, now, limit, activityTypes[0], reads)
	default:
		rows, err = b.pool.Query(ctx, dequeueBatchEncodedSQLManyTypes, workerIDPrefix, lease, b.queueName, now, limit, activityTypes, reads)
	}
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to batch dequeue: %v", err))
	}
	claims, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (storage.DequeuedActivity, error) {
		return scanClaim(row)
	})
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to batch dequeue: %v", err))
	}
	if len(claims) == 0 {
		return nil, nil
	}
	// UPDATE ... RETURNING has no order: return the claims in claim order.
	slices.SortStableFunc(claims, func(x, y storage.DequeuedActivity) int {
		a, b := x.Activity, y.Activity
		if a.Priority != b.Priority {
			return cmp.Compare(b.Priority, a.Priority)
		}
		if a.RetryCount != b.RetryCount {
			return cmp.Compare(b.RetryCount, a.RetryCount)
		}
		return dueAt(a).Compare(dueAt(b))
	})
	slog.Debug("Activities claimed", "count", len(claims), "limit", limit)
	return claims, nil
}

// dueAt is when a claimed activity became due: its schedule, else its creation.
func dueAt(a storage.QueuedActivity) time.Time {
	if a.ScheduledAt != nil {
		return *a.ScheduledAt
	}
	return a.CreatedAt
}

func (b *PostgresBackend) AckSuccess(ctx context.Context, activityID uuid.UUID, result json.RawMessage, workerID string) error {
	return b.ackSuccess(ctx, activityID, result, "", workerID)
}

func (b *PostgresBackend) AckSuccessEncoded(ctx context.Context, activityID uuid.UUID, result json.RawMessage, serialization string, workerID string) error {
	return b.ackSuccess(ctx, activityID, result, serialization, workerID)
}

func (b *PostgresBackend) ackSuccess(ctx context.Context, activityID uuid.UUID, result json.RawMessage, serialization string, workerID string) error {
	now := time.Now().UTC()

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	var actType *string
	err = tx.QueryRow(ctx, `
		UPDATE runnerq_activities
		SET status = 'completed',
			completed_at = $1,
			last_worker_id = $2,
			current_worker_id = NULL,
			lease_deadline_ms = NULL
		WHERE id = $3 AND queue_name = $4
		  AND status = 'processing'
		  AND current_worker_id = $2
		RETURNING activity_type`,
		now, workerID, activityID, b.queueName).Scan(&actType)
	if err != nil {
		if err == pgx.ErrNoRows {
			// Reconcile a previous commit whose reply was lost.
			var same bool
			readErr := tx.QueryRow(ctx, `SELECT EXISTS (
                SELECT 1 FROM runnerq_activities a JOIN runnerq_results r ON r.activity_id = a.id
                WHERE a.id = $1 AND a.queue_name = $2 AND a.status = 'completed'
                  AND a.last_worker_id = $3 AND r.queue_name = $2 AND r.state = 'Ok'
                  AND r.data IS NOT DISTINCT FROM $4::jsonb AND r.serialization = $5)`,
				activityID, b.queueName, workerID, result, storedSerialization(serialization)).Scan(&same)
			if readErr != nil {
				return databaseError(readErr, "failed to reconcile completion")
			}
			if same {
				return nil
			}
			var ownCompletion bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runnerq_activities WHERE id=$1 AND queue_name=$2 AND status='completed' AND last_worker_id=$3)`, activityID, b.queueName, workerID).Scan(&ownCompletion); err != nil {
				return databaseError(err, "failed to classify completion conflict")
			}
			if ownCompletion {
				return &storage.StorageError{Kind: storage.ErrCheckpointConflict, Message: "execution already completed with a different result"}
			}
			return claimLost(activityID)
		}
		return databaseError(err, fmt.Sprintf("Failed to ack success: %v", err))
	}

	// Always store a result row, even for nil: awaiters key on its existence
	// and would otherwise hang. Same transaction, so completion and result are
	// atomic.
	ar := &storage.ActivityResult{Data: result, State: storage.ResultOk, Serialization: serialization}
	detail := toDetail(map[string]any{"result_stored": true})
	if err := execAll(ctx, tx, append(b.resultStmts(activityID, activityID, ar, now, ""),
		b.eventStmt(activityID, storage.EventCompleted, &workerID, detail))...); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return databaseError(err, fmt.Sprintf("Failed to commit ack: %v", err))
	}

	b.signalResult(activityID)
	b.signalWork()
	return nil
}

func (b *PostgresBackend) AckFailure(ctx context.Context, activityID uuid.UUID, failure storage.FailureKind, workerID string) (bool, error) {
	// Details are stored as JSON: reject invalid ones before changing anything.
	if len(failure.Details) > 0 && !json.Valid(failure.Details) {
		return false, &storage.StorageError{Kind: storage.ErrInvalidArgument, Message: "failure details must be JSON", Field: "failure.Details"}
	}
	now := time.Now().UTC()

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return false, databaseError(err, fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	var ar activityRow
	err = tx.QueryRow(ctx, `
		SELECT id, retry_count, max_retries, retry_delay_seconds, max_retry_delay_seconds
		FROM runnerq_activities
		WHERE id = $1 AND queue_name = $2 AND status = 'processing' AND current_worker_id = $3
		FOR UPDATE`,
		activityID, b.queueName, workerID).Scan(
		&ar.id, &ar.retryCount, &ar.maxRetries, &ar.retryDelaySeconds, &ar.maxRetryDelaySeconds)
	if err != nil {
		if err == pgx.ErrNoRows {
			var event string
			readErr := tx.QueryRow(ctx, `SELECT event_type FROM runnerq_events
                WHERE queue_name=$1 AND activity_id=$2 AND worker_id=$3 AND detail->>'error'=$4
                  AND ((NOT $5 AND event_type='Failed') OR ($5 AND event_type IN ('Retrying','DeadLetter')))
                ORDER BY id DESC LIMIT 1`, b.queueName, activityID, workerID, failure.Reason, failure.Retryable).Scan(&event)
			if readErr == nil {
				return event == storage.EventDeadLetter, nil
			}
			if readErr != pgx.ErrNoRows {
				return false, databaseError(readErr, "failed to reconcile failure acknowledgement")
			}
			return false, claimLost(activityID)
		}
		return false, databaseError(err, fmt.Sprintf("Failed to lock activity: %v", err))
	}

	errorMessage := failure.Reason
	if failure.Retryable && storage.AttemptsRemain(int(ar.retryCount), int(ar.maxRetries)) {
		retryDelay := storage.RetryDelaySeconds(int(ar.retryCount), ar.retryDelaySeconds, ar.maxRetryDelaySeconds)
		// Event-detail estimate only: scheduled_at uses the database clock
		// below, so retry timing is immune to app-clock skew.
		scheduledAt := now.Add(time.Duration(retryDelay) * time.Second)

		// started_at is cleared (the next claim sets it); last_error_at keeps
		// the failed attempt's time for the runs-list sort.
		if err := execAll(ctx, tx, stmt{`
			UPDATE runnerq_activities
			SET status = 'retrying',
				retry_count = retry_count + 1,
				scheduled_at = NOW() + make_interval(secs => $1),
				last_error = $2,
				last_error_at = $3,
				last_worker_id = $4,
				current_worker_id = NULL,
				lease_deadline_ms = NULL,
				started_at = NULL
			WHERE id = $5 AND queue_name = $6 AND status = 'processing' AND current_worker_id = $4`,
			[]any{retryDelay, errorMessage, now, workerID, activityID, b.queueName},
			"Failed to schedule retry"},
			b.eventStmt(activityID, storage.EventRetrying, &workerID, toDetail(withFailure(map[string]any{
				"retry_count":  ar.retryCount + 1,
				"scheduled_at": scheduledAt.Format(time.RFC3339),
				"error":        errorMessage,
			}, failure.Details)))); err != nil {
			return false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return false, databaseError(err, fmt.Sprintf("Failed to commit ack_failure retry: %v", err))
		}
		if retryDelay == 0 {
			// Delayed retries are found by blocked dequeuers' periodic probes.
			b.signalWork()
		}
		return false, nil
	}

	// Terminal: non-retryable fails; retryable but out of attempts dead-letters.
	deadLetter := failure.Retryable
	status, resultType, eventType := "failed", "non_retryable", storage.EventFailed
	eventDetail := map[string]any{"error": errorMessage}
	if deadLetter {
		status, resultType, eventType = "dead_letter", "dead_letter", storage.EventDeadLetter
		eventDetail["reason"] = "attempts_exhausted"
	}
	res := &storage.ActivityResult{Data: toDetail(withFailure(map[string]any{
		"error":     errorMessage,
		"type":      resultType,
		"failed_at": now.Format(time.RFC3339),
	}, failure.Details)), State: storage.ResultErr}
	stmts := []stmt{{`
		UPDATE runnerq_activities
		SET status = $7,
			completed_at = $1,
			last_error = $2,
			last_error_at = $3,
			last_worker_id = $4,
			current_worker_id = NULL,
			lease_deadline_ms = NULL
		WHERE id = $5 AND queue_name = $6 AND status = 'processing' AND current_worker_id = $4`,
		[]any{now, errorMessage, now, workerID, activityID, b.queueName, status},
		"Failed to record terminal failure"}}
	stmts = append(stmts, b.resultStmts(activityID, activityID, res, now, "")...)
	stmts = append(stmts, b.eventStmt(activityID, eventType, &workerID, toDetail(withFailure(eventDetail, failure.Details))))
	if err := execAll(ctx, tx, stmts...); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, databaseError(err, fmt.Sprintf("Failed to commit ack_failure: %v", err))
	}
	b.signalResult(activityID)
	b.signalWork()
	return deadLetter, nil
}

func (b *PostgresBackend) ProcessScheduled(_ context.Context) (uint64, error) {
	return 0, nil
}

func (b *PostgresBackend) SchedulesNatively() bool {
	return true
}

func (b *PostgresBackend) RequeueExpired(ctx context.Context, batchSize int) (uint64, error) {
	now := time.Now().UTC()
	const leaseExpiredError = "lease expired before completion; worker presumed crashed or wedged"

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return 0, databaseError(err, fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	// A lease expiry counts as a failed attempt (dead-lettering when exhausted,
	// as in AckFailure); otherwise a handler that crashes its process would be
	// reclaimed and re-crash workers forever. started_at is cleared so pending
	// rows don't look running.
	rows, err := tx.Query(ctx, `
		UPDATE runnerq_activities
		SET retry_count = retry_count + CASE WHEN max_retries > 0 AND retry_count + 1 >= max_retries
				THEN 0 ELSE 1 END,
			status = CASE WHEN max_retries > 0 AND retry_count + 1 >= max_retries
				THEN 'dead_letter' ELSE 'pending' END,
			completed_at = CASE WHEN max_retries > 0 AND retry_count + 1 >= max_retries
				THEN $3::timestamptz ELSE completed_at END,
			last_error = $4,
			last_error_at = $3,
			last_worker_id = current_worker_id,
			current_worker_id = NULL,
			lease_deadline_ms = NULL,
			started_at = NULL
		WHERE id IN (
			SELECT id FROM runnerq_activities
			WHERE queue_name = $1
			  AND status = 'processing'
			  AND lease_deadline_ms < (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, status, retry_count`,
		b.queueName, batchSize, now, leaseExpiredError)
	if err != nil {
		return 0, databaseError(err, fmt.Sprintf("Failed to requeue expired: %v", err))
	}

	type reapedRow struct {
		id         uuid.UUID
		deadLetter bool
		retryCount int32
	}
	var reaped []reapedRow
	for rows.Next() {
		var r reapedRow
		var status string
		if err := rows.Scan(&r.id, &status, &r.retryCount); err != nil {
			rows.Close()
			return 0, databaseError(err, fmt.Sprintf("Failed to scan requeued row: %v", err))
		}
		r.deadLetter = status == "dead_letter"
		reaped = append(reaped, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, databaseError(err, fmt.Sprintf("Failed to read requeued rows: %v", err))
	}
	if len(reaped) == 0 {
		return 0, nil
	}

	var stmts []stmt
	for _, r := range reaped {
		if r.deadLetter {
			// The error result resolves awaiters. OnDeadLetter doesn't fire for
			// reaper dead-letters: there is no live handler to call it on.
			res := &storage.ActivityResult{Data: toDetail(map[string]any{
				"error":     leaseExpiredError,
				"type":      "dead_letter",
				"failed_at": now.Format(time.RFC3339),
			}), State: storage.ResultErr}
			stmts = append(stmts, b.resultStmts(r.id, r.id, res, now, "")...)
			stmts = append(stmts, b.eventStmt(r.id, storage.EventDeadLetter, nil,
				toDetail(map[string]any{"error": leaseExpiredError, "reason": "lease_expired"})))
			continue
		}
		stmts = append(stmts, b.eventStmt(r.id, storage.EventRequeued, nil,
			toDetail(map[string]any{"retry_count": r.retryCount, "reason": "lease_expired", "error": leaseExpiredError})))
	}
	if err := execAll(ctx, tx, stmts...); err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, databaseError(err, fmt.Sprintf("Failed to commit requeue expired: %v", err))
	}

	for _, r := range reaped {
		if r.deadLetter {
			b.signalResult(r.id)
		}
	}
	if len(reaped) > 0 {
		b.signalWork()
	}
	return uint64(len(reaped)), nil
}

// Yield parks a processing activity as 'waiting' until wakeAt: a durable
// continuation, not a failure, so retry_count is untouched. Fenced on workerID
// so a stale worker can't park a reclaimed row.
func (b *PostgresBackend) Yield(ctx context.Context, activityID uuid.UUID, wakeAt time.Time, workerID, kind, step string) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE runnerq_activities
		SET status = 'waiting',
			scheduled_at = $1,
			waiting_result_id = NULL,
			last_worker_id = $2,
			current_worker_id = NULL,
			lease_deadline_ms = NULL,
			started_at = NULL
		WHERE id = $3 AND queue_name = $4
		  AND status = 'processing'
		  AND current_worker_id = $2`,
		wakeAt, workerID, activityID, b.queueName)
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to yield activity: %v", err))
	}
	if tag.RowsAffected() == 0 {
		var recorded bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runnerq_events WHERE queue_name=$1 AND activity_id=$2 AND worker_id=$3 AND event_type='Yielded' AND COALESCE(detail->>'kind','')=$4 AND COALESCE(detail->>'step','')=$5 AND detail->>'wake_at'=$6)`, b.queueName, activityID, workerID, kind, step, wakeAt.Format(time.RFC3339)).Scan(&recorded); err != nil {
			return databaseError(err, "failed to reconcile park")
		}
		if recorded {
			return nil
		}
		return claimLost(activityID)
	}

	// kind/step let the console show why it is waiting.
	yieldDetail := map[string]any{"wake_at": wakeAt.Format(time.RFC3339), "result_id": nil, "ready": false}
	if kind != "" {
		yieldDetail["kind"] = kind
	}
	if step != "" {
		yieldDetail["step"] = step
	}
	if err := b.recordEvent(ctx, tx, activityID, storage.EventYielded, &workerID,
		toDetail(yieldDetail)); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return databaseError(err, fmt.Sprintf("Failed to commit yield: %v", err))
	}

	if !wakeAt.After(time.Now().UTC()) {
		b.signalWork()
	}
	return nil
}

// SignalActivity stores the payload under signalID (owned by the target for
// retention) and, atomically, wakes the target if it is parked as 'waiting'.
func (b *PostgresBackend) SignalActivity(ctx context.Context, activityID uuid.UUID, signalID uuid.UUID, name string, payload json.RawMessage) error {
	return b.signalActivity(ctx, activityID, signalID, name, payload, "")
}

func (b *PostgresBackend) SignalActivityEncoded(ctx context.Context, activityID uuid.UUID, signalID uuid.UUID, name string, payload json.RawMessage, serialization string) error {
	return b.signalActivity(ctx, activityID, signalID, name, payload, serialization)
}

func (b *PostgresBackend) signalActivity(ctx context.Context, activityID uuid.UUID, signalID uuid.UUID, name string, payload json.RawMessage, serialization string) error {
	now := time.Now().UTC()

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	// A signal for a missing activity would never be retention-collected, so
	// reject it. The lookup is also storeResultTx's owner lock: a target
	// mid-park holds FOR UPDATE, so this waits and the wake below sees the park.
	var found int
	err = tx.QueryRow(ctx, `
		SELECT 1 FROM runnerq_activities WHERE id = $1 AND queue_name = $2 FOR NO KEY UPDATE`,
		activityID, b.queueName).Scan(&found)
	if err == pgx.ErrNoRows {
		return storage.NewNotFoundError(fmt.Sprintf("Activity %s not found for signal delivery", activityID))
	}
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to look up signal target: %v", err))
	}

	signalStep := ""
	if name != "" {
		signalStep = "signal:" + name
	}
	res := &storage.ActivityResult{Data: payload, State: storage.ResultOk, Serialization: serialization}
	if err := b.storeResultTx(ctx, tx, signalID, activityID, res, now, signalStep); err != nil {
		return err
	}

	// Only 'waiting' is woken: running handlers see the stored signal when
	// they reach WaitForSignal; scheduled/retrying rows keep their schedule.
	tag, err := tx.Exec(ctx, `
		UPDATE runnerq_activities
		SET status = 'pending', scheduled_at = NULL, waiting_result_id = NULL
		WHERE id = $1 AND queue_name = $2 AND status = 'waiting'`,
		activityID, b.queueName)
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to wake signaled activity: %v", err))
	}
	woke := tag.RowsAffected() > 0

	signalDetail := map[string]any{"signal_id": signalID, "woke": woke}
	if name != "" {
		signalDetail["name"] = name
	}
	if err := b.recordEvent(ctx, tx, activityID, storage.EventSignaled, nil,
		toDetail(signalDetail)); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return databaseError(err, fmt.Sprintf("Failed to commit signal: %v", err))
	}

	b.signalResult(signalID)
	if woke {
		b.signalWork()
	}
	return nil
}

// WakeWaiting closes the park race: a result committed between a handler's
// last check and its park would otherwise never wake it.
func (b *PostgresBackend) WakeWaiting(ctx context.Context, activityID uuid.UUID) (bool, error) {
	tag, err := b.pool.Exec(ctx, `
		UPDATE runnerq_activities
		SET status = 'pending', scheduled_at = NULL, waiting_result_id = NULL
		WHERE id = $1 AND queue_name = $2 AND status = 'waiting'`,
		activityID, b.queueName)
	if err != nil {
		return false, databaseError(err, fmt.Sprintf("Failed to wake waiting activity: %v", err))
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	b.signalWork()
	return true, nil
}

func (b *PostgresBackend) LookupIdempotencyActivityID(ctx context.Context, idempotencyKey string) (uuid.UUID, error) {
	if legacy, activityType, encoded := storage.LegacyBusinessIdempotencyKey(idempotencyKey); encoded {
		var id uuid.UUID
		err := b.pool.QueryRow(ctx, `SELECT i.activity_id FROM runnerq_idempotency i
   JOIN runnerq_activities a ON a.id=i.activity_id AND a.queue_name=i.queue_name
   WHERE i.queue_name=$1 AND i.idempotency_key IN ($2,$3) AND a.activity_type=$4
   ORDER BY (i.idempotency_key=$2) DESC LIMIT 1`, b.queueName, idempotencyKey, legacy, activityType).Scan(&id)
		if err == pgx.ErrNoRows {
			return uuid.Nil, storage.NewNotFoundError("no activity owns this business key and type")
		}
		if err != nil {
			return uuid.Nil, databaseError(err, "failed to look up business key")
		}
		return id, nil
	}

	var id uuid.UUID
	err := b.pool.QueryRow(ctx, `
		SELECT activity_id FROM runnerq_idempotency
		WHERE queue_name = $1 AND idempotency_key = $2`,
		b.queueName, idempotencyKey).Scan(&id)
	if err != nil {
		if err == pgx.ErrNoRows {
			return uuid.Nil, storage.NewNotFoundError(fmt.Sprintf("no activity owns idempotency key %q", idempotencyKey))
		}
		return uuid.Nil, databaseError(err, fmt.Sprintf("Failed to look up idempotency key: %v", err))
	}
	return id, nil
}

// cleanupLockClass elects one retention sweeper per queue across processes.
// Arbitrary but must stay stable.
const cleanupLockClass = int32(1381913428) // "RQCT"

// CleanupExpired deletes whole trees whose root is terminal past its TTL with
// no live descendants. Per-row deletion would corrupt retries: a parent whose
// completed child vanished would re-run it via idempotency orphan repair.
// Checkpoint results (synthetic IDs) go via owner_activity_id. Non-leader
// sweepers return 0. Cutoffs use the database clock.
func (b *PostgresBackend) CleanupExpired(ctx context.Context, policy storage.RetentionPolicy, batchSize int) (uint64, error) {
	if policy.Completed <= 0 && policy.Failed <= 0 {
		return 0, nil
	}
	if batchSize <= 0 {
		batchSize = 100
	}

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return 0, databaseError(err, fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	var leader bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1::int4, hashtext($2))`,
		cleanupLockClass, b.queueName).Scan(&leader); err != nil {
		return 0, databaseError(err, fmt.Sprintf("Failed to acquire cleanup lock: %v", err))
	}
	if !leader {
		return 0, nil
	}

	// One root row at a time: dependency writers hold it FOR KEY SHARE, so a
	// registration either commits first or sees the producer gone. The
	// savepoint releases a pinned root at once; only deleted roots stay
	// locked until commit.
	var deletedRoots int64
	skippedRoots := make([]uuid.UUID, 0)
	for deletedRoots < int64(batchSize) {
		if _, err := tx.Exec(ctx, "SAVEPOINT cleanup_candidate"); err != nil {
			return 0, databaseError(err, "failed to start cleanup candidate")
		}

		var rootID uuid.UUID
		err := tx.QueryRow(ctx, `
			SELECT r.id
			FROM runnerq_activities r
			WHERE r.queue_name = $1
			  AND r.parent_activity_id IS NULL
			  AND r.id <> ALL($4::uuid[])
			  AND (
				(r.status = 'completed' AND $2::bigint > 0
					AND r.completed_at < NOW() - make_interval(secs => $2))
				OR (r.status IN ('failed', 'dead_letter', 'cancelled') AND $3::bigint > 0
					AND r.completed_at < NOW() - make_interval(secs => $3))
			  )
			  AND NOT EXISTS (
				SELECT 1 FROM runnerq_activities c
				WHERE c.queue_name = $1
				  AND c.root_activity_id = r.id
				  AND c.status NOT IN ('completed', 'failed', 'dead_letter', 'cancelled')
			  )
			ORDER BY r.completed_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED`,
			b.queueName,
			int64(policy.Completed/time.Second),
			int64(policy.Failed/time.Second),
			skippedRoots).Scan(&rootID)
		if err == pgx.ErrNoRows {
			if _, releaseErr := tx.Exec(ctx, "RELEASE SAVEPOINT cleanup_candidate"); releaseErr != nil {
				return 0, databaseError(releaseErr, "failed to release empty cleanup candidate")
			}
			break
		}
		if err != nil {
			return 0, databaseError(err, "failed to select expired workflow root")
		}

		pinned, err := b.lockTreeForDeleteTx(ctx, tx, rootID)
		if err != nil {
			return 0, err
		}
		if pinned {
			skippedRoots = append(skippedRoots, rootID)
			if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT cleanup_candidate"); err != nil {
				return 0, databaseError(err, "failed to release pinned cleanup candidate")
			}
			if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT cleanup_candidate"); err != nil {
				return 0, databaseError(err, "failed to finish pinned cleanup candidate")
			}
			continue
		}

		deleted, err := b.deleteTreeTx(ctx, tx, rootID)
		if err != nil {
			return 0, databaseError(err, "failed to clean up expired workflow tree")
		}
		if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT cleanup_candidate"); err != nil {
			return 0, databaseError(err, "failed to finish cleanup candidate")
		}
		if deleted > 0 {
			deletedRoots++
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, databaseError(err, fmt.Sprintf("Failed to commit cleanup: %v", err))
	}
	return uint64(deletedRoots), nil
}

func (b *PostgresBackend) ExtendLease(ctx context.Context, activityID uuid.UUID, extendBy time.Duration) (bool, error) {
	if extendBy <= 0 {
		return false, storage.NewConfigurationError("lease extension must be positive")
	}
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return false, databaseError(err, fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	// Database clock: see the claim SQL comment.
	var newDeadlineMS int64
	err = tx.QueryRow(ctx, `
		UPDATE runnerq_activities
		SET lease_deadline_ms = GREATEST(lease_deadline_ms, (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint + $1)
		WHERE id = $2 AND queue_name = $3 AND status = 'processing'
		RETURNING lease_deadline_ms`,
		extendBy.Milliseconds(), activityID, b.queueName).Scan(&newDeadlineMS)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, databaseError(err, fmt.Sprintf("Failed to extend lease: %v", err))
	}

	if err := b.recordEvent(ctx, tx, activityID, storage.EventLeaseExtended, nil,
		toDetail(map[string]any{
			"new_deadline_ms": newDeadlineMS,
			"extend_by_ms":    extendBy.Milliseconds(),
		})); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, databaseError(err, fmt.Sprintf("Failed to commit extend_lease: %v", err))
	}
	return true, nil
}

func (b *PostgresBackend) RecordSpawnLinked(ctx context.Context, childID, parentID uuid.UUID) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	detail := toDetail(map[string]any{"parent_activity_id": parentID})
	if err := b.recordEvent(ctx, tx, childID, storage.EventSpawnLinked, nil, detail); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return databaseError(err, fmt.Sprintf("Failed to commit spawn_linked: %v", err))
	}
	return nil
}

func (b *PostgresBackend) StoreResult(ctx context.Context, activityID uuid.UUID, ownerActivityID uuid.UUID, result storage.ActivityResult, step string) error {
	stateStr := "Ok"
	if result.State == storage.ResultErr {
		stateStr = "Err"
	}

	slog.Debug("Storing activity result",
		"activity_id", activityID,
		"owner_activity_id", ownerActivityID,
		"state", stateStr,
		"has_data", result.Data != nil)

	now := time.Now().UTC()
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	// No claim UPDATE here, so take storeResultTx's owner lock explicitly. A
	// missing owner (legacy rows) has nothing parked on it to miss.
	var found int
	err = tx.QueryRow(ctx, `
		SELECT 1 FROM runnerq_activities WHERE id = $1 AND queue_name = $2 FOR NO KEY UPDATE`,
		ownerActivityID, b.queueName).Scan(&found)
	if err != nil && err != pgx.ErrNoRows {
		return databaseError(err, fmt.Sprintf("Failed to lock result owner: %v", err))
	}
	if err := b.storeResultTx(ctx, tx, activityID, ownerActivityID, &result, now, step); err != nil {
		return err
	}
	if err := b.recordEvent(ctx, tx, activityID, storage.EventResultStored, nil,
		toDetail(map[string]any{"state": stateStr})); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return databaseError(err, fmt.Sprintf("Failed to commit store_result: %v", err))
	}

	b.signalResult(activityID)
	b.signalWork()

	slog.Debug("Activity result stored", "activity_id", activityID)
	return nil
}

func (b *PostgresBackend) GetResult(ctx context.Context, activityID uuid.UUID) (*storage.ActivityResult, error) {
	var stateStr, serialization string
	var data json.RawMessage

	err := b.pool.QueryRow(ctx, `SELECT state, data, serialization FROM runnerq_results WHERE activity_id = $1 AND queue_name = $2`, activityID, b.queueName).Scan(&stateStr, &data, &serialization)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, databaseError(err, fmt.Sprintf("Failed to get result: %v", err))
	}

	state := storage.ResultOk
	if stateStr != "Ok" {
		state = storage.ResultErr
	}

	return &storage.ActivityResult{Data: data, State: state, Serialization: reportedSerialization(serialization)}, nil
}

func (b *PostgresBackend) EnqueueIdempotent(ctx context.Context, a *storage.QueuedActivity) (*storage.IdempotencyResult, error) {
	return b.enqueueIdempotent(ctx, a, nil)
}

func (b *PostgresBackend) enqueueIdempotent(ctx context.Context, a *storage.QueuedActivity, fence *spawnFence) (*storage.IdempotencyResult, error) {
	if a.IdempotencyKey == nil {
		return nil, b.enqueue(ctx, *a, fence)
	}
	key := a.IdempotencyKey.Key
	behavior := a.IdempotencyKey.Behavior

	const maxAttempts = 3
	for range maxAttempts {
		result, done, err := b.tryEnqueueIdempotent(ctx, a, key, behavior, fence)
		if err != nil {
			return nil, err
		}
		if done {
			return result, nil
		}
		// The key row vanished between the INSERT conflict and the lock.
	}
	return nil, storage.NewInternalError(fmt.Sprintf("Failed to resolve idempotency key '%s' after %d attempts", key, maxAttempts))
}

// tryEnqueueIdempotent claims the key and enqueues in one transaction, so a
// key never points at an activity that was never enqueued. done=false means
// the key row vanished mid-attempt; retry.
func (b *PostgresBackend) tryEnqueueIdempotent(ctx context.Context, a *storage.QueuedActivity, key string, behavior storage.IdempotencyBehavior, fence *spawnFence) (*storage.IdempotencyResult, bool, error) {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, false, databaseError(err, fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	// Lock order: the spawner's own row, then the idempotency key row.
	if err := b.verifySpawnFenceTx(ctx, tx, fence); err != nil {
		return nil, false, err
	}

	resolvedKey, err := b.resolveBusinessKeyTx(ctx, tx, key, a.ActivityType)
	if err != nil {
		return nil, false, err
	}
	key = resolvedKey
	copied := *a
	copiedKey := *a.IdempotencyKey
	copiedKey.Key = key
	copied.IdempotencyKey = &copiedKey
	a = &copied
	enqueueTx := func() error {
		stmts, err := b.enqueueStmts(a)
		if err != nil {
			return err
		}
		return execAll(ctx, tx, stmts...)
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO runnerq_idempotency (queue_name, idempotency_key, activity_id, created_at, updated_at)
		VALUES ($1, $2, $3, NOW(), NOW())
		ON CONFLICT (queue_name, idempotency_key) DO NOTHING`,
		b.queueName, key, a.ID)
	if err != nil {
		return nil, false, databaseError(err, fmt.Sprintf("Failed to claim idempotency key: %v", err))
	}

	if tag.RowsAffected() > 0 {
		if err := enqueueTx(); err != nil {
			return nil, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, databaseError(err, fmt.Sprintf("Failed to commit idempotent enqueue: %v", err))
		}
		b.signalEnqueued(a)
		return nil, true, nil
	}

	// Lock the key row so behavior resolution can't race a concurrent claimer.
	// FOR UPDATE OF i is valid: i is the non-nullable side of the join.
	var existingID uuid.UUID
	var existingParentID *uuid.UUID
	var status *string
	err = tx.QueryRow(ctx, `
		SELECT i.activity_id, a.status, a.parent_activity_id
		FROM runnerq_idempotency i
		LEFT JOIN runnerq_activities a ON i.activity_id = a.id
		WHERE i.queue_name = $1 AND i.idempotency_key = $2
		FOR UPDATE OF i`,
		b.queueName, key).Scan(&existingID, &status, &existingParentID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, databaseError(err, fmt.Sprintf("Failed to get existing key: %v", err))
	}

	reclaim := func() (*storage.IdempotencyResult, bool, error) {
		if _, err := tx.Exec(ctx, `
			UPDATE runnerq_idempotency
			SET activity_id = $1, updated_at = NOW()
			WHERE queue_name = $2 AND idempotency_key = $3`,
			a.ID, b.queueName, key); err != nil {
			return nil, false, databaseError(err, fmt.Sprintf("Failed to update key: %v", err))
		}
		if err := enqueueTx(); err != nil {
			return nil, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, databaseError(err, fmt.Sprintf("Failed to commit idempotent enqueue: %v", err))
		}
		b.signalEnqueued(a)
		return nil, true, nil
	}

	// Orphan repair: older versions claimed the key and enqueued in separate
	// transactions, so a crash could leave a key with no activity. Reclaim it
	// regardless of behavior, or ReturnExisting would hand out a future that
	// never resolves.
	if status == nil {
		return reclaim()
	}

	switch behavior {
	case storage.BehaviorReturnExisting:
		if a.ParentActivityID != nil {
			if err := execAll(ctx, tx, b.linkStmt(*a.ParentActivityID, existingID)); err != nil {
				return nil, false, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, databaseError(err, "failed to commit reused dependency")
		}
		return &storage.IdempotencyResult{ExistingID: existingID, ExistingParentID: existingParentID}, true, nil

	case storage.BehaviorAllowReuse:
		return reclaim()

	case storage.BehaviorAllowReuseOnFailure:
		if *status == "dead_letter" || *status == "failed" || *status == "cancelled" {
			return reclaim()
		}
		return nil, false, storage.NewIdempotencyConflictError(fmt.Sprintf("Idempotency key '%s' exists with status '%s'", key, *status))

	case storage.BehaviorNoReuse:
		return nil, false, storage.NewDuplicateActivityError(fmt.Sprintf("Activity with idempotency key '%s' already exists: %s", key, existingID))

	default:
		return reclaim()
	}
}

func (b *PostgresBackend) ListDeadLetter(ctx context.Context, offset, limit int) ([]storage.DeadLetterRecord, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type,
			(SELECT i.payload FROM runnerq_inputs i WHERE i.activity_id = runnerq_activities.id),
			priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms,
			parent_activity_id, root_activity_id, depth
		FROM runnerq_activities
		WHERE queue_name = $1 AND status = 'dead_letter'
		ORDER BY completed_at DESC
		LIMIT $2 OFFSET $3`,
		b.queueName, limit, offset)
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to list dead letter: %v", err))
	}
	defer rows.Close()

	snapshots, err := b.scanSnapshots(rows)
	if err != nil {
		return nil, err
	}

	var records []storage.DeadLetterRecord
	for _, s := range snapshots {
		errorStr := ""
		if s.LastError != nil {
			errorStr = *s.LastError
		}
		failedAt := time.Now().UTC()
		if s.CompletedAt != nil {
			failedAt = *s.CompletedAt
		}
		records = append(records, storage.DeadLetterRecord{
			Activity: s,
			Error:    errorStr,
			FailedAt: failedAt,
		})
	}

	return records, nil
}

func (b *PostgresBackend) GetActivity(ctx context.Context, activityID uuid.UUID) (*storage.ActivitySnapshot, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type,
			(SELECT i.payload FROM runnerq_inputs i WHERE i.activity_id = runnerq_activities.id),
			priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms,
			parent_activity_id, root_activity_id, depth
		FROM runnerq_activities
		WHERE id = $1 AND queue_name = $2`,
		activityID, b.queueName)
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to get activity: %v", err))
	}
	defer rows.Close()

	snapshots, err := b.scanSnapshots(rows)
	if err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return nil, nil
	}
	return &snapshots[0], nil
}

func (b *PostgresBackend) GetActivityEvents(ctx context.Context, activityID uuid.UUID, limit int) ([]storage.ActivityEvent, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT activity_id, event_type, worker_id, detail, created_at
		FROM runnerq_events
		WHERE activity_id = $1
		ORDER BY created_at ASC
		LIMIT $2`,
		activityID, limit)
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to get events: %v", err))
	}
	defer rows.Close()

	var events []storage.ActivityEvent
	for rows.Next() {
		var e storage.ActivityEvent
		var eventTypeRaw string
		err := rows.Scan(&e.ActivityID, &eventTypeRaw, &e.WorkerID, &e.Detail, &e.Timestamp)
		if err != nil {
			return nil, databaseError(err, fmt.Sprintf("Failed to scan event: %v", err))
		}
		// Legacy rows store the type JSON-encoded ("\"Enqueued\"").
		var et string
		if err := json.Unmarshal([]byte(eventTypeRaw), &et); err != nil {
			et = strings.Trim(eventTypeRaw, "\"")
		}
		e.EventType = et
		events = append(events, e)
	}

	return events, nil
}

func (b *PostgresBackend) GetChildren(ctx context.Context, parentID uuid.UUID, offset, limit int) ([]storage.ActivitySnapshot, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type,
			(SELECT i.payload FROM runnerq_inputs i WHERE i.activity_id = runnerq_activities.id),
			priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms,
			parent_activity_id, root_activity_id, depth
		FROM runnerq_activities
		WHERE queue_name = $1 AND parent_activity_id = $2
		ORDER BY created_at ASC
		LIMIT $3 OFFSET $4`,
		b.queueName, parentID, limit, offset)
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to list children: %v", err))
	}
	defer rows.Close()

	return b.scanSnapshots(rows)
}

// GetActivitySteps is console-only; it uses idx_runnerq_results_owner. step is
// stored as "kind:name".
func (b *PostgresBackend) GetActivitySteps(ctx context.Context, ownerActivityID uuid.UUID) ([]storage.StepRecord, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT step, state, data, created_at
		FROM runnerq_results
		WHERE queue_name = $1 AND owner_activity_id = $2 AND step IS NOT NULL
		ORDER BY created_at ASC`,
		b.queueName, ownerActivityID)
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to get activity steps: %v", err))
	}
	defer rows.Close()

	var steps []storage.StepRecord
	for rows.Next() {
		var step, stateStr string
		var data json.RawMessage
		var createdAt time.Time
		if err := rows.Scan(&step, &stateStr, &data, &createdAt); err != nil {
			return nil, databaseError(err, fmt.Sprintf("Failed to scan step: %v", err))
		}
		kind, name := step, ""
		if i := strings.IndexByte(step, ':'); i >= 0 {
			kind, name = step[:i], step[i+1:]
		}
		state := storage.ResultOk
		if stateStr != "Ok" {
			state = storage.ResultErr
		}
		steps = append(steps, storage.StepRecord{
			Kind: kind, Name: name, State: state, Data: data, CreatedAt: createdAt,
		})
	}
	return steps, nil
}

func (b *PostgresBackend) GetSubtree(ctx context.Context, rootID uuid.UUID) ([]storage.ActivitySnapshot, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type,
			(SELECT i.payload FROM runnerq_inputs i WHERE i.activity_id = runnerq_activities.id),
			priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms,
			parent_activity_id, root_activity_id, depth
		FROM runnerq_activities
		WHERE queue_name = $1 AND root_activity_id = $2
		ORDER BY depth ASC, created_at ASC`,
		b.queueName, rootID)
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to get subtree: %v", err))
	}
	defer rows.Close()

	return b.scanSnapshots(rows)
}

type activityRow struct {
	id                   uuid.UUID
	activityType         string
	payload              json.RawMessage
	serialization        *string
	priority             int32
	status               string
	createdAt            time.Time
	scheduledAt          *time.Time
	startedAt            *time.Time
	completedAt          *time.Time
	currentWorkerID      *string
	lastWorkerID         *string
	retryCount           int32
	maxRetries           int32
	timeoutSeconds       int64
	retryDelaySeconds    int64
	maxRetryDelaySeconds int64
	lastError            *string
	lastErrorAt          *time.Time
	metadata             json.RawMessage
	idempotencyKey       *string
	leaseDeadlineMS      *int64
	parentActivityID     *uuid.UUID
	rootActivityID       *uuid.UUID
	depth                int16
}

func (r *activityRow) toQueuedActivity() *storage.QueuedActivity {
	meta := make(map[string]string)
	if r.metadata != nil {
		_ = json.Unmarshal(r.metadata, &meta)
	}

	var idempKey *storage.IdempotencyKeyConfig
	if r.idempotencyKey != nil {
		idempKey = &storage.IdempotencyKeyConfig{
			Key:      *r.idempotencyKey,
			Behavior: storage.BehaviorReturnExisting,
		}
	}

	rootID := r.id
	if r.rootActivityID != nil {
		rootID = *r.rootActivityID
	}
	var serialization string
	if r.serialization != nil {
		serialization = reportedSerialization(*r.serialization)
	}
	return &storage.QueuedActivity{
		ID:                   r.id,
		ActivityType:         r.activityType,
		Payload:              r.payload,
		Serialization:        serialization,
		Priority:             intToPriority(r.priority),
		MaxRetries:           uint32(r.maxRetries),
		RetryCount:           uint32(r.retryCount),
		TimeoutSeconds:       uint64(r.timeoutSeconds),
		RetryDelaySeconds:    uint64(r.retryDelaySeconds),
		MaxRetryDelaySeconds: uint64(r.maxRetryDelaySeconds),
		ScheduledAt:          r.scheduledAt,
		Metadata:             meta,
		IdempotencyKey:       idempKey,
		CreatedAt:            r.createdAt,
		ParentActivityID:     r.parentActivityID,
		RootActivityID:       rootID,
		Depth:                uint16(r.depth),
	}
}

func (r *activityRow) toSnapshot() storage.ActivitySnapshot {
	meta := make(map[string]string)
	if r.metadata != nil {
		_ = json.Unmarshal(r.metadata, &meta)
	}

	var status string
	switch r.status {
	case "pending":
		status = "Pending"
	case "processing":
		status = "Running"
	case "completed":
		status = "Completed"
	case "scheduled":
		status = "Scheduled"
	case "retrying":
		status = "Retrying"
	case "waiting":
		status = "Waiting"
	case "failed":
		status = "Failed"
	case "dead_letter":
		status = "DeadLetter"
	case "cancelled":
		status = "Cancelled"
	default:
		status = "Pending"
	}

	statusUpdatedAt := r.createdAt
	if r.startedAt != nil {
		statusUpdatedAt = *r.startedAt
	} else if r.completedAt != nil {
		statusUpdatedAt = *r.completedAt
	}

	return storage.ActivitySnapshot{
		ID:                r.id,
		ActivityType:      r.activityType,
		Payload:           r.payload,
		Priority:          intToPriority(r.priority),
		Status:            status,
		CreatedAt:         r.createdAt,
		ScheduledAt:       r.scheduledAt,
		StartedAt:         r.startedAt,
		CompletedAt:       r.completedAt,
		CurrentWorkerID:   r.currentWorkerID,
		LastWorkerID:      r.lastWorkerID,
		RetryCount:        uint32(r.retryCount),
		MaxRetries:        uint32(r.maxRetries),
		TimeoutSeconds:    uint64(r.timeoutSeconds),
		RetryDelaySeconds: uint64(r.retryDelaySeconds),
		Metadata:          meta,
		LastError:         r.lastError,
		LastErrorAt:       r.lastErrorAt,
		StatusUpdatedAt:   statusUpdatedAt,
		LeaseDeadlineMS:   r.leaseDeadlineMS,
		ProcessingMember:  r.currentWorkerID,
		IdempotencyKey:    r.idempotencyKey,
		ParentActivityID:  r.parentActivityID,
		RootActivityID:    r.rootActivityID,
		Depth:             uint16(r.depth),
	}
}

func (b *PostgresBackend) scanSnapshots(rows pgx.Rows) ([]storage.ActivitySnapshot, error) {
	var snapshots []storage.ActivitySnapshot
	for rows.Next() {
		var r activityRow
		err := rows.Scan(
			&r.id, &r.activityType, &r.payload, &r.priority, &r.status,
			&r.createdAt, &r.scheduledAt, &r.startedAt, &r.completedAt,
			&r.currentWorkerID, &r.lastWorkerID, &r.retryCount, &r.maxRetries,
			&r.timeoutSeconds, &r.retryDelaySeconds, &r.lastError, &r.lastErrorAt,
			&r.metadata, &r.idempotencyKey, &r.leaseDeadlineMS,
			&r.parentActivityID, &r.rootActivityID, &r.depth)
		if err != nil {
			return nil, databaseError(err, fmt.Sprintf("Failed to scan row: %v", err))
		}
		snapshots = append(snapshots, r.toSnapshot())
	}
	return snapshots, nil
}

var _ storage.EncodedStorage = (*PostgresBackend)(nil)
