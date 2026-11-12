package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/runnerq/runnerq-go/storage"
)

const (
	maxCommandIDs    = 1000
	maxCommandFilter = 10000
	// The command protocol promises replay for at least 24 hours.
	commandLedgerKeep = 7 * 24 * time.Hour
)

var (
	terminalStatuses = map[string]bool{"completed": true, "failed": true, "dead_letter": true, "cancelled": true}
	failedStatuses   = map[string]bool{"failed": true, "dead_letter": true, "cancelled": true}
)

type postCommit struct {
	work    bool
	results []uuid.UUID
}

type targetRow struct {
	id     uuid.UUID
	status string
	parent *uuid.UUID
}

type ledgerItem struct {
	ID         uuid.UUID `json:"id"`
	Outcome    string    `json:"outcome"`
	Status     string    `json:"status,omitempty"`
	ErrKind    int       `json:"err_kind,omitempty"`
	ErrMessage string    `json:"err_message,omitempty"`
}

type ledgerResult struct {
	Matched  int          `json:"matched"`
	Applied  int          `json:"applied"`
	Cascaded int          `json:"cascaded"`
	More     bool         `json:"more"`
	Items    []ledgerItem `json:"items"`
}

func toLedger(r *storage.CommandResult) ledgerResult {
	l := ledgerResult{Matched: r.Matched, Applied: r.Applied, Cascaded: r.Cascaded, More: r.More, Items: []ledgerItem{}}
	for _, it := range r.Items {
		l.Items = append(l.Items, ledgerItem{ID: it.ID, Outcome: it.Outcome, Status: it.Status, ErrKind: int(it.ErrKind), ErrMessage: it.ErrMessage})
	}
	return l
}

func fromLedger(l ledgerResult) *storage.CommandResult {
	r := &storage.CommandResult{Matched: l.Matched, Applied: l.Applied, Cascaded: l.Cascaded, More: l.More, Replayed: true}
	for _, it := range l.Items {
		r.Items = append(r.Items, storage.CommandItem{ID: it.ID, Outcome: it.Outcome, Status: it.Status,
			ErrKind: storage.StorageErrorKind(it.ErrKind), ErrMessage: it.ErrMessage})
	}
	return r
}

func validateCommand(cmd storage.Command) error {
	t := cmd.Target
	kinds := 0
	for _, set := range []bool{len(t.IDs) > 0, t.Filter != nil, t.IdempotencyKey != ""} {
		if set {
			kinds++
		}
	}
	switch {
	case kinds != 1:
		return storage.NewInvalidQueryError("target", "target needs exactly one of ids, filter or idempotency key")
	case len(t.IDs) > maxCommandIDs:
		return storage.NewInvalidQueryError("target.ids", fmt.Sprintf("at most %d ids", maxCommandIDs))
	case t.Filter != nil && (t.Max < 1 || t.Max > maxCommandFilter):
		return storage.NewInvalidQueryError("target.max", fmt.Sprintf("a filter target needs max between 1 and %d", maxCommandFilter))
	case t.IdempotencyKey != "" && cmd.Kind != storage.CommandSignal:
		return storage.NewInvalidQueryError("target.idempotency_key", "idempotency key targets are only valid for signal")
	}
	switch cmd.Kind {
	case storage.CommandCancel, storage.CommandRetry, storage.CommandRunNow, storage.CommandDelete:
	case storage.CommandReschedule:
		if cmd.At.IsZero() {
			return storage.NewInvalidQueryError("at", "reschedule needs a time")
		}
	case storage.CommandSetPriority:
		if cmd.Priority < storage.PriorityLow || cmd.Priority > storage.PriorityCritical {
			return storage.NewInvalidQueryError("priority", "priority must be 1 (low) to 4 (critical)")
		}
	case storage.CommandSignal:
		if cmd.SignalName == "" {
			return storage.NewInvalidQueryError("name", "signal needs a name")
		}
	default:
		return storage.NewUnsupportedQueryError("kind", fmt.Sprintf("unknown command %q", cmd.Kind))
	}
	return nil
}

// ApplyCommand is idempotent by command id through the runnerq_commands ledger.
func (b *PostgresBackend) ApplyCommand(ctx context.Context, cmd storage.Command) (*storage.CommandResult, error) {
	if err := validateCommand(cmd); err != nil {
		return nil, err
	}
	var (
		res  *storage.CommandResult
		post postCommit
		err  error
	)
	// Multi-row locking can lose a deadlock/serialization race; the
	// transaction rolled back whole, so rerunning is safe.
	for attempt := range 4 {
		res, post, err = b.applyCommandTx(ctx, cmd)
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || (pgErr.Code != "40P01" && pgErr.Code != "40001") {
			break
		}
		select {
		case <-time.After(time.Duration(20<<attempt) * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}
	if !res.Replayed && !cmd.DryRun {
		for _, id := range post.results {
			b.signalResult(id)
		}
		if post.work {
			b.signalWork()
		}
	}
	return res, nil
}

func (b *PostgresBackend) applyCommandTx(ctx context.Context, cmd storage.Command) (*storage.CommandResult, postCommit, error) {
	var post postCommit
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, post, databaseError(err, "failed to begin command")
	}
	defer tx.Rollback(ctx)

	ledger := cmd.ID != "" && !cmd.DryRun
	if ledger {
		// Serialize deliveries of one command id, then replay a recorded result
		// instead of applying twice.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
			"runnerq_command:"+b.queueName+":"+cmd.ID); err != nil {
			return nil, post, err
		}
		var fingerprint string
		var raw []byte
		err := tx.QueryRow(ctx, `SELECT fingerprint, result FROM runnerq_commands WHERE queue_name = $1 AND command_id = $2`,
			b.queueName, cmd.ID).Scan(&fingerprint, &raw)
		switch {
		case err == nil:
			if fingerprint != cmd.Fingerprint {
				return nil, post, storage.NewConflictError(fmt.Sprintf("command %q was already applied with different input", cmd.ID))
			}
			var l ledgerResult
			if err := json.Unmarshal(raw, &l); err != nil {
				return nil, post, databaseError(err, "failed to decode recorded command")
			}
			return fromLedger(l), post, nil
		case !errors.Is(err, pgx.ErrNoRows):
			return nil, post, databaseError(err, "failed to read command ledger")
		}
	}

	ids, more, err := b.resolveTargets(ctx, tx, cmd.Kind, cmd.Target)
	if err != nil {
		return nil, post, err
	}
	rows, err := b.lockRows(ctx, tx, ids)
	if err != nil {
		return nil, post, err
	}

	res := &storage.CommandResult{More: more, Items: make([]storage.CommandItem, 0, len(ids))}
	now := time.Now().UTC()
	var cancelled []uuid.UUID
	for _, id := range ids {
		row, ok := rows[id]
		if !ok {
			res.Items = append(res.Items, storage.CommandItem{ID: id, Outcome: storage.CommandSkipped,
				ErrKind: storage.ErrNotFound, ErrMessage: "no such activity in this queue"})
			continue
		}
		res.Matched++
		item, err := b.applyOne(ctx, tx, cmd, row, now, &post)
		if err != nil {
			return nil, post, err
		}
		if item.Outcome == storage.CommandApplied {
			res.Applied++
			if cmd.Kind == storage.CommandCancel {
				cancelled = append(cancelled, id)
			}
		}
		if item.Outcome == storage.CommandWouldApply && cmd.Kind == storage.CommandCancel {
			cancelled = append(cancelled, id)
		}
		res.Items = append(res.Items, item)
	}

	if cmd.Kind == storage.CommandCancel && cmd.CascadeChildren && len(cancelled) > 0 {
		n, err := b.cancelDescendants(ctx, tx, cmd, cancelled, now, &post)
		if err != nil {
			return nil, post, err
		}
		res.Cascaded = n
	}

	if ledger {
		raw, _ := json.Marshal(toLedger(res))
		if _, err := tx.Exec(ctx, `INSERT INTO runnerq_commands (queue_name, command_id, fingerprint, kind, result)
			VALUES ($1, $2, $3, $4, $5)`, b.queueName, cmd.ID, cmd.Fingerprint, string(cmd.Kind), raw); err != nil {
			return nil, post, databaseError(err, "failed to record command")
		}
		if rand.IntN(64) == 0 {
			if _, err := tx.Exec(ctx, `DELETE FROM runnerq_commands WHERE ctid IN (
				SELECT ctid FROM runnerq_commands WHERE created_at < $1 LIMIT 1000)`, now.Add(-commandLedgerKeep)); err != nil {
				return nil, post, databaseError(err, "failed to prune command ledger")
			}
		}
	}
	if cmd.DryRun {
		return res, post, nil // rolled back by the deferred Rollback
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, post, err
	}
	return res, post, nil
}

// eligibleSQL narrows filter targets so repeating a bounded command ("retry
// the next 100 dead letters") makes progress instead of re-selecting handled rows.
var eligibleSQL = map[storage.CommandKind]string{
	storage.CommandCancel:      "a.status NOT IN ('completed', 'failed', 'dead_letter', 'cancelled')",
	storage.CommandSetPriority: "a.status NOT IN ('completed', 'failed', 'dead_letter', 'cancelled')",
	storage.CommandSignal:      "a.status NOT IN ('completed', 'failed', 'dead_letter', 'cancelled')",
	storage.CommandRetry:       "a.status IN ('failed', 'dead_letter', 'cancelled')",
	storage.CommandRunNow:      "a.status IN ('scheduled', 'retrying', 'waiting')",
	storage.CommandReschedule:  "a.status IN ('scheduled', 'retrying')",
	storage.CommandDelete:      "a.status IN ('completed', 'failed', 'dead_letter', 'cancelled') AND a.parent_activity_id IS NULL",
}

func (b *PostgresBackend) resolveTargets(ctx context.Context, tx pgx.Tx, kind storage.CommandKind, t storage.CommandTarget) ([]uuid.UUID, bool, error) {
	switch {
	case len(t.IDs) > 0:
		seen := make(map[uuid.UUID]bool, len(t.IDs))
		ids := make([]uuid.UUID, 0, len(t.IDs))
		for _, id := range t.IDs {
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
		return ids, false, nil
	case t.IdempotencyKey != "":
		var ids []uuid.UUID
		rows, err := tx.Query(ctx, `SELECT activity_id FROM runnerq_idempotency WHERE queue_name = $1 AND idempotency_key = $2`,
			b.queueName, t.IdempotencyKey)
		if err != nil {
			return nil, false, databaseError(err, "failed to resolve idempotency key")
		}
		defer rows.Close()
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				return nil, false, databaseError(err, "failed to resolve idempotency key")
			}
			ids = append(ids, id)
		}
		return ids, false, rows.Err()
	}
	sb := &sqlBuilder{}
	where, err := sb.where(t.Filter, activityFields)
	if err != nil {
		return nil, false, err
	}
	sql := fmt.Sprintf(`SELECT a.id FROM runnerq_activities a WHERE a.queue_name = %s AND %s AND %s
		ORDER BY a.created_at, a.id LIMIT %d`, sb.arg(b.queueName), eligibleSQL[kind], where, t.Max+1)
	rows, err := tx.Query(ctx, sql, sb.args...)
	if err != nil {
		return nil, false, databaseError(err, "failed to resolve command filter")
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, false, databaseError(err, "failed to resolve command filter")
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, false, databaseError(err, "failed to resolve command filter")
	}
	if len(ids) > t.Max {
		return ids[:t.Max], true, nil
	}
	return ids, false, nil
}

// lockRows locks in id order so concurrent commands don't deadlock each other.
func (b *PostgresBackend) lockRows(ctx context.Context, tx pgx.Tx, ids []uuid.UUID) (map[uuid.UUID]targetRow, error) {
	out := make(map[uuid.UUID]targetRow, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := tx.Query(ctx, `SELECT id, status, parent_activity_id FROM runnerq_activities
		WHERE queue_name = $1 AND id = ANY($2) ORDER BY id FOR UPDATE`, b.queueName, ids)
	if err != nil {
		return nil, databaseError(err, "failed to lock command targets")
	}
	defer rows.Close()
	for rows.Next() {
		var r targetRow
		if err := rows.Scan(&r.id, &r.status, &r.parent); err != nil {
			return nil, databaseError(err, "failed to lock command targets")
		}
		out[r.id] = r
	}
	return out, rows.Err()
}

func skipped(row targetRow, msg string) storage.CommandItem {
	return storage.CommandItem{ID: row.id, Outcome: storage.CommandSkipped, Status: canonicalStatus(row.status),
		ErrKind: storage.ErrConflict, ErrMessage: msg}
}

func (b *PostgresBackend) applyOne(ctx context.Context, tx pgx.Tx, cmd storage.Command, row targetRow, now time.Time, post *postCommit) (storage.CommandItem, error) {
	ok := func(after string) storage.CommandItem {
		outcome := storage.CommandApplied
		if cmd.DryRun {
			outcome = storage.CommandWouldApply
		}
		return storage.CommandItem{ID: row.id, Outcome: outcome, Status: canonicalStatus(after)}
	}
	detail := map[string]any{"command_id": cmd.ID}
	if cmd.Reason != "" {
		detail["reason"] = cmd.Reason
	}
	exec := func(sql string, args ...any) error {
		if cmd.DryRun {
			return nil
		}
		_, err := tx.Exec(ctx, sql, args...)
		if err != nil {
			return databaseError(err, fmt.Sprintf("failed to apply %s", cmd.Kind))
		}
		return nil
	}
	event := func(eventType string) error {
		if cmd.DryRun {
			return nil
		}
		return b.recordEvent(ctx, tx, row.id, eventType, nil, toDetail(detail))
	}

	switch cmd.Kind {
	case storage.CommandCancel:
		if terminalStatuses[row.status] {
			return skipped(row, "already finished"), nil
		}
		if err := b.cancelRow(ctx, tx, cmd, row, now, post); err != nil {
			return storage.CommandItem{}, err
		}
		return ok("cancelled"), nil

	case storage.CommandRetry:
		if !failedStatuses[row.status] {
			return skipped(row, "only failed, dead-lettered or cancelled activities can be retried"), nil
		}
		if err := exec(`UPDATE runnerq_activities SET status = 'pending', completed_at = NULL, started_at = NULL,
			scheduled_at = NULL, current_worker_id = NULL, lease_deadline_ms = NULL, waiting_result_id = NULL,
			retry_count = CASE WHEN $3 THEN 0 ELSE retry_count END
			WHERE id = $1 AND queue_name = $2`, row.id, b.queueName, cmd.ResetAttempts); err != nil {
			return storage.CommandItem{}, err
		}
		// Checkpoints stay, so the rerun replays completed steps.
		if err := exec(`DELETE FROM runnerq_results WHERE queue_name = $1 AND activity_id = $2`, b.queueName, row.id); err != nil {
			return storage.CommandItem{}, err
		}
		ev := storage.EventRetried
		if row.status == "dead_letter" {
			ev = storage.EventRedriven
		}
		detail["from"] = row.status
		detail["reset_attempts"] = cmd.ResetAttempts
		if err := event(ev); err != nil {
			return storage.CommandItem{}, err
		}
		post.work = true
		return ok("pending"), nil

	case storage.CommandRunNow:
		switch row.status {
		case "scheduled", "retrying":
			// Database clock: dequeue compares against NOW(), and an app clock
			// ahead of the database would leave it not yet due.
			if err := exec(`UPDATE runnerq_activities SET scheduled_at = NOW() WHERE id = $1 AND queue_name = $2`,
				row.id, b.queueName); err != nil {
				return storage.CommandItem{}, err
			}
		case "waiting":
			if err := exec(`UPDATE runnerq_activities SET status = 'pending', scheduled_at = NULL, waiting_result_id = NULL
				WHERE id = $1 AND queue_name = $2`, row.id, b.queueName); err != nil {
				return storage.CommandItem{}, err
			}
		case "pending":
			return skipped(row, "already runnable"), nil
		default:
			return skipped(row, "only scheduled or waiting activities can be run now"), nil
		}
		if err := event(storage.EventRunNow); err != nil {
			return storage.CommandItem{}, err
		}
		post.work = true
		after := row.status
		if after == "waiting" {
			after = "pending"
		}
		return ok(after), nil

	case storage.CommandReschedule:
		if row.status != "scheduled" && row.status != "retrying" {
			return skipped(row, "only scheduled activities can be rescheduled"), nil
		}
		if err := exec(`UPDATE runnerq_activities SET scheduled_at = $3 WHERE id = $1 AND queue_name = $2`,
			row.id, b.queueName, cmd.At.UTC()); err != nil {
			return storage.CommandItem{}, err
		}
		detail["at"] = cmd.At.UTC().Format(time.RFC3339Nano)
		if err := event(storage.EventRescheduled); err != nil {
			return storage.CommandItem{}, err
		}
		if !cmd.At.After(now) {
			post.work = true
		}
		return ok(row.status), nil

	case storage.CommandSetPriority:
		if terminalStatuses[row.status] {
			return skipped(row, "already finished"), nil
		}
		if err := exec(`UPDATE runnerq_activities SET priority = $3 WHERE id = $1 AND queue_name = $2`,
			row.id, b.queueName, int32(cmd.Priority)); err != nil {
			return storage.CommandItem{}, err
		}
		detail["priority"] = int(cmd.Priority)
		if err := event(storage.EventPriorityChanged); err != nil {
			return storage.CommandItem{}, err
		}
		return ok(row.status), nil

	case storage.CommandDelete:
		return b.deleteRow(ctx, tx, cmd, row)

	case storage.CommandSignal:
		if terminalStatuses[row.status] {
			return skipped(row, "already finished; nothing is waiting for the signal"), nil
		}
		if cmd.DryRun {
			return ok(row.status), nil
		}
		sigID := storage.CheckpointID(row.id, "signal", cmd.SignalName)
		res := &storage.ActivityResult{Data: cmd.SignalPayload, State: storage.ResultOk}
		if err := b.storeResultTx(ctx, tx, sigID, row.id, res, now, "signal:"+cmd.SignalName); err != nil {
			return storage.CommandItem{}, err
		}
		tag, err := tx.Exec(ctx, `UPDATE runnerq_activities SET status = 'pending', scheduled_at = NULL, waiting_result_id = NULL
			WHERE id = $1 AND queue_name = $2 AND status = 'waiting'`, row.id, b.queueName)
		if err != nil {
			return storage.CommandItem{}, databaseError(err, "failed to wake signalled activity")
		}
		woke := tag.RowsAffected() > 0
		detail["signal_id"], detail["name"], detail["woke"] = sigID, cmd.SignalName, woke
		if err := b.recordEvent(ctx, tx, row.id, storage.EventSignaled, nil, toDetail(detail)); err != nil {
			return storage.CommandItem{}, err
		}
		post.results = append(post.results, sigID)
		after := row.status
		if woke {
			post.work, after = true, "pending"
		}
		return ok(after), nil
	}
	return storage.CommandItem{}, storage.NewUnsupportedQueryError("kind", string(cmd.Kind))
}

// A running handler finds its claim gone at the next heartbeat and its ack is
// fenced out. The stored error result wakes awaiters with a cancellation error.
func (b *PostgresBackend) cancelRow(ctx context.Context, tx pgx.Tx, cmd storage.Command, row targetRow, now time.Time, post *postCommit) error {
	if cmd.DryRun {
		return nil
	}
	msg := "activity cancelled"
	if cmd.Reason != "" {
		msg += ": " + cmd.Reason
	}
	if _, err := tx.Exec(ctx, `UPDATE runnerq_activities SET status = 'cancelled', completed_at = $3,
		last_worker_id = COALESCE(current_worker_id, last_worker_id), current_worker_id = NULL,
		lease_deadline_ms = NULL, waiting_result_id = NULL, scheduled_at = NULL,
		last_error = $4, last_error_at = $3
		WHERE id = $1 AND queue_name = $2`, row.id, b.queueName, now, msg); err != nil {
		return databaseError(err, "failed to cancel activity")
	}
	res := &storage.ActivityResult{State: storage.ResultErr, Data: toDetail(map[string]any{
		"error": msg, "type": "cancelled", "failed_at": now.Format(time.RFC3339),
	})}
	if err := b.storeResultTx(ctx, tx, row.id, row.id, res, now, ""); err != nil {
		return err
	}
	detail := map[string]any{"command_id": cmd.ID, "from": row.status}
	if cmd.Reason != "" {
		detail["reason"] = cmd.Reason
	}
	if err := b.recordEvent(ctx, tx, row.id, storage.EventCancelled, nil, toDetail(detail)); err != nil {
		return err
	}
	post.results = append(post.results, row.id)
	post.work = true // waiters woken by the result become runnable
	return nil
}

func (b *PostgresBackend) cancelDescendants(ctx context.Context, tx pgx.Tx, cmd storage.Command, roots []uuid.UUID, now time.Time, post *postCommit) (int, error) {
	rows, err := tx.Query(ctx, `
		WITH RECURSIVE d(id) AS (
			SELECT id FROM runnerq_activities WHERE queue_name = $1 AND parent_activity_id = ANY($2)
			UNION
			SELECT a.id FROM runnerq_activities a JOIN d ON a.parent_activity_id = d.id WHERE a.queue_name = $1
		)
		SELECT id FROM d`, b.queueName, roots)
	if err != nil {
		return 0, databaseError(err, "failed to find descendants")
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, databaseError(err, "failed to find descendants")
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, databaseError(err, "failed to find descendants")
	}
	locked, err := b.lockRows(ctx, tx, ids)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		row, ok := locked[id]
		if !ok || terminalStatuses[row.status] {
			continue
		}
		if err := b.cancelRow(ctx, tx, cmd, row, now, post); err != nil {
			return 0, err
		}
		n++
	}
	return n, nil
}

func (b *PostgresBackend) deleteRow(ctx context.Context, tx pgx.Tx, cmd storage.Command, row targetRow) (storage.CommandItem, error) {
	if row.parent != nil {
		return skipped(row, "not a root: delete the root to remove the whole tree"), nil
	}
	if !terminalStatuses[row.status] {
		return skipped(row, "still running: cancel it first"), nil
	}
	var live bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM runnerq_activities
		WHERE queue_name = $1 AND root_activity_id = $2 AND parent_activity_id IS NOT NULL
		  AND status NOT IN ('completed', 'failed', 'dead_letter', 'cancelled'))`,
		b.queueName, row.id).Scan(&live); err != nil {
		return storage.CommandItem{}, databaseError(err, "failed to check tree")
	}
	if live {
		return skipped(row, "part of its tree is still running"), nil
	}
	pinned, err := b.lockTreeForDeleteTx(ctx, tx, row.id)
	if err != nil {
		return storage.CommandItem{}, err
	}
	if pinned {
		return skipped(row, "another running workflow depends on this tree"), nil
	}
	outcome := storage.CommandWouldApply
	if !cmd.DryRun {
		if _, err := b.deleteTreeTx(ctx, tx, row.id); err != nil {
			return storage.CommandItem{}, databaseError(err, "failed to delete tree")
		}
		outcome = storage.CommandApplied
	}
	return storage.CommandItem{ID: row.id, Outcome: outcome}, nil
}

var _ storage.CommandStorage = (*PostgresBackend)(nil)
