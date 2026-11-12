package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/runnerq/runnerq-go/storage"
)

var _ storage.ScheduleStorage = (*PostgresBackend)(nil)

const scheduleColumns = `name, cron, timezone, activity_type, payload, serialization, options,
	catch_up, catch_up_limit, paused, next_fire_at, last_fire_at, created_at, updated_at`

func scanSchedule(row pgx.Row) (*storage.Schedule, error) {
	var s storage.Schedule
	var payload, options []byte
	var catchUp, serialization string
	if err := row.Scan(&s.Name, &s.Cron, &s.Timezone, &s.ActivityType, &payload, &serialization, &options,
		&catchUp, &s.CatchUpLimit, &s.Paused, &s.NextFireAt, &s.LastFireAt, &s.CreatedAt, &s.UpdatedAt); err != nil {
		return nil, err
	}
	s.Payload = payload
	s.Serialization = reportedSerialization(serialization)
	s.CatchUp = storage.CatchUp(catchUp)
	o, err := storage.DecodeScheduleOptions(options)
	if err != nil {
		return nil, err
	}
	s.Options = o
	return &s, nil
}

func (b *PostgresBackend) dbNow(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (time.Time, error) {
	var now time.Time
	if err := q.QueryRow(ctx, `SELECT NOW()`).Scan(&now); err != nil {
		return now, databaseError(err, "Failed to read the database clock")
	}
	return now, nil
}

func scheduleNotFound(name string) error {
	return storage.NewNotFoundError(fmt.Sprintf("schedule %q not found", name))
}

func (b *PostgresBackend) UpsertSchedule(ctx context.Context, s storage.Schedule) error {
	if err := storage.ValidateSchedule(&s); err != nil {
		return err
	}
	options, err := json.Marshal(s.Options)
	if err != nil {
		return storage.NewSerializationError(err.Error())
	}
	payload := s.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("null")
	}
	now, err := b.dbNow(ctx, b.pool)
	if err != nil {
		return err
	}
	next, err := storage.NextFireTime(s.Cron, s.Timezone, now)
	if err != nil {
		return err
	}
	_, err = b.pool.Exec(ctx, `
		INSERT INTO runnerq_schedules AS s (queue_name, name, cron, timezone, activity_type, payload,
			serialization, options, catch_up, catch_up_limit, next_fire_at)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8::jsonb, $9, $10, $11)
		ON CONFLICT (queue_name, name) DO UPDATE SET
			cron = EXCLUDED.cron, timezone = EXCLUDED.timezone, activity_type = EXCLUDED.activity_type,
			payload = EXCLUDED.payload, serialization = EXCLUDED.serialization, options = EXCLUDED.options,
			catch_up = EXCLUDED.catch_up, catch_up_limit = EXCLUDED.catch_up_limit,
			next_fire_at = CASE WHEN s.cron = EXCLUDED.cron AND s.timezone = EXCLUDED.timezone
				THEN s.next_fire_at ELSE EXCLUDED.next_fire_at END,
			updated_at = NOW()
		WHERE (s.cron, s.timezone, s.activity_type, s.payload, s.serialization, s.options, s.catch_up, s.catch_up_limit)
			IS DISTINCT FROM (EXCLUDED.cron, EXCLUDED.timezone, EXCLUDED.activity_type, EXCLUDED.payload,
				EXCLUDED.serialization, EXCLUDED.options, EXCLUDED.catch_up, EXCLUDED.catch_up_limit)`,
		b.queueName, s.Name, s.Cron, s.Timezone, s.ActivityType, string(payload),
		storedSerialization(s.Serialization), string(options), string(s.CatchUp), s.CatchUpLimit, next)
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to upsert schedule: %v", err))
	}
	return nil
}

func (b *PostgresBackend) GetSchedule(ctx context.Context, name string) (*storage.Schedule, error) {
	s, err := scanSchedule(b.pool.QueryRow(ctx, `SELECT `+scheduleColumns+` FROM runnerq_schedules
		WHERE queue_name = $1 AND name = $2`, b.queueName, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, scheduleNotFound(name)
	}
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to get schedule: %v", err))
	}
	return s, nil
}

func (b *PostgresBackend) ListSchedules(ctx context.Context) ([]storage.Schedule, error) {
	rows, err := b.pool.Query(ctx, `SELECT `+scheduleColumns+` FROM runnerq_schedules
		WHERE queue_name = $1 ORDER BY name`, b.queueName)
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to list schedules: %v", err))
	}
	defer rows.Close()
	out := []storage.Schedule{}
	for rows.Next() {
		s, err := scanSchedule(rows)
		if err != nil {
			return nil, databaseError(err, fmt.Sprintf("Failed to read schedule: %v", err))
		}
		out = append(out, *s)
	}
	if err := rows.Err(); err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to list schedules: %v", err))
	}
	return out, nil
}

func (b *PostgresBackend) PauseSchedule(ctx context.Context, name string) error {
	tag, err := b.pool.Exec(ctx, `UPDATE runnerq_schedules SET paused = TRUE,
		updated_at = CASE WHEN paused THEN updated_at ELSE NOW() END
		WHERE queue_name = $1 AND name = $2`, b.queueName, name)
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to pause schedule: %v", err))
	}
	if tag.RowsAffected() == 0 {
		return scheduleNotFound(name)
	}
	return nil
}

func (b *PostgresBackend) ResumeSchedule(ctx context.Context, name string) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)
	var cronExpr, timezone string
	var paused bool
	var now time.Time
	err = tx.QueryRow(ctx, `SELECT cron, timezone, paused, NOW() FROM runnerq_schedules
		WHERE queue_name = $1 AND name = $2 FOR UPDATE`, b.queueName, name).Scan(&cronExpr, &timezone, &paused, &now)
	if errors.Is(err, pgx.ErrNoRows) {
		return scheduleNotFound(name)
	}
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to resume schedule: %v", err))
	}
	if !paused {
		return nil
	}
	next, err := storage.NextFireTime(cronExpr, timezone, now)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE runnerq_schedules SET paused = FALSE, next_fire_at = $3, updated_at = NOW()
		WHERE queue_name = $1 AND name = $2`, b.queueName, name, next); err != nil {
		return databaseError(err, fmt.Sprintf("Failed to resume schedule: %v", err))
	}
	if err := tx.Commit(ctx); err != nil {
		return databaseError(err, fmt.Sprintf("Failed to resume schedule: %v", err))
	}
	return nil
}

func (b *PostgresBackend) DeleteSchedule(ctx context.Context, name string) error {
	tag, err := b.pool.Exec(ctx, `DELETE FROM runnerq_schedules WHERE queue_name = $1 AND name = $2`, b.queueName, name)
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to delete schedule: %v", err))
	}
	if tag.RowsAffected() == 0 {
		return scheduleNotFound(name)
	}
	return nil
}

func (b *PostgresBackend) TriggerSchedule(ctx context.Context, name string) (uuid.UUID, error) {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, databaseError(err, fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)
	var now time.Time
	s, err := scanScheduleWithNow(tx.QueryRow(ctx, `SELECT `+scheduleColumns+`, NOW() FROM runnerq_schedules
		WHERE queue_name = $1 AND name = $2 FOR SHARE`, b.queueName, name), &now)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, scheduleNotFound(name)
	}
	if err != nil {
		return uuid.Nil, databaseError(err, fmt.Sprintf("Failed to trigger schedule: %v", err))
	}
	a := storage.ScheduledActivity(s, now.Truncate(time.Second), now)
	id, created, err := b.enqueueFireTx(ctx, tx, &a)
	if err != nil {
		return uuid.Nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, databaseError(err, fmt.Sprintf("Failed to trigger schedule: %v", err))
	}
	if created {
		b.signalWork()
	}
	return id, nil
}

func scanScheduleWithNow(row pgx.Row, now *time.Time) (*storage.Schedule, error) {
	return scanSchedule(rowWithExtra{row, now})
}

// rowWithExtra appends one destination to a Scan.
type rowWithExtra struct {
	pgx.Row
	extra any
}

func (r rowWithExtra) Scan(dest ...any) error { return r.Row.Scan(append(dest, r.extra)...) }

// enqueueFireTx enqueues a schedule's activity unless its key exists, and
// returns the activity that owns the key.
func (b *PostgresBackend) enqueueFireTx(ctx context.Context, tx pgx.Tx, a *storage.QueuedActivity) (uuid.UUID, bool, error) {
	var existing uuid.UUID
	err := tx.QueryRow(ctx, `
		WITH ins AS (
			INSERT INTO runnerq_idempotency (queue_name, idempotency_key, activity_id, created_at, updated_at)
			VALUES ($1, $2, $3, NOW(), NOW())
			ON CONFLICT (queue_name, idempotency_key) DO NOTHING
			RETURNING activity_id
		)
		SELECT activity_id FROM ins
		UNION ALL
		SELECT activity_id FROM runnerq_idempotency WHERE queue_name = $1 AND idempotency_key = $2
		LIMIT 1`, b.queueName, a.IdempotencyKey.Key, a.ID).Scan(&existing)
	if err != nil {
		return uuid.Nil, false, databaseError(err, fmt.Sprintf("Failed to claim schedule key: %v", err))
	}
	if existing != a.ID {
		return existing, false, nil
	}
	stmts, err := b.enqueueStmts(a)
	if err != nil {
		return uuid.Nil, false, err
	}
	if err := execAll(ctx, tx, stmts...); err != nil {
		return uuid.Nil, false, err
	}
	return a.ID, true, nil
}

func (b *PostgresBackend) FireDueSchedules(ctx context.Context, limit int) (int, error) {
	fired := 0
	var failed []string
	var errs []error
	for range max(limit, 1) {
		n, name, err := b.fireOne(ctx, failed)
		if err != nil {
			if name == "" {
				errs = append(errs, err)
				break
			}
			failed = append(failed, name)
			errs = append(errs, fmt.Errorf("schedule %q: %w", name, err))
			continue
		}
		if name == "" {
			break
		}
		fired += n
	}
	if fired > 0 {
		b.signalWork()
	}
	return fired, errors.Join(errs...)
}

// fireOne fires the most overdue schedule not in skip, in one transaction.
// name is "" when none is due (or the claim itself failed).
func (b *PostgresBackend) fireOne(ctx context.Context, skip []string) (int, string, error) {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return 0, "", databaseError(err, fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)
	var now time.Time
	s, err := scanScheduleWithNow(tx.QueryRow(ctx, `SELECT `+scheduleColumns+`, NOW() FROM runnerq_schedules
		WHERE queue_name = $1 AND NOT paused AND next_fire_at <= NOW() AND NOT (name = ANY($2))
		ORDER BY next_fire_at LIMIT 1
		FOR UPDATE SKIP LOCKED`, b.queueName, append([]string{}, skip...)), &now)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", databaseError(err, fmt.Sprintf("Failed to claim a due schedule: %v", err))
	}
	fire, next, err := storage.ScheduleFireTimes(s, now)
	if err != nil {
		return 0, s.Name, err
	}
	n := 0
	for _, at := range fire {
		a := storage.ScheduledActivity(s, at, now)
		_, created, err := b.enqueueFireTx(ctx, tx, &a)
		if err != nil {
			return 0, s.Name, err
		}
		if created {
			n++
		}
	}
	var last *time.Time
	if len(fire) > 0 {
		last = &fire[len(fire)-1]
	}
	if _, err := tx.Exec(ctx, `UPDATE runnerq_schedules SET next_fire_at = $3, last_fire_at = COALESCE($4, last_fire_at)
		WHERE queue_name = $1 AND name = $2`, b.queueName, s.Name, next, last); err != nil {
		return 0, s.Name, databaseError(err, fmt.Sprintf("Failed to advance schedule: %v", err))
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, s.Name, databaseError(err, fmt.Sprintf("Failed to commit schedule fire: %v", err))
	}
	return n, s.Name, nil
}
