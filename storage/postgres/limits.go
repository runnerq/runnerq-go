package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/runnerq/runnerq-go/storage"
)

var _ storage.LimitStorage = (*PostgresBackend)(nil)

// limitCache is which types of the queue have a limit, read at most
// LimitsRefreshInterval ago. Only claims of those types take the limited
// path; the rest keep the plain claim statement.
type limitCache struct {
	mu     sync.Mutex
	types  map[string]bool
	loaded time.Time
}

func (b *PostgresBackend) limitedTypes(ctx context.Context) (map[string]bool, error) {
	c := &b.limits
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.loaded.IsZero() && time.Since(c.loaded) < storage.LimitsRefreshInterval {
		return c.types, nil
	}
	rows, err := b.pool.Query(ctx, `SELECT activity_type FROM runnerq_limits WHERE queue_name = $1`, b.queueName)
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to read limits: %v", err))
	}
	types, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to read limits: %v", err))
	}
	c.types = make(map[string]bool, len(types))
	for _, t := range types {
		c.types[t] = true
	}
	c.loaded = time.Now()
	return c.types, nil
}

// markLimited records a change this backend made, so its own next claim
// sees it without waiting for a refresh. The map is replaced, never
// mutated: claims in flight hold the old one.
func (b *PostgresBackend) markLimited(activityType string, limited bool) {
	c := &b.limits
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.loaded.IsZero() || c.types[activityType] == limited {
		return
	}
	next := make(map[string]bool, len(c.types)+1)
	for t := range c.types {
		next[t] = true
	}
	if limited {
		next[activityType] = true
	} else {
		delete(next, activityType)
	}
	c.types = next
}

func limitNotFound(activityType string) error {
	return storage.NewNotFoundError(fmt.Sprintf("no limit for activity type %q", activityType))
}

func ratePeriodSeconds(d time.Duration) float64 {
	return float64(d.Round(time.Millisecond).Milliseconds()) / 1000
}

func nullIfZero(n int) *int {
	if n == 0 {
		return nil
	}
	return &n
}

func (b *PostgresBackend) SetLimit(ctx context.Context, l storage.Limit) error {
	if err := storage.ValidateLimit(&l); err != nil {
		return err
	}
	var per *float64
	if l.RateLimit > 0 {
		s := ratePeriodSeconds(l.RatePeriod)
		per = &s
	}
	// A changed rate settles the bucket: the old rate's refill up to now,
	// capped at the new size (limits.md).
	_, err := b.pool.Exec(ctx, `
		INSERT INTO runnerq_limits AS l (queue_name, activity_type, max_concurrent, rate_tokens, rate_per_seconds,
			tokens, refilled_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, COALESCE($4, 0), clock_timestamp(), clock_timestamp())
		ON CONFLICT (queue_name, activity_type) DO UPDATE SET
			max_concurrent = EXCLUDED.max_concurrent,
			rate_tokens = EXCLUDED.rate_tokens,
			rate_per_seconds = EXCLUDED.rate_per_seconds,
			tokens = CASE
				WHEN EXCLUDED.rate_tokens IS NULL THEN 0
				WHEN (l.rate_tokens, l.rate_per_seconds) IS NOT DISTINCT FROM (EXCLUDED.rate_tokens, EXCLUDED.rate_per_seconds) THEN l.tokens
				WHEN l.rate_tokens IS NULL THEN EXCLUDED.rate_tokens
				ELSE LEAST(EXCLUDED.rate_tokens, l.rate_tokens,
					l.tokens + GREATEST(0, EXTRACT(EPOCH FROM EXCLUDED.refilled_at - l.refilled_at)) * l.rate_tokens / l.rate_per_seconds)
			END,
			refilled_at = CASE
				WHEN (l.rate_tokens, l.rate_per_seconds) IS NOT DISTINCT FROM (EXCLUDED.rate_tokens, EXCLUDED.rate_per_seconds) THEN l.refilled_at
				ELSE EXCLUDED.refilled_at
			END,
			updated_at = EXCLUDED.updated_at
		WHERE (l.max_concurrent, l.rate_tokens, l.rate_per_seconds)
			IS DISTINCT FROM (EXCLUDED.max_concurrent, EXCLUDED.rate_tokens, EXCLUDED.rate_per_seconds)`,
		b.queueName, l.ActivityType, nullIfZero(l.MaxConcurrent), nullIfZero(l.RateLimit), per)
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to set limit: %v", err))
	}
	b.markLimited(l.ActivityType, true)
	return nil
}

const limitColumns = `activity_type, max_concurrent, rate_tokens, rate_per_seconds, updated_at`

func scanLimit(row pgx.Row) (storage.Limit, error) {
	var l storage.Limit
	var maxConcurrent, rateTokens *int
	var per *float64
	if err := row.Scan(&l.ActivityType, &maxConcurrent, &rateTokens, &per, &l.UpdatedAt); err != nil {
		return l, err
	}
	if maxConcurrent != nil {
		l.MaxConcurrent = *maxConcurrent
	}
	if rateTokens != nil && per != nil {
		l.RateLimit = *rateTokens
		l.RatePeriod = time.Duration(math.Round(*per*1000)) * time.Millisecond
	}
	return l, nil
}

func (b *PostgresBackend) GetLimit(ctx context.Context, activityType string) (*storage.Limit, error) {
	l, err := scanLimit(b.pool.QueryRow(ctx, `SELECT `+limitColumns+` FROM runnerq_limits
		WHERE queue_name = $1 AND activity_type = $2`, b.queueName, activityType))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, limitNotFound(activityType)
	}
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to get limit: %v", err))
	}
	return &l, nil
}

func (b *PostgresBackend) ListLimits(ctx context.Context) ([]storage.Limit, error) {
	rows, err := b.pool.Query(ctx, `SELECT `+limitColumns+` FROM runnerq_limits
		WHERE queue_name = $1 ORDER BY activity_type`, b.queueName)
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to list limits: %v", err))
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (storage.Limit, error) { return scanLimit(row) })
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to list limits: %v", err))
	}
	if out == nil {
		out = []storage.Limit{}
	}
	return out, nil
}

func (b *PostgresBackend) DeleteLimit(ctx context.Context, activityType string) error {
	tag, err := b.pool.Exec(ctx, `DELETE FROM runnerq_limits WHERE queue_name = $1 AND activity_type = $2`,
		b.queueName, activityType)
	if err != nil {
		return databaseError(err, fmt.Sprintf("Failed to delete limit: %v", err))
	}
	b.markLimited(activityType, false)
	if tag.RowsAffected() == 0 {
		return limitNotFound(activityType)
	}
	return nil
}

// The limited claim (runnerq-spec limits.md). A lock-free check finds the
// limited types with claimable work and room; only those are locked. The
// claim statement runs after the lock statement, so its snapshot sees every
// claim committed before the lock was granted: the running count and the
// bucket it reads are current, and no other claimer of these types can
// change them until commit.
const (
	limitAvailableSQL = `LEAST(l.rate_tokens::float8,
		l.tokens + GREATEST(0, EXTRACT(EPOCH FROM %s - l.refilled_at)) * l.rate_tokens / l.rate_per_seconds)`
	limitRunningSQL = `(SELECT count(*) FROM runnerq_activities r
		WHERE r.queue_name = l.queue_name AND r.activity_type = l.activity_type AND r.status = 'processing')`

	// $1 queue, $2 limited types.
	limitCheckSQL = `
		SELECT l.activity_type,
			EXISTS (SELECT 1 FROM runnerq_activities %s AND activity_type = l.activity_type) AS has_work,
			(l.max_concurrent IS NULL OR l.max_concurrent > ` + limitRunningSQL + `)
				AND (l.rate_tokens IS NULL OR %s >= 1) AS has_room
		FROM runnerq_limits l
		WHERE l.queue_name = $1 AND l.activity_type = ANY($2)`

	limitLockSQL = `SELECT activity_type FROM runnerq_limits
		WHERE queue_name = $1 AND activity_type = ANY($2)
		ORDER BY activity_type
		FOR UPDATE`

	// $1 token (prefix for a batch), $2 default lease ms, $3 queue, $4 claim
	// limit, $5 locked types.
	limitClaimSQLHead = `
		WITH clock AS (SELECT clock_timestamp() AS at),
		lim AS (
			SELECT l.activity_type, l.rate_tokens, clock.at, avail,
				LEAST($4::bigint,
					COALESCE(l.max_concurrent - ` + limitRunningSQL + `, $4::bigint),
					COALESCE(floor(avail)::bigint, $4::bigint)) AS allowance
			FROM runnerq_limits l, clock,
				LATERAL (SELECT CASE WHEN l.rate_tokens IS NULL THEN NULL ELSE %s END AS avail) b
			WHERE l.queue_name = $3 AND l.activity_type = ANY($5)
		),
		picked AS (
			SELECT c.id AS claim_id, c.activity_type AS claim_type
			FROM lim CROSS JOIN LATERAL (
				SELECT id, activity_type, priority, retry_count, scheduled_at, created_at
				FROM runnerq_activities %s AND activity_type = lim.activity_type
				%s
				LIMIT GREATEST(lim.allowance, 0)
				FOR UPDATE SKIP LOCKED
			) c
			ORDER BY c.priority DESC, c.retry_count DESC, COALESCE(c.scheduled_at, c.created_at) ASC
			LIMIT $4
		),
		spent AS (
			UPDATE runnerq_limits l SET tokens = lim.avail - s.n, refilled_at = lim.at
			FROM lim, (SELECT claim_type, count(*) AS n FROM picked GROUP BY claim_type) s
			WHERE l.queue_name = $3 AND l.activity_type = lim.activity_type
			  AND s.claim_type = lim.activity_type AND lim.rate_tokens IS NOT NULL
		)
		UPDATE runnerq_activities AS a
		SET status = 'processing',
			current_worker_id = %s,
			started_at = NOW(),
			lease_deadline_ms = ` + claimLeaseSQL + `
		FROM picked
		WHERE a.id = picked.claim_id
		RETURNING ` + claimColumnsSQL
)

var (
	limitCheck      = fmt.Sprintf(limitCheckSQL, strings.ReplaceAll(claimEligibleSQL, "$3", "$1"), fmt.Sprintf(limitAvailableSQL, "clock_timestamp()"))
	limitClaimBatch = fmt.Sprintf(limitClaimSQLHead, fmt.Sprintf(limitAvailableSQL, "clock.at"), claimEligibleSQL, claimOrderSQL, `$1 || ':' || a.id::text`)
	limitClaimOne   = fmt.Sprintf(limitClaimSQLHead, fmt.Sprintf(limitAvailableSQL, "clock.at"), claimEligibleSQL, claimOrderSQL, `$1`)
)

// claimPlan splits a claim's types into the limited ones and the rest. With
// no type filter, rest is nil and except lists what the plain claim must
// leave out.
type claimPlan struct {
	limited []string
	rest    []string
	except  []string
	untyped bool
}

func (b *PostgresBackend) planClaim(ctx context.Context, types []string) (*claimPlan, error) {
	limited, err := b.limitedTypes(ctx)
	if err != nil || len(limited) == 0 {
		return nil, err
	}
	p := &claimPlan{untyped: len(types) == 0}
	if p.untyped {
		for t := range limited {
			p.limited = append(p.limited, t)
		}
		slices.Sort(p.limited)
		return p, nil
	}
	for _, t := range types {
		if limited[t] {
			p.limited = append(p.limited, t)
		} else {
			p.rest = append(p.rest, t)
		}
	}
	if len(p.limited) == 0 {
		return nil, nil
	}
	return p, nil
}

// claimLimited claims up to limit activities of the plan's limited types.
// heldBack reports that a limit kept claimable work back. Types whose limit
// has gone move to the plan's plain part.
func (b *PostgresBackend) claimLimited(ctx context.Context, p *claimPlan, token string, limit int, batch bool) (claims []storage.DequeuedActivity, heldBack bool, err error) {
	rows, err := b.pool.Query(ctx, limitCheck, b.queueName, p.limited)
	if err != nil {
		return nil, false, databaseError(err, fmt.Sprintf("Failed to check limits: %v", err))
	}
	found := map[string]bool{}
	var ready []string
	for rows.Next() {
		var t string
		var hasWork, hasRoom bool
		if err := rows.Scan(&t, &hasWork, &hasRoom); err != nil {
			rows.Close()
			return nil, false, databaseError(err, fmt.Sprintf("Failed to check limits: %v", err))
		}
		found[t] = true
		switch {
		case hasWork && hasRoom:
			ready = append(ready, t)
		case hasWork:
			heldBack = true
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, false, databaseError(err, fmt.Sprintf("Failed to check limits: %v", err))
	}
	var except []string
	for _, t := range p.limited {
		if found[t] {
			except = append(except, t)
		} else {
			p.rest = append(p.rest, t)
			b.markLimited(t, false)
		}
	}
	p.except = except
	if len(ready) == 0 {
		return nil, heldBack, nil
	}

	sql := limitClaimOne
	if batch {
		sql = limitClaimBatch
	}
	err = pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		locked, err := tx.Query(ctx, limitLockSQL, b.queueName, ready)
		if err != nil {
			return err
		}
		types, err := pgx.CollectRows(locked, pgx.RowTo[string])
		if err != nil || len(types) == 0 {
			return err
		}
		rows, err := tx.Query(ctx, sql, token, b.defaultLeaseMS.Load(), b.queueName, limit, types)
		if err != nil {
			return err
		}
		claims, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (storage.DequeuedActivity, error) {
			return scanClaim(row)
		})
		return err
	})
	if err != nil {
		return nil, false, databaseError(err, fmt.Sprintf("Failed to claim limited activities: %v", err))
	}
	// Fewer than the room suggested means the rest went to other claimers or
	// the check was stale; probing again soon is cheap either way.
	if len(claims) == 0 {
		heldBack = true
	}
	return claims, heldBack, nil
}
