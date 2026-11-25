package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// BenchmarkClaim measures one batch claim of 10 and its acks on a queue
// with a backlog. Unlimited types must cost what they did before limits
// existed, whether or not another type of the queue has a limit; the limited
// cases show the price of the lock and count (limits never bind here, so
// every claim takes a full batch).
//
//	RUNNERQ_TEST_DSN=... go test ./storage/postgres -run '^$' -bench Claim -benchtime 2000x
func BenchmarkClaim(b *testing.B) {
	for _, tc := range []struct {
		name  string
		limit *storage.Limit
	}{
		{"unlimited", nil},
		{"unlimited-beside-a-limited-type", &storage.Limit{ActivityType: "other", MaxConcurrent: 5}},
		{"limited-concurrency", &storage.Limit{ActivityType: "t", MaxConcurrent: 1_000_000}},
		{"limited-rate", &storage.Limit{ActivityType: "t", RateLimit: 1_000_000, RatePeriod: time.Millisecond}},
		{"limited-both", &storage.Limit{ActivityType: "t", MaxConcurrent: 1_000_000, RateLimit: 1_000_000, RatePeriod: time.Millisecond}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			be := benchBackend(b)
			if tc.limit != nil {
				if err := be.SetLimit(context.Background(), *tc.limit); err != nil {
					b.Fatal(err)
				}
			}
			benchClaims(b, be)
		})
	}
}

func benchBackend(b *testing.B) *PostgresBackend {
	b.Helper()
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		b.Skip("RUNNERQ_TEST_DSN not set")
	}
	queue := "bench_" + uuid.NewString()[:8]
	be, err := WithConfig(context.Background(), dsn, queue, 30_000, 5)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(be.Close)
	return be
}

const benchBatch = 10

func benchClaims(b *testing.B, be *PostgresBackend) {
	ctx := context.Background()
	backlog := (b.N + 1) * benchBatch
	if _, err := be.pool.Exec(ctx, `
		WITH a AS (
			INSERT INTO runnerq_activities (id, queue_name, activity_type, priority, status, max_retries,
				timeout_seconds, retry_delay_seconds, max_retry_delay_seconds, metadata, root_activity_id, created_at)
			SELECT id, $1, 't', 2, 'pending', 3, 30, 1, 0, '{}'::jsonb, id, NOW() - make_interval(secs => g)
			FROM (SELECT gen_random_uuid() AS id, g FROM generate_series(1, $2) g) s
			RETURNING id)
		INSERT INTO runnerq_inputs (activity_id, queue_name, payload, serialization)
		SELECT id, $1, '{}'::jsonb, 'json-v1' FROM a`, be.queueName, backlog); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_, _ = be.pool.Exec(ctx, `DELETE FROM runnerq_inputs WHERE queue_name = $1`, be.queueName)
		_, _ = be.pool.Exec(ctx, `DELETE FROM runnerq_activities WHERE queue_name = $1`, be.queueName)
	})
	if _, err := be.pool.Exec(ctx, `VACUUM ANALYZE runnerq_activities`); err != nil {
		b.Fatal(err)
	}
	types := []string{"t"}
	b.ResetTimer()
	for i := range b.N {
		claims, err := be.DequeueBatch(ctx, fmt.Sprintf("w%d", i), benchBatch, 0, types)
		if err != nil {
			b.Fatal(err)
		}
		if len(claims) != benchBatch {
			b.Fatalf("claimed %d, want %d", len(claims), benchBatch)
		}
		b.StopTimer()
		for _, c := range claims {
			if err := be.AckSuccess(ctx, c.Activity.ID, json.RawMessage(`null`), c.LeaseID); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(b.N*benchBatch)/b.Elapsed().Seconds(), "claims/s")
}
