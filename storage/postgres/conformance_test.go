package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
	"github.com/runnerq/runnerq-go/storage/storagetest"
)

// conformanceHarness runs the backend-agnostic durable-execution contract
// (storage/storagetest) against this backend.
type conformanceHarness struct{}

func (conformanceHarness) Open(t *testing.T, queue string) storage.Storage {
	return testBackendNamed(t, queue)
}

func (conformanceHarness) ExpireLease(ctx context.Context, b storage.Storage, id uuid.UUID) error {
	_, err := b.(*PostgresBackend).pool.Exec(ctx, `
		UPDATE runnerq_activities
		SET lease_deadline_ms = (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint - 10000
		WHERE id = $1`, id)
	return err
}

func (conformanceHarness) SetNextFireAt(b storage.Storage, name string, at time.Time) error {
	p := b.(*PostgresBackend)
	_, err := p.pool.Exec(context.Background(), `UPDATE runnerq_schedules SET next_fire_at = $3
		WHERE queue_name = $1 AND name = $2`, p.queueName, name, at)
	return err
}

func TestConformance(t *testing.T) {
	if os.Getenv("RUNNERQ_TEST_DSN") == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping conformance suite")
	}
	storagetest.Run(t, conformanceHarness{})
}
