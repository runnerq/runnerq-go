package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/runnerq/runnerq-go/storage"
)

const businessKeyLockClass = int32(1381913431)

// Prefers an existing v2 claim, else reuses a legacy claim of the same type in
// place; legacy rows of another type are never reused. Old and new enqueue
// writers require a coordinated cutover.
func (b *PostgresBackend) resolveBusinessKeyTx(ctx context.Context, tx pgx.Tx, key, activityType string) (string, error) {
	legacy, encodedType, ok := storage.LegacyBusinessIdempotencyKey(key)
	if !ok {
		return key, nil
	}
	if encodedType != activityType {
		return "", storage.NewConfigurationError("business key activity type does not match activity")
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1::int4,hashtext($2))`, businessKeyLockClass, b.queueName+":"+legacy); err != nil {
		return "", databaseError(err, "failed to serialize business key migration")
	}
	var v2Exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runnerq_idempotency WHERE queue_name=$1 AND idempotency_key=$2)`, b.queueName, key).Scan(&v2Exists); err != nil {
		return "", databaseError(err, "failed to find v2 business key")
	}
	if v2Exists {
		return key, nil
	}
	var matchingLegacy bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runnerq_idempotency i JOIN runnerq_activities a ON a.id=i.activity_id AND a.queue_name=i.queue_name WHERE i.queue_name=$1 AND i.idempotency_key=$2 AND a.activity_type=$3)`, b.queueName, legacy, activityType).Scan(&matchingLegacy); err != nil {
		return "", databaseError(err, "failed to find legacy business key")
	}
	if matchingLegacy {
		return legacy, nil
	}
	return key, nil
}
