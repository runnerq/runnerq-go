package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/runnerq/runnerq-go/storage"
)

// Result publication and durable parking must take turns per result, or a
// consumer can park just as the result lands and never wake (write skew under
// READ COMMITTED). They coordinate on the row lock of the result's OWNER
// activity (the child for an await, the target for a signal, the handler's
// activity for a checkpoint), which every producer already holds exclusively
// when publishing (its status UPDATE or claim FOR UPDATE). The parker takes the
// same row FOR SHARE before registering its dependency and checking readiness,
// so whichever commits second sees the other's write: no advisory lock needed,
// and sharers don't block each other, so fan-in consumers park concurrently.
//
// Invariant for producers: lock the owner row BEFORE storeResultTx / the wake
// UPDATE, never after.

// lockProducerRootTx orders this transaction against retention of the
// producer's tree (root FOR KEY SHARE: the root is the single coordination
// point for every result in the tree) and against result publication (producer
// FOR SHARE: conflicts with the publisher's status UPDATE, not other sharers).
// Returns false when the producer no longer exists in this queue.
func (b *PostgresBackend) lockProducerRootTx(ctx context.Context, tx pgx.Tx, producer uuid.UUID) (bool, error) {
	var root uuid.UUID
	err := tx.QueryRow(ctx, `
		SELECT root.id
		FROM runnerq_activities producer
		JOIN runnerq_activities root
		  ON root.id = COALESCE(producer.root_activity_id, producer.id)
		 AND root.queue_name = producer.queue_name
		WHERE producer.id = $1 AND producer.queue_name = $2
		FOR SHARE OF producer
		FOR KEY SHARE OF root`, producer, b.queueName).Scan(&root)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, databaseError(err, "failed to lock dependency producer")
	}
	return true, nil
}
func (b *PostgresBackend) addDependencyTx(ctx context.Context, tx pgx.Tx, waiter, result uuid.UUID, producer *uuid.UUID) error {
	_, err := tx.Exec(ctx, `INSERT INTO runnerq_dependencies(queue_name,waiter_activity_id,result_id,producer_activity_id)
 VALUES($1,$2,$3,$4) ON CONFLICT(queue_name,waiter_activity_id,result_id) DO NOTHING`, b.queueName, waiter, result, producer)
	if err != nil {
		return databaseError(err, "failed to register result dependency")
	}
	return nil
}

// linkStmt links child to parent only when the parent exists locally: imported
// lineage may have no local parent, and a dangling link would be uncollectable.
func (b *PostgresBackend) linkStmt(parent, child uuid.UUID) stmt {
	return stmt{`INSERT INTO runnerq_dependencies(queue_name,waiter_activity_id,result_id,producer_activity_id)
 SELECT $1::text, $2::uuid, $3::uuid, $3::uuid
 WHERE EXISTS(SELECT 1 FROM runnerq_activities WHERE id=$2 AND queue_name=$1)
 ON CONFLICT(queue_name,waiter_activity_id,result_id) DO NOTHING`,
		[]any{b.queueName, parent, child}, "failed to register result dependency"}
}
func (b *PostgresBackend) verifyClaimTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, worker string) error {
	var n int
	err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_activities WHERE id=$1 AND queue_name=$2 AND status='processing' AND current_worker_id=$3 FOR UPDATE`, id, b.queueName, worker).Scan(&n)
	if err == pgx.ErrNoRows {
		return claimLost(id)
	}
	if err != nil {
		return databaseError(err, "failed to verify execution claim")
	}
	return nil
}
func (b *PostgresBackend) RegisterDependency(ctx context.Context, waiter, result uuid.UUID, worker string) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return databaseError(err, "failed to begin dependency transaction")
	}
	defer tx.Rollback(ctx)
	if exists, err := b.lockProducerRootTx(ctx, tx, result); err != nil {
		return err
	} else if !exists {
		return storage.NewNotFoundError(fmt.Sprintf("activity result producer %s no longer exists in this queue", result))
	}
	if err := b.verifyClaimTx(ctx, tx, waiter, worker); err != nil {
		return err
	}
	if err := b.addDependencyTx(ctx, tx, waiter, result, &result); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return databaseError(err, "failed to commit dependency")
	}
	return nil
}

// Spawn links also live in runnerq_dependencies, so a dependency alone does
// not mean the activity is waiting for this result.
func (b *PostgresBackend) wakeStmt(result uuid.UUID) stmt {
	return stmt{`UPDATE runnerq_activities a
 SET status='pending', scheduled_at=NULL, waiting_result_id=NULL
 WHERE a.queue_name=$1 AND a.status='waiting' AND a.waiting_result_id=$2
 AND EXISTS(
 SELECT 1 FROM runnerq_dependencies d WHERE d.queue_name=$1 AND d.result_id=$2 AND d.waiter_activity_id=a.id)`,
		[]any{b.queueName, result}, "failed to wake result consumers"}
}
func (b *PostgresBackend) YieldForResult(ctx context.Context, waiter, result uuid.UUID, producer *uuid.UUID, wakeAt time.Time, worker, kind, step string) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return databaseError(err, "failed to begin durable park")
	}
	defer tx.Rollback(ctx)
	// Lock order: the waiter's row (claim check), then for an await the result
	// owner's via lockProducerRootTx. For a signal the owner IS the waiter, so
	// the claim's FOR UPDATE orders us against SignalActivity's FOR NO KEY UPDATE.
	if err := b.verifyClaimTx(ctx, tx, waiter, worker); err != nil {
		// Reconcile a lost commit reply, even after an early wake or re-claim:
		// the durable event identifies this exact execution's park.
		if se, ok := storage.IsStorageError(err); !ok || se.Kind != storage.ErrClaimLost {
			return err
		}
		var recorded bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runnerq_events WHERE queue_name=$1 AND activity_id=$2 AND worker_id=$3 AND event_type='Yielded' AND detail->>'result_id'=$4 AND detail->>'kind'=$5 AND detail->>'step'=$6)`, b.queueName, waiter, worker, result.String(), kind, step).Scan(&recorded); err != nil {
			return databaseError(err, "failed to reconcile durable park")
		}
		if recorded {
			return nil
		}
		return claimLost(waiter)
	}
	if producer != nil {
		exists, err := b.lockProducerRootTx(ctx, tx, *producer)
		if err != nil {
			return err
		}
		if !exists {
			return storage.NewNotFoundError("park result producer no longer exists")
		}
	}
	if err := b.addDependencyTx(ctx, tx, waiter, result, producer); err != nil {
		return err
	}
	var ready bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runnerq_results WHERE activity_id=$1 AND queue_name=$2)`, result, b.queueName).Scan(&ready); err != nil {
		return databaseError(err, "failed to check park readiness")
	}
	_, err = tx.Exec(ctx, `UPDATE runnerq_activities SET status=CASE WHEN $5 THEN 'pending' ELSE 'waiting' END,
 scheduled_at=CASE WHEN $5 THEN NULL ELSE $3::timestamptz END,
 waiting_result_id=CASE WHEN $5 THEN NULL ELSE $6::uuid END,
 last_worker_id=$4,current_worker_id=NULL,lease_deadline_ms=NULL,started_at=NULL
 WHERE id=$1 AND queue_name=$2`, waiter, b.queueName, wakeAt, worker, ready, result)
	if err != nil {
		return databaseError(err, "failed to park result consumer")
	}
	if err := b.recordEvent(ctx, tx, waiter, storage.EventYielded, &worker, toDetail(map[string]any{"result_id": result, "kind": kind, "step": step, "wake_at": wakeAt, "ready": ready})); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return databaseError(err, "failed to commit durable park")
	}
	if ready || !wakeAt.After(time.Now()) {
		b.signalWork()
	}
	return nil
}
