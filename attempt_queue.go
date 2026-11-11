package runnerq

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/runnerq/runnerq-go/storage"
)

// attemptQueueKey carries the executing attempt's queue on a handler's
// context. Its presence tells an in-handler ActivityFuture.GetResult (which
// may yield-park the activity) from an external caller's await, which must
// block: there is no activity row to park.
type attemptQueueKey struct{}

// attemptQueue is the queue as one execution sees it: its writes are fenced
// on the execution's claim, and checkpoint persistence may outlive the
// handler deadline, bounded by engine shutdown and claim ownership.
type attemptQueue struct {
	activityQueue
	backend        storage.Storage
	owner          uuid.UUID
	worker         string
	persistenceCtx context.Context
	metrics        MetricsSink
	awaitGrace     time.Duration // overrides awaitParkGrace when set
}

// attemptHeartbeatInterval bounds how long an execution that lost its lease
// (a stalled process, a long partition) keeps running unawares. Several beats
// fit in attemptLeaseExtension, so one missed beat does not cost the claim.
const attemptHeartbeatInterval = 10 * time.Second

// attemptLeaseExtension is how far each renewal pushes the lease out; a
// renewal never shortens it.
const attemptLeaseExtension = 60 * time.Second

func (q *attemptQueue) renew(ctx context.Context) error {
	if b, ok := q.backend.(storage.AttemptLeaseStorage); ok {
		owned, err := b.ExtendLeaseForWorker(ctx, q.owner, q.worker, attemptLeaseExtension)
		if err != nil {
			return err
		}
		if !owned {
			return &storage.StorageError{Kind: storage.ErrClaimLost, Message: "activity was cancelled or reclaimed by another execution"}
		}
	}
	return nil
}

// Spawns are fenced on the claim when the backend supports it: an execution
// that lost its lease may still be running user code while its replacement
// issues the same spawns, and only one may add children to the tree.
func (q *attemptQueue) Enqueue(ctx context.Context, a *activity) error {
	if b, ok := q.backend.(storage.SpawnStorage); ok {
		return b.EnqueueForWorker(ctx, activityToQueued(a), q.owner, q.worker)
	}
	return q.activityQueue.Enqueue(ctx, a)
}

func (q *attemptQueue) EnqueueIdempotent(ctx context.Context, a *activity) (*storage.IdempotencyResult, error) {
	if b, ok := q.backend.(storage.SpawnStorage); ok {
		queued := activityToQueued(a)
		return b.EnqueueIdempotentForWorker(ctx, &queued, q.owner, q.worker)
	}
	return q.activityQueue.EnqueueIdempotent(ctx, a)
}

// heartbeat renews the claim every interval until ctx ends or the claim is
// lost, when it calls revoke with the ErrClaimLost error. A failed renewal is
// retried on the next beat: the lease already covers the handler's whole
// timeout, so a shorter outage costs nothing. When the backend is a
// storage.ResultNotifier, a hint that the activity's own result was stored
// (a cancel stores one) beats at once, so a cancelled handler stops promptly.
//
// Beats run on a timer. The returned stop waits for a beat in progress, so no
// renewal races the acknowledgement after it (a late beat would find the
// claim released and report it lost).
func (q *attemptQueue) heartbeat(ctx context.Context, interval time.Duration, revoke context.CancelCauseFunc) (stop func()) {
	if _, ok := q.backend.(storage.AttemptLeaseStorage); !ok {
		return func() {}
	}
	if interval <= 0 {
		interval = attemptHeartbeatInterval
	}
	metrics := q.metrics
	if metrics == nil {
		metrics = NoopMetrics{}
	}
	ctx, cancel := context.WithCancel(ctx)
	var (
		mu      sync.Mutex // held by a running beat; stop waits on it
		stopped bool
		timer   *time.Timer
	)
	beat := func() {
		mu.Lock()
		defer mu.Unlock()
		if stopped || ctx.Err() != nil {
			return
		}
		attempt, attemptCancel := context.WithTimeout(ctx, storageAttemptTimeout)
		err := q.renew(attempt)
		attemptCancel()
		if err != nil && ctx.Err() == nil {
			if se, ok := storage.IsStorageError(err); ok && se.Kind == storage.ErrClaimLost {
				revoke(err)
				return
			}
			metrics.IncCounter(metricHeartbeatFailed, 1)
			slog.Warn("Could not renew activity claim; retrying on the next heartbeat", "activity_id", q.owner, "error", err)
		}
		timer.Reset(interval)
	}
	mu.Lock()
	timer = time.AfterFunc(interval, beat)
	mu.Unlock()
	if n, ok := q.backend.(storage.ResultNotifier); ok {
		hint, stopHint := n.NotifyResult(q.owner)
		go func() {
			defer stopHint()
			for {
				select {
				case <-ctx.Done():
					return
				case <-hint:
					mu.Lock()
					if !stopped {
						timer.Reset(0)
					}
					mu.Unlock()
				}
			}
		}()
	}
	return func() {
		cancel()
		mu.Lock()
		stopped = true
		timer.Stop()
		mu.Unlock()
	}
}

func (q *attemptQueue) GetResult(ctx context.Context, id uuid.UUID) (out *activityResult, err error) {
	err = q.retryRead(ctx, "read checkpoint", func(ctx context.Context) error {
		var err error
		out, err = q.activityQueue.GetResult(ctx, id)
		return err
	})
	return
}

func (q *attemptQueue) WaitForResult(ctx context.Context, id uuid.UUID) (out *activityResult, err error) {
	err = q.retryRead(ctx, "wait for result", func(ctx context.Context) error {
		var err error
		out, err = q.activityQueue.WaitForResult(ctx, id)
		return err
	})
	return
}

func (q *attemptQueue) StoreResult(_ context.Context, id, owner uuid.UUID, result activityResult, step string) error {
	return retryStorage(q.persistenceCtx, "persist checkpoint", q.metrics, q.renew, func(ctx context.Context) error {
		if b, ok := q.backend.(storage.CheckpointStorage); ok {
			return b.StoreCheckpoint(ctx, id, owner, q.worker, storage.ActivityResult{Data: result.Data, State: storage.ResultState(result.State)}, step)
		}
		return q.activityQueue.StoreResult(ctx, id, owner, result, step)
	})
}

func (q *attemptQueue) registerFuture(ctx context.Context, id uuid.UUID) error {
	if b, ok := q.backend.(storage.DependencyStorage); ok {
		return retryStorage(ctx, "register result dependency", q.metrics, q.renew, func(ctx context.Context) error {
			return b.RegisterDependency(ctx, q.owner, id, q.worker)
		})
	}
	return nil
}

// retryRead retries a read. Once renewal shows the claim is lost it stops
// reading: a missing checkpoint returned then could trigger a repeated effect.
func (q *attemptQueue) retryRead(ctx context.Context, operation string, fn func(context.Context) error) error {
	var lost error
	renew := func(ctx context.Context) error {
		err := q.renew(ctx)
		if se, ok := storage.IsStorageError(err); ok && se.Kind == storage.ErrClaimLost {
			lost = err
		}
		return err
	}
	return retryStorage(ctx, operation, q.metrics, renew, func(ctx context.Context) error {
		if lost != nil {
			return lost
		}
		return fn(ctx)
	})
}
