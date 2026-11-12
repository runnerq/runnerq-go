package runnerq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// ResultState says whether a stored result is a success or a failure.
type ResultState int

const (
	ResultOk ResultState = iota
	ResultErr
)

type activityResult struct {
	Data  json.RawMessage `json:"data,omitempty"`
	State ResultState     `json:"state"`
}

// activityQueue is the engine's view of its backend; backends implement
// storage.Storage.
type activityQueue interface {
	Enqueue(ctx context.Context, a *activity) error
	Dequeue(ctx context.Context, timeout time.Duration, workerID string) (*activity, error)
	// MarkCompleted stores the result atomically with completion, even a nil
	// one, so awaiting parents resolve.
	MarkCompleted(ctx context.Context, a *activity, result json.RawMessage, workerID string) error
	// MarkFailed reports whether the activity was dead-lettered.
	MarkFailed(ctx context.Context, a *activity, errorMessage string, retryable bool, workerID string) (bool, error)
	ProcessScheduledActivities(ctx context.Context) error
	// Yield parks the activity until wakeAt without consuming a retry; kind
	// and step only describe the wait on the Yielded event.
	Yield(ctx context.Context, a *activity, wakeAt time.Time, workerID, kind, step string) error
	RequeueExpired(ctx context.Context, maxToProcess int) (uint64, error)
	// EnqueueIdempotent claims the key and enqueues atomically. A non-nil
	// result is the activity already owning the key; nothing was enqueued.
	EnqueueIdempotent(ctx context.Context, a *activity) (*storage.IdempotencyResult, error)
	// StoreResult stores a result owned by owner's workflow tree; step is a
	// checkpoint's "kind:name", "" for an activity's own result.
	StoreResult(ctx context.Context, activityID uuid.UUID, owner uuid.UUID, result activityResult, step string) error
	GetResult(ctx context.Context, activityID uuid.UUID) (*activityResult, error)
	WaitForResult(ctx context.Context, activityID uuid.UUID) (*activityResult, error)
	RecordSpawnLinked(ctx context.Context, childID, parentID uuid.UUID) error
	SchedulesNatively() bool
}

// batchActivityQueue is implemented only when the backend can claim in bulk
// (storage.BatchQueueStorage).
type batchActivityQueue interface {
	activityQueue
	// DequeueBatch claims up to limit activities, blocking up to timeout for
	// the first. workerIDPrefix must be fresh per call; tokens derive from it.
	DequeueBatch(ctx context.Context, limit int, timeout time.Duration, workerIDPrefix string) ([]claimedActivity, error)
}

// claimedActivity pairs an activity with its claim token, which every
// acknowledgement must present.
type claimedActivity struct {
	activity *activity
	leaseID  string
}

// claimFilter lets Start set the claim filter once handlers are known.
type claimFilter interface {
	setClaimFilter(types, versions []string)
}

type backendQueueAdapter struct {
	backend       storage.Storage
	activityTypes []string
	versions      []string
}

type batchBackendQueueAdapter struct {
	*backendQueueAdapter
	batch storage.BatchQueueStorage
}

// newBackendQueueAdapter returns a batchActivityQueue when the backend can
// claim in bulk.
func newBackendQueueAdapter(backend storage.Storage, activityTypes []string) activityQueue {
	base := &backendQueueAdapter{backend: backend, activityTypes: activityTypes}
	if batch, ok := backend.(storage.BatchQueueStorage); ok {
		return &batchBackendQueueAdapter{backendQueueAdapter: base, batch: batch}
	}
	return base
}

func (a *backendQueueAdapter) setClaimFilter(types, versions []string) {
	a.activityTypes, a.versions = types, versions
}

func activityToQueued(a *activity) storage.QueuedActivity {
	var idempKey *storage.IdempotencyKeyConfig
	if a.IdempotencyKey != nil {
		idempKey = &storage.IdempotencyKeyConfig{
			Key:      a.IdempotencyKey.Key,
			Behavior: onDuplicateToStorageBehavior(a.IdempotencyKey.Behavior),
		}
	}
	return storage.QueuedActivity{
		ID:                   a.ID,
		ActivityType:         a.ActivityType,
		Payload:              a.Payload,
		Priority:             storage.ActivityPriority(a.Priority),
		MaxRetries:           a.MaxRetries,
		RetryCount:           a.RetryCount,
		TimeoutSeconds:       a.TimeoutSeconds,
		RetryDelaySeconds:    a.RetryDelaySeconds,
		MaxRetryDelaySeconds: a.MaxRetryDelaySeconds,
		ScheduledAt:          a.ScheduledAt,
		Metadata:             a.Metadata,
		IdempotencyKey:       idempKey,
		CreatedAt:            a.CreatedAt,
		ParentActivityID:     a.ParentActivityID,
		RootActivityID:       a.RootActivityID,
		Depth:                a.Depth,
		CodeVersion:          a.CodeVersion,
	}
}

func queuedToActivity(q *storage.QueuedActivity) *activity {
	var idempKey *IdempotencyConfig
	if q.IdempotencyKey != nil {
		idempKey = &IdempotencyConfig{
			Key:      q.IdempotencyKey.Key,
			Behavior: storageBehaviorToOnDuplicate(q.IdempotencyKey.Behavior),
		}
	}
	rootID := q.RootActivityID
	if rootID == (uuid.UUID{}) {
		rootID = q.ID
	}
	return &activity{
		ID:                   q.ID,
		ActivityType:         q.ActivityType,
		Payload:              q.Payload,
		Priority:             ActivityPriority(q.Priority),
		Status:               StatusPending,
		CreatedAt:            q.CreatedAt,
		ScheduledAt:          q.ScheduledAt,
		RetryCount:           q.RetryCount,
		MaxRetries:           q.MaxRetries,
		TimeoutSeconds:       q.TimeoutSeconds,
		RetryDelaySeconds:    q.RetryDelaySeconds,
		MaxRetryDelaySeconds: q.MaxRetryDelaySeconds,
		Metadata:             q.Metadata,
		IdempotencyKey:       idempKey,
		ParentActivityID:     q.ParentActivityID,
		RootActivityID:       rootID,
		Depth:                q.Depth,
		CodeVersion:          q.CodeVersion,
	}
}

func onDuplicateToStorageBehavior(od OnDuplicate) storage.IdempotencyBehavior {
	switch od {
	case AllowReuse:
		return storage.BehaviorAllowReuse
	case ReturnExisting:
		return storage.BehaviorReturnExisting
	case AllowReuseOnFailure:
		return storage.BehaviorAllowReuseOnFailure
	case NoReuse:
		return storage.BehaviorNoReuse
	default:
		return storage.BehaviorAllowReuse
	}
}

func storageBehaviorToOnDuplicate(b storage.IdempotencyBehavior) OnDuplicate {
	switch b {
	case storage.BehaviorAllowReuse:
		return AllowReuse
	case storage.BehaviorReturnExisting:
		return ReturnExisting
	case storage.BehaviorAllowReuseOnFailure:
		return AllowReuseOnFailure
	case storage.BehaviorNoReuse:
		return NoReuse
	default:
		return AllowReuse
	}
}

func (a *backendQueueAdapter) Enqueue(ctx context.Context, act *activity) error {
	return a.backend.Enqueue(ctx, activityToQueued(act))
}

func (a *backendQueueAdapter) Dequeue(ctx context.Context, timeout time.Duration, workerID string) (*activity, error) {
	q, err := a.backend.Dequeue(ctx, workerID, timeout, a.activityTypes, a.versions)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, nil
	}
	return queuedToActivity(q), nil
}

func (a *batchBackendQueueAdapter) DequeueBatch(ctx context.Context, limit int, timeout time.Duration, workerIDPrefix string) ([]claimedActivity, error) {
	claims, err := a.batch.DequeueBatch(ctx, workerIDPrefix, limit, timeout, a.activityTypes, a.versions)
	if err != nil {
		return nil, err
	}
	// Guard custom backends: an activity run under an invented token or
	// beyond the concurrency budget is worse than one left to lease recovery.
	if len(claims) > limit {
		return nil, storage.NewInternalError(fmt.Sprintf("batch dequeue returned %d activities for a limit of %d", len(claims), limit))
	}
	out := make([]claimedActivity, 0, len(claims))
	for i := range claims {
		c := &claims[i]
		if c.LeaseID == "" {
			return nil, storage.NewInternalError(fmt.Sprintf("batch dequeue returned activity %s without a lease token", c.Activity.ID))
		}
		out = append(out, claimedActivity{activity: queuedToActivity(&c.Activity), leaseID: c.LeaseID})
	}
	return out, nil
}

func (a *backendQueueAdapter) MarkCompleted(ctx context.Context, act *activity, result json.RawMessage, workerID string) error {
	return a.backend.AckSuccess(ctx, act.ID, result, workerID)
}

func (a *backendQueueAdapter) MarkFailed(ctx context.Context, act *activity, errorMessage string, retryable bool, workerID string) (bool, error) {
	var failure storage.FailureKind
	if retryable {
		failure = storage.NewRetryableFailure(errorMessage)
	} else {
		failure = storage.NewNonRetryableFailure(errorMessage)
	}
	return a.backend.AckFailure(ctx, act.ID, failure, workerID)
}

func (a *backendQueueAdapter) ProcessScheduledActivities(ctx context.Context) error {
	_, err := a.backend.ProcessScheduled(ctx)
	return err
}

func (a *backendQueueAdapter) RequeueExpired(ctx context.Context, maxToProcess int) (uint64, error) {
	return a.backend.RequeueExpired(ctx, maxToProcess)
}

func (a *backendQueueAdapter) Yield(ctx context.Context, act *activity, wakeAt time.Time, workerID, kind, step string) error {
	return a.backend.Yield(ctx, act.ID, wakeAt, workerID, kind, step)
}

func (a *backendQueueAdapter) EnqueueIdempotent(ctx context.Context, act *activity) (*storage.IdempotencyResult, error) {
	queued := activityToQueued(act)
	return a.backend.EnqueueIdempotent(ctx, &queued)
}

func (a *backendQueueAdapter) StoreResult(ctx context.Context, activityID uuid.UUID, owner uuid.UUID, result activityResult, step string) error {
	return a.backend.StoreResult(ctx, activityID, owner, storage.ActivityResult{Data: result.Data, State: storage.ResultState(result.State)}, step)
}

func (a *backendQueueAdapter) GetResult(ctx context.Context, activityID uuid.UUID) (*activityResult, error) {
	r, err := a.backend.GetResult(ctx, activityID)
	if err != nil || r == nil {
		return nil, err
	}
	return &activityResult{Data: r.Data, State: ResultState(r.State)}, nil
}

// WaitForResult uses the backend's storage.ResultWaiter when it has one, and
// otherwise polls GetResult every 100ms.
func (a *backendQueueAdapter) WaitForResult(ctx context.Context, activityID uuid.UUID) (*activityResult, error) {
	if rw, ok := a.backend.(storage.ResultWaiter); ok {
		r, err := rw.WaitForResult(ctx, activityID)
		if err != nil {
			return nil, err
		}
		return &activityResult{Data: r.Data, State: ResultState(r.State)}, nil
	}

	for {
		result, err := a.GetResult(ctx, activityID)
		if err != nil {
			return nil, err
		}
		if result != nil {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (a *backendQueueAdapter) RecordSpawnLinked(ctx context.Context, childID, parentID uuid.UUID) error {
	return a.backend.RecordSpawnLinked(ctx, childID, parentID)
}

func (a *backendQueueAdapter) SchedulesNatively() bool {
	return a.backend.SchedulesNatively()
}
