package storage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/internal/spec"
)

// ActivityPriority determines execution ordering.
type ActivityPriority = int

const (
	PriorityLow      ActivityPriority = 1
	PriorityNormal   ActivityPriority = 2
	PriorityHigh     ActivityPriority = 3
	PriorityCritical ActivityPriority = 4
)

// IdempotencyBehavior defines how duplicates are handled.
type IdempotencyBehavior int

const (
	BehaviorAllowReuse IdempotencyBehavior = iota
	BehaviorReturnExisting
	BehaviorAllowReuseOnFailure
	BehaviorNoReuse
)

// ResultState indicates success or failure of an activity result.
type ResultState int

const (
	ResultOk ResultState = iota
	ResultErr
)

// QueuedActivity represents an activity ready to be enqueued.
type QueuedActivity struct {
	ID                   uuid.UUID
	ActivityType         string
	Payload              json.RawMessage
	Priority             ActivityPriority
	MaxRetries           uint32
	RetryCount           uint32
	TimeoutSeconds       uint64
	RetryDelaySeconds    uint64
	MaxRetryDelaySeconds uint64
	ScheduledAt          *time.Time
	Metadata             map[string]string
	IdempotencyKey       *IdempotencyKeyConfig
	CreatedAt            time.Time
	ParentActivityID     *uuid.UUID
	RootActivityID       uuid.UUID
	Depth                uint16
	// Serialization is the payload's encoding: empty means plain JSON
	// (json-v1), all the Go SDK writes. The TypeScript SDK sets superjson-v1;
	// backends store it with the payload and return it with claims.
	Serialization string `json:",omitempty"`
	// CodeVersion pins the activity to a code version (runnerq-spec
	// versions.md): only workers serving it, or unversioned ones, claim it.
	// Empty is unstamped. Backends ignore it for a child and store its root's
	// version instead, and return the stored version with claims.
	CodeVersion string `json:",omitempty"`
}

// SerializationJSON is plain JSON, the encoding an empty Serialization means.
const SerializationJSON = spec.SerializationJSON

// IdempotencyKeyConfig holds a key and its behavior.
type IdempotencyKeyConfig struct {
	Key      string
	Behavior IdempotencyBehavior
}

// IdempotencyResult describes the existing activity that already owns an
// idempotency key. ExistingParentID is nil for root or legacy activities.
type IdempotencyResult struct {
	ExistingID       uuid.UUID
	ExistingParentID *uuid.UUID
}

// DequeuedActivity is an activity claimed by a worker.
type DequeuedActivity struct {
	Activity QueuedActivity
	// LeaseID is the execution token recorded as the claim's worker ID and
	// presented by every fenced acknowledgement. Unique per claimed activity.
	LeaseID       string
	Attempt       uint32
	LeaseDeadline time.Time
}

// BatchQueueStorage is an optional capability for claiming several activities
// in one round trip; when present the engine claims exactly as many as it has
// idle slots instead of looping Dequeue per slot.
//
// DequeueBatch claims at most limit activities under Dequeue's eligibility,
// version and ordering rules. workerIDPrefix is fresh per call; each LeaseID must be unique
// and derived from it (e.g. "<prefix>:<activity id>") so tokens never collide
// across calls. timeout means what it does for Dequeue; an empty result with a
// nil error means nothing became claimable in time.
type BatchQueueStorage interface {
	DequeueBatch(ctx context.Context, workerIDPrefix string, limit int, timeout time.Duration, activityTypes, versions []string) ([]DequeuedActivity, error)
}

// ActivityResult holds result data from a completed activity.
type ActivityResult struct {
	Data  json.RawMessage
	State ResultState
	// Serialization is Data's encoding, as for QueuedActivity.Serialization.
	Serialization string `json:",omitempty"`
}

// StepRecord is one durable Run or Sleep checkpoint, decoded for inspection.
// Kind is "run" or "sleep"; Data is the Run result or the Sleep wake deadline.
type StepRecord struct {
	Kind      string
	Name      string
	State     ResultState
	Data      json.RawMessage
	CreatedAt time.Time
}

// RetentionPolicy sets how long terminal workflow trees are kept before the
// sweeper deletes their activities, events, results (checkpoints included) and
// idempotency keys. Zero keeps that class forever. The unit is the whole tree
// under a terminal root with no non-terminal descendants, never single rows,
// so a retried parent never finds its children's results missing.
type RetentionPolicy struct {
	// Completed applies to trees whose root completed.
	Completed time.Duration
	// Failed applies to failed, dead_letter or cancelled roots, on a separate
	// clock so failures can be held longer for inspection.
	Failed time.Duration
	// Events trims the events of finished activities older than this, before
	// their tree goes. Events of unfinished activities are kept. 0 keeps events
	// as long as their tree.
	Events time.Duration
}

// FailureKind describes how an activity failed.
type FailureKind struct {
	Retryable bool
	Reason    string
	IsTimeout bool
	// Details is the structured failure (the TypeScript SDK's name, message,
	// stack, code and cause), stored in the error result as "failure". Empty
	// for the Go SDK.
	Details json.RawMessage `json:",omitempty"`
}

func NewRetryableFailure(reason string) FailureKind {
	return FailureKind{Retryable: true, Reason: reason}
}

func NewNonRetryableFailure(reason string) FailureKind {
	return FailureKind{Retryable: false, Reason: reason}
}

// NewTimeoutFailure returns a retryable timeout failure.
func NewTimeoutFailure() FailureKind {
	return FailureKind{Retryable: true, Reason: "Activity execution timed out", IsTimeout: true}
}

// ActivitySnapshot is an activity's stored state as storagetest.Reader
// returns it.
type ActivitySnapshot struct {
	ID                uuid.UUID         `json:"id"`
	ActivityType      string            `json:"activity_type"`
	Payload           json.RawMessage   `json:"payload"`
	Priority          ActivityPriority  `json:"priority"`
	Status            string            `json:"status"`
	CreatedAt         time.Time         `json:"created_at"`
	ScheduledAt       *time.Time        `json:"scheduled_at,omitempty"`
	StartedAt         *time.Time        `json:"started_at,omitempty"`
	CompletedAt       *time.Time        `json:"completed_at,omitempty"`
	CurrentWorkerID   *string           `json:"current_worker_id,omitempty"`
	LastWorkerID      *string           `json:"last_worker_id,omitempty"`
	RetryCount        uint32            `json:"retry_count"`
	MaxRetries        uint32            `json:"max_retries"`
	TimeoutSeconds    uint64            `json:"timeout_seconds"`
	RetryDelaySeconds uint64            `json:"retry_delay_seconds"`
	Metadata          map[string]string `json:"metadata"`
	LastError         *string           `json:"last_error,omitempty"`
	LastErrorAt       *time.Time        `json:"last_error_at,omitempty"`
	StatusUpdatedAt   time.Time         `json:"status_updated_at"`
	Score             *float64          `json:"score,omitempty"`
	LeaseDeadlineMS   *int64            `json:"lease_deadline_ms,omitempty"`
	ProcessingMember  *string           `json:"processing_member,omitempty"`
	IdempotencyKey    *string           `json:"idempotency_key,omitempty"`
	ParentActivityID  *uuid.UUID        `json:"parent_activity_id,omitempty"`
	RootActivityID    *uuid.UUID        `json:"root_activity_id,omitempty"`
	Depth             uint16            `json:"depth"`
	CodeVersion       *string           `json:"code_version,omitempty"`
}

// ActivityEventType classifies stored events: only what the activity row
// can't say (runnerq-spec schema/postgres/events.schema.json).
type ActivityEventType = string

const (
	EventFailed     ActivityEventType = "Failed"
	EventRetrying   ActivityEventType = "Retrying"
	EventDeadLetter ActivityEventType = "DeadLetter"
	// EventRequeued: the reaper returned an expired-lease activity to pending.
	EventRequeued ActivityEventType = "Requeued"
	// EventYielded: a durable wait parked the activity without consuming a retry.
	EventYielded  ActivityEventType = "Yielded"
	EventSignaled ActivityEventType = "Signaled"
	// EventSpawnLinked: idempotency reuse linked another parent to an existing
	// activity.
	EventSpawnLinked ActivityEventType = "SpawnLinked"
	// Operator actions applied through CommandStorage.
	EventCancelled       ActivityEventType = "Cancelled"
	EventRetried         ActivityEventType = "Retried"
	EventRedriven        ActivityEventType = "Redriven"
	EventRunNow          ActivityEventType = "RunNow"
	EventRescheduled     ActivityEventType = "Rescheduled"
	EventPriorityChanged ActivityEventType = "PriorityChanged"
	EventRepinned        ActivityEventType = "Repinned"
)

// ActivityEvent records a lifecycle event.
type ActivityEvent struct {
	ActivityID uuid.UUID         `json:"activity_id"`
	Timestamp  time.Time         `json:"timestamp"`
	EventType  ActivityEventType `json:"event_type"`
	WorkerID   *string           `json:"worker_id,omitempty"`
	Detail     json.RawMessage   `json:"detail,omitempty"`
}

// DeadLetterRecord is a dead-lettered activity.
type DeadLetterRecord struct {
	Activity ActivitySnapshot `json:"activity"`
	Error    string           `json:"error"`
	FailedAt time.Time        `json:"failed_at"`
}

// ResultStorage retrieves activity results.
type ResultStorage interface {
	GetResult(ctx context.Context, activityID uuid.UUID) (*ActivityResult, error)
}

// ResultWaiter is an optional capability for blocking until a result exists
// (e.g. via LISTEN/NOTIFY) instead of polling GetResult. The wait must work
// across processes: the waiter and the producing worker usually share only the
// database. It returns once the result exists, or with ctx's error.
type ResultWaiter interface {
	WaitForResult(ctx context.Context, activityID uuid.UUID) (*ActivityResult, error)
}

// ResultNotifier is an optional capability: hints that a result for an id may
// have been stored, in this process or another sharing the database. The
// engine uses it to stop a handler promptly when its activity is cancelled
// (a cancel stores the activity's error result): on a hint it renews the
// claim at once instead of at the next heartbeat. Hints may be lost or
// spurious, so consumers recheck stored state. stop ends the subscription.
type ResultNotifier interface {
	NotifyResult(id uuid.UUID) (hint <-chan struct{}, stop func())
}

// QueueStorage defines core queue operations.
type QueueStorage interface {
	ResultStorage

	Enqueue(ctx context.Context, activity QueuedActivity) error
	// Dequeue claims the next runnable activity, blocking up to timeout for
	// work (0: one non-blocking attempt); (nil, nil) means nothing was claimable.
	// Empty activityTypes claims every type. Non-empty versions claims only
	// unstamped activities and those whose CodeVersion is one of them (version
	// pinning); empty versions applies no version filter.
	Dequeue(ctx context.Context, workerID string, timeout time.Duration, activityTypes, versions []string) (*QueuedActivity, error)
	// AckSuccess completes an activity. The result MUST be stored atomically
	// with the status change, and a result record MUST be written even when
	// result is nil so waiters always resolve. workerID is the unique execution
	// token: retrying an identical committed ack with it must succeed; a stale
	// or conflicting ack must not change the row.
	AckSuccess(ctx context.Context, activityID uuid.UUID, result json.RawMessage, workerID string) error
	// AckFailure records a failure once per execution token and reports whether
	// the activity was dead-lettered. Retrying the same committed failure
	// returns the original decision without consuming another attempt.
	AckFailure(ctx context.Context, activityID uuid.UUID, failure FailureKind, workerID string) (bool, error)
	ProcessScheduled(ctx context.Context) (uint64, error)
	RequeueExpired(ctx context.Context, batchSize int) (uint64, error)
	// Yield parks a processing activity until wakeAt WITHOUT counting a retry
	// (for durable waits longer than the attempt's timeout). Fenced on workerID
	// like the acks: returns ErrClaimLost when that worker no longer holds the
	// claim. kind ("sleep"/"signal"/"await") and step are recorded on the
	// Yielded event only; either may be empty.
	Yield(ctx context.Context, activityID uuid.UUID, wakeAt time.Time, workerID, kind, step string) error
	// StoreResult persists a result row. ownerActivityID governs its lifetime
	// (retention deletes it with the owner's tree): the activity itself for its
	// own result, or the handler's activity for a checkpoint with a synthetic
	// activityID. step is the checkpoint's "kind:name" (e.g.
	// "run:create-transfer"), or "" for an activity's own result.
	StoreResult(ctx context.Context, activityID uuid.UUID, ownerActivityID uuid.UUID, result ActivityResult, step string) error
	// WakeWaiting makes a parked ('waiting') activity runnable now; false in
	// any other state. It closes the race where a result commits between a
	// handler's last check and its park, which produces no wake of its own.
	WakeWaiting(ctx context.Context, activityID uuid.UUID) (bool, error)
	// SignalActivity stores payload as a result row under signalID, owned by
	// activityID, and wakes the activity if parked; store and wake commit
	// atomically. The same signalID again overwrites (last write wins). name
	// (may be "") is recorded as "signal:<name>" on the row and on the Signaled
	// event. Must return a not-found error for an unknown activityID, or the
	// row would never be collected.
	SignalActivity(ctx context.Context, activityID uuid.UUID, signalID uuid.UUID, name string, payload json.RawMessage) error
	// LookupIdempotencyActivityID returns the one activity that owns
	// idempotencyKey in this queue (see runnerq.SignalActivityByKey), or a
	// not-found error when none does (never claimed, or retention-swept).
	LookupIdempotencyActivityID(ctx context.Context, idempotencyKey string) (uuid.UUID, error)
	// CleanupExpired deletes up to batchSize terminal trees older than the
	// policy allows and returns how many. It must (a) delete only trees whose
	// root is terminal with no non-terminal descendants, (b) delete the tree's
	// activities, events, results (by activity AND by owner) and idempotency
	// keys together, and (c) keep concurrent sweepers from duplicating work
	// (returning 0 while another holds the lease is correct).
	CleanupExpired(ctx context.Context, policy RetentionPolicy, batchSize int) (uint64, error)
	// EnqueueIdempotent claims the idempotency key and enqueues the activity
	// atomically, so a crash never leaves a key claimed by an activity that
	// was never enqueued (bricking the key). (nil, nil) means it was enqueued;
	// callers MUST NOT also call Enqueue. When an existing owner is kept under
	// the key's behavior, the result describes it and nothing is enqueued.
	EnqueueIdempotent(ctx context.Context, activity *QueuedActivity) (*IdempotencyResult, error)
	// RecordSpawnLinked records that parentID spawned the existing childID
	// (idempotency reuse). Best-effort: callers log errors and continue.
	RecordSpawnLinked(ctx context.Context, childID, parentID uuid.UUID) error
	// SchedulesNatively reports whether due scheduled activities become
	// claimable without the engine calling ProcessScheduled.
	SchedulesNatively() bool
}

// Storage is the surface a backend must provide. A new backend must pass the
// storage/storagetest conformance suite before it is trusted with durable
// execution. QueryStorage and CommandStorage are optional.
type Storage interface {
	QueueStorage
	ResultStorage
}

// LeaseConfigurer is an optional capability that receives the engine's lease
// duration at startup.
type LeaseConfigurer interface {
	SetLeaseMS(leaseMS int64)
}
