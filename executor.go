package runnerq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

// ActivityFuture is an activity's result, to be awaited with GetResult.
type ActivityFuture struct {
	queue      activityQueue
	activityID uuid.UUID
}

// ActivityID is the awaited activity's ID; another process can rebuild the
// future from it with FutureFor.
func (f *ActivityFuture) ActivityID() uuid.UUID {
	return f.activityID
}

// FutureFor rebuilds a future from an activity ID, in any process holding a
// backend for the same queue.
func FutureFor(backend storage.Storage, activityID uuid.UUID) *ActivityFuture {
	return &ActivityFuture{
		queue:      newBackendQueueAdapter(backend, nil),
		activityID: activityID,
	}
}

// WaitAll awaits every future and returns their results in order. Inside a
// handler the first pending child parks the parent and completed children
// fast-forward on replay, so awaiting in sequence costs nothing extra.
// Propagate the error unchanged, as with GetResult.
func WaitAll(ctx context.Context, futures ...*ActivityFuture) ([]json.RawMessage, error) {
	results := make([]json.RawMessage, len(futures))
	for i, f := range futures {
		if f == nil {
			return nil, &WorkerError{Kind: ErrQueue, Message: fmt.Sprintf("WaitAll: future at index %d is nil", i)}
		}
		r, err := f.GetResult(ctx)
		if err != nil {
			return nil, err
		}
		results[i] = r
	}
	return results, nil
}

// awaitParkGrace is how long an in-handler GetResult waits in-process before
// parking the parent. Fast children resolve without a park; slower ones cost
// one replay. It also bounds fan-out starvation: parents awaiting children
// all park within it and free their slots for the children.
const awaitParkGrace = 2 * time.Second

// GetResult waits for the activity's result, notified by the backend where
// it can (Postgres can, across processes).
//
// Inside a handler it waits briefly, then YIELDS: it returns a sentinel error
// that MUST be propagated unchanged (as with Sleep and WaitForSignal), and
// the parent is parked, holding no goroutine, lease or retry, until the
// child's completion wakes it. The handler then replays, earlier checkpoints
// fast-forward, and this call returns the result.
//
// Outside a handler it blocks until the result exists or ctx is done.
func (f *ActivityFuture) GetResult(ctx context.Context) (json.RawMessage, error) {
	scoped, inHandler := ctx.Value(attemptQueueKey{}).(*attemptQueue)
	if !inHandler {
		result, err := f.queue.WaitForResult(ctx, f.activityID)
		if err != nil {
			return nil, err
		}
		return translateResult(result)
	}
	grace := awaitParkGrace
	if scoped.awaitGrace > 0 {
		grace = scoped.awaitGrace
	}
	if err := scoped.registerFuture(ctx, f.activityID); err != nil {
		return nil, err
	}
	// Rehydrated futures also need the executing activity's retry/lease
	// policy, even though their original handle had no execution scope.
	attemptQ := *scoped
	attemptQ.activityQueue = f.queue
	if original, ok := f.queue.(*attemptQueue); ok {
		attemptQ.activityQueue = original.activityQueue
	}
	queue := &attemptQ

	// Wait up to the grace, clamped to the handler's budget as Sleep is,
	// then park.
	bound := time.Now().Add(grace)
	if deadline, ok := ctx.Deadline(); ok {
		margin := max(min(yieldMargin, time.Until(deadline)/2), 0)
		if budgetBound := deadline.Add(-margin); budgetBound.Before(bound) {
			bound = budgetBound
		}
	}
	if bound.After(time.Now()) {
		wctx, cancel := context.WithDeadline(ctx, bound)
		result, err := queue.WaitForResult(wctx, f.activityID)
		cancel()
		if err == nil {
			return translateResult(result)
		}
		if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			return nil, err
		}
	}

	// Every terminal ack stores a result and wakes the parent, so the horizon
	// is nominal. recheck closes the race with a child completing mid-park.
	return nil, &yieldPark{
		wakeAt:  time.Now().UTC().Add(signalParkHorizon),
		kind:    "await",
		step:    "await:" + f.activityID.String(),
		recheck: f.activityID,
	}
}

func translateResult(result *activityResult) (json.RawMessage, error) {
	if result.State == ResultOk {
		return result.Data, nil
	}
	resultJSON, _ := json.Marshal(result.Data)
	return nil, &WorkerError{Kind: ErrCustom, Message: string(resultJSON)}
}

// ActivityBuilder configures one spawn; Execute enqueues it.
type ActivityBuilder struct {
	exec           *ActivityExecutor
	activityType   string
	payload        json.RawMessage
	priority       *ActivityPriority
	maxRetries     *uint32
	timeout        *time.Duration
	maxRetryDelay  *time.Duration
	delay          *time.Duration
	idempotencyKey *IdempotencyConfig
	metadata       map[string]string
	asRoot         bool
	step           string
}

// Payload sets the JSON payload; required.
func (b *ActivityBuilder) Payload(payload json.RawMessage) *ActivityBuilder {
	b.payload = payload
	return b
}

// Priority sets the priority (default PriorityNormal).
func (b *ActivityBuilder) Priority(p ActivityPriority) *ActivityBuilder {
	b.priority = &p
	return b
}

// MaxRetries sets the total attempts allowed, including the first (default
// 3): 1 means no retries, 0 unlimited. The name is kept for compatibility.
func (b *ActivityBuilder) MaxRetries(retries uint32) *ActivityBuilder {
	b.maxRetries = &retries
	return b
}

// Timeout bounds one attempt (default 5 minutes).
func (b *ActivityBuilder) Timeout(d time.Duration) *ActivityBuilder {
	b.timeout = &d
	return b
}

// MaxRetryDelay caps the backoff between retries (default 1 hour, at least
// 1s); a non-positive duration is ignored.
func (b *ActivityBuilder) MaxRetryDelay(d time.Duration) *ActivityBuilder {
	if d <= 0 {
		return b
	}
	b.maxRetryDelay = &d
	return b
}

// Delay schedules the activity to run after d.
func (b *ActivityBuilder) Delay(d time.Duration) *ActivityBuilder {
	b.delay = &d
	return b
}

// IdempotencyKeyOption sets an idempotency key, scoped to the activity type,
// and what to do when it is taken.
func (b *ActivityBuilder) IdempotencyKeyOption(key string, behavior OnDuplicate) *ActivityBuilder {
	b.idempotencyKey = &IdempotencyConfig{Key: key, Behavior: behavior}
	return b
}

// Step makes this spawn a named, memoized step of the calling handler: its
// idempotency key derives from (root, parent, name) with ReturnExisting, so a
// retried parent gets the same child back, and its stored result if it has
// completed.
//
// Step names must be stable across retries and unique among the parent's
// spawns (two spawns sharing a name resolve to one child). Only valid inside
// a handler (its ActivityExecutor, or any executor of its queue on its
// context); incompatible with AsRoot and IdempotencyKeyOption.
func (b *ActivityBuilder) Step(name string) *ActivityBuilder {
	b.step = name
	return b
}

// Metadata sets one metadata key.
func (b *ActivityBuilder) Metadata(key, value string) *ActivityBuilder {
	if b.metadata == nil {
		b.metadata = make(map[string]string)
	}
	b.metadata[key] = value
	return b
}

// AsRoot makes the spawn a root (no parent, depth 0) even inside a handler or
// on its context, for side jobs whose lifecycle is independent of the parent.
func (b *ActivityBuilder) AsRoot() *ActivityBuilder {
	b.asRoot = true
	return b
}

// MetadataMap merges metadata into what is already set.
func (b *ActivityBuilder) MetadataMap(metadata map[string]string) *ActivityBuilder {
	if len(metadata) == 0 {
		return b
	}
	if b.metadata == nil {
		b.metadata = make(map[string]string, len(metadata))
	}
	maps.Copy(b.metadata, metadata)
	return b
}

// Execute enqueues the activity and returns its future.
func (b *ActivityBuilder) Execute(ctx context.Context) (*ActivityFuture, error) {
	if b.payload == nil {
		return nil, &WorkerError{Kind: ErrQueue, Message: "Activity payload is required"}
	}
	if b.step != "" {
		if b.idempotencyKey != nil {
			return nil, &WorkerError{Kind: ErrQueue, Message: "Step and IdempotencyKeyOption are mutually exclusive — Step derives its own key"}
		}
		if b.asRoot {
			return nil, &WorkerError{Kind: ErrQueue, Message: "Step requires parent lineage and cannot be combined with AsRoot"}
		}
	}

	hasOption := b.priority != nil || b.maxRetries != nil || b.timeout != nil || b.maxRetryDelay != nil || b.delay != nil || b.idempotencyKey != nil || len(b.metadata) > 0

	var option *ActivityOption
	if hasOption {
		maxRetries := uint32(3)
		if b.maxRetries != nil {
			maxRetries = *b.maxRetries
		}
		timeoutSec := uint64(300)
		if b.timeout != nil {
			timeoutSec = uint64(b.timeout.Seconds())
		}
		var maxRetryDelaySec *uint64
		if b.maxRetryDelay != nil {
			secs := uint64(b.maxRetryDelay.Seconds())
			if secs == 0 {
				secs = 1
			}
			maxRetryDelaySec = &secs
		}
		var delaySec *uint64
		if b.delay != nil {
			d := uint64(b.delay.Seconds())
			delaySec = &d
		}
		var idempKey *IdempotencyConfig
		if b.idempotencyKey != nil {
			// Keys are namespaced by activity type; SignalActivityByKey derives
			// the same stored key.
			idempKey = &IdempotencyConfig{Key: storage.BusinessIdempotencyKey(b.idempotencyKey.Key, b.activityType), Behavior: b.idempotencyKey.Behavior}
		}
		option = &ActivityOption{
			Priority:             b.priority,
			MaxRetries:           maxRetries,
			TimeoutSeconds:       timeoutSec,
			MaxRetryDelaySeconds: maxRetryDelaySec,
			DelaySeconds:         delaySec,
			IdempotencyKey:       idempKey,
			Metadata:             b.metadata, // newActivity copies it
		}
	}

	return b.exec.executeActivity(ctx, b.activityType, b.payload, option, b.asRoot, b.step)
}

// ActivityExecutor spawns activities. An ActivityContext's spawns children of
// the running activity. The engine's (GetActivityExecutor) spawns roots, except
// on a handler's context (ActivityContext.Ctx or one derived from it), where it
// too spawns children of that handler's activity, so code a handler calls
// needn't be handed its executor. AsRoot opts out. Name the target with
// Activity or ActivityNamed.
type ActivityExecutor struct {
	queue    activityQueue
	maxDepth uint16
	lineage  *lineageScope // nil outside a handler
	announce *announcer
}

type lineageScope struct {
	parentID   uuid.UUID
	rootID     uuid.UUID
	childDepth uint16 // parent.Depth + 1
}

// childScopeKey carries a handler's child-scoped ActivityExecutor on its
// context, for root executors spawning on that context.
type childScopeKey struct{}

// handlerScope returns the child-scoped executor of the handler running on
// ctx when it spawns into w's queue, or nil.
func (w *ActivityExecutor) handlerScope(ctx context.Context) *ActivityExecutor {
	scoped, ok := ctx.Value(childScopeKey{}).(*ActivityExecutor)
	if !ok {
		return nil
	}
	if aq, ok := scoped.queue.(*attemptQueue); !ok || aq.activityQueue != w.queue {
		return nil
	}
	return scoped
}

func newActivityExecutor(queue activityQueue, maxDepth uint16, announce *announcer) *ActivityExecutor {
	if maxDepth == 0 {
		maxDepth = DefaultMaxActivityDepth
	}
	return &ActivityExecutor{queue: queue, maxDepth: maxDepth, announce: announce}
}

func (w *ActivityExecutor) scopedForChild(parent *activity) *ActivityExecutor {
	rootID := parent.RootActivityID
	if rootID == (uuid.UUID{}) {
		rootID = parent.ID
	}
	// Saturate rather than wrap to 0; MaxUint16 then fails the depth check.
	childDepth := uint16(math.MaxUint16)
	if parent.Depth < math.MaxUint16 {
		childDepth = parent.Depth + 1
	}
	return &ActivityExecutor{
		queue:    w.queue,
		maxDepth: w.maxDepth,
		announce: w.announce,
		lineage: &lineageScope{
			parentID:   parent.ID,
			rootID:     rootID,
			childDepth: childDepth,
		},
	}
}

// Activity starts a spawn of the activity served by handler type T, as
// registered with RegisterActivity, so the two cannot drift:
//
//	fut, err := ctx.ActivityExecutor.Activity[ResizeImage]().Payload(p).Execute(ctx.Ctx)
//
// T may be the handler struct or a pointer to it. Handlers registered under
// a pinned name (RegisterActivityWithName) are spawned with ActivityNamed.
func (w *ActivityExecutor) Activity[T any]() *ActivityBuilder {
	return w.ActivityNamed(NameOf[T]())
}

// ActivityNamed starts a spawn of activityType by name: for handlers
// registered with RegisterActivityWithName, and for producers that never see
// the handler type.
func (w *ActivityExecutor) ActivityNamed(activityType string) *ActivityBuilder {
	return &ActivityBuilder{
		exec:         w,
		activityType: activityType,
	}
}

func (w *ActivityExecutor) executeActivity(ctx context.Context, activityType string, payload json.RawMessage, option *ActivityOption, asRoot bool, step string) (*ActivityFuture, error) {
	if w.lineage == nil && !asRoot {
		if scoped := w.handlerScope(ctx); scoped != nil {
			return scoped.executeActivity(ctx, activityType, payload, option, false, step)
		}
	}
	a := newActivity(activityType, payload, option)

	if w.lineage != nil && !asRoot {
		if w.lineage.childDepth > w.maxDepth {
			return nil, &WorkerError{
				Kind:    ErrDepthExceeded,
				Message: fmt.Sprintf("activity depth %d exceeds max %d", w.lineage.childDepth, w.maxDepth),
			}
		}
		p := w.lineage.parentID
		a.ParentActivityID = &p
		a.RootActivityID = w.lineage.rootID
		a.Depth = w.lineage.childDepth
	}

	if step != "" {
		if w.lineage == nil || asRoot {
			return nil, &WorkerError{Kind: ErrQueue, Message: "Step is only valid when spawning from inside an activity handler"}
		}
		a.IdempotencyKey = &IdempotencyConfig{
			Key:      storage.StepIdempotencyKey(a.RootActivityID, w.lineage.parentID, step),
			Behavior: ReturnExisting,
		}
	}

	activityID := a.ID

	if a.IdempotencyKey != nil {
		existing, err := w.queue.EnqueueIdempotent(ctx, a)
		if err != nil {
			return nil, WorkerErrorFromStorage(err)
		}
		if existing == nil {
			w.announce.submitted(a)
			return &ActivityFuture{queue: w.queue, activityID: activityID}, nil
		}
		// Another parent reusing an existing child: the row keeps its original
		// parent, and the link is recorded as an event (not for a same-parent
		// retry).
		if w.lineage != nil && !asRoot {
			sameParent := existing.ExistingParentID != nil && *existing.ExistingParentID == w.lineage.parentID
			if !sameParent {
				if err := w.queue.RecordSpawnLinked(ctx, existing.ExistingID, w.lineage.parentID); err != nil {
					slog.Warn("Failed to record spawn link",
						"child_id", existing.ExistingID,
						"parent_id", w.lineage.parentID,
						"error", err)
				}
			}
		}
		return &ActivityFuture{queue: w.queue, activityID: existing.ExistingID}, nil
	}

	if err := w.queue.Enqueue(ctx, a); err != nil {
		return nil, WorkerErrorFromStorage(err)
	}
	w.announce.submitted(a)

	return &ActivityFuture{queue: w.queue, activityID: activityID}, nil
}
