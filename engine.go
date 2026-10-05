package runnerq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"os"
	"os/signal"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/executor"
	"github.com/alob-mtc/runnerq-go/storage"
)

// WorkerEngine claims activities from its backend and runs their handlers.
type WorkerEngine struct {
	queue    activityQueue
	backend  storage.Storage
	handlers map[string]ActivityHandler
	config   WorkerConfig
	running  atomic.Bool
	metrics  MetricsSink

	// mu guards the fields below: Start and Stop usually run on different
	// goroutines, and it makes closing shutdownCh idempotent.
	mu           sync.Mutex
	active       bool               // true through startup and until the last drain finishes
	cancelFunc   context.CancelFunc // cancels the engine context (handlers, acks) — phase 3
	intakeCancel context.CancelFunc // cancels only the dequeue/poll loops — phase 1
	shutdownCh   chan struct{}

	// instanceID prefixes claim tokens so they are unique across processes;
	// otherwise a stale worker could pass the ack fence on a row another
	// process had reclaimed.
	instanceID string

	heartbeatInterval time.Duration // overrides attemptHeartbeatInterval when set
	awaitGrace        time.Duration // overrides awaitParkGrace when set

	inflight  sync.Map  // id -> *running: the activities executing here
	startedAt time.Time // guarded by mu; zero until Start

	// counts feeds Snapshot and forwards to the configured sink; metrics is
	// always counts.
	counts      *countingMetrics
	hostname    string
	sdk         executor.SDK
	observers   []executor.Observer // guarded by mu
	servedTypes []string            // guarded by mu; set by Start

	changes  executor.Signal
	announce *announcer
}

// Interrupt stops the handler running activityID here, as if its claim had
// been lost, and reports whether one was running. Call it after cancelling
// the activity in storage: the heartbeat would notice within an interval;
// this makes it immediate.
func (e *WorkerEngine) Interrupt(activityID uuid.UUID) bool {
	v, ok := e.inflight.Load(activityID)
	if !ok {
		return false
	}
	v.(*running).revoke(&storage.StorageError{
		Kind: storage.ErrClaimLost, Message: fmt.Sprintf("activity %s was cancelled", activityID),
	})
	return true
}

// InFlightActivity is an activity the engine is executing.
type InFlightActivity struct {
	ID        uuid.UUID
	Type      string
	Attempt   int
	StartedAt time.Time
}

type running struct {
	InFlightActivity
	revoke context.CancelCauseFunc
}

// InFlight returns the activities this engine is executing, oldest first.
func (e *WorkerEngine) InFlight() []InFlightActivity {
	var out []InFlightActivity
	e.inflight.Range(func(_, v any) bool {
		out = append(out, v.(*running).InFlightActivity)
		return true
	})
	slices.SortFunc(out, func(a, b InFlightActivity) int { return a.StartedAt.Compare(b.StartedAt) })
	return out
}

// Snapshot describes this executor now: who it is, what it is running and
// what it has done since it was built. Safe from any goroutine.
func (e *WorkerEngine) Snapshot() executor.Snapshot {
	e.mu.Lock()
	types := slices.Clone(e.servedTypes)
	started := e.startedAt
	e.mu.Unlock()
	if types == nil {
		types = e.ActivityTypes()
	}
	snap := executor.Snapshot{
		Info: executor.Info{
			ID:             e.instanceID,
			Queue:          e.config.QueueName,
			ActivityTypes:  types,
			MaxConcurrency: e.config.MaxConcurrentActivities,
			StartedAt:      started,
			Hostname:       e.hostname,
			SDK:            e.sdk,
			Labels:         maps.Clone(e.config.Labels),
		},
		State:    executor.State{Draining: e.Draining()},
		Counters: e.counts.counters(),
		At:       time.Now().UTC(),
	}
	for _, a := range e.InFlight() {
		snap.State.Running = append(snap.State.Running, executor.Running{ID: a.ID, Type: a.Type, Attempt: a.Attempt, StartedAt: a.StartedAt})
	}
	return snap
}

// Changed returns a channel closed at the engine's next change: an activity
// starting or finishing, or a drain beginning (executor.Notifier).
func (e *WorkerEngine) Changed() <-chan struct{} {
	return e.changes.Changed()
}

// Announce sends a every lifecycle change this engine makes from now on:
// submissions through its ActivityExecutors, claims and successes. nil stops.
// Safe at any time; RunnerQ Cloud's agent uses it while someone is watching.
func (e *WorkerEngine) Announce(a executor.Announcer) {
	e.announce.set(a)
}

// Observe tells o when this engine starts and stops, so it can report the
// engine's Snapshot while it runs. A backend that is an executor.Observer is
// attached automatically. Call before Start.
func (e *WorkerEngine) Observe(o executor.Observer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active {
		panic("cannot add an executor observer while engine is active")
	}
	e.observers = append(e.observers, o)
}

// StartedAt is when Start was last called, or zero.
func (e *WorkerEngine) StartedAt() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.startedAt
}

// Draining reports whether a shutdown has begun: intake has stopped and
// in-flight activities are finishing.
func (e *WorkerEngine) Draining() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	select {
	case <-e.shutdownCh:
		return true
	default:
		return false
	}
}

// NewWorkerEngineWithBackend creates a WorkerEngine from a backend and a
// full configuration; Builder is the shorter way.
func NewWorkerEngineWithBackend(backend storage.Storage, config WorkerConfig) *WorkerEngine {
	config = cloneWorkerConfig(config)
	if lc, ok := backend.(storage.LeaseConfigurer); ok && config.LeaseMS != nil {
		leaseMS := min(*config.LeaseMS, math.MaxInt64)
		lc.SetLeaseMS(int64(leaseMS))
	}

	adapter := newBackendQueueAdapter(backend, config.ActivityTypes)
	shutdownCh := make(chan struct{})
	counts := newCountingMetrics(NoopMetrics{})
	return &WorkerEngine{
		queue:      adapter,
		backend:    backend,
		handlers:   make(map[string]ActivityHandler),
		config:     config,
		shutdownCh: shutdownCh,
		metrics:    counts,
		counts:     counts,
		instanceID: uuid.New().String(),
		hostname:   executor.Hostname(),
		sdk:        executor.ThisSDK(),
		announce:   &announcer{},
	}
}

// SetMetrics sets the metrics sink. Panics once the engine is running.
func (e *WorkerEngine) SetMetrics(sink MetricsSink) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active {
		panic("cannot change metrics while engine is active")
	}
	if sink == nil {
		sink = NoopMetrics{}
	}
	e.counts.sink = sink
}

func (e *WorkerEngine) Backend() storage.Storage {
	return e.backend
}

func (e *WorkerEngine) MaxConcurrentActivities() int {
	return e.config.MaxConcurrentActivities
}

func (e *WorkerEngine) QueueName() string {
	return e.config.QueueName
}

// InstanceID is this engine's random, per-construction identity: it prefixes
// claim tokens and is RunnerQ Cloud's executor id.
func (e *WorkerEngine) InstanceID() string {
	return e.instanceID
}

// ActivityTypes returns the registered activity types, sorted.
func (e *WorkerEngine) ActivityTypes() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Sorted(maps.Keys(e.handlers))
}

// RegisterActivity registers a handler under its Go type name: &ResizeImage{}
// serves "ResizeImage", as NameOf derives for spawns. It panics for an
// unnamed type (use RegisterActivityWithName), a type already registered, or
// once the engine is running.
//
// The type is stored with every activity, so renaming the handler struct
// strands rows already enqueued; pin the name with RegisterActivityWithName
// where that matters.
func (e *WorkerEngine) RegisterActivity(handler ActivityHandler) {
	if isNilHandler(handler) {
		panic("handler must be non-nil")
	}
	activityType, err := activityTypeOf(reflect.TypeOf(handler))
	if err != nil {
		panic(err)
	}
	e.RegisterActivityWithName(activityType, handler)
}

// RegisterActivityWithName registers a handler under an explicit activity
// type, decoupling the stored type from the Go name or serving several types
// with one handler type. It panics on an empty type, a nil handler, a type
// already registered, or once the engine is running.
func (e *WorkerEngine) RegisterActivityWithName(activityType string, handler ActivityHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active {
		panic("cannot register activities while engine is active")
	}
	if activityType == "" || isNilHandler(handler) {
		panic("activity type and handler must be non-empty")
	}
	if _, dup := e.handlers[activityType]; dup {
		panic(fmt.Sprintf("activity type %q is already registered", activityType))
	}
	e.handlers[activityType] = handler
}

// isNilHandler also catches a typed nil such as (*ChargeCard)(nil), which
// compares unequal to nil yet fails on the first Handle call.
func isNilHandler(handler ActivityHandler) bool {
	if handler == nil {
		return true
	}
	v := reflect.ValueOf(handler)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return v.IsNil()
	}
	return false
}

// GetActivityExecutor returns an executor for spawning root activities. On a
// handler's context it spawns children of that handler's activity instead
// (see ActivityExecutor).
func (e *WorkerEngine) GetActivityExecutor() *ActivityExecutor {
	return newActivityExecutor(e.queue, e.config.MaxActivityDepth, e.announce)
}

// Start runs the engine until ctx ends, Stop is called or the process gets
// SIGINT/SIGTERM, then drains in-flight activities and returns.
func (e *WorkerEngine) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.active {
		e.mu.Unlock()
		return &WorkerError{Kind: ErrAlreadyRunning}
	}
	if err := ctx.Err(); err != nil {
		e.mu.Unlock()
		return err
	}
	if e.config.MaxConcurrentActivities <= 0 || len(e.handlers) == 0 {
		e.mu.Unlock()
		return &WorkerError{Kind: ErrConfiguration, Message: "at least one worker and registered handler are required"}
	}
	types := slices.Clone(e.config.ActivityTypes)
	if len(types) == 0 {
		types = slices.Sorted(maps.Keys(e.handlers))
	}
	for _, t := range types {
		if _, ok := e.handlers[t]; !ok {
			e.mu.Unlock()
			return &WorkerError{Kind: ErrConfiguration, Message: fmt.Sprintf("no handler registered for activity type %q", t)}
		}
	}
	if r := e.config.Retention; r != nil && (r.Completed < 0 || r.Failed < 0 || r.Interval < 0 || r.BatchSize < 0) {
		e.mu.Unlock()
		return &WorkerError{Kind: ErrConfiguration, Message: "retention values must be non-negative"}
	}
	managedMaintenance := false
	if managed, ok := e.backend.(storage.ManagedMaintenanceStorage); ok {
		managedMaintenance = managed.MaintenanceManaged()
	}
	if managedMaintenance && e.config.Retention != nil {
		e.mu.Unlock()
		return &WorkerError{Kind: ErrConfiguration, Message: "configure retention on the managed storage service"}
	}
	if q, ok := e.queue.(activityTypeFilter); ok {
		q.setActivityTypes(types)
	}
	e.active = true
	e.startedAt = time.Now().UTC()
	e.servedTypes = slices.Clone(types)
	observers := slices.Clone(e.observers)
	if o, ok := e.backend.(executor.Observer); ok {
		observers = append(observers, o)
	}
	e.running.Store(true)
	// Parent cancellation stops intake below. Handler and persistence lifetime
	// ends after the drain, including when the caller uses a signal context.
	engineCtx, engineCancel := context.WithCancel(context.WithoutCancel(ctx))
	intakeCtx, intakeCancel := context.WithCancel(engineCtx)
	e.cancelFunc = engineCancel
	e.intakeCancel = intakeCancel
	e.shutdownCh = make(chan struct{})
	shutdownCh := e.shutdownCh
	e.mu.Unlock()
	defer engineCancel()
	slog.Info("Starting worker engine", "max_concurrent_activities", e.config.MaxConcurrentActivities)
	for _, o := range observers {
		o.ExecutorStarted(e)
		defer o.ExecutorStopped(e.instanceID)
	}

	var wg sync.WaitGroup

	if !e.queue.SchedulesNatively() {
		wg.Go(func() {
			e.runScheduledProcessor(intakeCtx)
		})
	}

	if !managedMaintenance {
		wg.Go(func() {
			e.runReaperProcessor(intakeCtx)
		})
	}

	// The backend elects one retention sweeper per queue.
	if e.config.Retention != nil && !managedMaintenance {
		wg.Go(func() {
			e.runRetentionProcessor(intakeCtx)
		})
	}

	// Intake: a backend that claims in bulk gets one dispatcher claiming as
	// many activities as there are idle slots; others get one blocking-claim
	// loop per slot.
	if batchQueue, ok := e.queue.(batchActivityQueue); ok {
		wg.Go(func() {
			e.runBatchDispatcher(intakeCtx, engineCtx, batchQueue)
		})
	} else {
		for i := range e.config.MaxConcurrentActivities {
			wg.Go(func() {
				e.runWorkerLoop(intakeCtx, engineCtx, i)
			})
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Stop may have been called while the engine was starting.
	if !e.running.Load() {
		slog.Info("Shutdown requested during startup")
	} else {
		select {
		case sig := <-sigCh:
			slog.Info("Received shutdown signal", "signal", sig)
		case <-ctx.Done():
			slog.Info("Context cancelled")
		case <-shutdownCh:
			slog.Info("Shutdown requested")
		}
	}

	// Phase 1: stop intake. In-flight handlers keep engineCtx so they can
	// still ack; it is cancelled once the drain finishes or the grace ends.
	e.Stop()

	// Phase 2: drain, bounded by one shutdown grace. In-flight activities run
	// inside the intake goroutines, so wg covers them too.
	graceSec := uint64(30)
	if e.config.ShutdownGraceSeconds != nil {
		graceSec = max(*e.config.ShutdownGraceSeconds, 1)
	}
	graceCtx, graceCancel := context.WithTimeout(context.Background(), time.Duration(graceSec)*time.Second)
	defer graceCancel()

	drained := make(chan struct{})
	go func() { wg.Wait(); close(drained) }()
	defer func() {
		engineCancel()
		finish := func() { e.mu.Lock(); e.active = false; e.mu.Unlock() }
		select {
		case <-drained:
			finish()
		default:
			go func() { <-drained; finish() }()
		}
	}()

	select {
	case <-drained:
		slog.Debug("Shutdown drain complete")
	case <-graceCtx.Done():
		slog.Warn("Shutdown grace exceeded; returning with activities in flight",
			"grace_seconds", graceSec)
		signal.Stop(sigCh)
		return nil
	}

	signal.Stop(sigCh)
	slog.Info("Worker engine stopped")
	return nil
}

// Stop begins a graceful shutdown: intake stops, and Start returns once
// in-flight activities have finished (bounded by the shutdown grace).
//
// Stop leaves the engine context alive: cancelling it would make a handler
// that completes during the drain fail its ack and rerun elsewhere. Start
// drains and then cancels it.
func (e *WorkerEngine) Stop() {
	slog.Info("Stopping worker engine")
	e.mu.Lock()
	e.running.Store(false)
	defer e.mu.Unlock()
	if e.intakeCancel != nil {
		e.intakeCancel()
	}
	if e.shutdownCh != nil {
		select {
		case <-e.shutdownCh:
		default:
			close(e.shutdownCh)
			e.changes.Notify()
		}
	}
}

// workerDequeueBlock bounds one blocking claim. The backend wakes it on new
// work and ctx cancellation ends it, so this only sets how often the loop
// comes up for air.
const workerDequeueBlock = 15 * time.Second

// runWorkerLoop claims on ctx (intake, cancelled first at shutdown) and
// executes on handlerCtx, which outlives intake so a draining handler can
// still ack.
func (e *WorkerEngine) runWorkerLoop(ctx, handlerCtx context.Context, workerID int) {
	slog.Debug("Starting worker loop", "worker_id", workerID)
	for e.running.Load() {
		select {
		case <-ctx.Done():
			slog.Debug("Worker loop stopped (context)", "worker_id", workerID)
			return
		default:
		}

		// A fresh token per claim, so an old attempt's ack can't match a new
		// one on the same slot.
		workerLabel := fmt.Sprintf("%s:worker-%d:%s", e.instanceID, workerID, uuid.New())
		act, err := e.queue.Dequeue(ctx, workerDequeueBlock, workerLabel)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("Failed to dequeue activity", "worker_id", workerID, "error", err)
			if !pauseAfterDequeueError(ctx) {
				return
			}
			continue
		}

		if act == nil {
			continue
		}

		e.processActivity(handlerCtx, act, workerLabel, workerID)
	}

	slog.Debug("Worker loop stopped", "worker_id", workerID)
}

// runBatchDispatcher is the intake loop for batch-capable backends. Idle
// slots are tokens in a channel; it takes every idle slot, claims that many
// activities in one round trip, and runs each on its own goroutine, which
// returns the slot when the activity completes, parks or fails.
//
// Claiming only for idle slots means no lease is held by an activity queued
// in memory, and a busy engine claims once per wave of freed slots. Like
// runWorkerLoop it claims on ctx and executes on handlerCtx, and it returns
// only after every activity it dispatched has finished.
func (e *WorkerEngine) runBatchDispatcher(ctx, handlerCtx context.Context, queue batchActivityQueue) {
	n := e.config.MaxConcurrentActivities
	slog.Debug("Starting batch dispatcher", "max_concurrent_activities", n)

	// Slot numbers double as the worker_id in logs.
	idle := make(chan int, n)
	for slot := range n {
		idle <- slot
	}
	var inFlight sync.WaitGroup
	defer inFlight.Wait()

	var held []int // slots taken from idle and not yet assigned to a claim
	for e.running.Load() {
		if len(held) == 0 {
			select {
			case <-ctx.Done():
				return
			case slot := <-idle:
				held = append(held, slot)
			}
		}
		held = takeIdle(idle, held)

		// A fresh prefix per call; the backend appends each activity's id.
		prefix := fmt.Sprintf("%s:batch:%s", e.instanceID, uuid.New())
		claims, err := queue.DequeueBatch(ctx, len(held), workerDequeueBlock, prefix)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("Failed to batch dequeue activities", "error", err, "limit", len(held))
			if !pauseAfterDequeueError(ctx) {
				return
			}
			continue
		}
		for _, claim := range claims {
			slot := held[len(held)-1]
			held = held[:len(held)-1]
			inFlight.Go(func() {
				defer func() { idle <- slot }()
				e.processActivity(handlerCtx, claim.activity, claim.leaseID, slot)
			})
		}
	}

	slog.Debug("Batch dispatcher stopped")
}

// takeIdle appends every slot idle right now to held, without waiting.
func takeIdle(idle <-chan int, held []int) []int {
	for {
		select {
		case slot := <-idle:
			held = append(held, slot)
		default:
			return held
		}
	}
}

// pauseAfterDequeueError waits a second after a failed claim so a struggling
// backend is not hammered; false means ctx ended.
func pauseAfterDequeueError(ctx context.Context) bool {
	select {
	case <-time.After(time.Second):
		return true
	case <-ctx.Done():
		return false
	}
}

func (e *WorkerEngine) processActivity(ctx context.Context, act *activity, workerLabel string, workerID int) {
	activityID := act.ID
	activityType := act.ActivityType

	slog.Debug("Worker processing activity", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType)

	handler, ok := e.handlers[activityType]
	if !ok {
		slog.Error("No handler found for activity type", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType)
		if _, err := e.queue.MarkFailed(ctx, act, "handler_not_found", true, workerLabel); err != nil {
			slog.Error("Failed to mark activity as failed", "worker_id", workerID, "activity_id", activityID, "error", err)
		}
		return
	}

	activityTimeout := time.Duration(act.TimeoutSeconds) * time.Second
	deadlineCtx, timeoutCancel := context.WithTimeout(ctx, activityTimeout)
	defer timeoutCancel()
	// revoke cancels the handler when the claim is lost (heartbeat or
	// Interrupt); context.Cause then reports the ErrClaimLost error.
	timeoutCtx, revoke := context.WithCancelCause(deadlineCtx)
	defer revoke(nil)

	now := time.Now().UTC()
	e.inflight.Store(activityID, &running{InFlightActivity{
		ID: activityID, Type: activityType, Attempt: int(act.RetryCount) + 1, StartedAt: now,
	}, revoke})
	e.metrics.IncCounter(metricStarted, 1)
	e.metrics.ObserveDuration(metricClaimLag, claimLag(act, now))
	e.changes.Notify()
	e.announce.change(executor.AttemptStarted, act, now)
	defer func() {
		e.inflight.Delete(activityID)
		e.changes.Notify()
	}()
	// The attempt queue on the context lets in-handler GetResult calls
	// yield-park this activity.
	attemptQ := &attemptQueue{activityQueue: e.queue, backend: e.backend, owner: act.ID, worker: workerLabel, persistenceCtx: ctx, metrics: e.metrics, awaitGrace: e.awaitGrace}
	scopedExecutor := newActivityExecutor(attemptQ, e.config.MaxActivityDepth, e.announce).scopedForChild(act)
	timeoutCtx = context.WithValue(timeoutCtx, attemptQueueKey{}, attemptQ)
	timeoutCtx = context.WithValue(timeoutCtx, childScopeKey{}, scopedExecutor)

	actCtx := ActivityContext{
		ActivityID:       activityID,
		ActivityType:     activityType,
		RetryCount:       act.RetryCount,
		Metadata:         act.Metadata,
		Ctx:              timeoutCtx,
		ActivityExecutor: scopedExecutor,
		ParentActivityID: act.ParentActivityID,
		RootActivityID:   act.RootActivityID,
		Depth:            act.Depth,
		queue:            attemptQ,
	}

	payloadForDL := make(json.RawMessage, len(act.Payload))
	copy(payloadForDL, act.Payload)

	stopHeartbeat := attemptQ.heartbeat(timeoutCtx, e.heartbeatInterval, revoke)
	result, handlerErr := e.safeHandle(handler, actCtx, act.Payload)
	stopHeartbeat()

	// Another execution owns the activity now: whatever the handler returned
	// belongs to a superseded attempt, and the fence would reject its ack.
	if cause := context.Cause(timeoutCtx); cause != nil {
		if se, ok := storage.IsStorageError(cause); ok && se.Kind == storage.ErrClaimLost {
			e.metrics.IncCounter(metricClaimLost, 1)
			slog.Warn("Activity execution lost its claim; handler was cancelled", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType, "error", cause)
			return
		}
	}

	// A yield parks the activity without consuming a retry. Checked before
	// the timeout so a yield that raced the deadline is still a yield.
	var ys *yieldPark
	if errors.As(handlerErr, &ys) {
		e.handleYield(ctx, act, ys, workerLabel, workerID)
		return
	}

	// A returned result is persisted even if the deadline passed during
	// checkpoint recovery; ownership still fences it.
	if handlerErr == nil {
		e.handleSuccess(ctx, act, result, workerLabel, workerID)
		return
	}
	if se, ok := storage.IsStorageError(handlerErr); ok && se.Kind == storage.ErrClaimLost {
		e.metrics.IncCounter(metricClaimLost, 1)
		slog.Warn("Activity execution lost its claim", "activity_id", activityID, "error", handlerErr)
		return
	}
	retryable := retryableError(handlerErr)
	if !retryable {
		e.handleNonRetryableFailure(ctx, act, handlerErr.Error(), workerLabel, workerID)
		return
	}
	if timeoutCtx.Err() == context.DeadlineExceeded {
		e.handleTimeout(ctx, act, handler, payloadForDL, workerLabel, workerID, activityTimeout)
		return
	}
	e.handleRetryableFailure(ctx, act, handler, payloadForDL, handlerErr.Error(), workerLabel, workerID)
}

// safeHandle turns a handler panic into a retryable error.
func (e *WorkerEngine) safeHandle(handler ActivityHandler, ctx ActivityContext, payload json.RawMessage) (result json.RawMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			msg := "panic (unknown)"
			switch v := r.(type) {
			case string:
				msg = "panic: " + v
			case error:
				msg = "panic: " + v.Error()
			}
			err = NewRetryError(msg)
		}
	}()
	return handler.Handle(ctx, payload)
}

func (e *WorkerEngine) handleSuccess(ctx context.Context, act *activity, result json.RawMessage, workerLabel string, workerID int) {
	activityID, activityType := act.ID, act.ActivityType
	started := time.Now()
	e.metrics.IncCounter("activity_completion_pending_started", 1)
	defer func() {
		e.metrics.IncCounter("activity_completion_pending_finished", 1)
		e.metrics.ObserveDuration("activity_completion_persistence", time.Since(started))
	}()
	attemptQ := &attemptQueue{backend: e.backend, owner: act.ID, worker: workerLabel}
	err := retryStorage(ctx, "complete activity", e.metrics, attemptQ.renew, func(ctx context.Context) error {
		return e.queue.MarkCompleted(ctx, act, result, workerLabel)
	})
	if err != nil {
		counter := "activity_completion_error"
		if se, ok := storage.IsStorageError(err); ok && se.Kind == storage.ErrClaimLost {
			counter = metricClaimLost
		}
		e.metrics.IncCounter(counter, 1)
		slog.Error("Failed to confirm activity completion", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType, "error", err)
		return
	}
	e.metrics.IncCounter(metricCompleted, 1)
	e.announce.change(executor.AttemptSucceeded, act, time.Now().UTC())
	slog.Info("Activity completed successfully", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType)
}

// handleYield parks the activity until its wake time. If that fails the row
// stays processing until its lease expires and the reaper requeues it: one
// consumed retry and some latency, not lost work.
func (e *WorkerEngine) handleYield(ctx context.Context, act *activity, ys *yieldPark, workerLabel string, workerID int) {
	activityID, activityType := act.ID, act.ActivityType
	// Waits with a recheck replay at least every minute, even if this process
	// dies right after the park: the checkpointed deadline is unchanged, the
	// replay just finds a result or reparks.
	wakeAt := ys.wakeAt
	if ys.recheck != uuid.Nil {
		if recheckAt := time.Now().UTC().Add(time.Minute); wakeAt.After(recheckAt) {
			wakeAt = recheckAt
		}
	}
	attemptQ := &attemptQueue{backend: e.backend, owner: act.ID, worker: workerLabel}
	err := retryStorage(ctx, "park activity", e.metrics, attemptQ.renew, func(ctx context.Context) error {
		if b, ok := e.backend.(storage.DependencyStorage); ok && ys.recheck != uuid.Nil {
			var producer *uuid.UUID
			if ys.kind == "await" {
				producer = &ys.recheck
			}
			return b.YieldForResult(ctx, act.ID, ys.recheck, producer, wakeAt, workerLabel, ys.kind, ys.step)
		}
		return e.queue.Yield(ctx, act, wakeAt, workerLabel, ys.kind, ys.step)
	})
	if err != nil {
		slog.Error("Failed to confirm durable park", "activity_id", activityID, "error", err)
		return
	}
	e.metrics.IncCounter("activity_yielded", 1)
	slog.Debug("Activity yielded for durable wait",
		"worker_id", workerID, "activity_id", activityID, "activity_type", activityType,
		"step", ys.step, "wake_at", ys.wakeAt)

	// Close the park race: a result that committed between the handler's
	// last check and the park woke nothing, since the row wasn't 'waiting'
	// yet. Re-check now and self-wake. A backend with DependencyStorage closes
	// the race in YieldForResult; otherwise a failure here costs at most the
	// one-minute replay above.
	if ys.recheck == uuid.Nil {
		return
	}
	if _, durable := e.backend.(storage.DependencyStorage); durable {
		return
	}
	res, err := e.queue.GetResult(ctx, ys.recheck)
	if err != nil {
		slog.Warn("Post-park recheck failed; the park deadline will recover",
			"activity_id", activityID, "error", err)
		return
	}
	if res == nil {
		return
	}
	if _, err := e.backend.WakeWaiting(ctx, act.ID); err != nil {
		slog.Warn("Post-park self-wake failed; the park deadline will recover",
			"activity_id", activityID, "error", err)
	}
}

func (e *WorkerEngine) handleRetryableFailure(ctx context.Context, act *activity, handler ActivityHandler, payload json.RawMessage, reason string, workerLabel string, workerID int) {
	e.metrics.IncCounter(metricRetried, 1)
	slog.Warn("Activity requesting retry", "worker_id", workerID, "activity_id", act.ID, "activity_type", act.ActivityType, "reason", reason)
	e.retryOrDeadLetter(ctx, act, handler, payload, reason, workerLabel, workerID)
}

// retryOrDeadLetter records a retryable failure and, when it was the last
// attempt, runs the handler's dead-letter hook.
func (e *WorkerEngine) retryOrDeadLetter(ctx context.Context, act *activity, handler ActivityHandler, payload json.RawMessage, reason string, workerLabel string, workerID int) {
	deadLettered, err := e.persistFailure(ctx, act, reason, true, workerLabel)
	if err != nil {
		slog.Error("Failed to record activity failure", "worker_id", workerID, "activity_id", act.ID, "error", err)
		return
	}
	if deadLettered {
		e.callDeadLetter(ctx, act, handler, payload, reason)
	}
}

func (e *WorkerEngine) handleNonRetryableFailure(ctx context.Context, act *activity, reason string, workerLabel string, workerID int) {
	e.metrics.IncCounter(metricFailed, 1)
	slog.Error("Activity failed", "worker_id", workerID, "activity_id", act.ID, "activity_type", act.ActivityType, "reason", reason)
	// The backend writes the error result in the same transaction.
	if _, err := e.persistFailure(ctx, act, reason, false, workerLabel); err != nil {
		slog.Error("Failed to record activity failure", "worker_id", workerID, "activity_id", act.ID, "error", err)
	}
}

func (e *WorkerEngine) handleTimeout(ctx context.Context, act *activity, handler ActivityHandler, payload json.RawMessage, workerLabel string, workerID int, timeout time.Duration) {
	e.metrics.IncCounter(metricTimedOut, 1)
	slog.Error("Activity timed out", "worker_id", workerID, "activity_id", act.ID, "activity_type", act.ActivityType, "timeout", timeout)
	e.retryOrDeadLetter(ctx, act, handler, payload, "Activity execution timed out", workerLabel, workerID)
}

// callDeadLetter runs the handler's dead-letter hook, recovering a panic.
func (e *WorkerEngine) callDeadLetter(ctx context.Context, act *activity, handler ActivityHandler, payload json.RawMessage, reason string) {
	defer func() {
		if r := recover(); r != nil {
			e.metrics.IncCounter("activity_dead_letter_hook_panic", 1)
			slog.Error("Dead-letter hook panicked", "activity_id", act.ID, "panic", r)
		}
	}()
	handler.OnDeadLetter(ActivityContext{
		ActivityID:       act.ID,
		ActivityType:     act.ActivityType,
		RetryCount:       0,
		Metadata:         make(map[string]string),
		Ctx:              ctx,
		ActivityExecutor: newActivityExecutor(e.queue, e.config.MaxActivityDepth, e.announce).scopedForChild(act),
		ParentActivityID: act.ParentActivityID,
		RootActivityID:   act.RootActivityID,
		Depth:            act.Depth,
	}, payload, reason)
}

func (e *WorkerEngine) runScheduledProcessor(ctx context.Context) {
	pollInterval := uint64(5)
	if e.config.SchedulePollIntervalSeconds != nil {
		pollInterval = max(*e.config.SchedulePollIntervalSeconds, 1)
	}
	ticker := time.NewTicker(time.Duration(pollInterval) * time.Second)
	defer ticker.Stop()

	slog.Debug("Starting scheduled activities processor")
	for e.running.Load() {
		select {
		case <-ctx.Done():
			slog.Debug("Scheduled activities processor stopped")
			return
		case <-ticker.C:
			if err := e.queue.ProcessScheduledActivities(ctx); err != nil {
				slog.Error("Failed to process scheduled activities", "error", err)
			}
		}
	}
	slog.Debug("Scheduled activities processor stopped")
}

func (e *WorkerEngine) runReaperProcessor(ctx context.Context) {
	intervalSec := uint64(5)
	if e.config.ReaperIntervalSeconds != nil {
		intervalSec = max(*e.config.ReaperIntervalSeconds, 1)
	}
	batchSize := 100
	if e.config.ReaperBatchSize != nil {
		batchSize = *e.config.ReaperBatchSize
	}

	ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
	defer ticker.Stop()

	slog.Debug("Starting reaper processor")
	for e.running.Load() {
		select {
		case <-ctx.Done():
			slog.Debug("Reaper processor stopped")
			return
		case <-ticker.C:
			if _, err := e.queue.RequeueExpired(ctx, batchSize); err != nil {
				slog.Error("Reaper failed to requeue expired items", "error", err)
			}
		}
	}
	slog.Debug("Reaper processor stopped")
}

// runRetentionProcessor sweeps expired workflow trees. Each tick keeps
// sweeping until a batch comes back short, so a backlog clears at once
// rather than one batch per interval.
func (e *WorkerEngine) runRetentionProcessor(ctx context.Context) {
	cfg := e.config.Retention
	interval := cfg.Interval
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	policy := storage.RetentionPolicy{Completed: cfg.Completed, Failed: cfg.Failed, Events: cfg.Events}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Debug("Starting retention sweeper", "completed_ttl", cfg.Completed, "failed_ttl", cfg.Failed, "events_ttl", cfg.Events, "interval", interval)
	for e.running.Load() {
		select {
		case <-ctx.Done():
			slog.Debug("Retention sweeper stopped")
			return
		case <-ticker.C:
			for {
				n, err := e.backend.CleanupExpired(ctx, policy, batchSize)
				if err != nil {
					if ctx.Err() == nil {
						slog.Error("Retention sweep failed", "error", err)
					}
					break
				}
				if n > 0 {
					e.metrics.IncCounter("activity_trees_swept", n)
					slog.Debug("Retention sweep deleted workflow trees", "trees", n)
				}
				if int(n) < batchSize {
					break
				}
			}
		}
	}
	slog.Debug("Retention sweeper stopped")
}

// WorkerEngineBuilder configures a WorkerEngine; start with Builder.
type WorkerEngineBuilder struct {
	queueName     *string
	maxWorkers    *int
	pollInterval  *time.Duration
	metrics       MetricsSink
	backend       storage.Storage
	activityTypes []string
	shutdownGrace *time.Duration
	retention     *RetentionConfig
}

// Builder starts configuring a WorkerEngine from DefaultWorkerConfig.
func Builder() *WorkerEngineBuilder {
	return &WorkerEngineBuilder{}
}

// QueueName sets the queue to serve (default "default").
func (b *WorkerEngineBuilder) QueueName(name string) *WorkerEngineBuilder {
	b.queueName = &name
	return b
}

// MaxWorkers sets how many activities run at once (default 10).
func (b *WorkerEngineBuilder) MaxWorkers(max int) *WorkerEngineBuilder {
	b.maxWorkers = &max
	return b
}

// SchedulePollInterval sets how often due scheduled activities are released,
// for backends that don't schedule in Dequeue (default 5s).
func (b *WorkerEngineBuilder) SchedulePollInterval(interval time.Duration) *WorkerEngineBuilder {
	b.pollInterval = &interval
	return b
}

// ActivityTypes restricts the types this engine claims (default: all
// registered types).
func (b *WorkerEngineBuilder) ActivityTypes(types []string) *WorkerEngineBuilder {
	b.activityTypes = types
	return b
}

func (b *WorkerEngineBuilder) Metrics(sink MetricsSink) *WorkerEngineBuilder {
	b.metrics = sink
	return b
}

// Backend sets the storage backend; required.
func (b *WorkerEngineBuilder) Backend(backend storage.Storage) *WorkerEngineBuilder {
	b.backend = backend
	return b
}

// Retention opts the engine into deleting old terminal workflow trees (see
// RetentionConfig). Safe on every engine: the backend elects one sweeper.
func (b *WorkerEngineBuilder) Retention(cfg RetentionConfig) *WorkerEngineBuilder {
	b.retention = &cfg
	return b
}

// ShutdownGrace bounds the shutdown drain (default 30s, at least 1s).
func (b *WorkerEngineBuilder) ShutdownGrace(d time.Duration) *WorkerEngineBuilder {
	b.shutdownGrace = &d
	return b
}

// Build creates the WorkerEngine. It fails when no backend is set.
func (b *WorkerEngineBuilder) Build() (*WorkerEngine, error) {
	if b.backend == nil {
		return nil, &WorkerError{
			Kind:    ErrConfiguration,
			Message: "No backend configured. Call .Backend(yourBackend) before .Build(). Use PostgresBackend for PostgreSQL.",
		}
	}
	config := DefaultWorkerConfig()
	if b.queueName != nil {
		config.QueueName = *b.queueName
	}
	if b.maxWorkers != nil {
		config.MaxConcurrentActivities = *b.maxWorkers
	}
	pollIntervalSec := uint64(5)
	if b.pollInterval != nil {
		pollIntervalSec = uint64(b.pollInterval.Seconds())
	}
	config.SchedulePollIntervalSeconds = &pollIntervalSec
	config.ActivityTypes = b.activityTypes
	config.Retention = b.retention
	if b.shutdownGrace != nil {
		secs := max(uint64(b.shutdownGrace.Seconds()), 1)
		config.ShutdownGraceSeconds = &secs
	}

	engine := NewWorkerEngineWithBackend(b.backend, config)
	if b.metrics != nil {
		engine.SetMetrics(b.metrics)
	}
	return engine, nil
}

// persistFailure retries a failure's acknowledgement like any outcome; the
// backend recognises one that already committed.
func (e *WorkerEngine) persistFailure(ctx context.Context, act *activity, reason string, retryable bool, worker string) (dead bool, err error) {
	q := &attemptQueue{backend: e.backend, owner: act.ID, worker: worker}
	err = retryStorage(ctx, "record activity failure", e.metrics, q.renew, func(ctx context.Context) error {
		var err error
		dead, err = e.queue.MarkFailed(ctx, act, reason, retryable, worker)
		return err
	})
	if err == nil && dead {
		e.metrics.IncCounter(metricDeadLettered, 1)
	}
	return
}

// claimLag is how long act waited, from when it was due, to start at now.
func claimLag(act *activity, now time.Time) time.Duration {
	due := act.CreatedAt
	if act.ScheduledAt != nil && act.ScheduledAt.After(due) {
		due = *act.ScheduledAt
	}
	return max(now.Sub(due), 0)
}
