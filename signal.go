package runnerq

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// SignalActivity delivers a signal to an activity from any process sharing
// the database; no engine is needed, only a backend. The payload (nil is
// fine) is stored even if the activity hasn't reached its WaitForSignal yet,
// and a parked waiter wakes at once. A repeated signal overwrites the
// payload. It fails with IsActivityNotFound when the activity doesn't exist.
func SignalActivity(ctx context.Context, backend storage.Storage, activityID uuid.UUID, name string, payload json.RawMessage) error {
	if name == "" {
		return &WorkerError{Kind: ErrQueue, Message: "signal name must be non-empty"}
	}
	sigID := storage.CheckpointID(activityID, "signal", name)
	if err := backend.SignalActivity(ctx, activityID, sigID, name, payload); err != nil {
		return signalDeliveryError(err)
	}
	return nil
}

// Signal is SignalActivity on this engine's backend, traced by its Tracer.
func (e *WorkerEngine) Signal(ctx context.Context, activityID uuid.UUID, name string, payload json.RawMessage) error {
	return tracedSignal(ctx, e.tel, e.backend, activityID, name, payload)
}

func tracedSignal(ctx context.Context, tel *telemetry, backend storage.Storage, activityID uuid.UUID, name string, payload json.RawMessage) error {
	ctx, end := startSpan(ctx, tel.tracerOf(), SpanSignalSend, "", func() []TraceAttribute {
		return []TraceAttribute{{AttrActivityID, activityID.String()}, {AttrSignalName, name}}
	})
	err := SignalActivity(ctx, backend, activityID, name, payload)
	if err != nil {
		end(OutcomeError, err)
	} else {
		end(OutcomeSent, nil)
	}
	return err
}

// SignalActivityByKey delivers a signal to the activity that owns
// idempotencyKey (as given to IdempotencyKeyOption) for activityType, so a
// webhook can wake a workflow by its business key. Keys are scoped per type,
// hence the type. It fails with IsActivityNotFound when no activity owns the
// key (never enqueued, or swept by retention).
//
// Resolving the key and delivering are separate steps: under AllowReuse or
// AllowReuseOnFailure a key repointed in between sends the signal to its new
// owner.
func SignalActivityByKey(ctx context.Context, backend storage.Storage, activityType string, idempotencyKey string, name string, payload json.RawMessage) error {
	if name == "" {
		return &WorkerError{Kind: ErrQueue, Message: "signal name must be non-empty"}
	}
	if activityType == "" {
		return &WorkerError{Kind: ErrQueue, Message: "activity type must be non-empty"}
	}
	if idempotencyKey == "" {
		return &WorkerError{Kind: ErrQueue, Message: "idempotency key must be non-empty"}
	}
	activityID, err := backend.LookupIdempotencyActivityID(ctx, storage.BusinessIdempotencyKey(idempotencyKey, activityType))
	if err != nil {
		return signalDeliveryError(err)
	}
	return SignalActivity(ctx, backend, activityID, name, payload)
}

// SignalByKey is SignalActivityByKey on this engine's backend, delivered
// through Signal.
func (e *WorkerEngine) SignalByKey(ctx context.Context, activityType string, idempotencyKey string, name string, payload json.RawMessage) error {
	return tracedSignalByKey(ctx, e.tel, e.backend, activityType, idempotencyKey, name, payload)
}

func tracedSignalByKey(ctx context.Context, tel *telemetry, backend storage.Storage, activityType, idempotencyKey, name string, payload json.RawMessage) error {
	if name == "" || activityType == "" || idempotencyKey == "" {
		return SignalActivityByKey(ctx, backend, activityType, idempotencyKey, name, payload)
	}
	activityID, err := backend.LookupIdempotencyActivityID(ctx, storage.BusinessIdempotencyKey(idempotencyKey, activityType))
	if err != nil {
		return signalDeliveryError(err)
	}
	return tracedSignal(ctx, tel, backend, activityID, name, payload)
}

// signalDeliveryError maps a storage not-found to ErrActivityNotFoundW.
func signalDeliveryError(err error) error {
	if se, ok := storage.IsStorageError(err); ok && se.Kind == storage.ErrNotFound {
		return &WorkerError{Kind: ErrActivityNotFoundW, Message: se.Message, Cause: err}
	}
	return WorkerErrorFromStorage(err)
}
