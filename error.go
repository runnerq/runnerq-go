package runnerq

import (
	"errors"
	"fmt"

	"github.com/alob-mtc/runnerq-go/storage"
)

// ActivityError is a handler error that says whether to retry.
type ActivityError struct {
	Retryable bool
	Message   string
}

func (e *ActivityError) Error() string {
	if e.Retryable {
		return fmt.Sprintf("Retryable error: %s", e.Message)
	}
	return fmt.Sprintf("Non-retryable error: %s", e.Message)
}

// NewRetryError returns an error that retries the activity.
func NewRetryError(msg string) *ActivityError {
	return &ActivityError{Retryable: true, Message: msg}
}

// NewNonRetryError returns an error that fails the activity for good.
func NewNonRetryError(msg string) *ActivityError {
	return &ActivityError{Retryable: false, Message: msg}
}

func (e *ActivityError) IsRetryable() bool {
	return e.Retryable
}

// RetryableError is any error that decides whether its activity is retried;
// a handler error without it is retried.
type RetryableError interface {
	IsRetryable() bool
}

type WorkerErrorKind int

const (
	ErrCustom WorkerErrorKind = iota
	ErrQueue
	ErrSerializationW
	ErrTimeoutW
	ErrExecution
	ErrHandlerNotFound
	ErrBackend
	ErrDatabase
	ErrConfiguration
	ErrShutdown
	ErrAlreadyRunning
	ErrScheduling
	ErrDuplicateActivityW
	ErrIdempotencyConflictW
	ErrDepthExceeded
	ErrUnknown
	// ErrSignalTimeoutW: WaitForSignal timed out. Not retryable: a retry
	// replays the stored deadline and times out again.
	ErrSignalTimeoutW
	// ErrActivityNotFoundW: a signal's target doesn't exist (never enqueued,
	// or swept by retention).
	ErrActivityNotFoundW
)

// WorkerError is an error from the engine.
type WorkerError struct {
	Kind    WorkerErrorKind
	Message string
	Cause   error
}

func (e *WorkerError) Error() string {
	prefix := ""
	switch e.Kind {
	case ErrCustom:
		return e.Message
	case ErrQueue:
		prefix = "Activity queue error"
	case ErrSerializationW:
		prefix = "Activity serialization error"
	case ErrTimeoutW:
		return "Activity execution timeout"
	case ErrExecution:
		prefix = "Activity execution failed"
	case ErrHandlerNotFound:
		prefix = "Activity handler not found for activity type"
	case ErrBackend:
		prefix = "Backend error"
	case ErrDatabase:
		prefix = "Database error"
	case ErrConfiguration:
		prefix = "Configuration error"
	case ErrShutdown:
		return "Worker shutdown requested"
	case ErrAlreadyRunning:
		return "Worker is already running"
	case ErrScheduling:
		prefix = "Activity scheduling error"
	case ErrDuplicateActivityW:
		prefix = "Duplicate activity detected"
	case ErrIdempotencyConflictW:
		prefix = "Idempotency key conflict"
	case ErrDepthExceeded:
		prefix = "Activity depth limit exceeded"
	case ErrUnknown:
		prefix = "Unknown error"
	case ErrSignalTimeoutW:
		prefix = "Signal wait timed out"
	case ErrActivityNotFoundW:
		prefix = "Signal target activity not found"
	}
	return fmt.Sprintf("%s: %s", prefix, e.Message)
}

func (e *WorkerError) Unwrap() error {
	return e.Cause
}

// IsRetryable reports whether retrying may resolve the error.
func (e *WorkerError) IsRetryable() bool {
	switch e.Kind {
	case ErrQueue, ErrBackend, ErrDatabase, ErrTimeoutW, ErrExecution, ErrScheduling:
		return true
	default:
		return false
	}
}

// WorkerErrorFromStorage maps a storage error onto a WorkerError kind; an
// error that isn't a storage error becomes ErrUnknown.
func WorkerErrorFromStorage(err error) *WorkerError {
	se, ok := storage.IsStorageError(err)
	if !ok {
		return &WorkerError{Kind: ErrUnknown, Message: err.Error(), Cause: err}
	}
	switch se.Kind {
	case storage.ErrUnavailable:
		return &WorkerError{Kind: ErrBackend, Message: se.Message, Cause: err}
	case storage.ErrConflict, storage.ErrNotFound, storage.ErrInternal, storage.ErrSerialization:
		return &WorkerError{Kind: ErrQueue, Message: se.Message, Cause: err}
	case storage.ErrConfiguration:
		return &WorkerError{Kind: ErrConfiguration, Message: se.Message, Cause: err}
	case storage.ErrTimeout:
		return &WorkerError{Kind: ErrExecution, Message: se.Message, Cause: err}
	case storage.ErrDuplicateActivity:
		return &WorkerError{Kind: ErrDuplicateActivityW, Message: se.Message, Cause: err}
	case storage.ErrIdempotencyConflict:
		return &WorkerError{Kind: ErrIdempotencyConflictW, Message: se.Message, Cause: err}
	default:
		return &WorkerError{Kind: ErrUnknown, Message: se.Message, Cause: err}
	}
}

// IsSignalTimeout reports whether err is a WaitForSignal timeout.
func IsSignalTimeout(err error) bool {
	we, ok := IsWorkerError(err)
	return ok && we.Kind == ErrSignalTimeoutW
}

// IsActivityNotFound reports whether err is a signal to an activity that
// doesn't exist; deliverers usually treat it as already settled.
func IsActivityNotFound(err error) bool {
	we, ok := IsWorkerError(err)
	return ok && we.Kind == ErrActivityNotFoundW
}

// IsYield reports whether err is the sentinel a durable wait (Sleep,
// WaitForSignal, or GetResult on a handler's context) returns to park the
// activity. Code between the wait and the handler must return it unchanged
// (or wrapped with %w) rather than treat it as a failure.
func IsYield(err error) bool {
	var y *yieldPark
	return errors.As(err, &y)
}

// IsWorkerError returns the *WorkerError in err's chain, if any.
func IsWorkerError(err error) (*WorkerError, bool) {
	var we *WorkerError
	if errors.As(err, &we) {
		return we, true
	}
	return nil, false
}
