package storage

import (
	"errors"
	"fmt"

	"github.com/runnerq/runnerq-go/internal/spec"
)

// StorageErrorKind numbers are stored and sent on the wire; runnerq-spec
// fixes them.
type StorageErrorKind int

const (
	ErrUnavailable         StorageErrorKind = spec.StorageErrorKindUnavailable
	ErrConflict            StorageErrorKind = spec.StorageErrorKindConflict
	ErrNotFound            StorageErrorKind = spec.StorageErrorKindNotFound
	ErrInternal            StorageErrorKind = spec.StorageErrorKindInternal
	ErrSerialization       StorageErrorKind = spec.StorageErrorKindSerialization
	ErrConfiguration       StorageErrorKind = spec.StorageErrorKindConfiguration
	ErrTimeout             StorageErrorKind = spec.StorageErrorKindTimeout
	ErrDuplicateActivity   StorageErrorKind = spec.StorageErrorKindDuplicateActivity
	ErrIdempotencyConflict StorageErrorKind = spec.StorageErrorKindIdempotencyConflict
	// ErrClaimLost: the execution no longer owns the activity.
	ErrClaimLost StorageErrorKind = spec.StorageErrorKindClaimLost
	// ErrCheckpointConflict: an immutable checkpoint has a different outcome.
	ErrCheckpointConflict StorageErrorKind = spec.StorageErrorKindCheckpointConflict
	// ErrInvalidArgument: malformed or out-of-range input named by Field.
	ErrInvalidArgument StorageErrorKind = spec.StorageErrorKindInvalidArgument
	// ErrUnsupported: a query feature the backend cannot evaluate, named by
	// Field.
	ErrUnsupported StorageErrorKind = spec.StorageErrorKindUnsupported
)

// StorageError is a backend-agnostic storage error.
type StorageError struct {
	Kind    StorageErrorKind
	Message string
	Cause   error
	// Field names the offending input for ErrInvalidArgument/ErrUnsupported.
	Field string
}

func (e *StorageError) Error() string {
	prefix := ""
	switch e.Kind {
	case ErrUnavailable:
		prefix = "backend unavailable"
	case ErrConflict:
		prefix = "conflict"
	case ErrNotFound:
		prefix = "not found"
	case ErrInternal:
		prefix = "internal error"
	case ErrSerialization:
		prefix = "serialization error"
	case ErrConfiguration:
		prefix = "configuration error"
	case ErrTimeout:
		prefix = "operation timeout"
	case ErrDuplicateActivity:
		prefix = "duplicate activity"
	case ErrIdempotencyConflict:
		prefix = "idempotency conflict"
	case ErrClaimLost:
		prefix = "claim lost"
	case ErrCheckpointConflict:
		prefix = "checkpoint conflict"
	case ErrInvalidArgument:
		prefix = "invalid argument"
	case ErrUnsupported:
		prefix = "unsupported"
	}
	return fmt.Sprintf("%s: %s", prefix, e.Message)
}

func (e *StorageError) Unwrap() error {
	return e.Cause
}

// IsRetryable reports whether a retry may succeed.
func (e *StorageError) IsRetryable() bool {
	switch e.Kind {
	case ErrUnavailable, ErrTimeout, ErrConflict:
		return true
	default:
		return false
	}
}

func NewUnavailableError(msg string) *StorageError {
	return &StorageError{Kind: ErrUnavailable, Message: msg}
}

func NewConflictError(msg string) *StorageError {
	return &StorageError{Kind: ErrConflict, Message: msg}
}

func NewNotFoundError(msg string) *StorageError {
	return &StorageError{Kind: ErrNotFound, Message: msg}
}

func NewInternalError(msg string) *StorageError {
	return &StorageError{Kind: ErrInternal, Message: msg}
}

func NewSerializationError(msg string) *StorageError {
	return &StorageError{Kind: ErrSerialization, Message: msg}
}

func NewConfigurationError(msg string) *StorageError {
	return &StorageError{Kind: ErrConfiguration, Message: msg}
}

func NewTimeoutError(msg string) *StorageError {
	return &StorageError{Kind: ErrTimeout, Message: msg}
}

func NewDuplicateActivityError(msg string) *StorageError {
	return &StorageError{Kind: ErrDuplicateActivity, Message: msg}
}

func NewIdempotencyConflictError(msg string) *StorageError {
	return &StorageError{Kind: ErrIdempotencyConflict, Message: msg}
}

// IsStorageError extracts a *StorageError from err's chain.
func IsStorageError(err error) (*StorageError, bool) {
	var se *StorageError
	if errors.As(err, &se) {
		return se, true
	}
	return nil, false
}
