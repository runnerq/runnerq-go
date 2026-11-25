package storage

import (
	"context"
	"time"

	"github.com/runnerq/runnerq-go/internal/spec"
)

// Limit caps one activity type of a queue across every worker
// (runnerq-spec limits.md). It needs MaxConcurrent, a rate, or both.
type Limit struct {
	ActivityType string
	// MaxConcurrent caps the type's running activities in the queue; 0 means
	// no cap. Activities waiting on a sleep, a signal, a child or a retry's
	// backoff don't count.
	MaxConcurrent int
	// RateLimit claims per RatePeriod, with bursts of up to RateLimit; 0 means
	// no rate limit. RatePeriod is rounded to the millisecond.
	RateLimit  int
	RatePeriod time.Duration

	// UpdatedAt is set by the backend: when the definition last changed.
	UpdatedAt time.Time
}

// LimitsRefreshInterval bounds how long a backend may claim a type as
// unlimited after a limit was added to it.
const LimitsRefreshInterval = time.Duration(spec.LimitsRefreshMs) * time.Millisecond

// LimitProbeInterval is how soon a blocking claim tries again after a limit
// held back claimable work.
const LimitProbeInterval = time.Duration(spec.LimitProbeMs) * time.Millisecond

// LimitStorage is an optional capability: per-type limits stored with the
// queue. A backend that implements it enforces every stored limit in Dequeue
// and DequeueBatch: a claim never takes an activity of a limited type beyond
// the room under MaxConcurrent or the tokens of its rate.
type LimitStorage interface {
	// SetLimit creates or replaces the type's limit. The rate's bucket is
	// kept when the rate is unchanged, so an unchanged definition changes
	// nothing. The limit must pass ValidateLimit.
	SetLimit(ctx context.Context, limit Limit) error
	// GetLimit returns a not-found error for a type without a limit.
	GetLimit(ctx context.Context, activityType string) (*Limit, error)
	// ListLimits returns the queue's limits by activity type.
	ListLimits(ctx context.Context) ([]Limit, error)
	// DeleteLimit removes the type's limit; not found when it has none.
	DeleteLimit(ctx context.Context, activityType string) error
}

// ValidateLimit checks what SetLimit stores.
func ValidateLimit(l *Limit) error {
	invalid := func(field, msg string) error {
		return &StorageError{Kind: ErrInvalidArgument, Field: field, Message: msg}
	}
	switch {
	case l.ActivityType == "":
		return invalid("activity_type", "limit activity type is required")
	case l.MaxConcurrent < 0:
		return invalid("max_concurrent", "max concurrent must be positive, or 0 for no cap")
	case l.RateLimit < 0:
		return invalid("rate_limit", "rate limit must be positive, or 0 for none")
	case l.RateLimit > 0 && l.RatePeriod.Round(time.Millisecond) <= 0:
		return invalid("rate_period", "a rate limit needs a period of at least a millisecond")
	case l.RateLimit == 0 && l.RatePeriod != 0:
		return invalid("rate_limit", "a rate period needs a rate limit")
	case l.MaxConcurrent == 0 && l.RateLimit == 0:
		return invalid("limit", "a limit needs max concurrent, a rate or both; delete it to remove it")
	}
	return nil
}
