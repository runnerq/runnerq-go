package storage

import (
	"math"
	"time"

	"github.com/runnerq/runnerq-go/internal/spec"
)

// DefaultMaxRetryDelaySeconds caps retry backoff for activities that set no
// MaxRetryDelay.
const DefaultMaxRetryDelaySeconds = spec.DefaultMaxRetryDelaySeconds

// AttemptsRemain reports whether a failed attempt may be followed by another.
// retryCount counts the attempts that failed before it; maxRetries is the
// attempt limit, 0 meaning unlimited. A retryable failure with attempts
// remaining is retried, one without dead-letters, and a lease expiry counts as
// a retryable failure.
func AttemptsRemain(retryCount, maxRetries int) bool {
	return maxRetries == 0 || retryCount+1 < maxRetries
}

// RetryDelaySeconds is the backoff before the retry that follows attempt
// retryCount+1: retryDelaySeconds × 2^(retryCount+1), capped at
// maxRetryDelaySeconds (0 means DefaultMaxRetryDelaySeconds). The result
// always fits a time.Duration.
func RetryDelaySeconds(retryCount int, retryDelaySeconds, maxRetryDelaySeconds int64) int64 {
	if retryDelaySeconds <= 0 {
		return 0
	}
	limit := maxRetryDelaySeconds
	if limit <= 0 {
		limit = DefaultMaxRetryDelaySeconds
	}
	limit = min(limit, math.MaxInt64/int64(time.Second))
	shift := min(max(retryCount+1, 0), 62)
	if retryDelaySeconds > limit>>shift {
		return limit
	}
	return retryDelaySeconds << shift
}
