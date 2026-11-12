package runnerq

import (
	"encoding/json"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// ActivityPriority orders execution: higher priorities are claimed first.
type ActivityPriority int

const (
	PriorityLow      ActivityPriority = 1
	PriorityNormal   ActivityPriority = 2
	PriorityHigh     ActivityPriority = 3
	PriorityCritical ActivityPriority = 4
)

func (p ActivityPriority) String() string {
	switch p {
	case PriorityLow:
		return "Low"
	case PriorityNormal:
		return "Normal"
	case PriorityHigh:
		return "High"
	case PriorityCritical:
		return "Critical"
	default:
		return "Normal"
	}
}

func (p ActivityPriority) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.String())
}

func (p *ActivityPriority) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		var n int
		if err2 := json.Unmarshal(data, &n); err2 != nil {
			return err
		}
		*p = ActivityPriority(n)
		return nil
	}
	switch s {
	case "Low":
		*p = PriorityLow
	case "Normal":
		*p = PriorityNormal
	case "High":
		*p = PriorityHigh
	case "Critical":
		*p = PriorityCritical
	default:
		*p = PriorityNormal
	}
	return nil
}

// OnDuplicate is what an enqueue does when its idempotency key is taken.
type OnDuplicate int

const (
	// AllowReuse always creates a new activity and repoints the key at it.
	AllowReuse OnDuplicate = iota
	// ReturnExisting returns the existing activity's future.
	ReturnExisting
	// AllowReuseOnFailure creates a new activity only if the existing one
	// failed, dead-lettered or was cancelled.
	AllowReuseOnFailure
	// NoReuse fails the enqueue.
	NoReuse
)

// ActivityStatus is an activity's status as Client reports it (the canonical
// statuses every backend maps onto): a retry waiting for its backoff is
// scheduled, and a claimed activity is running.
type ActivityStatus string

const (
	StatusPending    ActivityStatus = storage.RecordStatusPending
	StatusScheduled  ActivityStatus = storage.RecordStatusScheduled
	StatusRunning    ActivityStatus = storage.RecordStatusRunning
	StatusWaiting    ActivityStatus = storage.RecordStatusWaiting
	StatusCompleted  ActivityStatus = storage.RecordStatusCompleted
	StatusFailed     ActivityStatus = storage.RecordStatusFailed
	StatusDeadLetter ActivityStatus = storage.RecordStatusDeadLetter
	StatusCancelled  ActivityStatus = storage.RecordStatusCancelled
)

// Finished reports whether s is terminal: completed, failed, dead_letter or
// cancelled.
func (s ActivityStatus) Finished() bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusDeadLetter, StatusCancelled:
		return true
	}
	return false
}

type ActivityOption struct {
	Priority             *ActivityPriority
	MaxRetries           uint32
	TimeoutSeconds       uint64
	MaxRetryDelaySeconds *uint64
	DelaySeconds         *uint64
	IdempotencyKey       *IdempotencyConfig
	Metadata             map[string]string
}

type IdempotencyConfig struct {
	Key      string
	Behavior OnDuplicate
}

type activity struct {
	ID                   uuid.UUID          `json:"id"`
	ActivityType         string             `json:"activity_type"`
	Payload              json.RawMessage    `json:"payload"`
	Priority             ActivityPriority   `json:"priority"`
	Status               ActivityStatus     `json:"status"`
	CreatedAt            time.Time          `json:"created_at"`
	ScheduledAt          *time.Time         `json:"scheduled_at,omitempty"`
	RetryCount           uint32             `json:"retry_count"`
	MaxRetries           uint32             `json:"max_retries"`
	TimeoutSeconds       uint64             `json:"timeout_seconds"`
	RetryDelaySeconds    uint64             `json:"retry_delay_seconds"`
	MaxRetryDelaySeconds uint64             `json:"max_retry_delay_seconds"`
	Metadata             map[string]string  `json:"metadata"`
	IdempotencyKey       *IdempotencyConfig `json:"idempotency_key,omitempty"`
	ParentActivityID     *uuid.UUID         `json:"parent_activity_id,omitempty"`
	RootActivityID       uuid.UUID          `json:"root_activity_id"`
	Depth                uint16             `json:"depth"`
}

func newActivity(activityType string, payload json.RawMessage, option *ActivityOption) *activity {
	priority := PriorityNormal
	maxRetries := uint32(3)
	timeoutSeconds := uint64(300)
	maxRetryDelaySeconds := uint64(3600) // default: 1 hour cap
	var scheduledAt *time.Time
	var idempotencyKey *IdempotencyConfig

	if option != nil {
		if option.Priority != nil {
			priority = *option.Priority
		}
		maxRetries = option.MaxRetries
		timeoutSeconds = option.TimeoutSeconds
		if option.MaxRetryDelaySeconds != nil {
			maxRetryDelaySeconds = *option.MaxRetryDelaySeconds
		}
		if option.DelaySeconds != nil {
			t := time.Now().UTC().Add(time.Duration(*option.DelaySeconds) * time.Second)
			scheduledAt = &t
		}
		idempotencyKey = option.IdempotencyKey
	}

	metadata := make(map[string]string)
	if option != nil && len(option.Metadata) > 0 {
		maps.Copy(metadata, option.Metadata)
	}

	id := uuid.New()
	return &activity{
		ID:                   id,
		ActivityType:         activityType,
		Payload:              payload,
		Priority:             priority,
		Status:               StatusPending,
		CreatedAt:            time.Now().UTC(),
		ScheduledAt:          scheduledAt,
		MaxRetries:           maxRetries,
		TimeoutSeconds:       timeoutSeconds,
		RetryDelaySeconds:    1,
		MaxRetryDelaySeconds: maxRetryDelaySeconds,
		Metadata:             metadata,
		IdempotencyKey:       idempotencyKey,
		RootActivityID:       id,
	}
}
