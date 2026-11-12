package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/internal/cron"
	"github.com/runnerq/runnerq-go/internal/spec"
)

// CatchUp says what firing does with ticks that came due while no worker
// fired them (runnerq-spec schedules.md).
type CatchUp string

const (
	// CatchUpSkip fires the latest due tick only when it is less than a
	// minute old.
	CatchUpSkip CatchUp = "skip"
	// CatchUpOne fires the latest due tick, however late. The default.
	CatchUpOne CatchUp = "one"
	// CatchUpAll fires every due tick, oldest first, up to CatchUpLimit.
	CatchUpAll CatchUp = "all"
)

// DefaultCatchUpLimit is how many due ticks CatchUpAll fires at once when a
// schedule sets no limit.
const DefaultCatchUpLimit = spec.ScheduleCatchUpLimitDefault

// MaxCatchUpLimit bounds CatchUpLimit.
const MaxCatchUpLimit = 1000

// ScheduleOptions are the execution options of the activities a schedule
// fires. JSON keys are runnerq_schedules.options'.
type ScheduleOptions struct {
	Priority             ActivityPriority  `json:"priority"`
	MaxRetries           uint32            `json:"max_retries"`
	TimeoutSeconds       uint64            `json:"timeout_seconds"`
	RetryDelaySeconds    uint64            `json:"retry_delay_seconds"`
	MaxRetryDelaySeconds uint64            `json:"max_retry_delay_seconds"`
	Metadata             map[string]string `json:"metadata"`
	// CodeVersion is the version fires are pinned to (runnerq-spec
	// versions.md); empty fires unstamped work. SDKs set it at upsert from
	// the upserting process's version.
	CodeVersion string `json:"code_version,omitempty"`
}

// DefaultScheduleOptions are what absent keys of a stored schedule's options
// mean.
func DefaultScheduleOptions() ScheduleOptions {
	return ScheduleOptions{Priority: PriorityNormal, TimeoutSeconds: 300, RetryDelaySeconds: 1, Metadata: map[string]string{}}
}

// DecodeScheduleOptions reads stored options, absent keys taking their
// defaults.
func DecodeScheduleOptions(raw []byte) (ScheduleOptions, error) {
	o := DefaultScheduleOptions()
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &o); err != nil {
			return o, NewSerializationError("schedule options: " + err.Error())
		}
	}
	if o.Metadata == nil {
		o.Metadata = map[string]string{}
	}
	return o, nil
}

// Schedule is a stored cron schedule: it enqueues an activity of
// ActivityType at each fire time of Cron in Timezone.
type Schedule struct {
	Name     string
	Cron     string
	Timezone string
	// ActivityType, Payload and Serialization are the fired activity's;
	// Serialization is as for QueuedActivity.
	ActivityType  string
	Payload       json.RawMessage
	Serialization string `json:",omitempty"`
	Options       ScheduleOptions
	CatchUp       CatchUp
	CatchUpLimit  int

	// Set by the backend; UpsertSchedule ignores them.
	Paused     bool
	NextFireAt time.Time
	LastFireAt *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// ScheduleStorage is an optional capability: cron schedules stored with the
// queue, which every engine of the queue fires (runnerq-spec schedules.md).
type ScheduleStorage interface {
	// UpsertSchedule creates or replaces a schedule by name. It keeps Paused
	// and LastFireAt, and NextFireAt unless Cron or Timezone changed; an
	// unchanged definition changes nothing. The schedule must pass
	// ValidateSchedule.
	UpsertSchedule(ctx context.Context, schedule Schedule) error
	// GetSchedule returns a not-found error for an unknown name.
	GetSchedule(ctx context.Context, name string) (*Schedule, error)
	// ListSchedules returns the queue's schedules by name.
	ListSchedules(ctx context.Context) ([]Schedule, error)
	PauseSchedule(ctx context.Context, name string) error
	// ResumeSchedule unpauses a paused schedule and moves NextFireAt to the
	// first fire time after now: ticks missed while paused are not caught up.
	ResumeSchedule(ctx context.Context, name string) error
	DeleteSchedule(ctx context.Context, name string) error
	// TriggerSchedule enqueues the schedule's activity now, paused or not,
	// keyed as a fire at now truncated to the second, and returns its ID (the
	// existing one when that key was fired already).
	TriggerSchedule(ctx context.Context, name string) (uuid.UUID, error)
	// FireDueSchedules fires up to limit due schedules, each in one
	// transaction that enqueues its fire times (ScheduleFireTimes) under
	// ScheduleIdempotencyKey and advances NextFireAt, so concurrent callers
	// fire each tick once. It returns how many activities it enqueued, and
	// the errors of schedules it could not fire, having fired the rest.
	FireDueSchedules(ctx context.Context, limit int) (int, error)
}

// ValidateSchedule checks what UpsertSchedule stores, filling in the zero
// Timezone (UTC), CatchUp (one) and CatchUpLimit (DefaultCatchUpLimit).
func ValidateSchedule(s *Schedule) error {
	invalid := func(field, msg string) error {
		return &StorageError{Kind: ErrInvalidArgument, Field: field, Message: msg}
	}
	if s.Timezone == "" {
		s.Timezone = "UTC"
	}
	if s.CatchUp == "" {
		s.CatchUp = CatchUpOne
	}
	if s.CatchUpLimit == 0 {
		s.CatchUpLimit = DefaultCatchUpLimit
	}
	if s.Options.Metadata == nil {
		s.Options.Metadata = map[string]string{}
	}
	switch {
	case strings.TrimSpace(s.Name) == "":
		return invalid("name", "schedule name is required")
	case s.ActivityType == "":
		return invalid("activity_type", "schedule activity type is required")
	case s.CatchUp != CatchUpSkip && s.CatchUp != CatchUpOne && s.CatchUp != CatchUpAll:
		return invalid("catch_up", fmt.Sprintf("catch_up must be skip, one or all, not %q", s.CatchUp))
	case s.CatchUpLimit < 1 || s.CatchUpLimit > MaxCatchUpLimit:
		return invalid("catch_up_limit", fmt.Sprintf("catch_up_limit must be 1 to %d", MaxCatchUpLimit))
	case s.Options.Priority < PriorityLow || s.Options.Priority > PriorityCritical:
		return invalid("priority", "priority must be 1 to 4")
	case len(s.Options.CodeVersion) > MaxCodeVersionBytes:
		return invalid("code_version", fmt.Sprintf("code_version must be at most %d bytes", MaxCodeVersionBytes))
	}
	if _, err := cron.Parse(s.Cron); err != nil {
		return invalid("cron", err.Error())
	}
	if _, err := cron.LoadZone(s.Timezone); err != nil {
		return invalid("timezone", err.Error())
	}
	return nil
}

// NextFireTime is the first fire time of cronExpr in timezone strictly after
// after.
func NextFireTime(cronExpr, timezone string, after time.Time) (time.Time, error) {
	e, loc, err := parseSchedule(cronExpr, timezone)
	if err != nil {
		return time.Time{}, err
	}
	next, ok := e.Next(after, loc)
	if !ok {
		return time.Time{}, &StorageError{Kind: ErrInvalidArgument, Field: "cron", Message: "the expression has no fire time within 50 years"}
	}
	return next, nil
}

// ScheduleFireTimes is what firing a due schedule at now enqueues, oldest
// first, and its next NextFireAt (runnerq-spec vectors/schedule_fire.json).
// A schedule not yet due fires nothing and keeps its NextFireAt.
func ScheduleFireTimes(s *Schedule, now time.Time) (fire []time.Time, next time.Time, err error) {
	if s.NextFireAt.After(now) {
		return nil, s.NextFireAt, nil
	}
	e, loc, err := parseSchedule(s.Cron, s.Timezone)
	if err != nil {
		return nil, time.Time{}, err
	}
	n := 1
	if s.CatchUp == CatchUpAll {
		n = max(s.CatchUpLimit, 1)
	}
	fire = e.Last(s.NextFireAt, now, n, loc)
	grace := time.Duration(spec.ScheduleCatchUpGraceSeconds) * time.Second
	if s.CatchUp == CatchUpSkip && len(fire) > 0 && now.Sub(fire[len(fire)-1]) >= grace {
		fire = nil
	}
	next, ok := e.Next(now, loc)
	if !ok {
		return nil, time.Time{}, &StorageError{Kind: ErrInvalidArgument, Field: "cron", Message: "the expression has no fire time within 50 years"}
	}
	return fire, next, nil
}

// ScheduleIdempotencyKey is the application key of the activity a schedule
// fires at a time: schedule:<name>:<time in RFC 3339 UTC, to the second>.
// Store it encoded, as BusinessIdempotencyKey(key, activity type).
func ScheduleIdempotencyKey(name string, at time.Time) string {
	return spec.ScheduleKeyPrefix + name + ":" + at.UTC().Truncate(time.Second).Format(time.RFC3339)
}

// ScheduledActivity is the activity a schedule fires at a time, keyed by
// ScheduleIdempotencyKey with BehaviorReturnExisting.
func ScheduledActivity(s *Schedule, at, now time.Time) QueuedActivity {
	id := uuid.New()
	payload := s.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("null")
	}
	return QueuedActivity{
		ID:                   id,
		ActivityType:         s.ActivityType,
		Payload:              payload,
		Priority:             s.Options.Priority,
		MaxRetries:           s.Options.MaxRetries,
		TimeoutSeconds:       s.Options.TimeoutSeconds,
		RetryDelaySeconds:    s.Options.RetryDelaySeconds,
		MaxRetryDelaySeconds: s.Options.MaxRetryDelaySeconds,
		Metadata:             s.Options.Metadata,
		IdempotencyKey: &IdempotencyKeyConfig{
			Key:      BusinessIdempotencyKey(ScheduleIdempotencyKey(s.Name, at), s.ActivityType),
			Behavior: BehaviorReturnExisting,
		},
		CreatedAt:      now,
		RootActivityID: id,
		Serialization:  s.Serialization,
		CodeVersion:    s.Options.CodeVersion,
	}
}

func parseSchedule(cronExpr, timezone string) (*cron.Expression, *time.Location, error) {
	e, err := cron.Parse(cronExpr)
	if err != nil {
		return nil, nil, &StorageError{Kind: ErrInvalidArgument, Field: "cron", Message: err.Error()}
	}
	loc, err := cron.LoadZone(timezone)
	if err != nil {
		return nil, nil, &StorageError{Kind: ErrInvalidArgument, Field: "timezone", Message: err.Error()}
	}
	return e, loc, nil
}
