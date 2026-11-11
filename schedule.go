package runnerq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"time"

	"github.com/runnerq/runnerq-go/storage"
)

// Schedule enqueues an activity at each fire time of a cron expression. Every
// engine of the queue fires the queue's schedules, and each tick fires once
// however many engines run (docs/schedules.md).
type Schedule struct {
	// Name identifies the schedule in its queue.
	Name string
	// Cron is a 5-field expression (minute hour day-of-month month
	// day-of-week), a 6-field one with seconds first, or a macro such as
	// @daily.
	Cron string
	// Timezone is the IANA zone Cron's wall-clock times are in (default
	// "UTC"). On a host without zoneinfo, import _ "time/tzdata".
	Timezone string
	// Activity is the activity type to enqueue, e.g. NameOf[Report]().
	Activity string
	// Payload is each fired activity's payload (default JSON null).
	Payload json.RawMessage

	// Execution options of the fired activities, defaulting as for
	// ActivityBuilder: normal priority, 3 attempts, a 300s timeout and a 1h
	// retry delay cap.
	Priority      ActivityPriority
	MaxRetries    *uint32
	Timeout       time.Duration
	MaxRetryDelay time.Duration
	Metadata      map[string]string

	// CatchUp says what to do with ticks that came due while no engine ran:
	// storage.CatchUpOne (default) fires the latest once, CatchUpSkip fires
	// nothing late by a minute or more, CatchUpAll fires each of them, up to
	// CatchUpLimit (default 100).
	CatchUp      storage.CatchUp
	CatchUpLimit int
}

func (s Schedule) stored() storage.Schedule {
	o := storage.ScheduleOptions{
		Priority:             storage.PriorityNormal,
		MaxRetries:           3,
		TimeoutSeconds:       300,
		RetryDelaySeconds:    1,
		MaxRetryDelaySeconds: 3600,
		Metadata:             map[string]string{},
	}
	if s.Priority != 0 {
		o.Priority = storage.ActivityPriority(s.Priority)
	}
	if s.MaxRetries != nil {
		o.MaxRetries = *s.MaxRetries
	}
	if s.Timeout > 0 {
		o.TimeoutSeconds = max(uint64(s.Timeout.Seconds()), 1)
	}
	if s.MaxRetryDelay > 0 {
		o.MaxRetryDelaySeconds = max(uint64(s.MaxRetryDelay.Seconds()), 1)
	}
	maps.Copy(o.Metadata, s.Metadata)
	return storage.Schedule{
		Name: s.Name, Cron: s.Cron, Timezone: s.Timezone, ActivityType: s.Activity,
		Payload: s.Payload, Options: o, CatchUp: s.CatchUp, CatchUpLimit: s.CatchUpLimit,
	}
}

// Schedules manages a queue's cron schedules from any process, worker or
// not. Get one with NewSchedules or WorkerEngine.Schedules.
type Schedules struct {
	store   storage.ScheduleStorage
	backend storage.Storage
}

// NewSchedules manages the schedules of backend's queue. It fails when the
// backend does not implement storage.ScheduleStorage.
func NewSchedules(backend storage.Storage) (*Schedules, error) {
	store, ok := backend.(storage.ScheduleStorage)
	if !ok {
		return nil, &WorkerError{Kind: ErrConfiguration, Message: fmt.Sprintf("storage %T does not support schedules", backend)}
	}
	return &Schedules{store: store, backend: backend}, nil
}

// Upsert creates the schedule or replaces its definition. A paused schedule
// stays paused, and its next fire time is kept unless Cron or Timezone
// changed.
func (m *Schedules) Upsert(ctx context.Context, s Schedule) error {
	return m.store.UpsertSchedule(ctx, s.stored())
}

// Get returns a schedule as stored; a not-found error for an unknown name.
func (m *Schedules) Get(ctx context.Context, name string) (*storage.Schedule, error) {
	return m.store.GetSchedule(ctx, name)
}

// List returns the queue's schedules by name.
func (m *Schedules) List(ctx context.Context) ([]storage.Schedule, error) {
	return m.store.ListSchedules(ctx)
}

// Pause stops a schedule firing until Resume.
func (m *Schedules) Pause(ctx context.Context, name string) error {
	return m.store.PauseSchedule(ctx, name)
}

// Resume restarts a paused schedule from its next fire time after now; ticks
// missed while paused are not caught up.
func (m *Schedules) Resume(ctx context.Context, name string) error {
	return m.store.ResumeSchedule(ctx, name)
}

// Trigger enqueues the schedule's activity now, even while paused, without
// changing its fire times. Triggers within the same second share one
// activity.
func (m *Schedules) Trigger(ctx context.Context, name string) (*ActivityFuture, error) {
	id, err := m.store.TriggerSchedule(ctx, name)
	if err != nil {
		return nil, err
	}
	return FutureFor(m.backend, id), nil
}

// Delete removes a schedule. Activities it already enqueued are unaffected.
func (m *Schedules) Delete(ctx context.Context, name string) error {
	return m.store.DeleteSchedule(ctx, name)
}

// Schedules manages the schedules of the engine's queue; nil when its
// backend does not support schedules.
func (e *WorkerEngine) Schedules() *Schedules {
	m, err := NewSchedules(e.backend)
	if err != nil {
		return nil
	}
	return m
}

// RegisterSchedule declares a schedule that Start upserts, so the schedules
// a deployment's code declares are the ones stored. It fails on an invalid
// schedule, a duplicate name, a backend without schedules, or once the engine
// is running.
func (e *WorkerEngine) RegisterSchedule(s Schedule) error {
	if _, ok := e.backend.(storage.ScheduleStorage); !ok {
		return &WorkerError{Kind: ErrConfiguration, Message: fmt.Sprintf("storage %T does not support schedules", e.backend)}
	}
	stored := s.stored()
	if err := storage.ValidateSchedule(&stored); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active {
		return &WorkerError{Kind: ErrConfiguration, Message: "register schedules before starting the engine"}
	}
	for _, d := range e.schedules {
		if d.Name == s.Name {
			return &WorkerError{Kind: ErrConfiguration, Message: fmt.Sprintf("schedule %q is already registered", s.Name)}
		}
	}
	e.schedules = append(e.schedules, s)
	return nil
}

const (
	scheduleFireInterval = time.Second
	scheduleFireBatch    = 100
)

// runScheduleFirer fires the queue's due schedules about every second.
func (e *WorkerEngine) runScheduleFirer(ctx context.Context, store storage.ScheduleStorage) {
	interval := scheduleFireInterval
	if e.scheduleInterval > 0 {
		interval = e.scheduleInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for e.running.Load() {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := store.FireDueSchedules(ctx, scheduleFireBatch)
			if n > 0 {
				e.metrics.IncCounter("schedule_fires", uint64(n))
			}
			if err != nil && ctx.Err() == nil {
				slog.Error("Failed to fire schedules", "error", err)
			}
		}
	}
}

// upsertSchedules stores the registered schedules.
func (e *WorkerEngine) upsertSchedules(ctx context.Context, schedules []Schedule) error {
	if len(schedules) == 0 {
		return nil
	}
	m, err := NewSchedules(e.backend)
	if err != nil {
		return err
	}
	for _, s := range schedules {
		if err := m.Upsert(ctx, s); err != nil {
			return fmt.Errorf("upsert schedule %q: %w", s.Name, err)
		}
	}
	return nil
}
