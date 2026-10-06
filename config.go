package runnerq

import (
	"maps"
	"time"
)

// RetentionConfig opts the engine into deleting old terminal workflow trees;
// without it everything is kept forever. A tree is deleted whole (a terminal
// root with no non-terminal descendants), so a retry never finds a child's
// result missing. The backend elects one sweeper per queue, so every engine
// may set it.
type RetentionConfig struct {
	// Completed is how long trees whose root completed are kept; zero is
	// forever.
	Completed time.Duration `json:"completed,omitempty"`
	// Failed is how long trees whose root failed or dead-lettered are kept,
	// so failures can be held longer for inspection; zero is forever.
	Failed time.Duration `json:"failed,omitempty"`
	// Events is how long the events of finished activities are kept, when
	// that should be shorter than their tree; zero keeps them with the tree.
	// Events of unfinished activities are always kept.
	Events time.Duration `json:"events,omitempty"`
	// Interval is the sweep cadence (default 10 minutes).
	Interval time.Duration `json:"interval,omitempty"`
	// BatchSize is the most trees deleted per sweep transaction (default 100).
	BatchSize int `json:"batch_size,omitempty"`
}

// WorkerConfig configures a WorkerEngine.
type WorkerConfig struct {
	// QueueName is the queue this engine serves; applications sharing a
	// database keep apart by using different queues.
	QueueName string `json:"queue_name"`

	// MaxConcurrentActivities is how many activities run at once.
	MaxConcurrentActivities int `json:"max_concurrent_activities"`

	// SchedulePollIntervalSeconds is how often due scheduled activities are
	// released (default 5), for backends that don't schedule in Dequeue.
	SchedulePollIntervalSeconds *uint64 `json:"schedule_poll_interval_seconds,omitempty"`

	// LeaseMS overrides the backend's minimum lease on a claim, in
	// milliseconds. Nil keeps the backend's own (postgres.WithConfig's
	// defaultLeaseMS; 60s for postgres.New).
	LeaseMS *uint64 `json:"lease_ms,omitempty"`

	// ReaperIntervalSeconds is how often expired leases are reclaimed
	// (default 5).
	ReaperIntervalSeconds *uint64 `json:"reaper_interval_seconds,omitempty"`

	// ReaperBatchSize is the most expired leases reclaimed per tick
	// (default 100).
	ReaperBatchSize *int `json:"reaper_batch_size,omitempty"`

	// ActivityTypes restricts the types this engine claims; empty means
	// every registered type.
	ActivityTypes []string `json:"activity_types,omitempty"`

	// MaxActivityDepth caps the depth of a parent/child tree (default 32); a
	// spawn beyond it fails with ErrDepthExceeded.
	MaxActivityDepth uint16 `json:"max_activity_depth,omitempty"`

	// Retention enables deleting old terminal workflow trees; nil keeps
	// everything.
	Retention *RetentionConfig `json:"retention,omitempty"`

	// ShutdownGraceSeconds bounds the whole shutdown drain (default 30).
	// When it runs out, Start returns with activities still in flight.
	ShutdownGraceSeconds *uint64 `json:"shutdown_grace_seconds,omitempty"`

	// Labels are free-form tags for this worker (region, deploy version),
	// carried in its executor snapshots.
	Labels map[string]string `json:"labels,omitempty"`
}

// DefaultMaxActivityDepth is MaxActivityDepth's default.
const DefaultMaxActivityDepth uint16 = 32

// DefaultWorkerConfig is the configuration Builder starts from.
func DefaultWorkerConfig() WorkerConfig {
	reaperInterval := uint64(5)
	reaperBatch := 100
	return WorkerConfig{
		QueueName:               "default",
		MaxConcurrentActivities: 10,
		ReaperIntervalSeconds:   &reaperInterval,
		ReaperBatchSize:         &reaperBatch,
	}
}

// cloneWorkerConfig detaches caller-owned slices and pointers.
func cloneWorkerConfig(c WorkerConfig) WorkerConfig {
	c.ActivityTypes = append([]string(nil), c.ActivityTypes...)
	c.Labels = maps.Clone(c.Labels)
	c.SchedulePollIntervalSeconds = cloneConfigPtr(c.SchedulePollIntervalSeconds)
	c.LeaseMS = cloneConfigPtr(c.LeaseMS)
	c.ReaperIntervalSeconds = cloneConfigPtr(c.ReaperIntervalSeconds)
	c.ReaperBatchSize = cloneConfigPtr(c.ReaperBatchSize)
	c.Retention = cloneConfigPtr(c.Retention)
	c.ShutdownGraceSeconds = cloneConfigPtr(c.ShutdownGraceSeconds)
	return c
}
func cloneConfigPtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
