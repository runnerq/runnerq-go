package runnerq

import (
	"sync/atomic"
	"time"

	"github.com/runnerq/runnerq-go/executor"
)

// MetricsSink receives the engine's counters and durations.
type MetricsSink interface {
	IncCounter(name string, value uint64)
	ObserveDuration(name string, dur time.Duration)
}

// NoopMetrics discards all metrics; it is the default sink.
type NoopMetrics struct{}

func (NoopMetrics) IncCounter(_ string, _ uint64)             {}
func (NoopMetrics) ObserveDuration(_ string, _ time.Duration) {}

// Metrics the engine emits that its executor snapshots count.
const (
	metricStarted         = "activity_started"
	metricCompleted       = "activity_completed"
	metricRetried         = "activity_retry"
	metricFailed          = "activity_failed_non_retry"
	metricTimedOut        = "activity_timeout"
	metricDeadLettered    = "activity_dead_lettered"
	metricClaimLost       = "activity_claim_lost"
	metricHeartbeatFailed = "activity_heartbeat_failed"
	// metricClaimLag: how long an activity waited to be claimed after it
	// was due.
	metricClaimLag = "activity_claim_lag"
)

// countingMetrics keeps the counts executor snapshots report, lock-free,
// and passes every metric on to the configured sink unchanged.
type countingMetrics struct {
	sink MetricsSink

	started, completed, retried, failed, timedOut atomic.Uint64
	deadLettered, claimsLost, heartbeatFailed     atomic.Uint64
	claimLag                                      atomic.Int64
}

func newCountingMetrics(sink MetricsSink) *countingMetrics {
	return &countingMetrics{sink: sink}
}

func (m *countingMetrics) counter(name string) *atomic.Uint64 {
	switch name {
	case metricStarted:
		return &m.started
	case metricCompleted:
		return &m.completed
	case metricRetried:
		return &m.retried
	case metricFailed:
		return &m.failed
	case metricTimedOut:
		return &m.timedOut
	case metricDeadLettered:
		return &m.deadLettered
	case metricClaimLost:
		return &m.claimsLost
	case metricHeartbeatFailed:
		return &m.heartbeatFailed
	}
	return nil
}

func (m *countingMetrics) IncCounter(name string, value uint64) {
	if c := m.counter(name); c != nil {
		c.Add(value)
	}
	m.sink.IncCounter(name, value)
}

func (m *countingMetrics) ObserveDuration(name string, dur time.Duration) {
	if name == metricClaimLag {
		m.claimLag.Store(int64(dur))
	}
	m.sink.ObserveDuration(name, dur)
}

func (m *countingMetrics) counters() executor.Counters {
	return executor.Counters{
		Claimed:           m.started.Load(),
		Succeeded:         m.completed.Load(),
		Retried:           m.retried.Load(),
		Failed:            m.failed.Load(),
		TimedOut:          m.timedOut.Load(),
		DeadLettered:      m.deadLettered.Load(),
		ClaimsLost:        m.claimsLost.Load(),
		HeartbeatFailures: m.heartbeatFailed.Load(),
		LastClaimLag:      time.Duration(m.claimLag.Load()),
	}
}
