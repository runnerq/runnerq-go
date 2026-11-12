package runnerq

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/runnerq/runnerq-go/executor"
)

// MetricsSink receives the engine's counters and durations by name, without
// labels or gauges. LabeledMetricsSink receives everything; Builder.Metrics
// adapts a MetricsSink with UnlabeledMetrics.
type MetricsSink interface {
	IncCounter(name string, value uint64)
	ObserveDuration(name string, dur time.Duration)
}

// MetricLabels are the labels of one observation. MetricCatalog lists which
// labels each metric carries; the others are empty.
type MetricLabels struct {
	Queue        string
	ActivityType string
	// Status is a canonical activity status (storage.RecordStatus*), on
	// MetricQueueDepth only.
	Status string
}

// LabeledMetricsSink receives every metric of MetricCatalog with its labels,
// named by the Metric* constants. Methods are called from the engine's hot
// paths and many goroutines: they must be safe for concurrent use and must
// not block.
type LabeledMetricsSink interface {
	// AddCounter adds value to a counter.
	AddCounter(name string, value uint64, labels MetricLabels)
	// RecordHistogram records one observation in the metric's unit (seconds
	// for durations).
	RecordHistogram(name string, value float64, labels MetricLabels)
	// SetGauge sets a gauge's current value for these labels.
	SetGauge(name string, value float64, labels MetricLabels)
}

// NoopMetrics discards all metrics; it is the default sink.
type NoopMetrics struct{}

func (NoopMetrics) IncCounter(string, uint64)                     {}
func (NoopMetrics) ObserveDuration(string, time.Duration)         {}
func (NoopMetrics) AddCounter(string, uint64, MetricLabels)       {}
func (NoopMetrics) RecordHistogram(string, float64, MetricLabels) {}
func (NoopMetrics) SetGauge(string, float64, MetricLabels)        {}

// UnlabeledMetrics adapts a MetricsSink to LabeledMetricsSink: counters and
// histograms arrive without their labels, histograms through ObserveDuration,
// and gauges are dropped.
func UnlabeledMetrics(sink MetricsSink) LabeledMetricsSink {
	if sink == nil {
		return NoopMetrics{}
	}
	if l, ok := sink.(LabeledMetricsSink); ok {
		return l
	}
	return unlabeled{sink}
}

type unlabeled struct{ sink MetricsSink }

func (u unlabeled) AddCounter(name string, value uint64, _ MetricLabels) {
	u.sink.IncCounter(name, value)
}

func (u unlabeled) RecordHistogram(name string, value float64, _ MetricLabels) {
	u.sink.ObserveDuration(name, fromSeconds(value))
}

func (unlabeled) SetGauge(string, float64, MetricLabels) {}

// countingMetrics keeps the counts executor snapshots report, lock-free,
// and passes every metric on to the configured sink unchanged.
type countingMetrics struct {
	sink LabeledMetricsSink

	started, completed, retried, failed, timedOut atomic.Uint64
	deadLettered, claimsLost, heartbeatFailed     atomic.Uint64
	claimLag                                      atomic.Int64
}

func newCountingMetrics(sink LabeledMetricsSink) *countingMetrics {
	return &countingMetrics{sink: sink}
}

func (m *countingMetrics) counter(name string) *atomic.Uint64 {
	switch name {
	case MetricActivityStarted:
		return &m.started
	case MetricActivityCompleted:
		return &m.completed
	case MetricActivityRetry:
		return &m.retried
	case MetricActivityFailedNonRetry:
		return &m.failed
	case MetricActivityTimeout:
		return &m.timedOut
	case MetricActivityDeadLettered:
		return &m.deadLettered
	case MetricActivityClaimLost:
		return &m.claimsLost
	case MetricActivityHeartbeatFailed:
		return &m.heartbeatFailed
	}
	return nil
}

func (m *countingMetrics) AddCounter(name string, value uint64, labels MetricLabels) {
	if c := m.counter(name); c != nil {
		c.Add(value)
	}
	m.sink.AddCounter(name, value, labels)
}

func (m *countingMetrics) RecordHistogram(name string, value float64, labels MetricLabels) {
	if name == MetricActivityClaimLag {
		m.claimLag.Store(int64(fromSeconds(value)))
	}
	m.sink.RecordHistogram(name, value, labels)
}

func (m *countingMetrics) SetGauge(name string, value float64, labels MetricLabels) {
	m.sink.SetGauge(name, value, labels)
}

func seconds(d time.Duration) float64 { return d.Seconds() }

func fromSeconds(s float64) time.Duration {
	return time.Duration(math.Round(s * float64(time.Second)))
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
