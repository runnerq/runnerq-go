package runnerq

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// telemetry is what an engine and its executors report through.
type telemetry struct {
	tracer Tracer
	queue  string
}

func (t *telemetry) tracerOf() Tracer {
	if t == nil {
		return nil
	}
	return t.tracer
}

func (t *telemetry) queueName() string {
	if t == nil {
		return ""
	}
	return t.queue
}

func (t *telemetry) labels(activityType string) MetricLabels {
	return MetricLabels{Queue: t.queueName(), ActivityType: activityType}
}

// activityAttrs are the attributes enqueue and attempt spans share.
func (t *telemetry) activityAttrs(a *activity) []TraceAttribute {
	root := a.RootActivityID
	if root == uuid.Nil {
		root = a.ID
	}
	attrs := []TraceAttribute{
		{AttrQueue, t.queueName()},
		{AttrActivityType, a.ActivityType},
		{AttrActivityID, a.ID.String()},
		{AttrRootID, root.String()},
	}
	if a.ParentActivityID != nil {
		attrs = append(attrs, TraceAttribute{AttrParentID, a.ParentActivityID.String()})
	}
	return attrs
}

// gaugeStatuses are the statuses MetricQueueDepth reports.
var gaugeStatuses = []string{
	storage.RecordStatusPending, storage.RecordStatusScheduled, storage.RecordStatusRunning,
	storage.RecordStatusWaiting, storage.RecordStatusDeadLetter,
}

// runGaugeSampler sets the gauges every interval, jittered by up to a fifth
// either way so a fleet's workers don't query in step.
func (e *WorkerEngine) runGaugeSampler(ctx context.Context, interval time.Duration, types []string) {
	reported := map[MetricLabels]bool{}
	for {
		wait := time.Duration(float64(interval) * (0.8 + 0.4*rand.Float64()))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		e.sampleGauges(ctx, types, reported)
	}
}

// sampleGauges sets the gauges once. reported holds the queue_depth label
// sets reported before, so one that has emptied drops to 0.
func (e *WorkerEngine) sampleGauges(ctx context.Context, types []string, reported map[MetricLabels]bool) {
	inFlight := make(map[string]int, len(types))
	e.inflight.Range(func(_, v any) bool {
		inFlight[v.(*running).Type]++
		return true
	})
	for _, t := range types {
		e.metrics.SetGauge(MetricActivitiesInFlight, float64(inFlight[t]), e.tel.labels(t))
	}

	qs, ok := e.backend.(storage.QueryStorage)
	if !ok {
		return
	}
	queue := e.tel.queueName()
	statuses := make([]any, len(gaugeStatuses))
	for i, s := range gaugeStatuses {
		statuses[i] = s
	}
	rows, err := qs.AggregateActivities(ctx, storage.AggregateQuery{
		Filter: &storage.QueryFilter{And: []storage.QueryFilter{
			{Field: "queue", Op: storage.OpEq, Value: queue},
			{Field: "status", Op: storage.OpIn, Value: statuses},
		}},
		GroupBy: []string{"type", "status"},
		Count:   true,
		Limit:   1000,
	})
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("Could not sample queue depth", "queue", queue, "error", err)
		}
		return
	}
	seen := make(map[MetricLabels]bool, len(rows.Rows))
	for _, r := range rows.Rows {
		l := MetricLabels{Queue: queue, ActivityType: r.Key["type"], Status: r.Key["status"]}
		seen[l] = true
		e.metrics.SetGauge(MetricQueueDepth, float64(r.Count), l)
	}
	for l := range reported {
		if !seen[l] {
			e.metrics.SetGauge(MetricQueueDepth, 0, l)
			delete(reported, l)
		}
	}
	for l := range seen {
		reported[l] = true
	}

	page, err := qs.QueryActivities(ctx, storage.ActivityQuery{
		Filter: &storage.QueryFilter{And: []storage.QueryFilter{
			{Field: "queue", Op: storage.OpEq, Value: queue},
			{Field: "status", Op: storage.OpEq, Value: storage.RecordStatusPending},
		}},
		Sort:  &storage.QuerySort{Field: "created_at"},
		Limit: 1,
	})
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("Could not sample the oldest pending activity", "queue", queue, "error", err)
		}
		return
	}
	age := 0.0
	if len(page.Items) > 0 {
		due := page.Items[0].CreatedAt
		if s := page.Items[0].ScheduledFor; s != nil && s.After(due) {
			due = *s
		}
		age = max(time.Since(due).Seconds(), 0)
	}
	e.metrics.SetGauge(MetricQueueOldestPendingAge, age, MetricLabels{Queue: queue})
}

func attemptAttr(n uint32) TraceAttribute {
	return TraceAttribute{AttrAttempt, int64(n)}
}
