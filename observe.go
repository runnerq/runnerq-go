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
	versions := &versionGauges{byVersion: map[string]bool{}, unserved: map[string]bool{}}
	for {
		wait := time.Duration(float64(interval) * (0.8 + 0.4*rand.Float64())) // #nosec G404 -- sampling jitter, not a secret
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		e.sampleGauges(ctx, types, reported)
		e.sampleVersionGauges(ctx, versions)
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

// versionGauges remembers the versions reported before, so one that has
// emptied drops to 0 and the stranded-work warning is logged once per spell.
type versionGauges struct {
	byVersion map[string]bool
	unserved  map[string]bool
}

// unfinishedStatuses are the statuses MetricActivitiesByVersion counts.
var unfinishedStatuses = []any{
	storage.RecordStatusPending, storage.RecordStatusScheduled, storage.RecordStatusRunning, storage.RecordStatusWaiting,
}

// sampleVersionGauges sets MetricActivitiesByVersion and
// MetricActivitiesUnserved, for an engine with a Version.
func (e *WorkerEngine) sampleVersionGauges(ctx context.Context, g *versionGauges) {
	served := e.config.servedVersions()
	qs, ok := e.backend.(storage.QueryStorage)
	if len(served) == 0 || !ok {
		return
	}
	queue := e.tel.queueName()
	inQueue := storage.QueryFilter{Field: "queue", Op: storage.OpEq, Value: queue}
	byVersion, err := e.countByVersion(ctx, qs, storage.QueryFilter{And: []storage.QueryFilter{
		inQueue, {Field: "status", Op: storage.OpIn, Value: unfinishedStatuses},
	}})
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("Could not sample activities by version", "queue", queue, "error", err)
		}
		return
	}
	setVersionGauge(e.metrics, MetricActivitiesByVersion, queue, byVersion, g.byVersion)

	servedAny := make([]any, len(served))
	for i, v := range served {
		servedAny[i] = v
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	unserved, err := e.countByVersion(ctx, qs, storage.QueryFilter{And: []storage.QueryFilter{
		inQueue,
		{Field: "version", Op: storage.OpExists, Value: true},
		{Field: "version", Op: storage.OpNin, Value: servedAny},
		{Or: []storage.QueryFilter{
			{Field: "status", Op: storage.OpEq, Value: storage.RecordStatusPending},
			{And: []storage.QueryFilter{
				{Field: "status", Op: storage.OpIn, Value: []any{storage.RecordStatusScheduled, storage.RecordStatusWaiting}},
				{Field: "scheduled_for", Op: storage.OpLte, Value: now},
			}},
		}},
	}})
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("Could not sample unserved activities", "queue", queue, "error", err)
		}
		return
	}
	for v, n := range unserved {
		if !g.unserved[v] {
			slog.Warn("Runnable work is pinned to a version this worker does not serve",
				"queue", queue, "version", v, "count", n)
		}
	}
	for _, v := range served {
		if _, ok := unserved[v]; !ok {
			unserved[v] = 0
		}
	}
	setVersionGauge(e.metrics, MetricActivitiesUnserved, queue, unserved, g.unserved)
}

// countByVersion counts the activities matching filter by version; "" is
// unstamped.
func (e *WorkerEngine) countByVersion(ctx context.Context, qs storage.QueryStorage, filter storage.QueryFilter) (map[string]int64, error) {
	rows, err := qs.AggregateActivities(ctx, storage.AggregateQuery{
		Filter: &filter, GroupBy: []string{"version"}, Count: true, Limit: 1000,
	})
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(rows.Rows))
	for _, r := range rows.Rows {
		out[r.Key["version"]] += r.Count
	}
	return out, nil
}

// setVersionGauge reports counts, and 0 for versions reported before and
// absent now; reported is updated to the versions with a count above 0.
func setVersionGauge(m LabeledMetricsSink, name, queue string, counts map[string]int64, reported map[string]bool) {
	for v, n := range counts {
		m.SetGauge(name, float64(n), MetricLabels{Queue: queue, Version: v})
	}
	for v := range reported {
		if _, ok := counts[v]; !ok {
			m.SetGauge(name, 0, MetricLabels{Queue: queue, Version: v})
		}
	}
	clear(reported)
	for v, n := range counts {
		if n > 0 {
			reported[v] = true
		}
	}
}
