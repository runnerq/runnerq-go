package conductor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go"
	"github.com/runnerq/runnerq-go/conductor/internal/wire"
	"github.com/runnerq/runnerq-go/executor"
	"github.com/runnerq/runnerq-go/internal/spec"
	"github.com/runnerq/runnerq-go/storage"
)

// maxEmbedded bounds steps and events embedded in activities.get.
const maxEmbedded = 1000

// handlerFunc returns a *wireError to pick the error code; other errors go
// through toWireError.
type handlerFunc func(ctx context.Context, data json.RawMessage) (any, error)

type handlers struct {
	engine  *runnerq.WorkerEngine // nil in a Handler
	backend storage.Storage
	// queue is the queue commands act on.
	queue        string
	qs           storage.QueryStorage   // nil when unsupported
	cs           storage.CommandStorage // nil when unsupported
	allowControl bool
	// forceMetadataOnly is the local setting; it always wins over
	// cloudMetadataOnly (welcome/config.update).
	forceMetadataOnly bool
	cloudMetadataOnly atomic.Bool
	started           time.Time
}

func newHandlers(engine *runnerq.WorkerEngine, forceMetadataOnly, allowControl bool) *handlers {
	backend := engine.Backend()
	qs, _ := backend.(storage.QueryStorage)
	cs, _ := backend.(storage.CommandStorage)
	return &handlers{engine: engine, backend: backend, queue: engine.QueueName(), qs: qs, cs: cs, allowControl: allowControl,
		forceMetadataOnly: forceMetadataOnly, started: time.Now()}
}

func (h *handlers) metadataOnly() bool { return h.forceMetadataOnly || h.cloudMetadataOnly.Load() }

func (h *handlers) table() map[string]handlerFunc {
	t := map[string]handlerFunc{wire.TypeExecutorDescribe: h.executorDescribe}
	if h.qs != nil {
		t[wire.TypeActivitiesList] = h.activitiesList
		t[wire.TypeActivitiesGet] = h.activitiesGet
		t[wire.TypeActivitiesCount] = h.activitiesCount
		t[wire.TypeActivitiesAggregate] = h.activitiesAggregate
		t[wire.TypeStepsList] = h.stepsList
		t[wire.TypeEventsList] = h.eventsList
		t[wire.TypeResultsGet] = h.resultsGet
		t[wire.TypeTreesGet] = h.treesGet
	}
	h.addCommands(t)
	return t
}

var (
	recordIncludes = []string{"last_error", "payload", "result"}
	getIncludes    = []string{"events", "last_error", "payload", "result", "steps"}
)

func (h *handlers) capabilities() map[string]wire.Capability {
	caps := map[string]wire.Capability{wire.TypeExecutorDescribe: {V: 1}, wire.TypeActivityNotices: {V: 1}}
	h.commandCapabilities(caps)
	if h.qs == nil {
		return caps
	}
	qc := h.qs.QueryCapabilities()
	caps[wire.TypeActivitiesList] = wire.Capability{V: 1, Filters: qc.ActivityFilters, Sorts: qc.ActivitySorts, Include: recordIncludes}
	caps[wire.TypeActivitiesGet] = wire.Capability{V: 1, Include: getIncludes}
	caps[wire.TypeActivitiesCount] = wire.Capability{V: 1, Filters: qc.ActivityFilters}
	metrics := []string{string(wire.MetricCount)}
	for _, d := range qc.Durations {
		metrics = append(metrics, string(wire.MetricDuration)+"."+d)
	}
	caps[wire.TypeActivitiesAggregate] = wire.Capability{V: 1, Filters: qc.ActivityFilters, GroupBy: qc.GroupBy, Buckets: qc.Buckets, Metrics: metrics}
	caps[wire.TypeStepsList] = wire.Capability{V: 1, Include: []string{"result"}}
	caps[wire.TypeEventsList] = wire.Capability{V: 1, Filters: qc.EventFilters, Sorts: []string{"at"}, Include: []string{"detail"}}
	caps[wire.TypeResultsGet] = wire.Capability{V: 1}
	caps[wire.TypeTreesGet] = wire.Capability{V: 1, Include: recordIncludes}
	caps[wire.TypeEventsSubscribe] = wire.Capability{V: 1, Filters: qc.EventFilters}
	caps[wire.TypeEventsUnsubscribe] = wire.Capability{V: 1}
	return caps
}

func decode[T any](data json.RawMessage) (T, error) {
	var v T
	if len(data) == 0 {
		return v, nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields() // an unknown request field could change meaning
	if err := dec.Decode(&v); err != nil {
		if field, ok := unknownField(err); ok {
			return v, fieldError(wire.CodeInvalidArgument, field, "unknown field %q", field)
		}
		return v, errorf(wire.CodeInvalidArgument, "decode request: %v", err)
	}
	return v, nil
}

// toStorageFilter keeps filter values in their JSON types.
func toStorageFilter(f *wire.Filter) (*storage.QueryFilter, error) {
	if f == nil {
		return nil, nil
	}
	out := &storage.QueryFilter{Field: f.Field, Op: string(f.Op)}
	if len(f.Value) > 0 {
		if err := json.Unmarshal(f.Value, &out.Value); err != nil {
			return nil, fieldError(wire.CodeInvalidArgument, f.Field, "invalid filter value")
		}
	}
	for _, t := range f.And {
		c, err := toStorageFilter(&t)
		if err != nil {
			return nil, err
		}
		out.And = append(out.And, *c)
	}
	for _, t := range f.Or {
		c, err := toStorageFilter(&t)
		if err != nil {
			return nil, err
		}
		out.Or = append(out.Or, *c)
	}
	if f.Not != nil {
		c, err := toStorageFilter(f.Not)
		if err != nil {
			return nil, err
		}
		out.Not = c
	}
	return out, nil
}

// includes refuses anything carrying customer data in metadata-only mode.
func (h *handlers) includes(list, allowed []string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, inc := range list {
		if !slices.Contains(allowed, inc) {
			return nil, fieldError(wire.CodeUnsupported, "include", "cannot include %q", inc)
		}
		if h.metadataOnly() && (inc == "payload" || inc == "result" || inc == "last_error" || inc == "detail") {
			return nil, fieldError(wire.CodeForbidden, "include", "%s is not sent in metadata-only mode", inc)
		}
		out[inc] = true
	}
	return out, nil
}

func recordInclude(inc map[string]bool) storage.RecordInclude {
	return storage.RecordInclude{Payload: inc["payload"], Result: inc["result"], LastError: inc["last_error"]}
}

// parseID reports false for an id this backend could not have issued; on
// the wire such an id simply does not exist.
func parseID(s string) (uuid.UUID, bool) {
	id, err := uuid.Parse(s)
	return id, err == nil
}

func ts(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z07:00") }

func tsp(t *time.Time) string {
	if t == nil {
		return ""
	}
	return ts(*t)
}

func toResult(r *storage.ActivityResult) *wire.Result {
	if r == nil {
		return nil
	}
	if r.State == storage.ResultOk {
		return &wire.Result{State: wire.ResultOK, Data: plainJSON(r)}
	}
	var e struct {
		Error string `json:"error"`
		Type  string `json:"type"`
	}
	if json.Unmarshal(r.Data, &e) == nil && e.Error != "" {
		return &wire.Result{State: wire.ResultError, Error: &wire.ErrorInfo{Message: e.Error, Kind: e.Type}}
	}
	return &wire.Result{State: wire.ResultError, Data: r.Data}
}

func toActivity(r storage.ActivityRecord) wire.Activity {
	v := wire.Activity{
		ID: r.ID.String(), Type: r.Type, Queue: r.Queue, Status: wire.ActivityStatus(r.Status), Priority: r.Priority,
		RootID: r.RootID.String(), Depth: r.Depth, IdempotencyKey: r.IdempotencyKey,
		Attempt: r.Attempt, MaxAttempts: r.MaxAttempts, CreatedAt: ts(r.CreatedAt),
		ScheduledFor: tsp(r.ScheduledFor), StartedAt: tsp(r.StartedAt), CompletedAt: tsp(r.CompletedAt),
		UpdatedAt: ts(r.UpdatedAt), TimeoutMS: r.Timeout.Milliseconds(), LeaseExpiresAt: tsp(r.LeaseExpiresAt),
		ExecutorID: r.ExecutorID, Version: r.Version, Metadata: r.Metadata, Payload: r.Payload, Result: toResult(r.Result),
	}
	if r.ParentID != nil {
		v.ParentID = r.ParentID.String()
	}
	if r.Wait != nil {
		v.Wait = &wire.Wait{Kind: wire.WaitKind(r.Wait.Kind), Name: r.Wait.Name, Until: tsp(r.Wait.Until)}
	}
	if r.LastError != nil {
		v.LastError = &wire.ErrorInfo{Message: r.LastError.Message, Kind: r.LastError.Kind, At: tsp(r.LastError.At)}
	}
	return v
}

func toActivities(items []storage.ActivityRecord) []wire.Activity {
	out := make([]wire.Activity, 0, len(items))
	for _, r := range items {
		out = append(out, toActivity(r))
	}
	return out
}

func toStep(s storage.StepEntry) wire.Step {
	v := wire.Step{
		ID: s.ID.String(), ActivityID: s.ActivityID.String(), Name: s.Name, Kind: wire.StepKind(s.Kind),
		Status: wire.StatusCompleted, CreatedAt: ts(s.CreatedAt),
	}
	if s.State != storage.ResultOk {
		v.Status = wire.StatusFailed
	}
	if s.Data != nil {
		v.Result = toResult(&storage.ActivityResult{State: s.State, Data: s.Data})
	}
	return v
}

func toEvent(e storage.EventRecord) wire.Event {
	return wire.Event{
		ID: e.Cursor, Cursor: e.Cursor, ActivityID: e.ActivityID.String(), Type: e.Type,
		At: ts(e.At), ExecutorID: e.ExecutorID, Detail: e.Detail,
	}
}

func (h *handlers) activitiesList(ctx context.Context, data json.RawMessage) (any, error) {
	q, err := decode[wire.Query](data)
	if err != nil {
		return nil, err
	}
	filter, err := toStorageFilter(q.Filter)
	if err != nil {
		return nil, err
	}
	inc, err := h.includes(q.Include, recordIncludes)
	if err != nil {
		return nil, err
	}
	sq := storage.ActivityQuery{Filter: filter, Include: recordInclude(inc), Limit: q.Limit, Cursor: q.Cursor}
	if sort, err := oneSort(q.Sort); err != nil {
		return nil, err
	} else if sort != nil {
		sq.Sort = sort
	}
	res, err := h.qs.QueryActivities(ctx, sq)
	if err != nil {
		return nil, err
	}
	return wire.ActivityPage{Items: toActivities(res.Items), NextCursor: res.NextCursor}, nil
}

// oneSort accepts at most one sort key (the backend adds the tiebreaker).
func oneSort(sorts []wire.Sort) (*storage.QuerySort, error) {
	switch len(sorts) {
	case 0:
		return nil, nil
	case 1:
	default:
		return nil, fieldError(wire.CodeUnsupported, "sort", "only one sort key is supported")
	}
	s := sorts[0]
	switch s.Order {
	case "", wire.OrderDesc:
		return &storage.QuerySort{Field: s.Field, Desc: true}, nil
	case wire.OrderAsc:
		return &storage.QuerySort{Field: s.Field}, nil
	}
	return nil, fieldError(wire.CodeInvalidArgument, "sort", "order must be asc or desc")
}

func (h *handlers) activitiesGet(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[wire.GetRequest](data)
	if err != nil {
		return nil, err
	}
	inc, err := h.includes(req.Include, getIncludes)
	if err != nil {
		return nil, err
	}
	id, ok := parseID(req.ID)
	if !ok {
		return nil, errorf(wire.CodeNotFound, "activity %q not found", req.ID)
	}
	res, err := h.qs.QueryActivities(ctx, storage.ActivityQuery{
		Filter:  &storage.QueryFilter{Field: "id", Op: storage.OpEq, Value: id.String()},
		Include: recordInclude(inc), Limit: 1,
	})
	if err != nil {
		return nil, err
	}
	if len(res.Items) == 0 {
		return nil, errorf(wire.CodeNotFound, "activity %q not found", req.ID)
	}
	v := toActivity(res.Items[0])
	if inc["steps"] {
		steps, err := h.qs.ListStepEntries(ctx, id, false, maxEmbedded, "")
		if err != nil {
			return nil, err
		}
		list := make([]wire.Step, 0, len(steps.Items))
		for _, s := range steps.Items {
			list = append(list, toStep(s))
		}
		v.Steps = &list
	}
	if inc["events"] {
		events, err := h.qs.QueryEvents(ctx, storage.EventQuery{
			Filter: &storage.QueryFilter{Field: "activity_id", Op: storage.OpEq, Value: id.String()},
			Limit:  maxEmbedded,
		})
		if err != nil {
			return nil, err
		}
		list := make([]wire.Event, 0, len(events.Items))
		for _, e := range events.Items {
			list = append(list, toEvent(e))
		}
		v.Events = &list
	}
	return v, nil
}

func (h *handlers) activitiesCount(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[wire.CountRequest](data)
	if err != nil {
		return nil, err
	}
	filter, err := toStorageFilter(req.Filter)
	if err != nil {
		return nil, err
	}
	n, exact, err := h.qs.CountActivities(ctx, filter, 100_000)
	if err != nil {
		return nil, err
	}
	return wire.CountResult{Count: n, Exact: exact}, nil
}

func parseTime(field, s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil, fieldError(wire.CodeInvalidArgument, field, "expected an RFC 3339 timestamp")
	}
	return &t, nil
}

func (h *handlers) activitiesAggregate(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[wire.AggregateRequest](data)
	if err != nil {
		return nil, err
	}
	filter, err := toStorageFilter(req.Filter)
	if err != nil {
		return nil, err
	}
	q := storage.AggregateQuery{Filter: filter, GroupBy: req.GroupBy, Limit: req.Limit}
	for _, m := range req.Metrics {
		switch m.Name {
		case wire.MetricCount:
			q.Count = true
		case wire.MetricDuration:
			q.Durations = append(q.Durations, storage.DurationMetric{Field: string(m.Field), Percentiles: m.Percentiles})
		default:
			return nil, fieldError(wire.CodeUnsupported, "metrics", "unknown metric %q", m.Name)
		}
	}
	if b := req.Bucket; b != nil {
		from, err := parseTime("bucket.from", b.From)
		if err != nil {
			return nil, err
		}
		to, err := parseTime("bucket.to", b.To)
		if err != nil {
			return nil, err
		}
		q.Bucket = &storage.AggregateBucket{Field: b.Field, Interval: time.Duration(b.IntervalMS) * time.Millisecond, From: from, To: to}
	}
	rows, err := h.qs.AggregateActivities(ctx, q)
	if err != nil {
		return nil, err
	}
	out := wire.AggregateResult{Groups: make([]wire.AggregateGroup, 0, len(rows.Rows)), Truncated: rows.Truncated}
	for _, r := range rows.Rows {
		g := wire.AggregateGroup{Key: r.Key, Bucket: tsp(r.Bucket), Durations: r.Durations}
		if q.Count {
			n := r.Count
			g.Count = &n
		}
		out.Groups = append(out.Groups, g)
	}
	return out, nil
}

func (h *handlers) stepsList(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[wire.StepsRequest](data)
	if err != nil {
		return nil, err
	}
	inc, err := h.includes(req.Include, []string{"result"})
	if err != nil {
		return nil, err
	}
	id, ok := parseID(req.ActivityID)
	if !ok {
		return wire.StepPage{Items: []wire.Step{}}, nil
	}
	res, err := h.qs.ListStepEntries(ctx, id, inc["result"], req.Limit, req.Cursor)
	if err != nil {
		return nil, err
	}
	out := wire.StepPage{Items: make([]wire.Step, 0, len(res.Items)), NextCursor: res.NextCursor}
	for _, s := range res.Items {
		out.Items = append(out.Items, toStep(s))
	}
	return out, nil
}

func (h *handlers) eventsList(ctx context.Context, data json.RawMessage) (any, error) {
	q, err := decode[wire.Query](data)
	if err != nil {
		return nil, err
	}
	filter, err := toStorageFilter(q.Filter)
	if err != nil {
		return nil, err
	}
	inc, err := h.includes(q.Include, []string{"detail"})
	if err != nil {
		return nil, err
	}
	eq := storage.EventQuery{Filter: filter, Limit: q.Limit, Cursor: q.Cursor, IncludeDetail: inc["detail"]}
	if sort, err := oneSort(q.Sort); err != nil {
		return nil, err
	} else if sort != nil {
		if sort.Field != "at" {
			return nil, fieldError(wire.CodeUnsupported, "sort", "events sort by at only")
		}
		eq.Desc = sort.Desc
	}
	res, err := h.qs.QueryEvents(ctx, eq)
	if err != nil {
		return nil, err
	}
	out := wire.EventPage{Items: make([]wire.Event, 0, len(res.Items)), NextCursor: res.NextCursor}
	for _, e := range res.Items {
		out.Items = append(out.Items, toEvent(e))
	}
	return out, nil
}

func (h *handlers) resultsGet(ctx context.Context, data json.RawMessage) (any, error) {
	if h.metadataOnly() {
		return nil, errorf(wire.CodeForbidden, "results are not sent in metadata-only mode")
	}
	req, err := decode[wire.ResultRequest](data)
	if err != nil {
		return nil, err
	}
	id, ok := parseID(req.ActivityID)
	if !ok {
		return nil, errorf(wire.CodeNotFound, "no result for activity %q", req.ActivityID)
	}
	res, err := h.backend.GetResult(ctx, id)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, errorf(wire.CodeNotFound, "no result for activity %q", req.ActivityID)
	}
	return toResult(res), nil
}

func (h *handlers) treesGet(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[wire.TreeRequest](data)
	if err != nil {
		return nil, err
	}
	inc, err := h.includes(req.Include, recordIncludes)
	if err != nil {
		return nil, err
	}
	id, ok := parseID(req.ID)
	if !ok {
		return nil, errorf(wire.CodeNotFound, "activity %q not found", req.ID)
	}
	tree, err := h.qs.GetActivityTree(ctx, id, recordInclude(inc), req.MaxNodes)
	if err != nil {
		return nil, err
	}
	return wire.Tree{RootID: tree.RootID.String(), Items: toActivities(tree.Items), Truncated: tree.Truncated}, nil
}

// state includes the in-flight list when withRunning (describe), not just
// its size (periodic reports).
func (h *handlers) state(withRunning bool) wire.ExecutorState {
	return stateOf(h.engine.Snapshot(), h.started, withRunning)
}

// stateOf counts uptime from started when the engine hasn't started.
func stateOf(snap executor.Snapshot, started time.Time, withRunning bool) wire.ExecutorState {
	if !snap.Info.StartedAt.IsZero() {
		started = snap.Info.StartedAt
	}
	c := snap.Counters
	st := wire.ExecutorState{
		ID:                snap.Info.ID,
		UptimeMS:          snap.At.Sub(started).Milliseconds(),
		MaxConcurrency:    snap.Info.MaxConcurrency,
		InFlight:          len(snap.State.Running),
		Draining:          snap.State.Draining,
		ClaimLagMS:        c.LastClaimLag.Milliseconds(),
		HeartbeatFailures: int64(c.HeartbeatFailures),
		Counters: wire.ExecutorCounters{
			Claimed: int64(c.Claimed), Succeeded: int64(c.Succeeded), Retried: int64(c.Retried), Failed: int64(c.Failed),
			TimedOut: int64(c.TimedOut), DeadLettered: int64(c.DeadLettered), ClaimsLost: int64(c.ClaimsLost),
		},
	}
	if withRunning {
		running := make([]wire.RunningActivity, 0, len(snap.State.Running))
		for _, a := range snap.State.Running {
			running = append(running, wire.RunningActivity{
				ActivityID: a.ID.String(), Type: a.Type, Attempt: a.Attempt, StartedAt: ts(a.StartedAt),
			})
		}
		st.Running = &running
	}
	return st
}

func (h *handlers) executorDescribe(_ context.Context, data json.RawMessage) (any, error) {
	if _, err := decode[wire.Empty](data); err != nil {
		return nil, err
	}
	return h.state(true), nil
}

func toWireError(err error) *wireError {
	var we *wireError
	if errors.As(err, &we) {
		return we
	}
	var se *storage.StorageError
	if errors.As(err, &se) {
		var e *wireError
		switch se.Kind {
		case storage.ErrInvalidArgument:
			e = errorf(wire.CodeInvalidArgument, "%s", se.Message)
		case storage.ErrUnsupported:
			e = errorf(wire.CodeUnsupported, "%s", se.Message)
		case storage.ErrNotFound:
			e = errorf(wire.CodeNotFound, "%s", se.Message)
		case storage.ErrConflict:
			e = errorf(wire.CodeConflict, "%s", se.Message)
		case storage.ErrUnavailable, storage.ErrTimeout:
			e = errorf(wire.CodeUnavailable, "%s", se.Message)
		default:
			e = errorf(wire.CodeInternal, "%s", errorMessage(err))
		}
		if se.Field != "" {
			e.Details = map[string]any{"field": se.Field}
		}
		return e
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errorf(wire.CodeDeadlineExceeded, "the request ran past its deadline")
	}
	return errorf(wire.CodeInternal, "%s", errorMessage(err))
}

// errorMessage keeps internal error text to one line on the wire.
func errorMessage(err error) string {
	msg := err.Error()
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	return msg
}

// plainJSON unwraps the "json" part of the TypeScript SDK's superjson-v1
// results, which is what the console reads; other data passes through.
func plainJSON(r *storage.ActivityResult) json.RawMessage {
	if r.Serialization != spec.SerializationSuperJSON {
		return r.Data
	}
	var envelope struct {
		JSON json.RawMessage `json:"json"`
	}
	if json.Unmarshal(r.Data, &envelope) != nil || envelope.JSON == nil {
		return r.Data
	}
	return envelope.JSON
}
