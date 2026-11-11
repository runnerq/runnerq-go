package runnerq

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/internal/spectest"
	"github.com/runnerq/runnerq-go/storage"
)

func TestSpecTraceparent(t *testing.T) {
	type in struct {
		Explicit  *string `json:"explicit"`
		Active    *string `json:"active"`
		Inherited *string `json:"inherited"`
	}
	deref := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}
	for _, c := range spectest.Load[in, *string](t, "vectors/traceparent.json") {
		got, ok := storedTraceparent(c.Input.Explicit, deref(c.Input.Active), deref(c.Input.Inherited))
		if want := c.Output; ok != (want != nil) || (ok && got != *want) {
			t.Errorf("%s: got %q (%v), want %v", c.Name, got, ok, deref(want))
		}
	}
}

func TestUnlabeledMetricsAdapter(t *testing.T) {
	sink := &lockedMetrics{counts: recoveryMetrics{}}
	var observed []time.Duration
	l := UnlabeledMetrics(durationSink{sink, &observed})
	l.AddCounter(MetricActivityStarted, 2, MetricLabels{Queue: "q", ActivityType: "t"})
	l.RecordHistogram(MetricActivityClaimLag, 1.5, MetricLabels{})
	l.SetGauge(MetricQueueDepth, 3, MetricLabels{})
	if sink.get(MetricActivityStarted) != 2 || !slices.Equal(observed, []time.Duration{1500 * time.Millisecond}) {
		t.Fatalf("counts %v, durations %v", sink.counts, observed)
	}
	if _, ok := UnlabeledMetrics(nil).(NoopMetrics); !ok {
		t.Fatal("nil sink is not a no-op")
	}
}

type durationSink struct {
	*lockedMetrics
	observed *[]time.Duration
}

func (d durationSink) ObserveDuration(_ string, v time.Duration) {
	*d.observed = append(*d.observed, v)
}

// labeledSink records every observation with its labels.
type labeledSink struct {
	mu       sync.Mutex
	counters map[string]map[MetricLabels]uint64
	hist     map[string][]MetricLabels
	gauges   map[string]map[MetricLabels]float64
}

func newLabeledSink() *labeledSink {
	return &labeledSink{counters: map[string]map[MetricLabels]uint64{}, hist: map[string][]MetricLabels{}, gauges: map[string]map[MetricLabels]float64{}}
}

func (s *labeledSink) AddCounter(name string, v uint64, l MetricLabels) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.counters[name] == nil {
		s.counters[name] = map[MetricLabels]uint64{}
	}
	s.counters[name][l] += v
}

func (s *labeledSink) RecordHistogram(name string, _ float64, l MetricLabels) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hist[name] = append(s.hist[name], l)
}

func (s *labeledSink) SetGauge(name string, v float64, l MetricLabels) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gauges[name] == nil {
		s.gauges[name] = map[MetricLabels]float64{}
	}
	s.gauges[name][l] = v
}

func (s *labeledSink) gauge(name string, l MetricLabels) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.gauges[name][l]
	return v, ok
}

func (s *labeledSink) counter(name string, l MetricLabels) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counters[name][l]
}

func (s *labeledSink) histograms(name string) []MetricLabels {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.hist[name])
}

func TestLabeledMetricsAndGauges(t *testing.T) {
	sink := newLabeledSink()
	release := make(chan struct{})
	var queue string
	rig := newStepsRig(t, func(e *WorkerEngine) {
		queue = e.QueueName()
		e.SetLabeledMetrics(sink)
		e.config.GaugeInterval = 50 * time.Millisecond
		e.RegisterActivityWithName("work", &funcHandler{fn: func(ActivityContext, json.RawMessage) (json.RawMessage, error) {
			<-release
			return json.RawMessage(`1`), nil
		}})
		e.RegisterActivityWithName("idle", &funcHandler{fn: func(ActivityContext, json.RawMessage) (json.RawMessage, error) {
			return nil, nil
		}})
	})
	exec := rig.engine.GetActivityExecutor()
	fut, err := exec.ActivityNamed("work").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := exec.ActivityNamed("work").Payload(json.RawMessage(`{}`)).Delay(time.Hour).Execute(context.Background()); err != nil {
		t.Fatal(err)
	}

	work := MetricLabels{Queue: queue, ActivityType: "work"}
	waitFor(t, "running and scheduled gauges", func() bool {
		inFlight, _ := sink.gauge(MetricActivitiesInFlight, work)
		idle, idleSet := sink.gauge(MetricActivitiesInFlight, MetricLabels{Queue: queue, ActivityType: "idle"})
		running, _ := sink.gauge(MetricQueueDepth, MetricLabels{Queue: queue, ActivityType: "work", Status: storage.RecordStatusRunning})
		scheduled, _ := sink.gauge(MetricQueueDepth, MetricLabels{Queue: queue, ActivityType: "work", Status: storage.RecordStatusScheduled})
		_, ageSet := sink.gauge(MetricQueueOldestPendingAge, MetricLabels{Queue: queue})
		return inFlight == 1 && idleSet && idle == 0 && running == 1 && scheduled == 1 && ageSet
	})
	close(release)
	rig.await(t, fut.activityID, 10*time.Second)
	waitFor(t, "an emptied status drops to 0", func() bool {
		running, ok := sink.gauge(MetricQueueDepth, MetricLabels{Queue: queue, ActivityType: "work", Status: storage.RecordStatusRunning})
		return ok && running == 0 && sink.counter(MetricActivityCompleted, work) == 1
	})
	if sink.counter(MetricActivityStarted, work) != 1 {
		t.Fatalf("started: %v", sink.counters)
	}
	for _, name := range []string{MetricActivityClaimLag, MetricActivityExecution, MetricActivityCompletionPersistence} {
		if got := sink.histograms(name); len(got) != 1 || got[0] != work {
			t.Errorf("%s labels %v, want [%v]", name, got, work)
		}
	}
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// fakeTracer records spans; a span's traceparent is its trace id and a
// counter-made span id.
type fakeTracer struct {
	mu    sync.Mutex
	next  atomic.Uint64
	spans []*fakeSpan
}

type fakeSpan struct {
	name, parent, traceparent, outcome string
	attrs                              map[string]any
	err                                error
	ended                              bool
}

type fakeSpanKey struct{}

func (f *fakeTracer) Start(ctx context.Context, name, parent string, attrs ...TraceAttribute) (context.Context, EndSpan) {
	if parent == "" {
		parent = f.Traceparent(ctx)
	}
	trace := fmt.Sprintf("%032x", f.next.Add(1)<<32)
	if parent != "" {
		trace = parent[3:35]
	}
	s := &fakeSpan{name: name, parent: parent, attrs: map[string]any{},
		traceparent: fmt.Sprintf("00-%s-%016x-01", trace, f.next.Add(1))}
	for _, a := range attrs {
		s.attrs[a.Key] = a.Value
	}
	f.mu.Lock()
	f.spans = append(f.spans, s)
	f.mu.Unlock()
	return context.WithValue(ctx, fakeSpanKey{}, s), func(outcome string, err error, attrs ...TraceAttribute) {
		f.mu.Lock()
		defer f.mu.Unlock()
		s.outcome, s.err, s.ended = outcome, err, true
		for _, a := range attrs {
			s.attrs[a.Key] = a.Value
		}
	}
}

func (f *fakeTracer) Traceparent(ctx context.Context) string {
	if s, ok := ctx.Value(fakeSpanKey{}).(*fakeSpan); ok {
		return s.traceparent
	}
	return ""
}

func (f *fakeTracer) named(name string) []fakeSpan {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fakeSpan
	for _, s := range f.spans {
		if s.name == name {
			out = append(out, *s)
		}
	}
	return out
}

// The core's spans form one tree: enqueue → attempts (a retry, a park and a
// resume) → steps, sleep and the child's enqueue → the child's attempt.
func TestTracerSpans(t *testing.T) {
	tracer := &fakeTracer{}
	var runs atomic.Int32
	rig := newStepsRig(t, func(e *WorkerEngine) {
		e.SetTracer(tracer)
		e.RegisterActivityWithName("parent", &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
			if _, err := ctx.RunStep("first", func(context.Context) (int, error) { return 1, nil }); err != nil {
				return nil, err
			}
			if runs.Add(1) == 1 {
				return nil, NewRetryError("once")
			}
			if err := ctx.Sleep("nap", 2*time.Second); err != nil {
				return nil, err
			}
			fut, err := ctx.ActivityExecutor.ActivityNamed("child").Step("child").Payload(json.RawMessage(`{}`)).Execute(ctx.Ctx)
			if err != nil {
				return nil, err
			}
			return fut.GetResult(ctx.Ctx)
		}})
		e.RegisterActivityWithName("child", &funcHandler{fn: func(ActivityContext, json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`"ok"`), nil
		}})
	})
	fut, err := rig.engine.GetActivityExecutor().ActivityNamed("parent").Payload(json.RawMessage(`{}`)).
		MaxRetries(3).Timeout(3 * time.Second).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res := rig.await(t, fut.activityID, 30*time.Second); res.State != storage.ResultOk {
		t.Fatalf("result %s", res.Data)
	}

	enqueues := tracer.named(SpanEnqueue)
	// The child's enqueue repeats as "existing" when the parent replays after
	// parking on the child's result.
	if len(enqueues) < 2 || enqueues[0].parent != "" || enqueues[0].outcome != OutcomeEnqueued || enqueues[1].outcome != OutcomeEnqueued {
		t.Fatalf("enqueues %+v", enqueues)
	}
	root := enqueues[0]
	snap, err := rig.backend.GetActivity(context.Background(), fut.activityID)
	if err != nil || snap.Metadata[MetadataTraceparent] != root.traceparent {
		t.Fatalf("stored traceparent %v, enqueue span %s (%v)", snap.Metadata, root.traceparent, err)
	}
	var parentOutcomes []string
	for _, a := range tracer.named(SpanAttempt) {
		if a.parent[3:35] != root.traceparent[3:35] {
			t.Errorf("attempt %+v is outside the root's trace", a)
		}
		if a.attrs[AttrActivityType] == "parent" {
			if a.parent != root.traceparent {
				t.Errorf("parent attempt's parent %s, want the enqueue span", a.parent)
			}
			parentOutcomes = append(parentOutcomes, a.outcome)
		}
	}
	if len(parentOutcomes) < 3 || parentOutcomes[0] != OutcomeRetry || parentOutcomes[1] != OutcomeYielded || parentOutcomes[len(parentOutcomes)-1] != OutcomeCompleted {
		t.Errorf("parent attempt outcomes %v, want retry, yielded (sleep), …, completed", parentOutcomes)
	}
	if steps := tracer.named(SpanStep); len(steps) != 1 || steps[0].outcome != OutcomeOk {
		t.Errorf("steps %+v (a replayed step starts none)", steps)
	}
	sleeps := tracer.named(SpanSleep)
	if len(sleeps) < 2 || sleeps[0].outcome != OutcomeParked || sleeps[len(sleeps)-1].outcome != OutcomeElapsed {
		t.Errorf("sleeps %+v", sleeps)
	}
	if enqueues[1].parent[3:35] != root.traceparent[3:35] || enqueues[1].attrs[AttrParentID] != fut.activityID.String() {
		t.Errorf("child enqueue %+v", enqueues[1])
	}
}

// Without a tracer a child stores its parent's traceparent unchanged, and a
// root with none stores none.
func TestTraceparentInheritedWithoutTracer(t *testing.T) {
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	childID := make(chan uuid.UUID, 1)
	rig := newStepsRig(t, func(e *WorkerEngine) {
		e.RegisterActivityWithName("parent", &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
			fut, err := ctx.ActivityExecutor.ActivityNamed("child").Step("c").Payload(json.RawMessage(`{}`)).Execute(ctx.Ctx)
			if err != nil {
				return nil, err
			}
			select {
			case childID <- fut.ActivityID():
			default:
			}
			return nil, nil
		}})
		e.RegisterActivityWithName("child", &funcHandler{fn: func(ActivityContext, json.RawMessage) (json.RawMessage, error) {
			return nil, nil
		}})
	})
	exec := rig.engine.GetActivityExecutor()
	plain, err := exec.ActivityNamed("child").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	fut, err := exec.ActivityNamed("parent").Payload(json.RawMessage(`{}`)).Metadata(MetadataTraceparent, tp).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rig.await(t, fut.activityID, 10*time.Second)
	child, err := rig.backend.GetActivity(context.Background(), <-childID)
	if err != nil || child.Metadata[MetadataTraceparent] != tp {
		t.Fatalf("child metadata %v (%v)", child.Metadata, err)
	}
	if snap, err := rig.backend.GetActivity(context.Background(), plain.activityID); err != nil || snap.Metadata[MetadataTraceparent] != "" {
		t.Fatalf("root without trace context stored %v (%v)", snap.Metadata, err)
	}
}
