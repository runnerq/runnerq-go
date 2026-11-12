package runnerq

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/executor"
	"github.com/alob-mtc/runnerq-go/storage"
)

// observingBackend is a lifecycle backend that is an executor.Observer.
type observingBackend struct {
	*lifecycleBackend
	observer
}

// observer records the executors it's told about.
type observer struct {
	mu      sync.Mutex
	started []executor.Source
	stopped chan string
}

func newObserver() *observer { return &observer{stopped: make(chan string, 1)} }

func (o *observer) ExecutorStarted(src executor.Source) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.started = append(o.started, src)
}

func (o *observer) ExecutorStopped(id string) { o.stopped <- id }

func (o *observer) sources() []executor.Source {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.started
}

func TestEngineSnapshotAndObservers(t *testing.T) {
	b := &observingBackend{lifecycleBackend: newLifecycleBackend(), observer: observer{stopped: make(chan string, 1)}}
	cfg := DefaultWorkerConfig()
	cfg.QueueName = "payments"
	cfg.MaxConcurrentActivities = 3
	cfg.Labels = map[string]string{"region": "eu"}
	e := NewWorkerEngineWithBackend(b, cfg)
	cfg.Labels["region"] = "changed after construction"
	sink := &lockedMetrics{counts: recoveryMetrics{}}
	e.SetMetrics(sink)
	extra := newObserver()
	e.Observe(extra)

	entered, release := make(chan struct{}), make(chan struct{})
	e.RegisterActivityWithName("charge", &funcHandler{fn: func(ActivityContext, json.RawMessage) (json.RawMessage, error) {
		close(entered)
		<-release
		return json.RawMessage(`true`), nil
	}})
	e.RegisterActivityWithName("refund", &funcHandler{})

	before := e.Snapshot()
	if before.Info.ID != e.InstanceID() || !before.Info.StartedAt.IsZero() || before.Counters != (executor.Counters{}) {
		t.Fatalf("snapshot before start: %+v", before)
	}

	id := uuid.New()
	due := time.Now().UTC().Add(-2 * time.Second)
	b.claims <- storage.QueuedActivity{ID: id, ActivityType: "charge", TimeoutSeconds: 30, CreatedAt: due.Add(-time.Hour), ScheduledAt: &due}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Start(ctx) }()
	receive(t, entered)

	if srcs := extra.sources(); len(srcs) != 1 || srcs[0] != executor.Source(e) {
		t.Fatalf("observer told about %v", srcs)
	}
	// A backend that is an Observer is told only when observed like any other.
	if srcs := b.observer.sources(); len(srcs) != 0 {
		t.Fatalf("an unobserved backend was told about %v", srcs)
	}
	snap := e.Snapshot()
	info := snap.Info
	if info.ID != e.InstanceID() || info.Queue != "payments" || info.MaxConcurrency != 3 || info.StartedAt.IsZero() ||
		len(info.ActivityTypes) != 2 || info.ActivityTypes[0] != "charge" || info.Labels["region"] != "eu" ||
		info.Hostname == "" || info.SDK.Name != "runnerq-go" || info.SDK.Language != "go" {
		t.Fatalf("info: %+v", info)
	}
	if st := snap.State; st.Draining || len(st.Running) != 1 || st.Running[0].ID != id || st.Running[0].Type != "charge" || st.Running[0].Attempt != 1 {
		t.Fatalf("state while running: %+v", st)
	}
	if c := snap.Counters; c.Claimed != 1 || c.Succeeded != 0 || c.LastClaimLag < 2*time.Second || c.LastClaimLag > time.Minute {
		t.Fatalf("counters while running: %+v", c)
	}

	cancel()
	deadline := time.Now().Add(4 * time.Second)
	for !e.Snapshot().State.Draining {
		if time.Now().After(deadline) {
			t.Fatal("not draining after the stop began")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	receive(t, b.ack)
	if got := receive(t, extra.stopped); got != e.InstanceID() {
		t.Fatalf("stopped %q", got)
	}
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
	if c := e.Snapshot().Counters; c.Claimed != 1 || c.Succeeded != 1 {
		t.Fatalf("counters after: %+v", c)
	}
	// The configured sink still sees every metric.
	if sink.get(metricStarted) != 1 || sink.get("activity_completed") != 1 {
		t.Fatalf("sink: %v", sink.counts)
	}
}

// lockedMetrics is a sink the engine's goroutines can share.
type lockedMetrics struct {
	mu     sync.Mutex
	counts recoveryMetrics
}

func (m *lockedMetrics) IncCounter(k string, n uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts.IncCounter(k, n)
}
func (m *lockedMetrics) ObserveDuration(string, time.Duration) {}
func (m *lockedMetrics) get(k string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[k]
}

func TestCountingMetrics(t *testing.T) {
	sink := recoveryMetrics{}
	m := newCountingMetrics(sink)
	for name, n := range map[string]uint64{
		metricStarted: 7, "activity_completed": 3, "activity_retry": 2, "activity_failed_non_retry": 1,
		"activity_timeout": 1, metricDeadLettered: 1, "activity_claim_lost": 1, "activity_heartbeat_failed": 4,
		"storage_retry": 9,
	} {
		m.IncCounter(name, n)
	}
	m.ObserveDuration(metricClaimLag, time.Second)
	m.ObserveDuration(metricClaimLag, 3*time.Millisecond)
	want := executor.Counters{
		Claimed: 7, Succeeded: 3, Retried: 2, Failed: 1, TimedOut: 1, DeadLettered: 1,
		ClaimsLost: 1, HeartbeatFailures: 4, LastClaimLag: 3 * time.Millisecond,
	}
	if got := m.counters(); got != want {
		t.Fatalf("counters %+v, want %+v", got, want)
	}
	if sink["storage_retry"] != 9 {
		t.Fatalf("sink: %v", sink)
	}
}

func TestClaimLag(t *testing.T) {
	now := time.Now()
	later := now.Add(time.Minute)
	for _, tc := range []struct {
		act  activity
		want time.Duration
	}{
		{activity{CreatedAt: now.Add(-5 * time.Second)}, 5 * time.Second},
		{activity{CreatedAt: now.Add(-time.Hour), ScheduledAt: ptr(now.Add(-time.Second))}, time.Second},
		{activity{CreatedAt: now, ScheduledAt: &later}, 0},
	} {
		if got := claimLag(&tc.act, now); got != tc.want {
			t.Errorf("claimLag(%+v) = %v, want %v", tc.act, got, tc.want)
		}
	}
}

func ptr[T any](v T) *T { return &v }

func TestEngineSignalsChanges(t *testing.T) {
	b := newLifecycleBackend()
	cfg := DefaultWorkerConfig()
	cfg.MaxConcurrentActivities = 1
	e := NewWorkerEngineWithBackend(b, cfg)
	var _ executor.Notifier = e
	entered, release := make(chan struct{}), make(chan struct{})
	e.RegisterActivityWithName("charge", &funcHandler{fn: func(ActivityContext, json.RawMessage) (json.RawMessage, error) {
		close(entered)
		<-release
		return json.RawMessage(`true`), nil
	}})
	changed := func(ch <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(4 * time.Second):
			t.Fatalf("no change signalled when %s", what)
		}
	}

	started := e.Changed()
	b.claims <- storage.QueuedActivity{ID: uuid.New(), ActivityType: "charge", TimeoutSeconds: 30}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Start(ctx) }()
	receive(t, entered)
	changed(started, "an activity started")

	finished := e.Changed()
	close(release)
	receive(t, b.ack)
	changed(finished, "an activity finished")

	draining := e.Changed()
	cancel()
	changed(draining, "the drain began")
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
}
