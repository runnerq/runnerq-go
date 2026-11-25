package runnerq

// Engine-level version pinning tests. Skipped unless RUNNERQ_TEST_DSN is set.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

func versionQueue() string {
	return "t_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
}

func TestVersionConfigValidation(t *testing.T) {
	b := scheduleTestBackend(t, versionQueue())
	handler := &funcHandler{fn: func(ActivityContext, json.RawMessage) (json.RawMessage, error) { return nil, nil }}
	for _, c := range []struct {
		name    string
		version string
		serves  []string
	}{
		{"serves without a version", "", []string{"A"}},
		{"version too long", strings.Repeat("v", 257), nil},
		{"empty served version", "B", []string{""}},
		{"too many served versions", "B", func() []string {
			out := make([]string, 64)
			for i := range out {
				out[i] = uuid.NewString()
			}
			return out
		}()},
	} {
		t.Run(c.name, func(t *testing.T) {
			e, _ := Builder().Backend(b).Version(c.version).Serves(c.serves...).Build()
			e.RegisterActivityWithName("t", handler)
			var we *WorkerError
			if err := e.Start(context.Background()); !errors.As(err, &we) || we.Kind != ErrConfiguration {
				t.Fatalf("Start = %v, want a configuration error", err)
			}
		})
	}
}

func TestServedVersions(t *testing.T) {
	c := WorkerConfig{Serves: []string{"A"}}
	if got := c.servedVersions(); got != nil {
		t.Fatalf("no version: served %v, want nil", got)
	}
	c = WorkerConfig{Version: "B", Serves: []string{"A", "B", "A", "C"}}
	if got := strings.Join(c.servedVersions(), ","); got != "B,A,C" {
		t.Fatalf("served %s, want B,A,C", got)
	}
}

// An engine with a version stamps its roots and serves only its own and
// unstamped work; a standalone client stamps with ClientVersion; children
// take their root's version.
func TestEnginePinsAndServesItsVersion(t *testing.T) {
	queue := versionQueue()
	b := scheduleTestBackend(t, queue)
	ran := make(chan string, 16)
	var mu sync.Mutex
	runs := map[uuid.UUID]string{}
	record := func(version string) ActivityHandler {
		return &funcHandler{fn: func(ctx ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
			mu.Lock()
			runs[ctx.ActivityID] = version
			mu.Unlock()
			ran <- version
			return nil, nil
		}}
	}
	parent := func() ActivityHandler {
		return &funcHandler{fn: func(ctx ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
			fut, err := ctx.ActivityExecutor.ActivityNamed("child").Payload(json.RawMessage(`{}`)).Step("child").Execute(ctx.Ctx)
			if err != nil {
				return nil, err
			}
			return fut.GetResult(ctx.Ctx)
		}}
	}

	clientA := NewClient(b, ClientVersion("A"))
	unstamped := NewClient(b)
	aRoot, err := clientA.ActivityNamed("work").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	plain, err := unstamped.ActivityNamed("work").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantVersion := func(id uuid.UUID, want string) {
		t.Helper()
		snap, err := b.GetActivity(context.Background(), id)
		if err != nil || snap == nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if got := versionOf(snap.CodeVersion); got != want {
			t.Fatalf("activity %s code_version %q, want %q", id, got, want)
		}
	}
	wantVersion(aRoot.ActivityID(), "A")
	wantVersion(plain.ActivityID(), "")

	start := func(version string) (*WorkerEngine, func()) {
		e, err := Builder().Backend(scheduleTestBackend(t, queue)).QueueName(queue).Version(version).ShutdownGrace(2 * time.Second).Build()
		if err != nil {
			t.Fatal(err)
		}
		e.RegisterActivityWithName("work", record(version))
		e.RegisterActivityWithName("parent", parent())
		e.RegisterActivityWithName("child", record(version))
		done := make(chan error, 1)
		go func() { done <- e.Start(context.Background()) }()
		return e, func() { e.Stop(); <-done }
	}

	eB, stopB := start("B")
	defer stopB()
	if got := <-ran; got != "B" {
		t.Fatalf("first run on %s, want the unstamped root on B", got)
	}
	select {
	case v := <-ran:
		t.Fatalf("a B engine ran something else (on %s): version A work leaked", v)
	case <-time.After(500 * time.Millisecond):
	}
	bRoot, err := eB.Client().ActivityNamed("parent").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantVersion(bRoot.ActivityID(), "B")
	if _, err := bRoot.GetResult(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-ran // the child, on B

	_, stopA := start("A")
	defer stopA()
	if got := <-ran; got != "A" {
		t.Fatalf("the A root ran on %s", got)
	}

	mu.Lock()
	defer mu.Unlock()
	if runs[aRoot.ActivityID()] != "A" || runs[plain.ActivityID()] != "B" {
		t.Fatalf("runs %v", runs)
	}
	for id, v := range runs {
		snap, _ := b.GetActivity(context.Background(), id)
		if want := versionOf(snap.CodeVersion); want != "" && want != v {
			t.Errorf("%s pinned to %s ran on %s", id, want, v)
		}
		if snap.ParentActivityID != nil && versionOf(snap.CodeVersion) != "B" {
			t.Errorf("child %s has version %q, want its root's B", id, versionOf(snap.CodeVersion))
		}
	}
}

func TestEngineVersionLabelAndSchedules(t *testing.T) {
	queue := versionQueue()
	b := scheduleTestBackend(t, queue)
	e, _ := Builder().Backend(b).QueueName(queue).Version("v7").Build()
	e.config.Labels = map[string]string{"region": "eu", LabelVersion: "ignored"}
	if got := e.Snapshot().Info.Labels; got["version"] != "v7" || got["region"] != "eu" {
		t.Fatalf("labels %v, want version v7 and region eu", got)
	}
	plain, _ := Builder().Backend(b).QueueName(queue).Build()
	if got := plain.Snapshot().Info.Labels; got != nil {
		t.Fatalf("unversioned labels %v, want none", got)
	}

	if err := e.Schedules().Upsert(context.Background(), Schedule{Name: "s", Cron: "@daily", Activity: "t"}); err != nil {
		t.Fatal(err)
	}
	got, err := b.GetSchedule(context.Background(), "s")
	if err != nil || got.Options.CodeVersion != "v7" {
		t.Fatalf("schedule %+v (%v), want CodeVersion v7", got, err)
	}
	m, _ := NewSchedules(b)
	if err := m.Upsert(context.Background(), Schedule{Name: "s", Cron: "@daily", Activity: "t"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := b.GetSchedule(context.Background(), "s"); got.Options.CodeVersion != "" {
		t.Fatalf("an unversioned upsert kept CodeVersion %q", got.Options.CodeVersion)
	}
}

func TestClientRepin(t *testing.T) {
	queue := versionQueue()
	b := scheduleTestBackend(t, queue)
	c := NewClient(b, ClientVersion("A"))
	fut, err := c.ActivityNamed("t").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Repin(context.Background(), IDs(fut.ActivityID()), ""); err == nil {
		t.Fatal("a repin without a version was accepted")
	}
	res, err := c.Repin(context.Background(), IDs(fut.ActivityID()), "B", WithCommandID("r1"), WithReason("compatible"))
	if err != nil {
		t.Fatal(err)
	}
	if it, _ := res.Item(fut.ActivityID()); it.Outcome != Applied || it.Status != StatusPending {
		t.Fatalf("repin item %+v", it)
	}
	again, err := c.Repin(context.Background(), IDs(fut.ActivityID()), "B", WithCommandID("r1"), WithReason("compatible"))
	if err != nil || !again.Replayed {
		t.Fatalf("replay: %+v %v", again, err)
	}
	if _, err := c.Repin(context.Background(), IDs(fut.ActivityID()), "C", WithCommandID("r1")); !IsCommandConflict(err) {
		t.Fatalf("reused id with another version: %v, want a conflict", err)
	}
	rec, err := c.Get(context.Background(), fut.ActivityID())
	if err != nil || rec.Version != "B" {
		t.Fatalf("record %+v (%v), want version B", rec, err)
	}
	page, err := c.List(context.Background(), ListOptions{Filter: &storage.QueryFilter{Field: "version", Op: storage.OpEq, Value: "B"}})
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("list by version: %+v %v", page, err)
	}
}

// The version gauges count unfinished work by version and runnable work this
// engine doesn't serve, and drop a version to 0 once it has drained.
func TestVersionGauges(t *testing.T) {
	queue := versionQueue()
	b := scheduleTestBackend(t, queue)
	sink := newLabeledSink()
	old := NewClient(b, ClientVersion("A"))
	fut, err := old.ActivityNamed("work").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	e, _ := Builder().Backend(b).QueueName(queue).Version("B").Build()
	e.SetLabeledMetrics(sink)
	e.config.GaugeInterval = 50 * time.Millisecond
	e.RegisterActivityWithName("work", &funcHandler{fn: func(ActivityContext, json.RawMessage) (json.RawMessage, error) { return nil, nil }})
	done := make(chan error, 1)
	go func() { done <- e.Start(context.Background()) }()
	defer func() { e.Stop(); <-done }()

	labels := func(v string) MetricLabels { return MetricLabels{Queue: queue, Version: v} }
	waitFor(t, "A counted and unserved", func() bool {
		byA, _ := sink.gauge(MetricActivitiesByVersion, labels("A"))
		unservedA, _ := sink.gauge(MetricActivitiesUnserved, labels("A"))
		unservedB, okB := sink.gauge(MetricActivitiesUnserved, labels("B"))
		return byA == 1 && unservedA == 1 && okB && unservedB == 0
	})
	if _, err := old.Repin(context.Background(), IDs(fut.ActivityID()), "B"); err != nil {
		t.Fatal(err)
	}
	if _, err := fut.GetResult(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "A drained to 0", func() bool {
		byA, okA := sink.gauge(MetricActivitiesByVersion, labels("A"))
		unservedA, okU := sink.gauge(MetricActivitiesUnserved, labels("A"))
		return okA && byA == 0 && okU && unservedA == 0
	})
}

func versionOf(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
