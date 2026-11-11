package conductor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/conductor/internal/wire"
	"github.com/runnerq/runnerq-go/executor"
)

// collectNotices gathers activity.notices items until every want
// ("<activity id>/<type>") arrived.
func (g *fakeGateway) collectNotices(t *testing.T, want ...string) map[string]wire.Notice {
	t.Helper()
	seen := map[string]wire.Notice{}
	for {
		missing := false
		for _, w := range want {
			if _, ok := seen[w]; !ok {
				missing = true
			}
		}
		if !missing {
			return seen
		}
		var batch wire.ActivityNotices
		if err := json.Unmarshal(g.waitEvent(wire.TypeActivityNotices).Data, &batch); err != nil {
			t.Fatal(err)
		}
		for _, n := range batch.Items {
			seen[n.ActivityID+"/"+string(n.Type)] = n
		}
	}
}

// While the Cloud asks for notices, the agent announces what its engine
// submits, claims and completes; once it stops asking, nothing is sent.
func TestNoticesFollowTheCloud(t *testing.T) {
	e, _, queue := pgEngine(t)
	on, off := true, false
	g := newFakeGateway(t, wire.SessionConfig{Notices: &on})
	startAgent(t, e, g, Config{})
	h := g.waitHello()
	if _, ok := h.Capabilities[wire.TypeActivityNotices]; !ok {
		t.Fatal("activity.notices not advertised")
	}
	runEngine(t, e)

	fut, err := e.GetActivityExecutor().Activity[Echo]().Payload(json.RawMessage(`{"n":1}`)).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id := fut.ActivityID().String()
	seen := g.collectNotices(t, id+"/activity.created", id+"/attempt.started", id+"/attempt.succeeded")
	started := seen[id+"/attempt.started"]
	if started.Queue != queue || started.ActivityType != "Echo" || started.RootID != id || started.Attempt != 1 ||
		started.ExecutorID != h.Executor.ID || seen[id+"/activity.created"].Attempt != 0 {
		t.Fatalf("notices %+v", seen)
	}

	g.send(wire.TypeConfigUpdate, wire.SessionConfig{Notices: &off})
	time.Sleep(100 * time.Millisecond) // the update is applied before the next submission
	later, err := e.GetActivityExecutor().Activity[Echo]().Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := later.GetResult(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		select {
		case env := <-g.events:
			if env.Type == wire.TypeActivityNotices {
				t.Fatalf("notices after the Cloud turned them off: %s", env.Data)
			}
		case <-deadline:
			return
		}
	}
}

// Past its buffer the oldest notices go, and the next batch says how many.
func TestNoticesDropTheOldest(t *testing.T) {
	n := &notices{queue: "q", executor: "exec-1"}
	first := uuid.New()
	n.Announce(executor.Change{Kind: executor.Created, ActivityID: first, At: time.Now()})
	for range noticeBuffer {
		n.Announce(executor.Change{Kind: executor.AttemptStarted, ActivityID: uuid.New(), Attempt: 1, At: time.Now()})
	}
	items, dropped := n.take()
	if len(items) != noticeBuffer || dropped != 1 || items[0].ActivityID == first.String() {
		t.Fatalf("%d items, %d dropped", len(items), dropped)
	}
	if items, dropped := n.take(); len(items) != 0 || dropped != 0 {
		t.Fatalf("taken twice: %d, %d", len(items), dropped)
	}
}
