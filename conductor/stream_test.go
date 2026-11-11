package conductor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/runnerq/runnerq-go/conductor/internal/wire"
	"github.com/runnerq/runnerq-go/storage"
)

// nextStream returns the next stream.events batch (or fails on a gap unless
// wantGap).
func (g *fakeGateway) nextBatch(t *testing.T) wire.StreamEvents {
	t.Helper()
	var b wire.StreamEvents
	if err := json.Unmarshal(g.waitEvent(wire.TypeStreamEvents).Data, &b); err != nil {
		t.Fatal(err)
	}
	return b
}

// collect gathers streamed events until want ids have all appeared.
func (g *fakeGateway) collect(t *testing.T, want ...string) (map[string]bool, string) {
	t.Helper()
	seen := map[string]bool{}
	cursor := ""
	deadline := time.Now().Add(15 * time.Second)
	for {
		missing := false
		for _, id := range want {
			missing = missing || !seen[id]
		}
		if !missing {
			return seen, cursor
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream never delivered %v (saw %v)", want, seen)
		}
		b := g.nextBatch(t)
		for _, ev := range b.Items {
			seen[ev.ID] = true
			seen[ev.ActivityID+"/"+ev.Type] = true
		}
		cursor = b.Cursor
	}
}

func queueFilter(queue string) *wire.Filter {
	return mustFilter(map[string]any{"field": "queue", "op": "eq", "value": queue})
}

func TestStreamDeliversNewEventsAndResumes(t *testing.T) {
	e, b, queue := pgEngine(t)
	g := newFakeGateway(t, wire.SessionConfig{})
	startAgent(t, e, g, Config{})
	h := g.waitHello()
	if _, ok := h.Capabilities[wire.TypeEventsSubscribe]; !ok {
		t.Fatal("events.subscribe not advertised")
	}

	before := signaled(t, b) // before the subscription: not streamed
	var sub wire.Subscription
	g.ok(wire.TypeEventsSubscribe, wire.EventsSubscribe{Filter: queueFilter(queue), MaxDelayMS: 50}, &sub)
	if sub.SubscriptionID == "" || sub.Cursor == "" {
		t.Fatalf("subscription %+v", sub)
	}
	first := signaled(t, b)
	seen, cursor := g.collect(t, first.String()+"/"+storage.RecordEventSignalReceived)
	if seen[before.String()+"/"+storage.RecordEventSignalReceived] {
		t.Fatal("an event from before the wire.Subscription was streamed")
	}
	g.ok(wire.TypeEventsUnsubscribe, wire.EventsUnsubscribe{SubscriptionID: sub.SubscriptionID}, nil)
	g.fails(wire.TypeEventsUnsubscribe, wire.EventsUnsubscribe{SubscriptionID: sub.SubscriptionID}, wire.CodeNotFound)

	// Events while nobody is subscribed are picked up by a wire.Subscription that
	// resumes after the last cursor; nothing already delivered is repeated.
	missed := signaled(t, b)
	g.ok(wire.TypeEventsSubscribe, wire.EventsSubscribe{Filter: queueFilter(queue), AfterCursor: cursor, MaxDelayMS: 50}, &sub)
	again, _ := g.collect(t, missed.String()+"/"+storage.RecordEventSignalReceived)
	if again[first.String()+"/"+storage.RecordEventSignalReceived] {
		t.Fatal("a resumed stream repeated an event before its cursor")
	}
}

func TestStreamCatchesLateCommits(t *testing.T) {
	e, b, queue := pgEngine(t)
	g := newFakeGateway(t, wire.SessionConfig{})
	startAgent(t, e, g, Config{})
	g.waitHello()
	var sub wire.Subscription
	g.ok(wire.TypeEventsSubscribe, wire.EventsSubscribe{Filter: queueFilter(queue), MaxDelayMS: 50}, &sub)

	// A transaction takes an event id, then commits after later events have
	// already been streamed past it.
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, os.Getenv("RUNNERQ_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lateActivity := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO runnerq_events (activity_id, queue_name, event_type, created_at)
		VALUES ($1, $2, 'Signaled', now())`, lateActivity, queue); err != nil {
		t.Fatal(err)
	}
	after := signaled(t, b)
	g.collect(t, after.String()+"/"+storage.RecordEventSignalReceived)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	g.collect(t, lateActivity.String()+"/"+storage.RecordEventSignalReceived)
}

func TestStreamGapLimitsAndFilters(t *testing.T) {
	e, b, queue := pgEngine(t)
	g := newFakeGateway(t, wire.SessionConfig{})
	startAgent(t, e, g, Config{})
	g.waitHello()

	var sub wire.Subscription
	g.ok(wire.TypeEventsSubscribe, wire.EventsSubscribe{Filter: queueFilter(queue), AfterCursor: "not-a-cursor", MaxDelayMS: 50}, &sub)
	var gap wire.StreamGap
	if err := json.Unmarshal(g.waitEvent(wire.TypeStreamGap).Data, &gap); err != nil || gap.SubscriptionID != sub.SubscriptionID {
		t.Fatalf("gap %+v %v", gap, err)
	}
	if _, err := strconv.ParseInt(sub.Cursor, 10, 64); err != nil {
		t.Fatalf("subscription after a bad cursor starts at %q", sub.Cursor)
	}

	g.fails(wire.TypeEventsSubscribe, wire.EventsSubscribe{Filter: mustFilter(map[string]any{"field": "colour", "op": "eq", "value": "x"})}, wire.CodeUnsupported)
	for range maxSubscriptions - 1 {
		g.ok(wire.TypeEventsSubscribe, wire.EventsSubscribe{Filter: queueFilter(queue)}, nil)
	}
	g.fails(wire.TypeEventsSubscribe, wire.EventsSubscribe{Filter: queueFilter(queue)}, wire.CodeResourceExhausted)

	// Filters apply to the stream: only this queue's signal events, read
	// from the start of the log.
	g.ok(wire.TypeEventsUnsubscribe, wire.EventsUnsubscribe{SubscriptionID: sub.SubscriptionID}, nil)
	id := signaled(t, b)
	g.ok(wire.TypeEventsSubscribe, wire.EventsSubscribe{AfterCursor: "0", MaxDelayMS: 50,
		Filter: mustFilter(inQueue(queue, map[string]any{"field": "type", "op": "eq", "value": storage.RecordEventSignalReceived}))}, nil)
	for {
		batch := g.nextBatch(t)
		done := false
		for _, ev := range batch.Items {
			if ev.Type != storage.RecordEventSignalReceived {
				t.Fatalf("filtered stream delivered %s", ev.Type)
			}
			done = done || ev.ActivityID == id.String()
		}
		if done {
			break
		}
	}
}

// A catch-up batch bigger than the Cloud's frame limit goes out as several
// frames, each under the limit, with cursors that only move forward; an event
// too large for any frame goes without its detail rather than jamming the
// stream.
func TestStreamSplitsBatchesToTheFrameLimit(t *testing.T) {
	e, _, queue := pgEngine(t)
	g := newFakeGateway(t, wire.SessionConfig{})
	g.frame = 16 << 10
	startAgent(t, e, g, Config{})
	g.waitHello()
	var sub wire.Subscription
	g.ok(wire.TypeEventsSubscribe, wire.EventsSubscribe{Filter: queueFilter(queue), MaxDelayMS: 50}, &sub)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, os.Getenv("RUNNERQ_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	want := map[string]int{} // activity id -> detail bytes
	for i := range 13 {
		size := 3000
		if i == 12 {
			size = 40 << 10 // over the whole frame limit on its own
		}
		id := uuid.New()
		want[id.String()] = size
		if _, err := conn.Exec(ctx, `INSERT INTO runnerq_events (activity_id, queue_name, event_type, detail, created_at)
			VALUES ($1, $2, 'Signaled', jsonb_build_object('blob', repeat('x', $3::int)), now())`, id, queue, size); err != nil {
			t.Fatal(err)
		}
	}

	seen := map[string]int{}
	frames, last := 0, int64(0)
	deadline := time.Now().Add(15 * time.Second)
	for len(seen) < len(want) {
		if time.Now().After(deadline) {
			t.Fatalf("stream delivered %d of %d events", len(seen), len(want))
		}
		env := g.waitEvent(wire.TypeStreamEvents)
		raw, _ := json.Marshal(env)
		if len(raw) > g.frame {
			t.Fatalf("a %d-byte frame is over the %d-byte limit", len(raw), g.frame)
		}
		var b wire.StreamEvents
		if err := json.Unmarshal(env.Data, &b); err != nil {
			t.Fatal(err)
		}
		cursor, err := strconv.ParseInt(b.Cursor, 10, 64)
		if err != nil || cursor < last {
			t.Fatalf("cursor went from %d to %q", last, b.Cursor)
		}
		last = cursor
		frames++
		for _, ev := range b.Items {
			if _, ok := want[ev.ActivityID]; ok {
				seen[ev.ActivityID]++
				if want[ev.ActivityID] > 16<<10 && ev.Detail != nil {
					t.Fatal("the oversized event kept its detail")
				}
				if want[ev.ActivityID] <= 16<<10 && ev.Detail == nil {
					t.Fatal("an event that fits lost its detail")
				}
			}
		}
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("event for %s delivered %d times", id, n)
		}
	}
	if frames < 3 {
		t.Fatalf("36KB of events arrived in %d frames under a 16KB limit", frames)
	}
}

// A wire.Subscription can end while its tailer is writing a frame (the Cloud
// unsubscribes when the last viewer leaves). That must not take the session
// down: coder/websocket closes the whole connection when a write's context
// ends mid-write, so pushes don't write under the subscription's context.
func TestStreamUnsubscribeMidWriteKeepsTheConnection(t *testing.T) {
	read := make(chan struct{})
	got := make(chan string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(64 << 20)
		<-read // hold the push mid-write until the wire.Subscription has ended
		for {
			var env wire.Envelope
			if err := wsjson.Read(context.Background(), c, &env); err != nil {
				return
			}
			got <- env.Type
		}
	}))
	defer srv.Close()
	conn, _, err := websocket.Dial(context.Background(), srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()

	tl := &tailer{s: &streams{conn: conn}, id: "sub_test"}
	ctx, cancel := context.WithCancel(context.Background())
	pushed := make(chan error, 1)
	go func() {
		// Far more than the socket buffers hold, so the write blocks.
		pushed <- tl.push(ctx, wire.TypeStreamEvents, map[string]string{"blob": strings.Repeat("x", 32<<20)})
	}()
	time.Sleep(300 * time.Millisecond)
	cancel() // the Cloud unsubscribed while the frame was being written
	time.Sleep(100 * time.Millisecond)
	close(read)

	select {
	case err := <-pushed:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("push: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("push never returned")
	}
	evt, _ := newEvent("after", struct{}{})
	wctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	if err := wsjson.Write(wctx, conn, evt); err != nil {
		t.Fatalf("the connection died with the subscription: %v", err)
	}
	for {
		select {
		case typ := <-got:
			if typ == "after" {
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatal("nothing arrived after the wire.Subscription ended")
		}
	}
}
