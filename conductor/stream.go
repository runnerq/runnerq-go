package conductor

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/conductor/internal/wire"
	"github.com/runnerq/runnerq-go/storage"
)

// Event streams tail the event log via QueryStorage and push stream.events
// batches. They resume after a cursor, so the Cloud can move a stream to
// another executor without loss.
//
// Ids grow in insertion order, but a late-committing transaction can surface
// an event below ids already sent, so each poll rescans the streamRescan ids
// below the cursor and sends only unsent ones. A resumed subscription treats
// that window as already sent, so failover doesn't replay it. Delivery is at
// least once in edge cases; consumers dedupe by event id.

const (
	maxSubscriptions   = 4
	defaultStreamBatch = 200
	maxStreamBatch     = 1000
	defaultStreamDelay = 500 * time.Millisecond
	minStreamDelay     = 50 * time.Millisecond
	streamRescan       = 256
)

// streams holds one session's subscriptions, which end with the session.
type streams struct {
	a    *Agent
	conn *websocket.Conn

	mu   sync.Mutex
	subs map[string]context.CancelFunc
	wg   sync.WaitGroup
}

func newStreams(a *Agent, conn *websocket.Conn) *streams {
	return &streams{a: a, conn: conn, subs: map[string]context.CancelFunc{}}
}

func (s *streams) close() {
	s.mu.Lock()
	for _, cancel := range s.subs {
		cancel()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *streams) subscribe(ctx context.Context, data json.RawMessage) (any, error) {
	qs := s.a.h.qs
	if qs == nil {
		return nil, errorf(wire.CodeUnsupported, "this agent's storage cannot stream events")
	}
	req, err := decode[wire.EventsSubscribe](data)
	if err != nil {
		return nil, err
	}
	filter, err := toStorageFilter(req.Filter)
	if err != nil {
		return nil, err
	}
	batch := req.MaxBatch
	if batch <= 0 {
		batch = defaultStreamBatch
	}
	batch = min(batch, maxStreamBatch)
	delay := time.Duration(req.MaxDelayMS) * time.Millisecond
	if delay <= 0 {
		delay = defaultStreamDelay
	}
	delay = max(delay, minStreamDelay)

	var cursor int64
	gap := false
	if req.AfterCursor != "" {
		c, err := strconv.ParseInt(req.AfterCursor, 10, 64)
		if err != nil || c < 0 {
			// Not a cursor this backend issued: start from the end and say so.
			gap = true
		} else {
			cursor = c
		}
	}
	if req.AfterCursor == "" || gap {
		// Validates the filter and finds the log's end in one query.
		last, err := qs.QueryEvents(ctx, storage.EventQuery{Filter: filter, Desc: true, Limit: 1})
		if err != nil {
			return nil, err
		}
		if len(last.Items) > 0 {
			cursor = last.Items[0].ID
		}
	} else if _, err := qs.QueryEvents(ctx, storage.EventQuery{Filter: filter, Limit: 1}); err != nil {
		return nil, err // an invalid filter fails the subscribe, not the stream
	}

	s.mu.Lock()
	if len(s.subs) >= maxSubscriptions {
		s.mu.Unlock()
		return nil, errorf(wire.CodeResourceExhausted, "at most %d event subscriptions per executor", maxSubscriptions)
	}
	id := "sub_" + uuid.NewString()
	sctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.subs[id] = cancel
	s.wg.Add(1)
	s.mu.Unlock()

	t := &tailer{s: s, id: id, filter: filter, batch: batch, delay: delay, cursor: cursor, sent: map[int64]bool{}}
	// The window below the start was already delivered (or predates the
	// subscription): only late commits into it are new.
	if seen, err := t.window(ctx, false); err == nil {
		for _, ev := range seen {
			t.sent[ev.ID] = true
		}
	}
	go func() {
		defer s.wg.Done()
		if gap {
			t.push(sctx, wire.TypeStreamGap, wire.StreamGap{SubscriptionID: id, SinceCursor: req.AfterCursor})
		}
		t.run(sctx)
	}()
	return wire.Subscription{SubscriptionID: id, Cursor: strconv.FormatInt(cursor, 10)}, nil
}

func (s *streams) unsubscribe(_ context.Context, data json.RawMessage) (any, error) {
	req, err := decode[wire.EventsUnsubscribe](data)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	cancel, ok := s.subs[req.SubscriptionID]
	delete(s.subs, req.SubscriptionID)
	s.mu.Unlock()
	if !ok {
		return nil, errorf(wire.CodeNotFound, "no subscription %q", req.SubscriptionID)
	}
	cancel()
	return wire.Empty{}, nil
}

type tailer struct {
	s      *streams
	id     string
	filter *storage.QueryFilter
	batch  int
	delay  time.Duration
	cursor int64
	sent   map[int64]bool // ids delivered within the rescan window
}

func (t *tailer) run(ctx context.Context) {
	defer func() {
		t.s.mu.Lock()
		delete(t.s.subs, t.id)
		t.s.mu.Unlock()
	}()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		full, err := t.poll(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Warn("Conductor event stream poll failed; retrying", "subscription", t.id, "error", err)
		}
		next := t.delay
		if full {
			next = 0 // catching up: keep reading
		}
		timer.Reset(next)
	}
}

// query reads events in (after, upTo] (upTo 0: unbounded), oldest first.
func (t *tailer) query(ctx context.Context, after, upTo int64, limit int, detail bool) ([]storage.EventRecord, error) {
	terms := []storage.QueryFilter{{Field: "seq", Op: storage.OpGt, Value: float64(after)}}
	if upTo > 0 {
		terms = append(terms, storage.QueryFilter{Field: "seq", Op: storage.OpLte, Value: float64(upTo)})
	}
	if t.filter != nil {
		terms = append(terms, *t.filter)
	}
	page, err := t.s.a.h.qs.QueryEvents(ctx, storage.EventQuery{
		Filter: &storage.QueryFilter{And: terms}, Limit: limit, IncludeDetail: detail && !t.s.a.h.metadataOnly(),
	})
	if err != nil {
		return nil, err
	}
	return page.Items, nil
}

func (t *tailer) window(ctx context.Context, detail bool) ([]storage.EventRecord, error) {
	if t.cursor <= 0 {
		return nil, nil
	}
	return t.query(ctx, max(t.cursor-streamRescan, 0), t.cursor, streamRescan, detail)
}

// poll sends late commits in the rescan window plus the next batch past the
// cursor, and reports whether that batch was full.
func (t *tailer) poll(ctx context.Context) (bool, error) {
	late, err := t.window(ctx, true)
	if err != nil {
		return false, err
	}
	fresh, err := t.query(ctx, t.cursor, 0, t.batch, true)
	if err != nil {
		return false, err
	}
	var pending []storage.EventRecord
	for _, ev := range append(late, fresh...) {
		if !t.sent[ev.ID] {
			pending = append(pending, ev)
		}
	}
	if err := t.send(ctx, pending); err != nil {
		return false, err
	}
	for id := range t.sent {
		if id <= t.cursor-streamRescan {
			delete(t.sent, id)
		}
	}
	return len(fresh) == t.batch, nil
}

// send splits events into frames within the Cloud's frame limit (an
// oversized frame would drop the session and the resumed stream would reread
// the same batch). Events count as sent, and move the cursor, only once their
// frame is written. An event too large for a frame alone loses its detail.
func (t *tailer) send(ctx context.Context, events []storage.EventRecord) error {
	budget := int(t.s.a.peerFrameLimit.Load()) - 1024 // the margin serve leaves too
	var (
		items []wire.Event
		ids   []int64
		size  int
	)
	flush := func() error {
		if len(items) == 0 {
			return nil
		}
		cursor := t.cursor
		for _, id := range ids {
			cursor = max(cursor, id)
		}
		if err := t.push(ctx, wire.TypeStreamEvents, wire.StreamEvents{SubscriptionID: t.id, Items: items, Cursor: strconv.FormatInt(cursor, 10)}); err != nil {
			return err
		}
		for _, id := range ids {
			t.sent[id] = true
		}
		t.cursor = cursor
		items, ids, size = nil, nil, 0
		return nil
	}
	for _, ev := range events {
		item := toEvent(ev)
		n := encodedSize(item)
		if budget > 0 && n > budget && item.Detail != nil {
			slog.Warn("Conductor event detail is over the frame limit; streaming the event without it",
				"subscription", t.id, "event", item.ID, "bytes", n, "limit", budget)
			item.Detail = nil
			n = encodedSize(item)
		}
		if budget > 0 && len(items) > 0 && size+n+1 > budget {
			if err := flush(); err != nil {
				return err
			}
		}
		items = append(items, item)
		ids = append(ids, ev.ID)
		size += n + 1
	}
	return flush()
}

func encodedSize(v any) int {
	b, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	return len(b)
}

// push doesn't write under the subscription's context: coder/websocket
// closes the whole connection when a write's context ends mid-write, so an
// unsubscribe during a push would drop the session. An ended subscription
// stops before writing; the write is bounded by writeTimeout.
func (t *tailer) push(ctx context.Context, msgType string, data any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	evt, err := newEvent(msgType, data)
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
	defer cancel()
	return wsjson.Write(wctx, t.s.conn, evt)
}
