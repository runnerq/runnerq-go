package conductor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/runnerq/runnerq-spec/schemacheck"

	"github.com/runnerq/runnerq-go"
	"github.com/runnerq/runnerq-go/conductor/internal/wire"
	"github.com/runnerq/runnerq-go/internal/spec"
	"github.com/runnerq/runnerq-go/internal/spectest"
	"github.com/runnerq/runnerq-go/storage"
)

// protocolSchema is the spec's conductor schema and its messages by type.
type protocolSchema struct {
	*schemacheck.Schema
	messages []specMessage
	byType   map[string]specMessage
}

type specMessage struct {
	Type     string `json:"type"`
	Kind     string `json:"kind"`
	From     string `json:"from"`
	Data     string `json:"data"`
	Response string `json:"response"`
}

var (
	schemaOnce sync.Once
	schemaDoc  *protocolSchema
	schemaErr  error
)

func loadSchema(t testing.TB) *protocolSchema {
	t.Helper()
	schemaOnce.Do(func() {
		s, err := schemacheck.Load(spectest.Path("protocol/conductor/conductor.schema.json"))
		if err != nil {
			schemaErr = fmt.Errorf("%w (is the spec submodule checked out? git submodule update --init)", err)
			return
		}
		raw, err := json.Marshal(s.Doc["x-messages"])
		if err != nil {
			schemaErr = err
			return
		}
		p := &protocolSchema{Schema: s, byType: map[string]specMessage{}}
		if schemaErr = json.Unmarshal(raw, &p.messages); schemaErr != nil {
			return
		}
		for i, m := range p.messages {
			m.Data, m.Response = strings.TrimPrefix(m.Data, "#/$defs/"), strings.TrimPrefix(m.Response, "#/$defs/")
			p.messages[i] = m
			p.byType[m.Type] = m
		}
		schemaDoc = p
	})
	if schemaErr != nil {
		t.Fatal(schemaErr)
	}
	return schemaDoc
}

// checkFrame checks a frame the agent sent: the envelope, and its data
// against what the spec says the message carries. Types the spec doesn't
// define (test-only handlers) get the envelope check alone.
func (p *protocolSchema) checkFrame(raw []byte) error {
	if err := p.CheckJSON("Envelope", raw); err != nil {
		return err
	}
	var env wire.Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return err
	}
	m, ok := p.byType[env.Type]
	if !ok {
		return nil
	}
	def := m.Data
	switch {
	case env.Kind == wire.KindResponse:
		if m.From != "cloud" || m.Kind != string(wire.KindRequest) {
			return fmt.Errorf("a response to %s, which the Cloud doesn't send", env.Type)
		}
		if env.Error != nil {
			return nil
		}
		def = m.Response
	case m.From != "agent" || m.Kind != string(env.Kind):
		return fmt.Errorf("%s as %s, which the agent doesn't send", env.Type, env.Kind)
	}
	if err := p.CheckJSON(def, env.Data); err != nil {
		return fmt.Errorf("%s %s: %w", env.Type, def, err)
	}
	return nil
}

// specExample is one of the spec's examples/<type>.json.
type specExample struct {
	Type     string          `json:"type"`
	Data     json.RawMessage `json:"data"`
	Response json.RawMessage `json:"response"`
}

func loadExample(t *testing.T, msgType string) specExample {
	t.Helper()
	var ex specExample
	if err := json.Unmarshal(spectest.Read(t, "protocol/conductor/examples/"+msgType+".json"), &ex); err != nil {
		t.Fatalf("%s example: %v", msgType, err)
	}
	return ex
}

func decodeStrict(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// TestSpecExamples serves every request in the spec's examples from a fake
// backend; the gateway checks each reply, and every frame the agent sends,
// against the schema.
func TestSpecExamples(t *testing.T) {
	p := loadSchema(t)
	b := &fakeBackend{queue: "payments"}
	e := runnerq.NewWorkerEngineWithBackend(b, runnerq.WorkerConfig{QueueName: b.queue, MaxConcurrentActivities: 4})
	e.RegisterActivity(&Echo{})
	g := newFakeGateway(t, wire.SessionConfig{})
	startAgent(t, e, g, Config{AllowControl: true})
	h := g.waitHello()

	var sub wire.Subscription
	for _, m := range p.messages {
		if m.From != "cloud" || m.Kind != string(wire.KindRequest) {
			continue
		}
		if _, ok := h.Capabilities[m.Type]; !ok {
			t.Errorf("%s is not advertised", m.Type)
		}
		ex := loadExample(t, m.Type)
		if m.Type == wire.TypeEventsUnsubscribe {
			ex.Data, _ = json.Marshal(wire.EventsUnsubscribe{SubscriptionID: sub.SubscriptionID})
		}
		res := g.call(m.Type, ex.Data)
		if res.Error != nil {
			t.Errorf("%s: %v", m.Type, (*wireError)(res.Error))
			continue
		}
		if m.Type == wire.TypeEventsSubscribe {
			if err := json.Unmarshal(res.Data, &sub); err != nil {
				t.Fatal(err)
			}
			g.waitEvent(wire.TypeStreamEvents)
			// Unsubscribing takes the subscription id alone.
			werr := g.fails(wire.TypeEventsUnsubscribe, map[string]string{"subscription_id": sub.SubscriptionID, "cursor": sub.Cursor}, wire.CodeInvalidArgument)
			if werr.Details["field"] != "cursor" {
				t.Errorf("unsubscribe with a cursor: %+v", werr)
			}
		}
	}

	// What the Cloud sends besides requests decodes too.
	var w wire.Welcome
	if err := decodeStrict(loadExample(t, wire.TypeHello).Response, &w); err != nil {
		t.Errorf("welcome: %v", err)
	}
	var c wire.SessionConfig
	if err := decodeStrict(loadExample(t, wire.TypeConfigUpdate).Data, &c); err != nil || c.DataMode != wire.DataModeMetadataOnly {
		t.Errorf("config.update: %+v %v", c, err)
	}
}

func TestCommandsAcceptOnlyTheirOwnFields(t *testing.T) {
	b := &fakeBackend{queue: "payments"}
	h := NewHandler(b, HandlerConfig{AllowControl: true})
	id := uuid.NewString()
	target := map[string]any{"ids": []string{id}}
	serve := func(msgType string, req map[string]any) (*storage.Command, *Error) {
		t.Helper()
		req["target"] = target
		raw, _ := json.Marshal(req)
		b.mu.Lock()
		b.commands = nil
		b.mu.Unlock()
		if _, err := h.Serve(context.Background(), msgType, raw); err != nil {
			return nil, err
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		if len(b.commands) != 1 {
			t.Fatalf("%s: %d commands applied", msgType, len(b.commands))
		}
		return &b.commands[0], nil
	}

	for _, tc := range []struct {
		msgType string
		req     map[string]any
		field   string
	}{
		{wire.TypeActivitiesCancel, map[string]any{"priority": 3}, "priority"},
		{wire.TypeActivitiesCancel, map[string]any{"cascade": "sideways"}, "cascade"},
		{wire.TypeActivitiesRetry, map[string]any{"cascade": "children"}, "cascade"},
		{wire.TypeActivitiesRunNow, map[string]any{"at": "2026-10-03T09:00:00Z"}, "at"},
		{wire.TypeActivitiesReschedule, map[string]any{"at": "tomorrow"}, "at"},
		{wire.TypeActivitiesSetPriority, map[string]any{"priority": 2, "name": "x"}, "name"},
		{wire.TypeActivitiesDelete, map[string]any{"cascade": "children"}, "cascade"},
		{wire.TypeActivitiesSignal, map[string]any{"name": "go", "reset_attempts": true}, "reset_attempts"},
	} {
		_, err := serve(tc.msgType, tc.req)
		if err == nil || err.Code != string(wire.CodeInvalidArgument) || err.Details["field"] != tc.field {
			t.Errorf("%s %v: got %v, want invalid_argument on %s", tc.msgType, tc.req, err, tc.field)
		}
	}

	at := "2026-10-03T09:00:00Z"
	for _, tc := range []struct {
		msgType string
		req     map[string]any
		check   func(storage.Command) bool
	}{
		{wire.TypeActivitiesCancel, map[string]any{}, func(c storage.Command) bool { return c.CascadeChildren }},
		{wire.TypeActivitiesCancel, map[string]any{"cascade": "none"}, func(c storage.Command) bool { return !c.CascadeChildren }},
		{wire.TypeActivitiesRetry, map[string]any{"reset_attempts": true, "command_id": "c1"}, func(c storage.Command) bool { return c.ResetAttempts && c.ID == "c1" }},
		{wire.TypeActivitiesRunNow, map[string]any{"dry_run": true, "reason": "now"}, func(c storage.Command) bool { return c.DryRun && c.Reason == "now" }},
		{wire.TypeActivitiesReschedule, map[string]any{"at": at}, func(c storage.Command) bool { return c.At.Format(time.RFC3339) == at }},
		{wire.TypeActivitiesSetPriority, map[string]any{"priority": 4}, func(c storage.Command) bool { return c.Priority == storage.PriorityCritical }},
		{wire.TypeActivitiesDelete, map[string]any{"cascade": "tree"}, func(c storage.Command) bool { return c.Kind == storage.CommandDelete }},
		{wire.TypeActivitiesDelete, map[string]any{}, func(c storage.Command) bool { return c.Kind == storage.CommandDelete }},
		{wire.TypeActivitiesSignal, map[string]any{"name": "go", "payload": map[string]int{"n": 1}}, func(c storage.Command) bool {
			return c.SignalName == "go" && string(c.SignalPayload) == `{"n":1}`
		}},
	} {
		cmd, err := serve(tc.msgType, tc.req)
		if err != nil {
			t.Errorf("%s %v: %v", tc.msgType, tc.req, err)
		} else if !tc.check(*cmd) || len(cmd.Target.IDs) != 1 || cmd.Target.IDs[0].String() != id {
			t.Errorf("%s %v: applied %+v", tc.msgType, tc.req, cmd)
		}
	}
}

// fakeBackend answers queries with fixed records and records commands.
type fakeBackend struct {
	storage.Storage
	queue string

	mu       sync.Mutex
	commands []storage.Command
}

var (
	fakeRoot  = uuid.MustParse("0192f3a4-5b6c-7d8e-9f01-000000000001")
	fakeChild = uuid.MustParse("0192f3a4-5b6c-7d8e-9f01-23456789abcd")
	fakeAt    = time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
)

func (b *fakeBackend) QueueName() string { return b.queue }

func (b *fakeBackend) GetResult(context.Context, uuid.UUID) (*storage.ActivityResult, error) {
	return &storage.ActivityResult{State: storage.ResultOk, Serialization: spec.SerializationSuperJSON,
		Data: json.RawMessage(`{"json":{"charge_id":"ch_1"},"meta":{}}`)}, nil
}

func (b *fakeBackend) QueryCapabilities() storage.QueryCapabilities {
	return storage.QueryCapabilities{
		ActivityFilters: []string{"status", "type", "queue", "metadata", "parent_id", "created_at"},
		ActivitySorts:   []string{"created_at"},
		EventFilters:    []string{"seq", "queue", "activity_id", "type"},
		GroupBy:         []string{"status", "type"},
		Buckets:         []string{"completed_at"},
		Durations:       []string{"run"},
	}
}

func (b *fakeBackend) record(id uuid.UUID, inc storage.RecordInclude) storage.ActivityRecord {
	at := fakeAt.Add(5 * time.Second)
	r := storage.ActivityRecord{
		ID: id, Type: "charge_card", Queue: b.queue, Status: storage.RecordStatusWaiting, Priority: 2,
		RootID: fakeRoot, Attempt: 2, MaxAttempts: 4, CreatedAt: fakeAt, ScheduledFor: &at, StartedAt: &at,
		UpdatedAt: at, Timeout: time.Minute, LeaseExpiresAt: &at, ExecutorID: "exec-1",
		Wait:     &storage.RecordWait{Kind: "signal", Name: "approve", Until: &at},
		Metadata: map[string]string{"tenant": "acme"}, IdempotencyKey: "order-42",
	}
	if id != fakeRoot {
		r.ParentID, r.Depth = &fakeRoot, 1
	}
	if inc.Payload {
		r.Payload = json.RawMessage(`{"amount":4200}`)
	}
	if inc.Result {
		r.Result = &storage.ActivityResult{State: storage.ResultErr, Data: json.RawMessage(`{"error":"card declined","type":"non_retryable"}`)}
	}
	if inc.LastError {
		r.LastError = &storage.RecordError{Message: "card declined", Kind: "retryable", At: &at}
	}
	return r
}

func (b *fakeBackend) QueryActivities(_ context.Context, q storage.ActivityQuery) (*storage.ActivityRecordPage, error) {
	return &storage.ActivityRecordPage{Items: []storage.ActivityRecord{b.record(fakeChild, q.Include)}, NextCursor: "c2"}, nil
}

func (b *fakeBackend) CountActivities(context.Context, *storage.QueryFilter, int64) (int64, bool, error) {
	return 1204, true, nil
}

func (b *fakeBackend) AggregateActivities(_ context.Context, q storage.AggregateQuery) (*storage.AggregateRows, error) {
	row := storage.AggregateRow{Key: map[string]string{}, Count: 1204, Durations: map[string]map[string]float64{}}
	for _, k := range q.GroupBy {
		row.Key[k] = "x"
	}
	if q.Bucket != nil {
		row.Bucket = &fakeAt
	}
	for _, d := range q.Durations {
		row.Durations[d.Field] = map[string]float64{}
		for _, p := range d.Percentiles {
			row.Durations[d.Field][fmt.Sprintf("p%g", p)] = 180
		}
	}
	return &storage.AggregateRows{Rows: []storage.AggregateRow{row}}, nil
}

// QueryEvents serves seqs 1201 to 1203, honouring the seq bounds tailers use.
func (b *fakeBackend) QueryEvents(_ context.Context, q storage.EventQuery) (*storage.EventRecordPage, error) {
	lo, hi := int64(0), int64(1<<62)
	var bounds func(f *storage.QueryFilter)
	bounds = func(f *storage.QueryFilter) {
		if f == nil {
			return
		}
		for i := range f.And {
			bounds(&f.And[i])
		}
		if v, ok := f.Value.(float64); ok && f.Field == "seq" {
			switch f.Op {
			case storage.OpGt:
				lo = int64(v)
			case storage.OpLte:
				hi = int64(v)
			}
		}
	}
	bounds(q.Filter)
	page := &storage.EventRecordPage{Items: []storage.EventRecord{}}
	for seq := int64(1201); seq <= 1203; seq++ {
		if seq <= lo || seq > hi {
			continue
		}
		ev := storage.EventRecord{ID: seq, Cursor: fmt.Sprint(seq), ActivityID: fakeChild,
			Type: storage.RecordEventAttemptStarted, At: fakeAt, ExecutorID: "exec-1"}
		if q.IncludeDetail {
			ev.Detail = json.RawMessage(`{"worker":"exec-1"}`)
		}
		page.Items = append(page.Items, ev)
	}
	if q.Desc && len(page.Items) > 0 {
		page.Items = page.Items[len(page.Items)-1:]
	}
	return page, nil
}

func (b *fakeBackend) ListStepEntries(_ context.Context, activityID uuid.UUID, includeData bool, _ int, _ string) (*storage.StepEntryPage, error) {
	s := storage.StepEntry{ID: uuid.New(), ActivityID: activityID, Kind: "run", Name: "charge", State: storage.ResultOk, CreatedAt: fakeAt}
	if includeData {
		s.Data = json.RawMessage(`{"charge_id":"ch_1"}`)
	}
	return &storage.StepEntryPage{Items: []storage.StepEntry{s}}, nil
}

func (b *fakeBackend) GetActivityTree(_ context.Context, _ uuid.UUID, inc storage.RecordInclude, _ int) (*storage.ActivityTree, error) {
	return &storage.ActivityTree{RootID: fakeRoot, Items: []storage.ActivityRecord{b.record(fakeRoot, inc), b.record(fakeChild, inc)}}, nil
}

func (b *fakeBackend) ApplyCommand(_ context.Context, cmd storage.Command) (*storage.CommandResult, error) {
	b.mu.Lock()
	b.commands = append(b.commands, cmd)
	b.mu.Unlock()
	ids := cmd.Target.IDs
	if len(ids) == 0 {
		ids = []uuid.UUID{fakeChild}
	}
	res := &storage.CommandResult{Matched: len(ids)}
	for i, id := range ids {
		it := storage.CommandItem{ID: id, Outcome: storage.CommandApplied, Status: storage.RecordStatusPending}
		switch {
		case cmd.DryRun:
			it.Outcome = storage.CommandWouldApply
		case i > 0:
			it.Outcome, it.ErrKind, it.ErrMessage = storage.CommandSkipped, storage.ErrConflict, "already running"
			it.Status = storage.RecordStatusRunning
		default:
			res.Applied++
		}
		res.Items = append(res.Items, it)
	}
	return res, nil
}
