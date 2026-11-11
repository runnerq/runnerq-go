package conductor

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/runnerq/runnerq-go/conductor/internal/wire"
	"github.com/runnerq/runnerq-go/storage"
)

func TestHandlerServesStorage(t *testing.T) {
	_, b, queue := pgEngine(t)
	id := enqueue(t, b, `{"n":1}`)
	ctx := context.Background()
	h := NewHandler(b, HandlerConfig{AllowControl: true})

	var caps map[string]json.RawMessage
	if err := json.Unmarshal(h.Capabilities(), &caps); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{wire.TypeActivitiesList, wire.TypeActivitiesGet, wire.TypeActivitiesAggregate, wire.TypeActivitiesCancel} {
		if _, ok := caps[want]; !ok {
			t.Errorf("capabilities miss %s: %v", want, caps)
		}
	}
	for _, not := range []string{wire.TypeExecutorDescribe, wire.TypeEventsSubscribe} {
		if _, ok := caps[not]; ok {
			t.Errorf("capabilities include %s", not)
		}
	}

	list, err := h.Serve(ctx, wire.TypeActivitiesList, mustJSON(t, map[string]any{"filter": inQueue(queue)}))
	if err != nil {
		t.Fatal(err)
	}
	var page struct {
		Items []struct {
			ID    string `json:"id"`
			Queue string `json:"queue"`
		} `json:"items"`
	}
	if err := json.Unmarshal(list, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != id.String() || page.Items[0].Queue != queue {
		t.Fatalf("list: %s", list)
	}

	cancel := map[string]any{"command_id": "c1", "target": map[string]any{"ids": []string{id.String()}, "queue": queue}}
	res, err := h.Serve(ctx, wire.TypeActivitiesCancel, mustJSON(t, cancel))
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Applied int `json:"applied"`
	}
	if json.Unmarshal(res, &out); out.Applied != 1 {
		t.Fatalf("cancel: %s", res)
	}

	other := map[string]any{"command_id": "c2", "target": map[string]any{"ids": []string{id.String()}, "queue": "elsewhere"}}
	if _, err := h.Serve(ctx, wire.TypeActivitiesCancel, mustJSON(t, other)); err == nil || err.Code != string(wire.CodeFailedPrecondition) {
		t.Fatalf("another queue's command: %v", err)
	}
	if _, err := h.Serve(ctx, wire.TypeExecutorDescribe, nil); err == nil || err.Code != string(wire.CodeUnsupported) {
		t.Fatalf("executor.describe: %v", err)
	}
	if _, err := h.Serve(ctx, wire.TypeActivitiesGet, mustJSON(t, map[string]any{"id": "not-an-id"})); err == nil {
		t.Fatal("bad id accepted")
	}

	readOnly := NewHandler(b, HandlerConfig{})
	if _, err := readOnly.Serve(ctx, wire.TypeActivitiesCancel, mustJSON(t, cancel)); err == nil || err.Code != string(wire.CodeUnsupported) {
		t.Fatalf("command without AllowControl: %v", err)
	}

	// Commands act on the queue the backend reports: one that can't say
	// which queue it acts on gets none, and still serves reads.
	anonymous := NewHandler(unnamed{b, b, b}, HandlerConfig{AllowControl: true})
	if _, err := anonymous.Serve(ctx, wire.TypeActivitiesCancel, mustJSON(t, cancel)); err == nil || err.Code != string(wire.CodeUnsupported) {
		t.Fatalf("command on a backend with no queue: %v", err)
	}
	var anonCaps map[string]json.RawMessage
	if json.Unmarshal(anonymous.Capabilities(), &anonCaps); anonCaps[wire.TypeActivitiesCancel] != nil || anonCaps[wire.TypeActivitiesList] == nil {
		t.Fatalf("capabilities of a backend with no queue: %v", anonCaps)
	}
	if _, err := anonymous.Serve(ctx, wire.TypeActivitiesList, mustJSON(t, map[string]any{"filter": inQueue(queue)})); err != nil {
		t.Fatalf("read on a backend with no queue: %v", err)
	}
}

// unnamed is a backend that doesn't report its queue.
type unnamed struct {
	storage.Storage
	storage.QueryStorage
	storage.CommandStorage
}

func TestHandlerRecoversPanics(t *testing.T) {
	h := NewHandler(unnamed{}, HandlerConfig{})
	h.table["boom"] = func(context.Context, json.RawMessage) (any, error) { panic("boom") }
	if _, err := h.Serve(context.Background(), "boom", nil); err == nil || err.Code != string(wire.CodeInternal) {
		t.Fatalf("panic: %v", err)
	}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
