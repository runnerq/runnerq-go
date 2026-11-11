// 08 — Exactly-Once Webhook Ingestion
//
// Webhooks and event streams redeliver. An idempotency key makes enqueuing
// exactly-once: duplicate deliveries of the same event collapse into one
// activity, so the work runs once no matter how many times the event arrives.
//
// A /webhook endpoint enqueues a "process_event" activity keyed by the event
// ID with ReturnExisting. We deliver the same event three times and a second
// event once; the processor runs twice total.
//
//	docker compose up -d        # from the examples/ directory
//	go run .
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/runnerq/runnerq-go"
	"github.com/runnerq/runnerq-go/storage/postgres"
)

// processed counts how many times each event was actually handled.
var (
	mu        sync.Mutex
	processed = map[string]int{}
)

type ProcessEvent struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *ProcessEvent) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	var in struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		return nil, runnerq.NewNonRetryError("bad payload: " + err.Error())
	}
	mu.Lock()
	processed[in.EventID]++
	n := processed[in.EventID]
	mu.Unlock()
	fmt.Printf("  ▶ processing %s (handler run #%d for this event)\n", in.EventID, n)
	return json.RawMessage(`{"ok":true}`), nil
}

func main() {
	ctx := context.Background()

	backend, err := postgres.New(ctx, databaseURL(), "webhook_dedup")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	engine, err := runnerq.Builder().Backend(backend).MaxWorkers(4).Build()
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	engine.RegisterActivity(&ProcessEvent{})

	engineDone := make(chan struct{})
	go func() {
		defer close(engineDone)
		if err := engine.Start(ctx); err != nil {
			log.Printf("engine stopped: %v", err)
		}
	}()
	defer func() {
		engine.Stop()
		<-engineDone
		backend.Close()
	}()

	// The webhook receiver: enqueue keyed by the delivery's event ID. A
	// duplicate delivery returns the existing activity instead of a new one.
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		eventID := r.URL.Query().Get("id")
		if eventID == "" {
			http.Error(w, "missing event id", http.StatusBadRequest)
			return
		}
		payload, _ := json.Marshal(map[string]string{"event_id": eventID})
		_, err := engine.GetActivityExecutor().
			Activity[ProcessEvent]().
			IdempotencyKeyOption(eventID, runnerq.ReturnExisting).
			Payload(payload).
			Execute(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintln(w, "accepted")
	})
	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("webhook server failed (port in use?): %v", err)
		}
	}()
	defer srv.Close()
	time.Sleep(200 * time.Millisecond) // let the listener bind

	// Event IDs are unique per run (the queue persists, and an idempotency
	// key claimed by a previous run would correctly dedupe against it — so a
	// fresh run uses fresh IDs to show the within-run dedup clearly).
	run := time.Now().UnixNano()
	evt1 := fmt.Sprintf("evt-%d-1", run)
	evt2 := fmt.Sprintf("evt-%d-2", run)

	// Simulate the provider delivering evt1 three times and evt2 once.
	fmt.Println("delivering webhooks: first event ×3, second event ×1")
	for _, id := range []string{evt1, evt1, evt1, evt2} {
		resp, err := http.Post("http://localhost:8080/webhook?id="+id, "", nil)
		if err != nil {
			log.Fatalf("deliver: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	time.Sleep(2 * time.Second) // let the processors run
	mu.Lock()
	defer mu.Unlock()
	fmt.Printf("\n✓ first event processed %d time(s); second event processed %d time(s).\n", processed[evt1], processed[evt2])
	fmt.Println("  → the three duplicate deliveries collapsed into a single activity.")
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:runnerq@localhost:5432/runnerq"
}
