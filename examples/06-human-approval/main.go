// 06 — Human-in-the-Loop with Signals
//
// A workflow can pause — durably, holding no worker — until something
// external happens: an approval click, a webhook, a payment confirmation.
// ctx.WaitForSignal parks the workflow; runnerq.SignalActivity (or
// engine.Signal) delivers the payload from anywhere, even another process.
//
// This expense-approval workflow waits for a decision. An HTTP endpoint
// delivers it. While it waits, the workflow occupies no goroutine and no
// worker slot — it's just a row in Postgres.
//
//	docker compose up -d        # from the examples/ directory
//	go run .                    # then, in another terminal:
//	curl -X POST localhost:8080/approve
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/runnerq/runnerq-go"
	"github.com/runnerq/runnerq-go/storage/postgres"
)

type ExpenseApproval struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *ExpenseApproval) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	if _, err := ctx.Run("file-request", func() (json.RawMessage, error) {
		fmt.Println("  ▶ expense filed, routing for approval")
		return payload, nil
	}); err != nil {
		return nil, err
	}

	fmt.Println("  ⏸ awaiting a decision (POST /approve or /reject) — parked, holding no worker")

	// Block until a "decision" signal arrives, or 2 minutes pass.
	decision, err := ctx.WaitForSignal("decision", 2*time.Minute)
	if runnerq.IsSignalTimeout(err) {
		fmt.Println("  ⌛ no decision in time — auto-rejecting")
		return json.RawMessage(`{"approved":false,"reason":"timeout"}`), nil
	}
	if err != nil {
		return nil, err
	}

	fmt.Printf("  ✓ decision received: %s\n", decision)
	return decision, nil
}

func main() {
	ctx := context.Background()

	backend, err := postgres.New(ctx, databaseURL(), "human_approval")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	engine, err := runnerq.Builder().Backend(backend).MaxWorkers(4).Build()
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	engine.RegisterActivity(&ExpenseApproval{})

	// Start the engine; on exit, stop it and wait for the graceful drain to
	// finish before closing the backend (correct shutdown ordering).
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

	// Start the workflow; its activity ID is the handle we signal.
	future, err := engine.GetActivityExecutor().
		Activity[ExpenseApproval]().
		Payload(json.RawMessage(`{"amount":4200,"who":"katherine"}`)).
		Execute(ctx)
	if err != nil {
		log.Fatalf("enqueue: %v", err)
	}
	activityID := future.ActivityID()

	// An HTTP endpoint delivers the decision. In a real app this is your
	// approval UI, a Slack action handler, or a webhook — any process with a
	// backend handle can call SignalActivity; here we use engine.Signal.
	decide := func(approved bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			payload, _ := json.Marshal(map[string]any{"approved": approved})
			if err := engine.Signal(r.Context(), activityID, "decision", payload); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			fmt.Fprintln(w, "delivered")
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/approve", decide(true))
	mux.HandleFunc("/reject", decide(false))
	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("approval server failed (port in use?): %v", err)
		}
	}()
	defer srv.Close()

	fmt.Printf("\nworkflow %s is waiting.\n", activityID)
	fmt.Println("approve it:  curl -X POST localhost:8080/approve")
	fmt.Println("reject it:   curl -X POST localhost:8080/reject")
	fmt.Println()

	resultCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	result, err := future.GetResult(resultCtx)
	if err != nil {
		log.Fatalf("workflow failed: %v", err)
	}
	fmt.Printf("\n✓ resolved: %s\n", result)
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:runnerq@localhost:5432/runnerq"
}
