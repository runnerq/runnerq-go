// 02 — Crash & Resume  (the flagship demo)
//
// Proves the one thing that makes RunnerQ a durable execution engine and not
// just a queue: a workflow survives the process being killed mid-flight and
// resumes WITHOUT redoing completed work.
//
// An order workflow runs three steps — reserve inventory, charge the card,
// ship. Each ctx.Run step prints "▶ EXECUTING" only when its side effect
// actually runs. Kill the process after it charges, restart it, and watch:
// reserve and charge do NOT print again (they're replayed from their
// Postgres checkpoints) — only shipping runs. The card is charged once.
//
//	docker compose up -d        # from the examples/ directory
//	go run .                    # run it; press Ctrl-C at the "kill me now" prompt
//	go run .                    # run it again — it resumes and ships
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/runnerq/runnerq-go"
	"github.com/runnerq/runnerq-go/storage/postgres"
)

// orderID is fixed so re-running the program reattaches to the SAME workflow
// (via its idempotency key) instead of starting a fresh one.
const orderID = "order-1001"

type FulfillOrder struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *FulfillOrder) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	// Step 1 — reserve inventory.
	if _, err := runnerq.RunStep(ctx, "reserve-inventory", func(context.Context) (bool, error) {
		fmt.Println("  ▶ EXECUTING reserve-inventory   (real side effect)")
		return true, nil
	}); err != nil {
		return nil, err
	}
	fmt.Println("  ✓ reserve-inventory")

	// Step 2 — charge the card. This is the side effect we must never repeat.
	if _, err := runnerq.RunStep(ctx, "charge-card", func(context.Context) (string, error) {
		fmt.Println("  ▶ EXECUTING charge-card         (real side effect — charges $$$)")
		return "ch_777", nil // the charge ID, checkpointed
	}); err != nil {
		return nil, err
	}
	fmt.Println("  ✓ charge-card")

	// The crash window: the card is charged but the order isn't shipped.
	fmt.Println()
	fmt.Println("  ⏸  CHARGED but not shipped. Press Ctrl-C NOW, then run `go run .` again.")
	fmt.Println("     (or wait — it ships on its own in a few seconds)")
	fmt.Println()
	time.Sleep(6 * time.Second)

	// Step 3 — ship.
	if _, err := runnerq.RunStep(ctx, "ship-order", func(context.Context) (string, error) {
		fmt.Println("  ▶ EXECUTING ship-order          (real side effect)")
		return "1Z999", nil // the tracking number, checkpointed
	}); err != nil {
		return nil, err
	}
	fmt.Println("  ✓ ship-order")

	return json.RawMessage(`{"status":"fulfilled"}`), nil
}

func main() {
	ctx := context.Background()

	// A short lease makes recovery after a crash quick: when the killed
	// process's lease expires, the reaper requeues the workflow and another
	// run (or another worker) picks it up. The 8s activity timeout keeps the
	// lease short (~18s) so this demo resumes within ~20s of the crash.
	backend, err := postgres.WithConfig(ctx, databaseURL(), "crash_demo", 8000, 5)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	engine, err := runnerq.Builder().Backend(backend).MaxWorkers(4).Build()
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	engine.RegisterActivity(&FulfillOrder{})

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

	fmt.Printf("fulfilling %s (re-run after Ctrl-C to watch it resume)\n\n", orderID)

	// The fixed idempotency key is what makes re-running reattach to the same
	// workflow rather than starting a new one.
	future, err := runnerq.Activity[FulfillOrder](engine.GetActivityExecutor()).
		IdempotencyKeyOption(orderID, runnerq.ReturnExisting).
		Timeout(8 * time.Second).
		Payload(json.RawMessage(`{"order_id":"` + orderID + `"}`)).
		Execute(ctx)
	if err != nil {
		log.Fatalf("enqueue: %v", err)
	}

	// Wait long enough to cover crash recovery (lease expiry + reaper).
	resultCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	result, err := future.GetResult(resultCtx)
	if err != nil {
		log.Fatalf("workflow failed: %v", err)
	}
	fmt.Printf("\n✓ ORDER COMPLETE: %s — the card wasn't charged again.\n", result)
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:runnerq@localhost:5432/runnerq"
}
