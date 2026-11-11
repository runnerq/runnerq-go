// 03 — Steps & Retries
//
// Shows step memoization across a retry. When a workflow spawns a child with
// .Step("name") and then fails partway through, the retry does NOT re-run the
// child — it reattaches to the one already running/completed and gets its
// result back instantly. The workflow fast-forwards through finished work.
//
// Here a payment workflow reserves inventory (a child), then hits a transient
// error on its first attempt. On the automatic retry, the reserve child is
// reused — proven by a counter that shows it executed exactly once.
//
//	docker compose up -d        # from the examples/ directory
//	go run .
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/runnerq/runnerq-go"
	"github.com/runnerq/runnerq-go/storage/postgres"
)

// reserveRuns counts how many times the reserve child actually executes.
// Memoization should keep it at 1 even though the parent runs twice.
var reserveRuns atomic.Int32

type ProcessOrder struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *ProcessOrder) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	fmt.Printf("process_order attempt #%d\n", ctx.RetryCount)

	// Spawn the reserve child as a named step. On a parent retry this exact
	// spawn reattaches to the existing child instead of creating a new one.
	fut, err := ctx.ActivityExecutor.
		Activity[ReserveInventory]().
		Step("reserve").
		Payload(payload).
		Execute(ctx.Ctx)
	if err != nil {
		return nil, err
	}
	reserved, err := fut.GetResult(ctx.Ctx)
	if err != nil {
		return nil, err
	}
	fmt.Printf("  ✓ reserved: %s\n", reserved)

	// Simulate a transient failure on the first attempt (a flaky downstream,
	// a deploy, an OOM). The engine retries the whole handler.
	if ctx.RetryCount == 0 {
		fmt.Println("  ✗ transient failure after reserve — will retry")
		return nil, runnerq.NewRetryError("payment gateway timeout")
	}

	fmt.Println("  ✓ payment captured")
	return json.RawMessage(`{"status":"paid"}`), nil
}

type ReserveInventory struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *ReserveInventory) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	n := reserveRuns.Add(1)
	fmt.Printf("    ▶ reserve_inventory EXECUTING (run #%d)\n", n)
	return json.RawMessage(`{"sku":"WIDGET-1","qty":1}`), nil
}

func main() {
	ctx := context.Background()

	backend, err := postgres.New(ctx, databaseURL(), "steps_retries")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	engine, err := runnerq.Builder().Backend(backend).MaxWorkers(4).Build()
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	engine.RegisterActivity(&ProcessOrder{})
	engine.RegisterActivity(&ReserveInventory{})

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

	future, err := engine.GetActivityExecutor().
		Activity[ProcessOrder]().
		Payload(json.RawMessage(`{"order_id":"42"}`)).
		Execute(ctx)
	if err != nil {
		log.Fatalf("enqueue: %v", err)
	}

	resultCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	result, err := future.GetResult(resultCtx)
	if err != nil {
		log.Fatalf("workflow failed: %v", err)
	}

	fmt.Printf("\n✓ done: %s\n", result)
	fmt.Printf("  reserve child executed %d time(s) across 2 parent attempts (memoized).\n", reserveRuns.Load())
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:runnerq@localhost:5432/runnerq"
}
