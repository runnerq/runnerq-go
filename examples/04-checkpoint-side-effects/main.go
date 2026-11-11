// 04 — Checkpoint Side Effects with ctx.Run
//
// ctx.Run(name, fn) runs fn at most once per recorded success and stores its
// result. When a handler retries, every step that already succeeded returns
// its stored result without re-running — only the step that failed runs
// again. This is how you make non-idempotent side effects (charges, emails,
// external API writes) safe under at-least-once execution.
//
// Here a billing workflow charges credits once, then sends a batch that fails
// twice before succeeding. The charge never repeats; only the send retries.
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

var (
	chargeRuns atomic.Int32
	sendRuns   atomic.Int32
)

type BillingRun struct {
	runnerq.DefaultDeadLetterHandler
}

// Receipt is the typed, checkpointed result of the charge step.
type Receipt struct {
	ChargeID string `json:"charge_id"`
}

func (h *BillingRun) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	fmt.Printf("billing_run attempt #%d\n", ctx.RetryCount)

	// Charge once. Memoized on every retry after the first success.
	receipt, err := ctx.RunStep("charge-credits", func(context.Context) (Receipt, error) {
		n := chargeRuns.Add(1)
		fmt.Printf("  ▶ charge-credits EXECUTING (run #%d) — real money moves here\n", n)
		return Receipt{ChargeID: "ch_42"}, nil
	})
	if err != nil {
		return nil, err
	}

	// Send the batch. Fails transiently on the first two attempts; RunStep
	// does not checkpoint retryable failures, so this step re-runs while the
	// charge above stays memoized.
	delivered, err := ctx.RunStep("send-batch", func(context.Context) (int, error) {
		n := sendRuns.Add(1)
		fmt.Printf("  ▶ send-batch EXECUTING (run #%d)\n", n)
		if n < 3 {
			return 0, runnerq.NewRetryError("smtp temporarily unavailable")
		}
		return 1000, nil
	})
	if err != nil {
		return nil, err
	}

	fmt.Printf("  ✓ batch delivered (%d messages)\n", delivered)
	return json.Marshal(receipt)
}

func main() {
	ctx := context.Background()

	backend, err := postgres.New(ctx, databaseURL(), "checkpoint_demo")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	engine, err := runnerq.Builder().Backend(backend).MaxWorkers(4).Build()
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	engine.RegisterActivity(&BillingRun{})

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
		Activity[BillingRun]().
		Payload(json.RawMessage(`{"campaign":"june"}`)).
		Execute(ctx)
	if err != nil {
		log.Fatalf("enqueue: %v", err)
	}

	resultCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	result, err := future.GetResult(resultCtx)
	if err != nil {
		log.Fatalf("workflow failed: %v", err)
	}

	fmt.Printf("\n✓ done: %s\n", result)
	fmt.Printf("  charge ran %d time(s); send ran %d time(s).\n", chargeRuns.Load(), sendRuns.Load())
	fmt.Println("  → the charge ran once despite the retries.")
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:runnerq@localhost:5432/runnerq"
}
