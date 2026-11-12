// 10 — Retries and the Dead-Letter Queue
//
// A retryable failure is retried with exponential backoff up to MaxRetries.
// When the attempts are exhausted the activity is dead-lettered, and its
// OnDeadLetter callback fires — your hook for alerting, compensation, or
// logging.
//
// Here a charge against a down payment gateway fails every time. With
// MaxRetries(2) it tries, backs off, tries again, then dead-letters.
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

var attempts atomic.Int32

// ChargeCard always fails (the gateway is down). Note it does NOT embed
// DefaultDeadLetterHandler — it implements OnDeadLetter itself.
type ChargeCard struct{}

func (h *ChargeCard) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	n := attempts.Add(1)
	fmt.Printf("  ▶ attempt #%d (RetryCount=%d) — calling gateway...\n", n, ctx.RetryCount)
	return nil, runnerq.NewRetryError("payment gateway 503")
}

func (h *ChargeCard) OnDeadLetter(ctx runnerq.ActivityContext, payload json.RawMessage, errorMsg string) {
	fmt.Printf("  ☠ dead-lettered %s after exhausting retries: %s\n", ctx.ActivityID, errorMsg)
	// Real apps: alert on-call, queue a refund/compensation, flag the order.
}

func main() {
	ctx := context.Background()

	backend, err := postgres.New(ctx, databaseURL(), "dlq_demo")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	engine, err := runnerq.Builder().Backend(backend).MaxWorkers(4).Build()
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	engine.RegisterActivity(&ChargeCard{})

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
		Activity[ChargeCard]().
		MaxRetries(2). // 2 attempts, then dead-letter
		Payload(json.RawMessage(`{"amount":4200}`)).
		Execute(ctx)
	if err != nil {
		log.Fatalf("enqueue: %v", err)
	}

	// A dead-lettered activity resolves with an error result, so awaiting it
	// returns an error rather than hanging.
	resultCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err = future.GetResult(resultCtx)
	fmt.Printf("\n✓ workflow ended after %d attempts; awaiting it returned: %v\n", attempts.Load(), err)
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:runnerq@localhost:5432/runnerq"
}
