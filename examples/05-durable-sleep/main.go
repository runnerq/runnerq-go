// 05 — Durable Sleep
//
// ctx.Sleep(name, d) is a timer that survives restarts. The wake deadline is
// persisted, so a workflow that sleeps for 24 hours and is redeployed an hour
// in resumes with 23 hours left — it does not restart the clock. For sleeps
// longer than the handler's timeout budget the activity is parked in the
// database (freeing the worker entirely) and re-dispatched when the timer
// fires; nothing holds a goroutine for a day.
//
// This onboarding workflow sends a welcome message, waits, then sends tips.
// The wait here is short so the demo finishes quickly — change it to
// 24*time.Hour and it behaves identically, just longer.
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
	"time"

	"github.com/runnerq/runnerq-go"
	"github.com/runnerq/runnerq-go/storage/postgres"
)

type Onboarding struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *Onboarding) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	if _, err := runnerq.RunStep(ctx, "send-welcome", func(context.Context) (string, error) {
		fmt.Printf("  ▶ %s  welcome email sent\n", time.Now().Format("15:04:05"))
		return "welcome", nil
	}); err != nil {
		return nil, err
	}

	// Durable wait. Persisted deadline — survives crashes and deploys.
	fmt.Println("  ⏳ waiting 5s before the follow-up (this is a durable timer)")
	if err := ctx.Sleep("drip-delay", 5*time.Second); err != nil {
		return nil, err
	}

	if _, err := runnerq.RunStep(ctx, "send-tips", func(context.Context) (string, error) {
		fmt.Printf("  ▶ %s  tips email sent\n", time.Now().Format("15:04:05"))
		return "tips", nil
	}); err != nil {
		return nil, err
	}

	return json.RawMessage(`{"status":"onboarded"}`), nil
}

func main() {
	ctx := context.Background()

	backend, err := postgres.New(ctx, databaseURL(), "durable_sleep")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	engine, err := runnerq.Builder().Backend(backend).MaxWorkers(4).Build()
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	engine.RegisterActivity(&Onboarding{})

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

	future, err := runnerq.Activity[Onboarding](engine.GetActivityExecutor()).
		Payload(json.RawMessage(`{"user":"grace"}`)).
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
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:runnerq@localhost:5432/runnerq"
}
