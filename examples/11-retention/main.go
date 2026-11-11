// 11 — Retention
//
// By default RunnerQ keeps every activity, event, result, and idempotency key
// forever. Retention deletes whole terminal workflow trees once they age past
// a TTL, in the background — safe to enable on every engine (the backend
// elects one sweeper per queue).
//
// This runs a quick workflow, then watches the sweeper delete it once it's
// older than the (deliberately tiny) TTL.
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

type QuickJob struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *QuickJob) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(`{"done":true}`), nil
}

func main() {
	ctx := context.Background()

	backend, err := postgres.New(ctx, databaseURL(), "retention_demo")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	// Tiny TTLs so the demo finishes fast. In production these are days.
	engine, err := runnerq.Builder().
		Backend(backend).
		MaxWorkers(4).
		Retention(runnerq.RetentionConfig{
			Completed: 3 * time.Second,
			Interval:  2 * time.Second,
		}).
		Build()
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	engine.RegisterActivity(&QuickJob{})

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
		Activity[QuickJob]().
		Payload(json.RawMessage(`{}`)).
		Execute(ctx)
	if err != nil {
		log.Fatalf("enqueue: %v", err)
	}
	id := future.ActivityID()

	if _, err := future.GetResult(ctx); err != nil {
		log.Fatalf("workflow failed: %v", err)
	}
	fmt.Printf("workflow %s completed; waiting for retention to sweep it...\n", id)

	// Poll until the row is gone (GetActivity returns nil,nil when deleted).
	deadline := time.After(30 * time.Second)
	for {
		snap, err := backend.GetActivity(ctx, id)
		if err != nil {
			log.Fatalf("get activity: %v", err)
		}
		if snap == nil {
			fmt.Println("✓ swept — the completed workflow tree was deleted by retention.")
			return
		}
		select {
		case <-deadline:
			log.Fatal("not swept within 30s")
		case <-time.After(time.Second):
		}
	}
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:runnerq@localhost:5432/runnerq"
}
