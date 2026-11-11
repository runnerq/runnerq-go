// 13 — Cron schedules
//
// A schedule enqueues an activity on a cron expression. Declare it on the
// engine and every engine of the queue fires it; each tick runs once however
// many engines you start. Run this in two terminals: each heartbeat is
// printed by exactly one of them.
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
	"os/signal"

	"github.com/runnerq/runnerq-go"
	"github.com/runnerq/runnerq-go/storage"
	"github.com/runnerq/runnerq-go/storage/postgres"
)

type Heartbeat struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *Heartbeat) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	fmt.Printf("heartbeat %s (payload %s)\n", ctx.ActivityID, payload)
	return nil, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	backend, err := postgres.New(ctx, databaseURL(), "schedules_demo")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer backend.Close()

	engine, err := runnerq.Builder().Backend(backend).MaxWorkers(2).Build()
	if err != nil {
		log.Fatal(err)
	}
	engine.RegisterActivity(&Heartbeat{})

	// Every 10 seconds (six fields: seconds first). Upserted when the engine
	// starts; a missed tick fires once when an engine comes back.
	if err := engine.RegisterSchedule(runnerq.Schedule{
		Name:     "heartbeat",
		Cron:     "*/10 * * * * *",
		Timezone: "UTC",
		Activity: runnerq.NameOf[Heartbeat](),
		Payload:  json.RawMessage(`{"source":"cron"}`),
		CatchUp:  storage.CatchUpOne,
	}); err != nil {
		log.Fatal(err)
	}

	go func() {
		<-ctx.Done()
		engine.Stop()
	}()
	if err := engine.Start(ctx); err != nil {
		log.Fatal(err)
	}

	// The schedule outlives the process; manage it from anywhere.
	schedules, _ := runnerq.NewSchedules(backend)
	if s, err := schedules.Get(context.Background(), "heartbeat"); err == nil {
		fmt.Printf("next heartbeat at %s\n", s.NextFireAt.Format("15:04:05"))
	}
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:runnerq@localhost:5432/runnerq"
}
