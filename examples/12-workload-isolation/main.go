// 12 — Workload Isolation
//
// Many worker fleets, one queue. By default an engine dequeues every activity
// type; .ActivityTypes(...) restricts it, so you can give slow jobs their own
// workers and keep them from starving latency-sensitive ones — without
// separate queues, brokers, or binaries.
//
// Three engines share one queue, each restricted to its own activity types:
// a notifications fleet (email/SMS), a slow reports fleet, and a finance
// fleet. Per-fleet counters prove each engine only ran the types it owns.
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

// Per-fleet counters, tagged by which engine's handler ran.
var notif, reports, finance atomic.Int32

// One handler type serves several activity types, so each registration pins
// its name with RegisterActivityWithName.
func handler(counter *atomic.Int32, work time.Duration) runnerq.ActivityHandler {
	return &countingHandler{counter: counter, work: work}
}

type countingHandler struct {
	runnerq.DefaultDeadLetterHandler
	counter *atomic.Int32
	work    time.Duration
}

func (h *countingHandler) Handle(_ runnerq.ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
	h.counter.Add(1)
	time.Sleep(h.work)
	return json.RawMessage(`{"ok":true}`), nil
}

func main() {
	ctx := context.Background()

	backend, err := postgres.New(ctx, databaseURL(), "isolation")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer backend.Close()

	// Notifications fleet — only send_email / send_sms.
	notifEngine := mustBuild(backend, []string{"send_email", "send_sms"})
	notifEngine.RegisterActivityWithName("send_email", handler(&notif, 200*time.Millisecond))
	notifEngine.RegisterActivityWithName("send_sms", handler(&notif, 200*time.Millisecond))

	// Reports fleet — only generate_report (slow; isolated so it can't starve notifications).
	reportEngine := mustBuild(backend, []string{"generate_report"})
	reportEngine.RegisterActivityWithName("generate_report", handler(&reports, time.Second))

	// Finance fleet — only reconcile_ledger.
	financeEngine := mustBuild(backend, []string{"reconcile_ledger"})
	financeEngine.RegisterActivityWithName("reconcile_ledger", handler(&finance, 300*time.Millisecond))

	engines := []*runnerq.WorkerEngine{notifEngine, reportEngine, financeEngine}
	var dones []chan struct{}
	for _, e := range engines {
		done := make(chan struct{})
		dones = append(dones, done)
		go func(e *runnerq.WorkerEngine) {
			defer close(done)
			if err := e.Start(ctx); err != nil {
				log.Printf("fleet engine stopped: %v", err)
			}
		}(e)
	}
	defer func() {
		for i, e := range engines {
			e.Stop()
			<-dones[i]
		}
	}()

	// Enqueue a mix (any engine's executor writes to the same shared queue).
	exec := financeEngine.GetActivityExecutor()
	mix := []string{"send_email", "send_sms", "generate_report", "reconcile_ledger"}
	fmt.Println("enqueuing a mix of 12 activities across 4 types...")
	for i := range 12 {
		if _, err := exec.ActivityNamed(mix[i%len(mix)]).Payload(json.RawMessage(`{}`)).Execute(ctx); err != nil {
			log.Fatalf("enqueue: %v", err)
		}
	}

	time.Sleep(6 * time.Second) // let the fleets drain
	fmt.Printf("\n✓ handled — notifications fleet: %d, reports fleet: %d, finance fleet: %d\n",
		notif.Load(), reports.Load(), finance.Load())
	fmt.Println("  each fleet only ran the activity types it was assigned.")
}

func mustBuild(backend *postgres.PostgresBackend, types []string) *runnerq.WorkerEngine {
	b := runnerq.Builder().Backend(backend).QueueName("isolation").MaxWorkers(3)
	if types != nil {
		b = b.ActivityTypes(types)
	}
	engine, err := b.Build()
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	return engine
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:runnerq@localhost:5432/runnerq"
}
