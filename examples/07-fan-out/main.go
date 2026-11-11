// 07 — Fan-Out and WaitAll
//
// A workflow spawns many children and waits for all of them. The children run
// in parallel across the worker pool; while the parent waits it parks in the
// database, holding no worker. WaitAll returns the results in spawn order.
//
// Because awaiting is durable, this whole fan-out survives a restart: the
// parent reattaches to its existing children (named steps) and any that
// already finished return instantly.
//
// Here a document is split into pages, each page processed concurrently, and
// the word counts summed.
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

type ProcessDocument struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *ProcessDocument) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	pages := []string{"intro", "body", "appendix"}

	// Spawn one child per page. Each gets a stable step name so the fan-out
	// is replay-safe.
	futures := make([]*runnerq.ActivityFuture, len(pages))
	for i, page := range pages {
		p, _ := json.Marshal(map[string]any{"page": page, "index": i})
		fut, err := runnerq.Activity[ProcessPage](ctx.ActivityExecutor).
			Step(fmt.Sprintf("page-%d", i)).
			Payload(p).
			Execute(ctx.Ctx)
		if err != nil {
			return nil, err
		}
		futures[i] = fut
	}
	fmt.Printf("  spawned %d page workers; parking until all finish\n", len(pages))

	// Wait for every child. The parent parks here, freeing its worker.
	results, err := runnerq.WaitAll(ctx.Ctx, futures...)
	if err != nil {
		return nil, err
	}

	total := 0
	for i, r := range results {
		var out struct {
			Words int `json:"words"`
		}
		if err := json.Unmarshal(r, &out); err != nil {
			return nil, runnerq.NewNonRetryError("bad page result: " + err.Error())
		}
		fmt.Printf("  ✓ page %q: %d words\n", pages[i], out.Words)
		total += out.Words
	}
	return json.Marshal(map[string]int{"total_words": total})
}

type ProcessPage struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *ProcessPage) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	var in struct {
		Page  string `json:"page"`
		Index int    `json:"index"`
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		return nil, runnerq.NewNonRetryError("bad page payload: " + err.Error())
	}
	time.Sleep(2 * time.Second) // simulate real work
	words := 100 * (in.Index + 1)
	return json.Marshal(map[string]any{"words": words})
}

func main() {
	ctx := context.Background()

	backend, err := postgres.New(ctx, databaseURL(), "fan_out")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	engine, err := runnerq.Builder().Backend(backend).MaxWorkers(8).Build()
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	engine.RegisterActivity(&ProcessDocument{})
	engine.RegisterActivity(&ProcessPage{})

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

	future, err := runnerq.Activity[ProcessDocument](engine.GetActivityExecutor()).
		Payload(json.RawMessage(`{"doc":"report.pdf"}`)).
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
