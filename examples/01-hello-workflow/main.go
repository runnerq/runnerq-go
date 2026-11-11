// 01 — Hello, Workflow
//
// The smallest useful RunnerQ program: a durable two-step workflow.
//
// A "workflow" is just an activity handler. Inside it, each ctx.Run step is
// checkpointed in Postgres — so if this process crashed between the two
// steps, a restart would skip the first and resume at the second instead of
// redoing it. (Example 02 proves that.) Here we just run it end to end.
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

// SignupWorkflow registers a user and sends a welcome email — two side
// effects we don't want to repeat if the process restarts mid-way.
type SignupWorkflow struct {
	runnerq.DefaultDeadLetterHandler
}

// Account is the typed result of the create-account step. RunStep encodes it
// into the checkpoint and hands it back decoded on replay.
type Account struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

func (h *SignupWorkflow) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	var in struct {
		Email string `json:"email"`
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		return nil, runnerq.NewNonRetryError("invalid payload")
	}

	// Step 1: create the account. ctx.RunStep checkpoints the result, so this
	// runs at most once across any number of retries or restarts.
	user, err := ctx.RunStep("create-account", func(context.Context) (Account, error) {
		fmt.Printf("  ▶ creating account for %s\n", in.Email)
		return Account{UserID: "u_1001", Email: in.Email}, nil
	})
	if err != nil {
		return nil, err
	}

	// Step 2: send the welcome email — only fires once the account is durably
	// recorded, and never twice.
	_, err = ctx.RunStep("send-welcome", func(context.Context) (bool, error) {
		fmt.Printf("  ▶ sending welcome email to %s\n", in.Email)
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(user)
}

func main() {
	ctx := context.Background()

	backend, err := postgres.New(ctx, databaseURL(), "hello_workflow")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	engine, err := runnerq.Builder().Backend(backend).MaxWorkers(4).Build()
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	engine.RegisterActivity(&SignupWorkflow{})

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

	// Kick off the workflow and wait for its result.
	fmt.Println("starting signup workflow...")
	future, err := engine.GetActivityExecutor().
		Activity[SignupWorkflow]().
		Payload(json.RawMessage(`{"email":"ada@example.com"}`)).
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
	fmt.Printf("✓ signup complete: %s\n", result)
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:runnerq@localhost:5432/runnerq"
}
