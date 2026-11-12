package runnerq

// End-to-end retention test: a real workflow (parent + Step child + Run
// checkpoint) completes and is then swept by the engine's retention loop.
// Skipped unless RUNNERQ_TEST_DSN is set.

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage/postgres"
)

func TestRetentionSweepsCompletedWorkflowEndToEnd(t *testing.T) {
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping integration test")
	}
	ctx := context.Background()
	queueName := "t_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]

	backend, err := postgres.New(ctx, dsn, queueName)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	defer backend.Close()

	engine, err := Builder().
		Backend(backend).
		QueueName(queueName).
		MaxWorkers(4).
		Retention(RetentionConfig{
			Completed: time.Second,
			Interval:  time.Second,
		}).
		Build()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}

	child := &funcHandler{fn: func(_ ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	}}
	parent := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		if _, err := ctx.Run("checkpoint", func() (json.RawMessage, error) {
			return json.RawMessage(`{"cp":1}`), nil
		}); err != nil {
			return nil, err
		}
		fut, err := ctx.ActivityExecutor.ActivityNamed("leaf").Step("leaf").
			Payload(json.RawMessage(`{}`)).Execute(ctx.Ctx)
		if err != nil {
			return nil, err
		}
		return fut.GetResult(ctx.Ctx)
	}}
	engine.RegisterActivityWithName("root", parent)
	engine.RegisterActivityWithName("leaf", child)

	startDone := make(chan error, 1)
	go func() { startDone <- engine.Start(ctx) }()
	t.Cleanup(func() {
		engine.Stop()
		select {
		case err := <-startDone:
			if err != nil {
				t.Errorf("engine.Start: %v", err)
			}
		case <-time.After(35 * time.Second):
			t.Error("engine did not stop")
		}
	})

	fut, err := engine.GetActivityExecutor().
		ActivityNamed("root").Payload(json.RawMessage(`{}`)).Execute(ctx)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if _, err := backend.WaitForResult(waitCtx, fut.activityID); err != nil {
		t.Fatalf("workflow did not complete: %v", err)
	}

	// The 1s TTL + 1s sweep interval should erase the tree shortly after
	// completion: activity row, its result, and the Run checkpoint all gone.
	deadline := time.After(15 * time.Second)
	for {
		snap, err := backend.GetActivity(ctx, fut.activityID)
		if err == nil && snap == nil {
			break // swept
		}
		select {
		case <-deadline:
			t.Fatalf("workflow tree was never swept: snap=%v err=%v", snap, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
	if res, err := backend.GetResult(ctx, fut.activityID); err != nil || res != nil {
		t.Fatalf("root result survived the sweep: res=%v err=%v", res, err)
	}
}
