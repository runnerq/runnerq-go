package runnerq

// Integration tests for parent re-dispatch: in-handler GetResult parks the
// parent after a short grace, child completion wakes it, and the replay
// fast-forwards. Skipped unless RUNNERQ_TEST_DSN is set.

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/runnerq/runnerq-go/storage"
)

// A parent awaiting a slow child must park (yield, no retry consumed) and be
// woken by the child's completion — not by its own timeout or a retry.
func TestParentParksAwaitingSlowChild(t *testing.T) {
	var parentRuns, childRuns atomic.Int32

	child := &funcHandler{fn: func(_ ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		childRuns.Add(1)
		time.Sleep(4 * time.Second) // well past the 2s await grace
		return json.RawMessage(`{"child":"done"}`), nil
	}}
	parent := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		parentRuns.Add(1)
		fut, err := ctx.ActivityExecutor.ActivityNamed("slow_child").Step("work").
			Payload(json.RawMessage(`{}`)).Execute(ctx.Ctx)
		if err != nil {
			return nil, err
		}
		return fut.GetResult(ctx.Ctx)
	}}

	rig := newStepsRig(t, func(e *WorkerEngine) {
		e.RegisterActivityWithName("parent", parent)
		e.RegisterActivityWithName("slow_child", child)
	})

	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("parent").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	res := rig.await(t, fut.activityID, 30*time.Second)
	if res.State != storage.ResultOk || !strings.Contains(string(res.Data), "done") {
		t.Fatalf("result = %v %s, want child result", res.State, res.Data)
	}
	if got := parentRuns.Load(); got != 2 {
		t.Fatalf("parent ran %d times, want 2 (park + resume)", got)
	}
	if got := childRuns.Load(); got != 1 {
		t.Fatalf("child ran %d times, want 1", got)
	}

	snap, err := rig.backend.GetActivity(context.Background(), fut.activityID)
	if err != nil || snap == nil {
		t.Fatalf("get parent: snap=%v err=%v", snap, err)
	}
	if snap.RetryCount != 0 {
		t.Fatalf("parent retry_count = %d, want 0 — awaiting a child must not consume retries", snap.RetryCount)
	}
	events, err := rig.backend.GetActivityEvents(context.Background(), fut.activityID, 100)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	sawYield := false
	for _, ev := range events {
		if ev.EventType == storage.EventYielded {
			sawYield = true
		}
	}
	if !sawYield {
		t.Fatal("parent never recorded a Yielded event despite the slow child")
	}
}

// A fast child resolves within the await grace: single parent invocation,
// no park.
func TestAwaitFastChildStaysInProcess(t *testing.T) {
	var parentRuns atomic.Int32

	child := &funcHandler{fn: func(_ ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"quick":true}`), nil
	}}
	parent := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		parentRuns.Add(1)
		fut, err := ctx.ActivityExecutor.ActivityNamed("fast_child").Step("work").
			Payload(json.RawMessage(`{}`)).Execute(ctx.Ctx)
		if err != nil {
			return nil, err
		}
		return fut.GetResult(ctx.Ctx)
	}}

	rig := newStepsRig(t, func(e *WorkerEngine) {
		// The child is fast, but a loaded runner (CI under -race) can take
		// longer than the default two-second grace to claim and finish it, and
		// the parent then parks as designed. A long grace keeps this test about
		// the fast path, not the machine's speed.
		e.awaitGrace = 15 * time.Second
		e.RegisterActivityWithName("parent_fast", parent)
		e.RegisterActivityWithName("fast_child", child)
	})

	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("parent_fast").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	res := rig.await(t, fut.activityID, 20*time.Second)
	if res.State != storage.ResultOk {
		t.Fatalf("result = %v %s, want Ok", res.State, res.Data)
	}
	if got := parentRuns.Load(); got != 1 {
		t.Fatalf("parent ran %d times, want 1 — a fast child must resolve in-process", got)
	}
	events, err := rig.backend.GetActivityEvents(context.Background(), fut.activityID, 100)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	for _, ev := range events {
		if ev.EventType == storage.EventYielded {
			t.Fatal("parent parked despite the child resolving within the grace window")
		}
	}
}

// Three-level chain: root awaits mid, mid awaits a slow leaf. Both ancestors
// park; the leaf's completion wakes mid, mid's completion wakes root —
// recursive wake chains work and nobody burns a retry.
func TestAwaitChainWakesRecursively(t *testing.T) {
	leaf := &funcHandler{fn: func(_ ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		time.Sleep(4 * time.Second)
		return json.RawMessage(`{"leaf":"done"}`), nil
	}}
	mid := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		fut, err := ctx.ActivityExecutor.ActivityNamed("chain_leaf").Step("leaf").
			Payload(json.RawMessage(`{}`)).Execute(ctx.Ctx)
		if err != nil {
			return nil, err
		}
		return fut.GetResult(ctx.Ctx)
	}}
	root := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		fut, err := ctx.ActivityExecutor.ActivityNamed("chain_mid").Step("mid").
			Payload(json.RawMessage(`{}`)).Execute(ctx.Ctx)
		if err != nil {
			return nil, err
		}
		return fut.GetResult(ctx.Ctx)
	}}

	rig := newStepsRig(t, func(e *WorkerEngine) {
		e.RegisterActivityWithName("chain_root", root)
		e.RegisterActivityWithName("chain_mid", mid)
		e.RegisterActivityWithName("chain_leaf", leaf)
	})

	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("chain_root").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	res := rig.await(t, fut.activityID, 40*time.Second)
	if res.State != storage.ResultOk || !strings.Contains(string(res.Data), "leaf") {
		t.Fatalf("result = %v %s, want leaf result propagated to root", res.State, res.Data)
	}
	snap, err := rig.backend.GetActivity(context.Background(), fut.activityID)
	if err != nil || snap == nil || snap.RetryCount != 0 {
		t.Fatalf("root snapshot %+v err=%v, want retry_count 0", snap, err)
	}
}

// WaitAll fans out children and awaits them all; parked or not, every result
// comes back in order.
func TestWaitAllFanOut(t *testing.T) {
	child := &funcHandler{fn: func(ctx ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
		time.Sleep(3 * time.Second)
		return payload, nil // echo
	}}
	parent := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		var futs []*ActivityFuture
		for _, name := range []string{"a", "b", "c"} {
			fut, err := ctx.ActivityExecutor.ActivityNamed("echo").Step(name).
				Payload(json.RawMessage(`"` + name + `"`)).Execute(ctx.Ctx)
			if err != nil {
				return nil, err
			}
			futs = append(futs, fut)
		}
		results, err := WaitAll(ctx.Ctx, futs...)
		if err != nil {
			return nil, err
		}
		joined := make([]string, len(results))
		for i, r := range results {
			joined[i] = string(r)
		}
		return json.RawMessage(`[` + strings.Join(joined, ",") + `]`), nil
	}}

	rig := newStepsRig(t, func(e *WorkerEngine) {
		e.RegisterActivityWithName("fanout", parent)
		e.RegisterActivityWithName("echo", child)
	})

	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("fanout").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	res := rig.await(t, fut.activityID, 40*time.Second)
	if res.State != storage.ResultOk {
		t.Fatalf("result state = %v (%s), want Ok", res.State, res.Data)
	}
	var decoded []string
	if err := json.Unmarshal(res.Data, &decoded); err != nil ||
		len(decoded) != 3 || decoded[0] != "a" || decoded[1] != "b" || decoded[2] != "c" {
		t.Fatalf("result data = %s (decode err %v), want ordered [a b c]", res.Data, err)
	}
	snap, _ := rig.backend.GetActivity(context.Background(), fut.activityID)
	if snap == nil || snap.RetryCount != 0 {
		t.Fatalf("snapshot %+v, want retry_count 0", snap)
	}
}

// A Delay-scheduled activity must NOT be started early by a signal — only
// yield-parked ('waiting') rows are woken. Regression for the caveat fixed
// by the dedicated waiting status.
func TestDelayScheduledNotWokenBySignal(t *testing.T) {
	var startedAt atomic.Int64
	h := &funcHandler{fn: func(_ ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		startedAt.Store(time.Now().UnixMilli())
		return json.RawMessage(`{}`), nil
	}}
	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivityWithName("delayed", h) })

	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("delayed").
		Delay(4 * time.Second).
		Payload(json.RawMessage(`{}`)).
		Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	// Anchor against the row's persisted schedule, not a pre-enqueue wall
	// clock — enqueue latency must not eat into the assertion.
	snap, err := rig.backend.GetActivity(context.Background(), fut.activityID)
	if err != nil || snap == nil || snap.ScheduledAt == nil {
		t.Fatalf("get scheduled activity: snap=%+v err=%v", snap, err)
	}
	scheduledFor := *snap.ScheduledAt

	if err := rig.engine.Signal(context.Background(), fut.activityID, "poke", nil); err != nil {
		t.Fatalf("signal: %v", err)
	}

	rig.await(t, fut.activityID, 30*time.Second)
	started := time.UnixMilli(startedAt.Load())
	// Small epsilon for clock granularity between the DB schedule and the
	// handler's app-clock timestamp.
	if started.Before(scheduledFor.Add(-250 * time.Millisecond)) {
		t.Fatalf("delayed activity started %v before its schedule — the signal woke it early", scheduledFor.Sub(started))
	}
}
