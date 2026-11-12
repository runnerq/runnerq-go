package runnerq

// Integration tests for durable steps: Step() memoized spawns, ctx.Run
// checkpoints, and ctx.Sleep durable timers (in-process and yielding).
// Skipped unless RUNNERQ_TEST_DSN is set — see engine_shutdown_test.go.

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
)

type stepsTestRig struct {
	backend *postgres.PostgresBackend
	engine  *WorkerEngine
}

func newStepsRig(t *testing.T, register func(e *WorkerEngine)) *stepsTestRig {
	t.Helper()
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping integration test")
	}
	queueName := "t_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	backend, err := postgres.New(context.Background(), dsn, queueName)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(backend.Close)

	engine, err := Builder().Backend(backend).QueueName(queueName).MaxWorkers(4).Build()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}
	register(engine)
	// Some signal tests need only the engine's client methods, not workers.
	if len(engine.handlers) == 0 {
		return &stepsTestRig{backend: backend, engine: engine}
	}

	startDone := make(chan error, 1)
	go func() { startDone <- engine.Start(context.Background()) }()
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
	return &stepsTestRig{backend: backend, engine: engine}
}

// await resolves an activity's result via the backend with a test deadline.
func (r *stepsTestRig) await(t *testing.T, id uuid.UUID, within time.Duration) *storage.ActivityResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	res, err := r.backend.WaitForResult(ctx, id)
	if err != nil {
		t.Fatalf("await result of %s: %v", id, err)
	}
	return res
}

type funcHandler struct {
	DefaultDeadLetterHandler
	fn func(ctx ActivityContext, payload json.RawMessage) (json.RawMessage, error)
}

func (h *funcHandler) Handle(ctx ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	return h.fn(ctx, payload)
}

// Step(): a retried parent must reattach to the child it already spawned and
// get its memoized result — the child runs exactly once.
func TestStepSpawnMemoizedAcrossParentRetry(t *testing.T) {
	// resolved counts parent runs that got the child's result. Plain runs
	// can be more: GetResult waits in-process for a couple of seconds, then
	// parks the parent and runs it again when the child finishes (slow CI,
	// -race), which is correct behaviour and not what this test is about.
	var childRuns, resolved atomic.Int32

	child := &funcHandler{fn: func(_ ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		childRuns.Add(1)
		return json.RawMessage(`{"reserved":true}`), nil
	}}
	parent := &funcHandler{fn: func(ctx ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
		fut, err := ctx.ActivityExecutor.
			ActivityNamed("reserve").
			Step("reserve").
			Payload(json.RawMessage(`{}`)).
			Execute(ctx.Ctx)
		if err != nil {
			return nil, err
		}
		res, err := fut.GetResult(ctx.Ctx)
		if err != nil {
			return nil, err
		}
		resolved.Add(1)
		if ctx.RetryCount == 0 {
			return nil, NewRetryError("forced crash after child completed")
		}
		return res, nil
	}}

	rig := newStepsRig(t, func(e *WorkerEngine) {
		e.RegisterActivityWithName("orchestrate", parent)
		e.RegisterActivityWithName("reserve", child)
	})

	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("orchestrate").
		Payload(json.RawMessage(`{}`)).
		Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	res := rig.await(t, fut.activityID, 30*time.Second)
	if res.State != storage.ResultOk {
		t.Fatalf("parent result = %v %s, want Ok", res.State, res.Data)
	}
	if got := resolved.Load(); got != 2 {
		t.Fatalf("parent got the child's result %d times, want 2 (original + retry)", got)
	}
	if got := childRuns.Load(); got != 1 {
		t.Fatalf("child ran %d times, want exactly 1 — the retried parent must reattach, not respawn", got)
	}
}

// ctx.Run: the checkpointed function runs once; the retried handler gets the
// stored result without re-executing the side effect.
func TestRunCheckpointSurvivesRetry(t *testing.T) {
	var fnRuns atomic.Int32

	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		out, err := ctx.Run("charge-payment", func() (json.RawMessage, error) {
			fnRuns.Add(1)
			return json.RawMessage(`{"charge_id":"ch_1"}`), nil
		})
		if err != nil {
			return nil, err
		}
		if ctx.RetryCount == 0 {
			return nil, NewRetryError("forced crash after charge")
		}
		return out, nil
	}}

	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivityWithName("pay", h) })

	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("pay").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	res := rig.await(t, fut.activityID, 30*time.Second)
	if res.State != storage.ResultOk || !strings.Contains(string(res.Data), "ch_1") {
		t.Fatalf("result = %v %s, want Ok with ch_1", res.State, res.Data)
	}
	if got := fnRuns.Load(); got != 1 {
		t.Fatalf("checkpointed fn ran %d times, want exactly 1 — the customer was charged %d times", got, got)
	}
}

// ctx.Run: a NonRetryError is itself checkpointed — a handler retried for an
// unrelated reason must NOT re-run a step that failed permanently.
func TestRunCheckpointsPermanentFailure(t *testing.T) {
	var fnRuns atomic.Int32

	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		_, runErr := ctx.Run("doomed-step", func() (json.RawMessage, error) {
			fnRuns.Add(1)
			return nil, NewNonRetryError("card declined")
		})
		if ctx.RetryCount == 0 {
			return nil, NewRetryError("forced crash; unrelated to the step")
		}
		// Second attempt: the stored failure must come back without fn
		// re-running, and it must still be non-retryable.
		if runErr == nil {
			return nil, NewNonRetryError("expected stored step failure, got success")
		}
		if re, ok := runErr.(RetryableError); !ok || re.IsRetryable() {
			return nil, NewNonRetryError("stored step failure lost its non-retryable kind")
		}
		return json.RawMessage(`{"observed":"stored failure"}`), nil
	}}

	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivityWithName("doomed", h) })

	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("doomed").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	res := rig.await(t, fut.activityID, 30*time.Second)
	if res.State != storage.ResultOk {
		t.Fatalf("result = %v %s, want Ok", res.State, res.Data)
	}
	if got := fnRuns.Load(); got != 1 {
		t.Fatalf("failed step ran %d times, want exactly 1", got)
	}
}

// ctx.Sleep, short form: waits in-process; a retried handler replaying past
// an already-elapsed sleep continues immediately instead of re-waiting.
func TestSleepResumesRemainderOnRetry(t *testing.T) {
	var sleepDurations [2]atomic.Int64 // per-attempt time spent in Sleep

	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		start := time.Now()
		if err := ctx.Sleep("nap", 700*time.Millisecond); err != nil {
			return nil, err
		}
		if int(ctx.RetryCount) < len(sleepDurations) {
			sleepDurations[ctx.RetryCount].Store(int64(time.Since(start)))
		}
		if ctx.RetryCount == 0 {
			return nil, NewRetryError("forced crash after sleep elapsed")
		}
		return json.RawMessage(`{"woke":true}`), nil
	}}

	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivityWithName("napper", h) })

	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("napper").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	res := rig.await(t, fut.activityID, 30*time.Second)
	if res.State != storage.ResultOk {
		t.Fatalf("result = %v %s, want Ok", res.State, res.Data)
	}
	first := time.Duration(sleepDurations[0].Load())
	second := time.Duration(sleepDurations[1].Load())
	if first < 600*time.Millisecond {
		t.Fatalf("first attempt slept %v, want ~700ms", first)
	}
	if second > 300*time.Millisecond {
		t.Fatalf("second attempt slept %v, want near-instant replay of an elapsed sleep", second)
	}
}

// ctx.Sleep, margin regression: a short sleep inside a short-timeout handler
// must wait in-process, not yield — the yield margin is capped at half the
// remaining budget so small timeouts don't force a reschedule for every wait.
func TestShortSleepInShortTimeoutStaysInProcess(t *testing.T) {
	var invocations atomic.Int32

	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		invocations.Add(1)
		// 300ms sleep, 2s timeout: wake lands well before deadline/2.
		if err := ctx.Sleep("blink", 300*time.Millisecond); err != nil {
			return nil, err
		}
		return json.RawMessage(`{"ok":true}`), nil
	}}

	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivityWithName("blinker", h) })

	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("blinker").
		Timeout(2 * time.Second).
		Payload(json.RawMessage(`{}`)).
		Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	res := rig.await(t, fut.activityID, 15*time.Second)
	if res.State != storage.ResultOk {
		t.Fatalf("result = %v %s, want Ok", res.State, res.Data)
	}
	if got := invocations.Load(); got != 1 {
		t.Fatalf("handler invoked %d times, want 1 — a 300ms sleep in a 2s budget must not yield", got)
	}
	events, err := rig.backend.GetActivityEvents(context.Background(), fut.activityID, 100)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	for _, ev := range events {
		if ev.EventType == storage.EventYielded {
			t.Fatal("short in-budget sleep recorded a Yielded event")
		}
	}
}

// ctx.Sleep, long form: a sleep that exceeds the handler's timeout budget
// yields — the activity parks as scheduled until the wake time, consumes NO
// retry, and the handler resumes afterwards.
func TestSleepYieldsBeyondTimeoutBudget(t *testing.T) {
	var invocations atomic.Int32

	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		invocations.Add(1)
		// 5s sleep inside a 2s timeout: must yield, not burn the attempt.
		if err := ctx.Sleep("cooling-off", 5*time.Second); err != nil {
			return nil, err
		}
		return json.RawMessage(`{"cooled":true}`), nil
	}}

	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivityWithName("cooler", h) })

	start := time.Now()
	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("cooler").
		Timeout(2 * time.Second).
		Payload(json.RawMessage(`{}`)).
		Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	res := rig.await(t, fut.activityID, 30*time.Second)
	elapsed := time.Since(start)
	if res.State != storage.ResultOk {
		t.Fatalf("result = %v %s, want Ok", res.State, res.Data)
	}
	if elapsed < 5*time.Second {
		t.Fatalf("completed in %v — the 5s durable sleep was not honored", elapsed)
	}
	if got := invocations.Load(); got != 2 {
		t.Fatalf("handler invoked %d times, want 2 (yield + resume)", got)
	}

	snap, err := rig.backend.GetActivity(context.Background(), fut.activityID)
	if err != nil || snap == nil {
		t.Fatalf("get activity: snap=%v err=%v", snap, err)
	}
	if snap.RetryCount != 0 {
		t.Fatalf("retry_count = %d after yield, want 0 — a durable sleep must not consume retries", snap.RetryCount)
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
		t.Fatal("no Yielded event recorded")
	}
}

// A workflow's ctx.Run and ctx.Sleep checkpoints are recorded with their human
// identity so step history can be listed. (Feature A.)
func TestStepHistoryRecorded(t *testing.T) {
	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		if _, err := ctx.Run("alpha", func() (json.RawMessage, error) {
			return json.RawMessage(`{"a":1}`), nil
		}); err != nil {
			return nil, err
		}
		if err := ctx.Sleep("nap", 10*time.Millisecond); err != nil {
			return nil, err
		}
		if _, err := ctx.Run("beta", func() (json.RawMessage, error) {
			return json.RawMessage(`"ok"`), nil
		}); err != nil {
			return nil, err
		}
		return json.RawMessage(`{"done":true}`), nil
	}}
	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivityWithName("func", h) })

	fut, err := rig.engine.GetActivityExecutor().ActivityNamed("func").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	rig.await(t, fut.activityID, 20*time.Second)

	steps, err := rig.backend.GetActivitySteps(context.Background(), fut.activityID)
	if err != nil {
		t.Fatalf("steps: %v", err)
	}
	want := []struct{ kind, name string }{{"run", "alpha"}, {"sleep", "nap"}, {"run", "beta"}}
	if len(steps) != len(want) {
		t.Fatalf("got %d steps, want %d: %+v", len(steps), len(want), steps)
	}
	for i, w := range want {
		if steps[i].Kind != w.kind || steps[i].Name != w.name {
			t.Fatalf("step %d = %q:%q, want %q:%q", i, steps[i].Kind, steps[i].Name, w.kind, w.name)
		}
		if steps[i].State != storage.ResultOk {
			t.Fatalf("step %q state = %v, want Ok", steps[i].Name, steps[i].State)
		}
	}
	var alpha map[string]int
	if err := json.Unmarshal(steps[0].Data, &alpha); err != nil || alpha["a"] != 1 {
		t.Fatalf("alpha stored data = %s, want {\"a\":1}", steps[0].Data)
	}
	// The activity's own final result must NOT appear as a step.
	for _, s := range steps {
		if s.Name == "" {
			t.Fatalf("unnamed (own-result) row leaked into step history: %+v", s)
		}
	}
}

// A parked durable wait records WHY on the Yielded event (kind + step), so it
// can be shown as "Sleeping…/Waiting for signal…" instead of bare "Waiting".
// (Feature B.)
func TestYieldEventCarriesWaitReason(t *testing.T) {
	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		// 4s wait in a 2s budget → parks, recording a Yielded event.
		if err := ctx.Sleep("cool-off", 4*time.Second); err != nil {
			return nil, err
		}
		return json.RawMessage(`{"slept":true}`), nil
	}}
	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivityWithName("func", h) })

	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("func").Timeout(2 * time.Second).Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	rig.await(t, fut.activityID, 30*time.Second)

	events, err := rig.backend.GetActivityEvents(context.Background(), fut.activityID, 100)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	found := false
	for _, ev := range events {
		if ev.EventType != storage.EventYielded {
			continue
		}
		var d map[string]any
		_ = json.Unmarshal(ev.Detail, &d)
		if d["kind"] == "sleep" && d["step"] == "cool-off" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no Yielded event carrying kind=sleep step=cool-off")
	}
}

// A ctx.Step child carries the Step name in its idempotency key — the data the
// Graph parses parent→child edge labels from. (E: edge labels.)
func TestStepChildCarriesStepNameInKey(t *testing.T) {
	child := &funcHandler{fn: func(_ ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"child":true}`), nil
	}}
	parent := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		fut, err := ctx.ActivityExecutor.ActivityNamed("child").Step("worker").Payload(json.RawMessage(`{}`)).Execute(ctx.Ctx)
		if err != nil {
			return nil, err
		}
		return fut.GetResult(ctx.Ctx)
	}}
	rig := newStepsRig(t, func(e *WorkerEngine) {
		e.RegisterActivityWithName("parent", parent)
		e.RegisterActivityWithName("child", child)
	})

	fut, err := rig.engine.GetActivityExecutor().ActivityNamed("parent").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	rig.await(t, fut.activityID, 20*time.Second)

	kids, err := rig.backend.GetChildren(context.Background(), fut.activityID, 0, 10)
	if err != nil || len(kids) != 1 {
		t.Fatalf("children = %v (err %v), want 1", kids, err)
	}
	key := kids[0].IdempotencyKey
	if key == nil || !strings.HasPrefix(*key, "rq:step:") || !strings.HasSuffix(*key, ":worker") {
		t.Fatalf("child idempotency key = %v, want rq:step:...:worker", key)
	}
}

// ctx.RunStep: the typed result is encoded into the checkpoint and decoded
// back on replay — fn runs once, the retried attempt gets the struct.
func TestRunStepTypedResultSurvivesRetry(t *testing.T) {
	type receipt struct {
		ChargeID string `json:"charge_id"`
		Cents    int    `json:"cents"`
	}
	var fnRuns atomic.Int32
	var sawCtx atomic.Bool

	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		r, err := ctx.RunStep("charge", func(c context.Context) (receipt, error) {
			fnRuns.Add(1)
			sawCtx.Store(c != nil && c.Err() == nil)
			return receipt{ChargeID: "ch_1", Cents: 4200}, nil
		})
		if err != nil {
			return nil, err
		}
		if ctx.RetryCount == 0 {
			return nil, NewRetryError("forced crash after charge")
		}
		if r.ChargeID != "ch_1" || r.Cents != 4200 {
			return nil, NewNonRetryError("replayed receipt mismatch: " + r.ChargeID)
		}
		return json.Marshal(r)
	}}

	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivityWithName("typed", h) })
	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("typed").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	res := rig.await(t, fut.activityID, 30*time.Second)
	var got receipt
	if res.State != storage.ResultOk || json.Unmarshal(res.Data, &got) != nil || got != (receipt{ChargeID: "ch_1", Cents: 4200}) {
		t.Fatalf("result = %v %s, want Ok with the decoded receipt", res.State, res.Data)
	}
	if n := fnRuns.Load(); n != 1 {
		t.Fatalf("typed step ran %d times, want exactly 1", n)
	}
	if !sawCtx.Load() {
		t.Fatal("step received a nil or already-cancelled context")
	}
}

// ctx.RunStep: a stored result that no longer decodes into R (the type
// changed between deploys) is a permanent error — retrying can't fix it.
func TestRunStepDecodeMismatchIsNonRetryable(t *testing.T) {
	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		if ctx.RetryCount == 0 {
			if _, err := ctx.RunStep("shape", func(context.Context) (map[string]string, error) {
				return map[string]string{"v": "1"}, nil
			}); err != nil {
				return nil, err
			}
			return nil, NewRetryError("forced crash; the next attempt reads with a different R")
		}
		_, err := ctx.RunStep("shape", func(context.Context) (int, error) { return 0, nil })
		if err == nil {
			return nil, NewNonRetryError("decoding an object into int succeeded")
		}
		if re, ok := err.(RetryableError); !ok || re.IsRetryable() {
			return nil, NewNonRetryError("decode mismatch was not a non-retryable error: " + err.Error())
		}
		if !strings.Contains(err.Error(), `"shape"`) {
			return nil, NewNonRetryError("error does not name the step: " + err.Error())
		}
		return json.RawMessage(`{"observed":"decode mismatch"}`), nil
	}}

	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivityWithName("shape", h) })
	fut, err := rig.engine.GetActivityExecutor().
		ActivityNamed("shape").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res := rig.await(t, fut.activityID, 30*time.Second); res.State != storage.ResultOk {
		t.Fatalf("result = %v %s, want Ok", res.State, res.Data)
	}
}

// ctx.RunStep on a hand-built context (no checkpoint storage) still runs fn
// with a live context, round-trips the value, and turns an unencodable
// result into a permanent error.
func TestRunStepWithoutQueue(t *testing.T) {
	c := ActivityContext{ActivityID: uuid.New()}

	n, err := c.RunStep("n", func(sc context.Context) (int, error) {
		if sc == nil {
			t.Fatal("nil step context")
		}
		return 7, nil
	})
	if err != nil || n != 7 {
		t.Fatalf("RunStep = %d, %v; want 7", n, err)
	}

	raw, err := c.RunStep("raw", func(context.Context) (json.RawMessage, error) {
		return json.RawMessage(`{"k":[1,2]}`), nil
	})
	if err != nil || string(raw) != `{"k":[1,2]}` {
		t.Fatalf("RawMessage passthrough = %s, %v", raw, err)
	}

	_, err = c.RunStep("bad", func(context.Context) (chan int, error) { return make(chan int), nil })
	if re, ok := err.(RetryableError); !ok || re.IsRetryable() {
		t.Fatalf("unencodable result err = %v, want non-retryable", err)
	}
	if _, err := c.RunStep("", func(context.Context) (int, error) { return 1, nil }); err == nil {
		t.Fatal("empty step name accepted")
	}
}
