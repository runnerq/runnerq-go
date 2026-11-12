// Command chaosworker is the Go SDK's worker for runnerq-spec's chaos
// harness (tools/chaos/README.md in runnerq-spec). "work" runs an engine
// serving <prefix>.flow and <prefix>.limited; "client" enqueues roots and delivers signals for the
// harness, one JSON request per stdin line.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/runnerq/runnerq-go"
	"github.com/runnerq/runnerq-go/storage/postgres"
)

type env struct {
	dsn, queue, prefix, worker string
	concurrency, limit         int
	leaseMS                    int64
}

func main() {
	if len(os.Args) < 2 {
		fatal(errors.New("usage: chaosworker work|client"))
	}
	e := env{
		dsn:         os.Getenv("RUNNERQ_CHAOS_DSN"),
		queue:       os.Getenv("RUNNERQ_CHAOS_QUEUE"),
		prefix:      os.Getenv("RUNNERQ_CHAOS_PREFIX"),
		worker:      os.Getenv("RUNNERQ_CHAOS_WORKER"),
		concurrency: atoi(os.Getenv("RUNNERQ_CHAOS_CONCURRENCY"), 8),
		leaseMS:     int64(atoi(os.Getenv("RUNNERQ_CHAOS_LEASE_MS"), 60_000)),
		limit:       atoi(os.Getenv("RUNNERQ_CHAOS_LIMIT"), 2),
	}
	if e.dsn == "" || e.queue == "" || e.prefix == "" {
		fatal(errors.New("RUNNERQ_CHAOS_DSN, RUNNERQ_CHAOS_QUEUE and RUNNERQ_CHAOS_PREFIX are required"))
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	var err error
	switch os.Args[1] {
	case "work":
		err = work(e)
	case "client":
		err = client(e)
	default:
		err = fmt.Errorf("unknown mode %q", os.Args[1])
	}
	if err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "chaosworker:", err)
	os.Exit(1)
}

func atoi(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil && n > 0 {
		return n
	}
	return def
}

func work(e env) error {
	ctx := context.Background()
	backend, err := postgres.WithConfig(ctx, e.dsn, e.queue, e.leaseMS, int32(e.concurrency+5))
	if err != nil {
		return err
	}
	defer backend.Close()
	ledger, err := pgxpool.New(ctx, e.dsn)
	if err != nil {
		return err
	}
	defer ledger.Close()

	engine, err := runnerq.Builder().Backend(backend).MaxWorkers(e.concurrency).ShutdownGrace(10 * time.Second).Build()
	if err != nil {
		return err
	}
	f := &flow{ledger: ledger, queue: e.queue, worker: e.worker}
	engine.RegisterActivityWithName(e.prefix+".flow", f)
	engine.RegisterActivityWithName(e.prefix+".limited", f, runnerq.MaxConcurrent(e.limit))

	started := make(chan error, 1)
	go func() { started <- engine.Start(ctx) }()
	// Start returns only at shutdown; report ready once it is claiming.
	select {
	case err := <-started:
		return err
	case <-time.After(200 * time.Millisecond):
	}
	fmt.Println(`{"ready":true}`)
	return <-started // SIGTERM: the engine drains and returns
}

// flowInput is the workload payload (tools/chaos/README.md).
type flowInput struct {
	Label    string      `json:"label"`
	Steps    []string    `json:"steps"`
	StepMS   int         `json:"step_ms"`
	SleepMS  int64       `json:"sleep_ms"`
	Signal   string      `json:"signal"`
	Children []childSpec `json:"children"`
	After    []string    `json:"after"`
}

type childSpec struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	TimeoutS       int             `json:"timeout_s"`
	MaxRetryDelayS int             `json:"max_retry_delay_s"`
}

type flowResult struct {
	Label    string            `json:"label"`
	Steps    map[string]string `json:"steps"`
	Signal   json.RawMessage   `json:"signal,omitempty"`
	Children []json.RawMessage `json:"children,omitempty"`
}

type flow struct {
	runnerq.DefaultDeadLetterHandler
	ledger *pgxpool.Pool
	queue  string
	worker string
}

func (f *flow) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	var in flowInput
	if err := json.Unmarshal(payload, &in); err != nil {
		return nil, runnerq.NewNonRetryError("bad payload: " + err.Error())
	}
	if _, err := f.ledger.Exec(ctx.Ctx,
		`INSERT INTO runnerq_chaos_ledger (queue_name, activity_id, kind, worker) VALUES ($1, $2, 'invoke', $3)`,
		f.queue, ctx.ActivityID, f.worker); err != nil {
		return nil, err
	}
	out := flowResult{Label: in.Label, Steps: map[string]string{}}
	runSteps := func(names []string) error {
		for _, name := range names {
			raw, err := ctx.Run(name, func() (json.RawMessage, error) { return f.effect(ctx, name, in.StepMS) })
			if err != nil {
				return err
			}
			var nonce string
			if err := json.Unmarshal(raw, &nonce); err != nil {
				return runnerq.NewNonRetryError(fmt.Sprintf("step %s: stored value %s: %v", name, raw, err))
			}
			out.Steps[name] = nonce
		}
		return nil
	}

	if err := runSteps(in.Steps); err != nil {
		return nil, err
	}
	switch {
	case in.SleepMS > 0:
		if err := ctx.Sleep("nap", time.Duration(in.SleepMS)*time.Millisecond); err != nil {
			return nil, err
		}
	case in.Signal != "":
		v, err := ctx.WaitForSignal(in.Signal, 0)
		if err != nil {
			return nil, err
		}
		out.Signal = v
	case len(in.Children) > 0:
		futures := make([]*runnerq.ActivityFuture, len(in.Children))
		for i, c := range in.Children {
			fut, err := ctx.ActivityExecutor.ActivityNamed(c.Type).
				Step(fmt.Sprintf("child-%d", i)).
				Payload(c.Payload).
				MaxRetries(0).
				Timeout(time.Duration(c.TimeoutS) * time.Second).
				MaxRetryDelay(time.Duration(c.MaxRetryDelayS) * time.Second).
				Execute(ctx.Ctx)
			if err != nil {
				return nil, err
			}
			futures[i] = fut
		}
		results, err := runnerq.WaitAll(ctx.Ctx, futures...)
		if err != nil {
			return nil, err
		}
		out.Children = results
	}
	if err := runSteps(in.After); err != nil {
		return nil, err
	}
	return json.Marshal(out)
}

// effect is a step's side effect: a ledger row, with a pause on either side
// so kills land before, during and after it.
func (f *flow) effect(ctx runnerq.ActivityContext, step string, stepMS int) (json.RawMessage, error) {
	var begun time.Time
	if err := f.ledger.QueryRow(ctx.Ctx, `SELECT clock_timestamp()`).Scan(&begun); err != nil {
		return nil, err
	}
	half := time.Duration(stepMS) * time.Millisecond / 2
	if err := pause(ctx.Ctx, half); err != nil {
		return nil, err
	}
	nonce := uuid.New()
	if _, err := f.ledger.Exec(ctx.Ctx,
		`INSERT INTO runnerq_chaos_ledger (queue_name, activity_id, kind, step, nonce, worker, begun_at) VALUES ($1, $2, 'effect', $3, $4, $5, $6)`,
		f.queue, ctx.ActivityID, step, nonce, f.worker, begun); err != nil {
		return nil, err
	}
	if err := pause(ctx.Ctx, half); err != nil {
		return nil, err
	}
	return json.Marshal(nonce.String())
}

func pause(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func client(e env) error {
	ctx := context.Background()
	backend, err := postgres.New(ctx, e.dsn, e.queue)
	if err != nil {
		return err
	}
	defer backend.Close()
	engine, err := runnerq.Builder().Backend(backend).Build()
	if err != nil {
		return err
	}
	exec := engine.GetActivityExecutor()
	out := json.NewEncoder(os.Stdout)
	_ = out.Encode(map[string]bool{"ready": true})

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 16<<20)
	for in.Scan() {
		var req struct {
			Op             string          `json:"op"`
			Type           string          `json:"type"`
			Key            string          `json:"key"`
			Payload        json.RawMessage `json:"payload"`
			TimeoutS       int             `json:"timeout_s"`
			MaxRetryDelayS int             `json:"max_retry_delay_s"`
			ID             uuid.UUID       `json:"id"`
			Name           string          `json:"name"`
		}
		if err := json.Unmarshal(in.Bytes(), &req); err != nil {
			return err
		}
		reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		var res any
		switch req.Op {
		case "enqueue":
			var fut *runnerq.ActivityFuture
			fut, err = exec.ActivityNamed(req.Type).
				Payload(req.Payload).
				IdempotencyKeyOption(req.Key, runnerq.ReturnExisting).
				MaxRetries(0).
				Timeout(time.Duration(req.TimeoutS) * time.Second).
				MaxRetryDelay(time.Duration(req.MaxRetryDelayS) * time.Second).
				Execute(reqCtx)
			if err == nil {
				res = map[string]string{"id": fut.ActivityID().String()}
			}
		case "signal":
			err = runnerq.SignalActivity(reqCtx, backend, req.ID, req.Name, req.Payload)
			res = struct{}{}
		default:
			err = fmt.Errorf("unknown op %q", req.Op)
		}
		cancel()
		if err != nil {
			_ = out.Encode(map[string]string{"error": err.Error()})
			continue
		}
		_ = out.Encode(map[string]any{"ok": res})
	}
	return in.Err()
}
