package runnerq

// Stress harness for the batch-claim intake path. Real engines, real
// Postgres, several engines sharing one queue. Skipped unless both
// RUNNERQ_TEST_DSN and RUNNERQ_STRESS=1 are set; it is a sign-off tool, not
// a CI gate:
//
//	RUNNERQ_STRESS=1 RUNNERQ_TEST_DSN=... go test -race -run Stress -v -count=1 .
//
// Each scenario prints a STRESS summary line. Assertions are on invariants,
// not on absolute numbers: every activity terminal exactly as expected, one
// result row each, one claim per execution, a crash strands at most one
// engine's worth of slots, and an idle engine issues (almost) no claims.

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/runnerq/runnerq-go/storage"
	"github.com/runnerq/runnerq-go/storage/postgres"
)

func stressDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" || os.Getenv("RUNNERQ_STRESS") == "" {
		t.Skip("set RUNNERQ_TEST_DSN and RUNNERQ_STRESS=1 to run the stress harness")
	}
	return dsn
}

func stressQueue(name string) string {
	return "s_" + name + "_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
}

// sqlPool is the harness's own connection for assertions and counters.
func sqlPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func xactCommits(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), `SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func statusCounts(t *testing.T, pool *pgxpool.Pool, queue string) map[string]int {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT status, count(*) FROM runnerq_activities WHERE queue_name = $1 GROUP BY status`, queue)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var s string
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			t.Fatal(err)
		}
		out[s] = n
	}
	return out
}

// waitTerminal polls until every activity in the queue is in one of the
// terminal statuses and the total matches, or the deadline passes.
func waitTerminal(t *testing.T, pool *pgxpool.Pool, queue string, total int, timeout time.Duration) map[string]int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		counts := statusCounts(t, pool, queue)
		terminal := counts["completed"] + counts["failed"] + counts["dead_letter"]
		sum := 0
		for _, n := range counts {
			sum += n
		}
		if sum == total && terminal == total {
			return counts
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue %s did not drain in %v: %v (want %d terminal)", queue, timeout, counts, total)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// assertOneResultAndOneClaimPerExecution checks the durable side of
// exactly-once completion: one result row per activity, and as many
// Dequeued events as handler executions (no claim without an execution and
// no execution without a claim).
func assertResultsAndClaims(t *testing.T, pool *pgxpool.Pool, queue string, runs *sync.Map) {
	t.Helper()
	ctx := context.Background()
	var multiResults, noResult int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE r.n > 1), count(*) FILTER (WHERE r.n IS NULL OR r.n = 0)
		FROM runnerq_activities a
		LEFT JOIN (SELECT activity_id, count(*) AS n FROM runnerq_results GROUP BY activity_id) r ON r.activity_id = a.id
		WHERE a.queue_name = $1`, queue).Scan(&multiResults, &noResult); err != nil {
		t.Fatal(err)
	}
	if multiResults != 0 || noResult != 0 {
		t.Fatalf("result rows: %d activities with several, %d with none", multiResults, noResult)
	}
	rows, err := pool.Query(ctx, `SELECT activity_id, count(*) FROM runnerq_events WHERE queue_name = $1 AND event_type = 'Dequeued' GROUP BY activity_id`, queue)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	mismatches := 0
	for rows.Next() {
		var id uuid.UUID
		var claims int
		if err := rows.Scan(&id, &claims); err != nil {
			t.Fatal(err)
		}
		v, ok := runs.Load(id)
		got := 0
		if ok {
			got = int(v.(*atomic.Int32).Load())
		}
		if got != claims {
			mismatches++
			if mismatches <= 5 {
				t.Errorf("activity %s: %d claims but %d handler executions", id, claims, got)
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d activities with claim/execution mismatch", mismatches)
	}
}

type runCounter struct{ runs sync.Map }

func (c *runCounter) hit(id uuid.UUID) int32 {
	v, _ := c.runs.LoadOrStore(id, new(atomic.Int32))
	return v.(*atomic.Int32).Add(1)
}

func (c *runCounter) reruns() (activities, extra int) {
	c.runs.Range(func(_, v any) bool {
		if n := v.(*atomic.Int32).Load(); n > 1 {
			activities++
			extra += int(n - 1)
		}
		return true
	})
	return
}

// singleClaimBackend is the Postgres backend with DequeueBatch hidden, so
// the engine takes the fixed one-claim-per-worker path. Every other optional
// capability stays promoted, which keeps the comparison to the batch path
// fair. The shadowing method has a different signature on purpose.
type singleClaimBackend struct{ *postgres.PostgresBackend }

func (singleClaimBackend) DequeueBatch() {}

// countingBackend counts batch claims and what they returned.
type countingBackend struct {
	*postgres.PostgresBackend
	calls, claimed, maxLimit atomic.Int64
}

func (c *countingBackend) DequeueBatch(ctx context.Context, prefix string, limit int, timeout time.Duration, types []string) ([]storage.DequeuedActivity, error) {
	c.calls.Add(1)
	for {
		cur := c.maxLimit.Load()
		if int64(limit) <= cur || c.maxLimit.CompareAndSwap(cur, int64(limit)) {
			break
		}
	}
	out, err := c.PostgresBackend.DequeueBatch(ctx, prefix, limit, timeout, types)
	c.claimed.Add(int64(len(out)))
	return out, err
}

type stressEngine struct {
	engine  *WorkerEngine
	backend *postgres.PostgresBackend
	done    chan struct{} // closed when Start returns
	err     error
	cancel  context.CancelFunc
}

// startEngines boots n engines on one queue, each with its own backend
// (its own pool and listener), the way separate processes would.
func startEngines(t *testing.T, dsn, queue string, n, workers int, wrap func(*postgres.PostgresBackend) storage.Storage, register func(*WorkerEngine, int), cfg func(*WorkerConfig)) []*stressEngine {
	t.Helper()
	engines := make([]*stressEngine, 0, n)
	for i := range n {
		backend, err := postgres.WithConfig(context.Background(), dsn, queue, 2_000, 15)
		if err != nil {
			t.Fatal(err)
		}
		var st storage.Storage = backend
		if wrap != nil {
			st = wrap(backend)
		}
		c := DefaultWorkerConfig()
		c.QueueName = queue
		c.MaxConcurrentActivities = workers
		if cfg != nil {
			cfg(&c)
		}
		e := NewWorkerEngineWithBackend(st, c)
		register(e, i)
		ctx, cancel := context.WithCancel(context.Background())
		se := &stressEngine{engine: e, backend: backend, done: make(chan struct{}), cancel: cancel}
		go func() { se.err = e.Start(ctx); close(se.done) }()
		engines = append(engines, se)
		t.Cleanup(func() {
			cancel()
			select {
			case <-se.done:
			case <-time.After(15 * time.Second):
			}
			backend.Close()
		})
	}
	return engines
}

func stopEngines(t *testing.T, engines []*stressEngine) {
	t.Helper()
	for _, se := range engines {
		se.engine.Stop()
	}
	for _, se := range engines {
		select {
		case <-se.done:
			if se.err != nil {
				t.Errorf("engine exit: %v", se.err)
			}
		case <-time.After(30 * time.Second):
			t.Error("engine did not stop")
		}
	}
}

func enqueueMany(t *testing.T, e *WorkerEngine, typ string, n int, opts func(*ActivityBuilder, int)) []uuid.UUID {
	t.Helper()
	ids := make([]uuid.UUID, 0, n)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errs := make(chan error, n)
	sem := make(chan struct{}, 16)
	for i := range n {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			b := e.GetActivityExecutor().ActivityNamed(typ).Payload(json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)))
			if opts != nil {
				opts(b, i)
			}
			fut, err := b.Execute(context.Background())
			if err != nil {
				errs <- err
				return
			}
			mu.Lock()
			ids = append(ids, fut.activityID)
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("enqueue: %v", err)
	}
	return ids
}

// ---------------------------------------------------------------------------

// Mixed load on three engines: short work with mixed priorities plus fan-out
// parents that park on slow children. Run once through the batch path and
// once through the fixed pool for a like-for-like comparison.
func TestStressThroughputBatchVsSingle(t *testing.T) {
	dsn := stressDSN(t)
	pool := sqlPool(t, dsn)
	const (
		engines  = 3
		workers  = 10
		children = 5
	)

	run := func(t *testing.T, mode string, wrap func(*postgres.PostgresBackend) storage.Storage, work, parents int) {
		queue := stressQueue(mode)
		counter := &runCounter{}
		register := func(e *WorkerEngine, _ int) {
			e.RegisterActivityWithName("work", &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
				counter.hit(ctx.ActivityID)
				time.Sleep(time.Duration(1+rand.IntN(8)) * time.Millisecond)
				return json.RawMessage(`"ok"`), nil
			}})
			e.RegisterActivityWithName("child", &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
				counter.hit(ctx.ActivityID)
				time.Sleep(2500 * time.Millisecond) // past the 2s await grace: parents park
				return json.RawMessage(`"child"`), nil
			}})
			e.RegisterActivityWithName("parent", &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
				counter.hit(ctx.ActivityID)
				futs := make([]*ActivityFuture, 0, children)
				for i := range children {
					f, err := ctx.ActivityExecutor.ActivityNamed("child").Step(fmt.Sprintf("c%d", i)).Payload(json.RawMessage(`{}`)).Execute(ctx.Ctx)
					if err != nil {
						return nil, err
					}
					futs = append(futs, f)
				}
				for _, f := range futs {
					if _, err := f.GetResult(ctx.Ctx); err != nil {
						return nil, err
					}
				}
				return json.RawMessage(`"fanout-done"`), nil
			}})
		}
		// Load the backlog first through an engine that is never started, so
		// the timed section measures the engines draining it, not the
		// producer's enqueue rate.
		producerBackend, err := postgres.WithConfig(context.Background(), dsn, queue, 2_000, 15)
		if err != nil {
			t.Fatal(err)
		}
		defer producerBackend.Close()
		producerCfg := DefaultWorkerConfig()
		producerCfg.QueueName = queue
		producer := NewWorkerEngineWithBackend(producerBackend, producerCfg)
		enqueueStart := time.Now()
		enqueueMany(t, producer, "parent", parents, nil)
		enqueueMany(t, producer, "work", work, func(b *ActivityBuilder, i int) {
			b.Priority(ActivityPriority(1 + i%4))
		})
		enqueued := time.Since(enqueueStart)

		before := xactCommits(t, pool)
		start := time.Now()
		es := startEngines(t, dsn, queue, engines, workers, wrap, register, nil)
		total := work + parents + parents*children
		counts := waitTerminal(t, pool, queue, total, 3*time.Minute)
		elapsed := time.Since(start)
		commits := xactCommits(t, pool) - before
		stopEngines(t, es)

		if counts["completed"] != total {
			t.Fatalf("statuses %v, want %d completed", counts, total)
		}
		assertResultsAndClaims(t, pool, queue, &counter.runs)
		reranActivities, _ := counter.reruns()
		// Only parents legitimately run more than once (park + resume).
		if reranActivities > parents {
			t.Fatalf("%d activities executed more than once; only the %d parking parents may", reranActivities, parents)
		}
		t.Logf("STRESS %-6s activities=%d drain=%.1fs (preload %.1fs) drain_rate=%.0f/s commits/activity=%.2f reran=%d/%d parents",
			mode, total, elapsed.Seconds(), enqueued.Seconds(), float64(total)/elapsed.Seconds(), float64(commits)/float64(total), reranActivities, parents)
	}

	single := func(b *postgres.PostgresBackend) storage.Storage { return singleClaimBackend{b} }
	// Mixed load: correctness under parking parents plus short work. Wall
	// time here is dominated by the 2.5s children, so read the commits per
	// activity, not the throughput.
	t.Run("mixed/batch", func(t *testing.T) { run(t, "batch", nil, 6000, 20) })
	t.Run("mixed/single", func(t *testing.T) { run(t, "single", single, 6000, 20) })
	// Work only: the claim path is the whole story.
	t.Run("work/batch", func(t *testing.T) { run(t, "batch", nil, 12000, 0) })
	t.Run("work/single", func(t *testing.T) { run(t, "single", single, 12000, 0) })
}

// One engine of three hangs mid-run: its in-flight handlers never return
// and never ack. Leases expire, the reaper requeues, the survivors finish.
// Because the batch dispatcher claims only what it can run right now, the
// crash strands at most one engine's worth of slots.
func TestStressCrashRecovery(t *testing.T) {
	dsn := stressDSN(t)
	pool := sqlPool(t, dsn)
	const (
		engines = 3
		workers = 8
		work    = 3000
		victim  = 1
	)
	queue := stressQueue("crash")
	counter := &runCounter{}
	crashed := make(chan struct{})
	var stranded atomic.Int32
	register := func(e *WorkerEngine, idx int) {
		e.RegisterActivityWithName("work", &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
			counter.hit(ctx.ActivityID)
			select {
			case <-time.After(time.Duration(20+rand.IntN(30)) * time.Millisecond):
			case <-crashed:
				if idx == victim {
					stranded.Add(1)
					select {} // hung process: never returns, never acks
				}
			}
			select {
			case <-crashed:
				if idx == victim {
					stranded.Add(1)
					select {}
				}
			default:
			}
			return json.RawMessage(`"ok"`), nil
		}})
	}
	es := startEngines(t, dsn, queue, engines, workers, nil, register, func(c *WorkerConfig) {
		lease, reaper, grace := uint64(2_000), uint64(1), uint64(1)
		c.LeaseMS, c.ReaperIntervalSeconds, c.ShutdownGraceSeconds = &lease, &reaper, &grace
	})
	start := time.Now()
	enqueueMany(t, es[0].engine, "work", work, func(b *ActivityBuilder, _ int) { b.Timeout(time.Second) })

	time.Sleep(1500 * time.Millisecond)
	close(crashed)
	es[victim].cancel() // the process is gone: intake stops, drain gives up after the 1s grace
	select {
	case <-es[victim].done:
	case <-time.After(10 * time.Second):
		t.Fatal("crashed engine's Start did not return")
	}
	crashedAt := time.Since(start)

	// Lease = max(2s floor, 1s timeout + 10s) = 11s, then the survivors'
	// reapers (1s tick) requeue and the work finishes.
	counts := waitTerminal(t, pool, queue, work, 90*time.Second)
	elapsed := time.Since(start)
	stopEngines(t, es[:victim])
	stopEngines(t, es[victim+1:])

	if counts["completed"] != work {
		t.Fatalf("statuses %v, want %d completed", counts, work)
	}
	assertResultsAndClaims(t, pool, queue, &counter.runs)
	reran, extra := counter.reruns()
	if reran > workers || extra != reran {
		t.Fatalf("%d activities re-executed (%d extra runs); a crash may strand at most %d and each should rerun once", reran, extra, workers)
	}
	if int(stranded.Load()) != reran {
		t.Fatalf("%d handlers hung on the crashed engine but %d activities were re-executed", stranded.Load(), reran)
	}
	var doubleCompleted int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM (SELECT activity_id FROM runnerq_events WHERE queue_name = $1 AND event_type = 'Completed' GROUP BY activity_id HAVING count(*) > 1) d`, queue).Scan(&doubleCompleted); err != nil {
		t.Fatal(err)
	}
	if doubleCompleted != 0 {
		t.Fatalf("%d activities completed twice", doubleCompleted)
	}
	t.Logf("STRESS crash  activities=%d crashed_at=%.1fs elapsed=%.1fs stranded=%d rerun=%d (max %d) double_completions=0",
		work, crashedAt.Seconds(), elapsed.Seconds(), stranded.Load(), reran, workers)
}

// An idle engine must not spin on claims, and a burst must be claimed in
// far fewer round trips than activities, never more than its slots at once.
func TestStressIdleAndBurstClaimRate(t *testing.T) {
	dsn := stressDSN(t)
	pool := sqlPool(t, dsn)
	const (
		workers = 10
		burst   = 2000
	)
	queue := stressQueue("burst")
	var cb *countingBackend
	counter := &runCounter{}
	es := startEngines(t, dsn, queue, 1, workers,
		func(b *postgres.PostgresBackend) storage.Storage {
			cb = &countingBackend{PostgresBackend: b}
			return cb
		},
		func(e *WorkerEngine, _ int) {
			e.RegisterActivityWithName("work", &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
				counter.hit(ctx.ActivityID)
				time.Sleep(2 * time.Millisecond)
				return nil, nil
			}})
		}, nil)

	time.Sleep(5 * time.Second)
	idleCalls := cb.calls.Load()
	if idleCalls > 3 {
		t.Fatalf("idle engine issued %d batch claims in 5s; the dispatcher must park, not spin", idleCalls)
	}

	before := cb.calls.Load()
	start := time.Now()
	enqueueMany(t, es[0].engine, "work", burst, nil)
	counts := waitTerminal(t, pool, queue, burst, 2*time.Minute)
	elapsed := time.Since(start)
	calls := cb.calls.Load() - before
	stopEngines(t, es)

	if counts["completed"] != burst {
		t.Fatalf("statuses %v", counts)
	}
	assertResultsAndClaims(t, pool, queue, &counter.runs)
	if cb.maxLimit.Load() > workers {
		t.Fatalf("a claim asked for %d activities with %d slots", cb.maxLimit.Load(), workers)
	}
	if cb.claimed.Load() != burst {
		t.Fatalf("claimed %d activities in total, want %d", cb.claimed.Load(), burst)
	}
	if calls > burst/2 {
		t.Fatalf("%d claim round trips for %d activities; batching is not happening", calls, burst)
	}
	t.Logf("STRESS burst  idle_claims_5s=%d activities=%d claim_calls=%d activities/claim=%.1f max_limit=%d elapsed=%.1fs",
		idleCalls, burst, calls, float64(burst)/float64(calls), cb.maxLimit.Load(), elapsed.Seconds())
}

// Retries, non-retryable failures and dead letters all flow back through the
// batch claim; the final ledger must match the injected mix exactly.
func TestStressFailureMix(t *testing.T) {
	dsn := stressDSN(t)
	pool := sqlPool(t, dsn)
	const (
		total   = 1500
		workers = 8
	)
	queue := stressQueue("mix")
	counter := &runCounter{}
	kind := func(i int) string {
		switch {
		case i%20 == 0:
			return "dead" // always fails, MaxRetries 2 → dead_letter after 2 attempts
		case i%20 == 1:
			return "nonretry"
		case i%10 == 2:
			return "flaky" // fails once, then succeeds
		default:
			return "ok"
		}
	}
	es := startEngines(t, dsn, queue, 2, workers, nil, func(e *WorkerEngine, _ int) {
		e.RegisterActivityWithName("work", &funcHandler{fn: func(ctx ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
			n := counter.hit(ctx.ActivityID)
			var p struct{ I int }
			_ = json.Unmarshal(payload, &p)
			switch kind(p.I) {
			case "dead":
				return nil, NewRetryError("always")
			case "nonretry":
				return nil, NewNonRetryError("declined")
			case "flaky":
				if n == 1 {
					return nil, NewRetryError("first attempt")
				}
			}
			return json.RawMessage(`"ok"`), nil
		}})
	}, func(c *WorkerConfig) {
		lease := uint64(2_000)
		c.LeaseMS = &lease
	})
	want := map[string]int{}
	for i := range total {
		switch kind(i) {
		case "dead":
			want["dead_letter"]++
		case "nonretry":
			want["failed"]++
		default:
			want["completed"]++
		}
	}
	start := time.Now()
	enqueueMany(t, es[0].engine, "work", total, func(b *ActivityBuilder, _ int) {
		b.MaxRetries(2).MaxRetryDelay(500 * time.Millisecond)
	})
	counts := waitTerminal(t, pool, queue, total, 2*time.Minute)
	elapsed := time.Since(start)
	stopEngines(t, es)
	for status, n := range want {
		if counts[status] != n {
			t.Fatalf("status %s = %d, want %d (all: %v)", status, counts[status], n, counts)
		}
	}
	assertResultsAndClaims(t, pool, queue, &counter.runs)
	t.Logf("STRESS mix    activities=%d completed=%d failed=%d dead_letter=%d elapsed=%.1fs",
		total, counts["completed"], counts["failed"], counts["dead_letter"], elapsed.Seconds())
}
