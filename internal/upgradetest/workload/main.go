// Command workload is the upgrade test's workload (see the package test).
// The same source is built against the previous SDK, which runs -phase park,
// and against this one, which runs -phase finish on the same database.
//
// It uses only API the previous SDK also has; the test rewrites the module
// path in its imports when the previous SDK had another one.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/runnerq/runnerq-go"
	"github.com/runnerq/runnerq-go/storage/postgres"
)

const (
	queue = "upgrade"

	typeSleeper = "upgrade_sleeper"
	typeWaiter  = "upgrade_waiter"
	typeParent  = "upgrade_parent"
	typeChild   = "upgrade_child"
	typeSimple  = "upgrade_simple"

	// handlerTimeout is short so the waits below park the activity rather
	// than wait in the handler.
	handlerTimeout = 5 * time.Second
	sleepFor       = 10 * time.Second
)

// state is what the park phase hands the finish phase.
type state struct {
	Sleeper   uuid.UUID `json:"sleeper"`
	Waiter    uuid.UUID `json:"waiter"`
	Parent    uuid.UUID `json:"parent"`
	Scheduled uuid.UUID `json:"scheduled"`
	Pending   uuid.UUID `json:"pending"`
}

type input struct {
	Label string `json:"label"`
}

// effects records each step's side effect in upgrade_effects, so the finish
// phase can check that no recorded step ran twice across the upgrade.
type effects struct{ db *pgxpool.Pool }

func (e effects) step(ctx runnerq.ActivityContext, payload json.RawMessage, step string) error {
	var in input
	if err := json.Unmarshal(payload, &in); err != nil {
		return runnerq.NewNonRetryError(err.Error())
	}
	_, err := ctx.Run(step, func() (json.RawMessage, error) {
		if _, err := e.db.Exec(ctx.Ctx, `INSERT INTO upgrade_effects (label, step) VALUES ($1, $2)`, in.Label, step); err != nil {
			return nil, err
		}
		return json.RawMessage(`true`), nil
	})
	return err
}

type sleeper struct{ effects }

func (h *sleeper) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	if err := h.step(ctx, payload, "before"); err != nil {
		return nil, err
	}
	if err := ctx.Sleep("nap", sleepFor); err != nil {
		return nil, err
	}
	if err := h.step(ctx, payload, "after"); err != nil {
		return nil, err
	}
	return json.RawMessage(`"slept"`), nil
}

func (h *sleeper) OnDeadLetter(runnerq.ActivityContext, json.RawMessage, string) {}

type waiter struct{ effects }

func (h *waiter) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	if err := h.step(ctx, payload, "before"); err != nil {
		return nil, err
	}
	got, err := ctx.WaitForSignal("go", 10*time.Minute)
	if err != nil {
		return nil, err
	}
	if err := h.step(ctx, payload, "after"); err != nil {
		return nil, err
	}
	return got, nil
}

func (h *waiter) OnDeadLetter(runnerq.ActivityContext, json.RawMessage, string) {}

type parent struct{ effects }

func (h *parent) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	if err := h.step(ctx, payload, "before"); err != nil {
		return nil, err
	}
	fut, err := ctx.ActivityExecutor.ActivityNamed(typeChild).Step("child").
		Payload(json.RawMessage(`{"label":"child"}`)).Execute(ctx.Ctx)
	if err != nil {
		return nil, err
	}
	res, err := fut.GetResult(ctx.Ctx)
	if err != nil {
		return nil, err
	}
	if err := h.step(ctx, payload, "after"); err != nil {
		return nil, err
	}
	return res, nil
}

func (h *parent) OnDeadLetter(runnerq.ActivityContext, json.RawMessage, string) {}

type simple struct {
	effects
	result string
}

func (h *simple) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	if err := h.step(ctx, payload, "work"); err != nil {
		return nil, err
	}
	return json.Marshal(h.result)
}

func (h *simple) OnDeadLetter(runnerq.ActivityContext, json.RawMessage, string) {}

func main() {
	phase := flag.String("phase", "", "park (previous SDK) or finish (current SDK)")
	dsn := flag.String("dsn", "", "Postgres DSN")
	statePath := flag.String("state", "", "file the park phase writes and the finish phase reads")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var err error
	switch *phase {
	case "park":
		err = park(ctx, *dsn, *statePath)
	case "finish":
		err = finish(ctx, *dsn, *statePath)
	default:
		err = fmt.Errorf("unknown -phase %q", *phase)
	}
	if err != nil {
		log.Fatalf("%s: %v", *phase, err)
	}
	log.Printf("%s: ok", *phase)
}

type worker struct {
	db      *pgxpool.Pool
	backend *postgres.PostgresBackend
	engine  *runnerq.WorkerEngine
	done    chan struct{}
}

// start runs an engine on queue that claims only types.
func start(ctx context.Context, dsn string, types []string) (*worker, error) {
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	backend, err := postgres.New(ctx, dsn, queue)
	if err != nil {
		db.Close()
		return nil, err
	}
	engine, err := runnerq.Builder().Backend(backend).MaxWorkers(8).ActivityTypes(types).Build()
	if err != nil {
		backend.Close()
		db.Close()
		return nil, err
	}
	fx := effects{db}
	engine.RegisterActivityWithName(typeSleeper, &sleeper{fx})
	engine.RegisterActivityWithName(typeWaiter, &waiter{fx})
	engine.RegisterActivityWithName(typeParent, &parent{fx})
	engine.RegisterActivityWithName(typeChild, &simple{fx, "child done"})
	engine.RegisterActivityWithName(typeSimple, &simple{fx, "done"})

	w := &worker{db: db, backend: backend, engine: engine, done: make(chan struct{})}
	go func() {
		defer close(w.done)
		if err := engine.Start(ctx); err != nil {
			log.Printf("engine: %v", err)
		}
	}()
	return w, nil
}

func (w *worker) stop() {
	w.engine.Stop()
	<-w.done
	w.backend.Close()
	w.db.Close()
}

// park enqueues one activity per parked state and stops once each is there:
// sleeping, waiting on a signal, waiting on a pending child, scheduled, and
// pending. The child and plain types are not claimed, so they stay put.
func park(ctx context.Context, dsn, statePath string) error {
	w, err := start(ctx, dsn, []string{typeSleeper, typeWaiter, typeParent})
	if err != nil {
		return err
	}
	defer w.stop()

	exec := w.engine.GetActivityExecutor()
	enqueue := func(activityType, label string, delay time.Duration) (uuid.UUID, error) {
		b := exec.ActivityNamed(activityType).Timeout(handlerTimeout).
			Payload(json.RawMessage(fmt.Sprintf(`{"label":%q}`, label)))
		if delay > 0 {
			b = b.Delay(delay)
		}
		fut, err := b.Execute(ctx)
		if err != nil {
			return uuid.Nil, fmt.Errorf("enqueue %s: %w", label, err)
		}
		return fut.ActivityID(), nil
	}

	var s state
	for _, e := range []struct {
		id    *uuid.UUID
		typ   string
		label string
		delay time.Duration
	}{
		{&s.Sleeper, typeSleeper, "sleeper", 0},
		{&s.Waiter, typeWaiter, "waiter", 0},
		{&s.Parent, typeParent, "parent", 0},
		{&s.Scheduled, typeSimple, "scheduled", 5 * time.Second},
		{&s.Pending, typeSimple, "pending", 0},
	} {
		if *e.id, err = enqueue(e.typ, e.label, e.delay); err != nil {
			return err
		}
	}

	want := map[uuid.UUID]string{
		s.Sleeper:   "waiting",
		s.Waiter:    "waiting",
		s.Parent:    "waiting",
		s.Scheduled: "scheduled",
		s.Pending:   "pending",
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		got, err := statuses(ctx, w.db)
		if err != nil {
			return err
		}
		parked := true
		for id, status := range want {
			if got.set[id] != status {
				parked = false
			}
		}
		// The parent's child exists and is still pending.
		children := 0
		for _, row := range got.rows {
			if row.parent == s.Parent {
				children++
				if row.status != "pending" {
					parked = false
				}
			}
		}
		if children != 1 {
			parked = false
		}
		if parked {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("activities did not park: %v", got.rows)
		}
		time.Sleep(200 * time.Millisecond)
	}

	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return os.WriteFile(statePath, data, 0o600)
}

// finish runs every type on the parked work, signals the waiter, and checks
// that each activity completes with its result and every step's effect
// happened exactly once.
func finish(ctx context.Context, dsn, statePath string) error {
	data, err := os.ReadFile(statePath) // #nosec G304 -- the test passes its own temp file
	if err != nil {
		return err
	}
	var s state
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	w, err := start(ctx, dsn, []string{typeSleeper, typeWaiter, typeParent, typeChild, typeSimple})
	if err != nil {
		return err
	}
	defer w.stop()

	if err := w.engine.Signal(ctx, s.Waiter, "go", json.RawMessage(`"approved"`)); err != nil {
		return fmt.Errorf("signal waiter: %w", err)
	}

	var errs []error
	for id, want := range map[uuid.UUID]string{
		s.Sleeper:   `"slept"`,
		s.Waiter:    `"approved"`,
		s.Parent:    `"child done"`,
		s.Scheduled: `"done"`,
		s.Pending:   `"done"`,
	} {
		res, err := runnerq.FutureFor(w.backend, id).GetResult(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("activity %s: %w", id, err))
		} else if string(res) != want {
			errs = append(errs, fmt.Errorf("activity %s: result %s, want %s", id, res, want))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	got, err := statuses(ctx, w.db)
	if err != nil {
		return err
	}
	if len(got.rows) != 6 {
		errs = append(errs, fmt.Errorf("%d activities, want 6: %v", len(got.rows), got.rows))
	}
	for _, row := range got.rows {
		if row.status != "completed" {
			errs = append(errs, fmt.Errorf("activity %s is %s, want completed", row.id, row.status))
		}
	}

	wantEffects := map[string]int{
		"sleeper/before": 1, "sleeper/after": 1,
		"waiter/before": 1, "waiter/after": 1,
		"parent/before": 1, "parent/after": 1,
		"child/work": 1, "scheduled/work": 1, "pending/work": 1,
	}
	rows, err := w.db.Query(ctx, `SELECT label || '/' || step, count(*) FROM upgrade_effects GROUP BY 1`)
	if err != nil {
		return err
	}
	gotEffects := map[string]int{}
	for rows.Next() {
		var k string
		var n int
		if err := rows.Scan(&k, &n); err != nil {
			return err
		}
		gotEffects[k] = n
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for k, n := range wantEffects {
		if gotEffects[k] != n {
			errs = append(errs, fmt.Errorf("effect %s ran %d times, want %d", k, gotEffects[k], n))
		}
	}
	for k := range gotEffects {
		if _, ok := wantEffects[k]; !ok {
			errs = append(errs, fmt.Errorf("unexpected effect %s", k))
		}
	}
	return errors.Join(errs...)
}

type activityRow struct {
	id, parent uuid.UUID
	status     string
}

func (r activityRow) String() string { return r.id.String() + "=" + r.status }

// statuses reads the queue's activity rows directly: the previous SDK may
// not offer the query API this one does.
func statuses(ctx context.Context, db *pgxpool.Pool) (statusMap, error) {
	rows, err := db.Query(ctx, `SELECT id, COALESCE(parent_activity_id, '00000000-0000-0000-0000-000000000000'::uuid), status
		FROM runnerq_activities WHERE queue_name = $1`, queue)
	if err != nil {
		return statusMap{}, err
	}
	defer rows.Close()
	m := statusMap{set: map[uuid.UUID]string{}}
	for rows.Next() {
		var r activityRow
		if err := rows.Scan(&r.id, &r.parent, &r.status); err != nil {
			return statusMap{}, err
		}
		m.set[r.id] = r.status
		m.rows = append(m.rows, r)
	}
	return m, rows.Err()
}

// statusMap is the queue's activity statuses by ID, and its rows.
type statusMap struct {
	set  map[uuid.UUID]string
	rows []activityRow
}
