package runnerq

// Engine-level schedule tests. Skipped unless RUNNERQ_TEST_DSN is set.

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/runnerq/runnerq-go/storage"
	"github.com/runnerq/runnerq-go/storage/postgres"
)

func scheduleTestBackend(t *testing.T, queue string) *postgres.PostgresBackend {
	t.Helper()
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping integration test")
	}
	b, err := postgres.New(context.Background(), dsn, queue)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(b.Close)
	return b
}

func TestRegisterScheduleValidates(t *testing.T) {
	b := scheduleTestBackend(t, "t_"+strings.ReplaceAll(uuid.NewString(), "-", "")[:16])
	engine, _ := Builder().Backend(b).Build()
	for _, s := range []Schedule{
		{Name: "x", Cron: "61 * * * *", Activity: "t"},
		{Name: "x", Cron: "@daily", Timezone: "Nowhere/Else", Activity: "t"},
		{Name: "x", Cron: "@daily"},
		{Cron: "@daily", Activity: "t"},
		{Name: "x", Cron: "@daily", Activity: "t", CatchUp: "maybe"},
	} {
		var se *storage.StorageError
		if err := engine.RegisterSchedule(s); !errors.As(err, &se) || se.Kind != storage.ErrInvalidArgument {
			t.Errorf("RegisterSchedule(%+v) = %v, want invalid argument", s, err)
		}
	}
	if err := engine.RegisterSchedule(Schedule{Name: "x", Cron: "@daily", Activity: "t"}); err != nil {
		t.Fatal(err)
	}
	if err := engine.RegisterSchedule(Schedule{Name: "x", Cron: "@hourly", Activity: "t"}); err == nil {
		t.Fatal("a duplicate schedule name was accepted")
	}
}

// Two engines declare the same every-second schedule; one is stopped midway.
// Every tick between the first and last fire runs exactly once.
func TestEnginesFireEachTickOnce(t *testing.T) {
	queue := "t_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	var mu sync.Mutex
	runs := map[string]int{}
	handler := &funcHandler{fn: func(ctx ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
		mu.Lock()
		runs[ctx.ActivityID.String()]++
		mu.Unlock()
		return nil, nil
	}}
	maxRetries := uint32(1)
	decl := Schedule{Name: "tick", Cron: "* * * * * *", Activity: "tick", Payload: json.RawMessage(`{"a":1}`),
		MaxRetries: &maxRetries, CatchUp: storage.CatchUpAll}

	var engines []*WorkerEngine
	var stopped []chan error
	for range 2 {
		b := scheduleTestBackend(t, queue)
		e, err := Builder().Backend(b).QueueName(queue).MaxWorkers(4).ShutdownGrace(2 * time.Second).Build()
		if err != nil {
			t.Fatal(err)
		}
		e.scheduleInterval = 150 * time.Millisecond
		e.RegisterActivityWithName("tick", handler)
		if err := e.RegisterSchedule(decl); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- e.Start(context.Background()) }()
		engines = append(engines, e)
		stopped = append(stopped, done)
	}
	time.Sleep(2 * time.Second)
	engines[0].Stop()
	<-stopped[0]
	time.Sleep(2 * time.Second)
	if err := engines[1].Schedules().Pause(context.Background(), "tick"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	engines[1].Stop()
	<-stopped[1]

	conn, err := pgx.Connect(context.Background(), os.Getenv("RUNNERQ_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(context.Background(), `SELECT id, idempotency_key, status FROM runnerq_activities WHERE queue_name = $1 ORDER BY idempotency_key`, queue)
	if err != nil {
		t.Fatal(err)
	}
	var fires []time.Time
	for rows.Next() {
		var id uuid.UUID
		var key, status string
		if err := rows.Scan(&id, &key, &status); err != nil {
			t.Fatal(err)
		}
		app := storage.ApplicationIdempotencyKey(key, "tick")
		at, err := time.Parse(time.RFC3339, strings.TrimPrefix(app, "schedule:tick:"))
		if err != nil {
			t.Fatalf("key %q: %v", app, err)
		}
		fires = append(fires, at)
		if status != "completed" || runs[id.String()] != 1 {
			t.Errorf("fire %s: status %s, ran %d times", at, status, runs[id.String()])
		}
	}
	if len(fires) < 3 {
		t.Fatalf("only %d fires", len(fires))
	}
	for i := 1; i < len(fires); i++ {
		if d := fires[i].Sub(fires[i-1]); d != time.Second {
			t.Fatalf("fires %v and %v are %v apart: a tick was missed or doubled", fires[i-1], fires[i], d)
		}
	}
}
