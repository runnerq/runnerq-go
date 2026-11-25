package runnerq

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// A per-worker cap bounds how many of the type one claim asks for, leaves a
// type at its cap out of the claim, and restarts a blocked claim when the
// type has room again.
func TestPerWorkerCapNarrowsBatchClaims(t *testing.T) {
	q := newScriptedBatchQueue()
	h := newGatedHandler()
	cfg := DefaultWorkerConfig()
	cfg.MaxConcurrentActivities = 3
	e := &WorkerEngine{queue: q, handlers: map[string]ActivityHandler{"a": h, "b": h}, metrics: NoopMetrics{}, config: cfg,
		servedTypes: []string{"a", "b"}, local: newLocalLimits(map[string]int{"a": 1})}
	e.running.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	handlerCtx, cancelHandlers := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { defer close(stopped); e.runBatchDispatcher(ctx, handlerCtx, q) }()
	t.Cleanup(func() { e.running.Store(false); cancel(); cancelHandlers(); <-stopped })

	first := receive(t, q.requests)
	if first.limit != 1 || !slices.Equal(first.types, []string{"a", "b"}) {
		t.Fatalf("first claim = %d of %v, want 1 of [a b]", first.limit, first.types)
	}
	a := claimedActivity{activity: newActivity("a", nil, nil), leaseID: uuid.NewString()}
	q.replies <- batchReply{claims: []claimedActivity{a}}
	receive(t, h.started)

	second := receive(t, q.requests)
	if second.limit != 2 || !slices.Equal(second.types, []string{"b"}) {
		t.Fatalf("with a at its cap the claim = %d of %v, want 2 of [b]", second.limit, second.types)
	}
	// a finishes while the claim for b blocks: the claim starts over with a.
	h.releaseN(1)
	receive(t, q.completed)
	third := receive(t, q.requests)
	if third.limit != 1 || !slices.Equal(third.types, []string{"a", "b"}) {
		t.Fatalf("after a finished the claim = %d of %v, want 1 of [a b]", third.limit, third.types)
	}
}

func TestRegisterOptionsRejectNonPositive(t *testing.T) {
	for name, f := range map[string]func(){
		"MaxConcurrent":          func() { MaxConcurrent(0) },
		"RateLimit count":        func() { RateLimit(0, time.Second) },
		"RateLimit period":       func() { RateLimit(1, 0) },
		"MaxConcurrentPerWorker": func() { MaxConcurrentPerWorker(-1) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s accepted a non-positive value", name)
				}
			}()
			f()
		}()
	}
}

func limitsTestQueue() string {
	return "t_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
}

// Two engines serve a type limited to 2: Start stores the declared limit, and
// between them they never run more than 2 at once.
func TestEnginesShareAConcurrencyLimit(t *testing.T) {
	queue := limitsTestQueue()
	var running, peak atomic.Int64
	handler := &funcHandler{fn: func(ActivityContext, json.RawMessage) (json.RawMessage, error) {
		n := running.Add(1)
		defer running.Add(-1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(40 * time.Millisecond)
		return nil, nil
	}}
	client := NewClient(scheduleTestBackend(t, queue))
	var futures []*ActivityFuture
	for range 12 {
		f, err := client.ActivityNamed("send").Payload(json.RawMessage(`{}`)).Execute(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		futures = append(futures, f)
	}
	var wg sync.WaitGroup
	var engines []*WorkerEngine
	for range 2 {
		e, err := Builder().Backend(scheduleTestBackend(t, queue)).MaxWorkers(5).ShutdownGrace(2 * time.Second).Build()
		if err != nil {
			t.Fatal(err)
		}
		e.RegisterActivityWithName("send", handler, MaxConcurrent(2), RateLimit(100, time.Second))
		engines = append(engines, e)
		wg.Go(func() {
			if err := e.Start(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, f := range futures {
		if _, err := f.GetResult(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range engines {
		e.Stop()
	}
	wg.Wait()
	if p := peak.Load(); p > 2 {
		t.Fatalf("%d ran at once, over the limit of 2", p)
	}
	got, err := client.GetLimit(context.Background(), "send")
	if err != nil || got.MaxConcurrent != 2 || got.RateLimit != 100 || got.RatePeriod != time.Second {
		t.Fatalf("stored limit = %+v, %v", got, err)
	}
}

// MaxConcurrentPerWorker caps one type on this engine while the other types
// use the rest of its workers.
func TestPerWorkerCapLeavesRoomForOtherTypes(t *testing.T) {
	queue := limitsTestQueue()
	var mu sync.Mutex
	running, peak := map[string]int{}, map[string]int{}
	handler := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		typ := ctx.ActivityType
		mu.Lock()
		running[typ]++
		peak[typ] = max(peak[typ], running[typ])
		mu.Unlock()
		time.Sleep(60 * time.Millisecond)
		mu.Lock()
		running[typ]--
		mu.Unlock()
		return nil, nil
	}}
	b := scheduleTestBackend(t, queue)
	client := NewClient(b)
	var futures []*ActivityFuture
	for i := range 16 {
		typ := "slow"
		if i%2 == 1 {
			typ = "fast"
		}
		f, err := client.ActivityNamed(typ).Payload(json.RawMessage(`{}`)).Execute(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		futures = append(futures, f)
	}
	e, err := Builder().Backend(b).MaxWorkers(6).ShutdownGrace(2 * time.Second).Build()
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterActivityWithName("slow", handler, MaxConcurrentPerWorker(2))
	e.RegisterActivityWithName("fast", handler)
	done := make(chan error, 1)
	go func() { done <- e.Start(context.Background()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, f := range futures {
		if _, err := f.GetResult(ctx); err != nil {
			t.Fatal(err)
		}
	}
	e.Stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if peak["slow"] > 2 || peak["fast"] < 3 {
		t.Fatalf("peaks %v: want slow at most 2 and fast using the other workers", peak)
	}
	if _, err := client.GetLimit(context.Background(), "slow"); !isNotFound(err) {
		t.Fatalf("a per-worker cap was stored: %v", err)
	}
}

func TestClientManagesLimits(t *testing.T) {
	client := NewClient(scheduleTestBackend(t, limitsTestQueue()))
	ctx := context.Background()
	if err := client.SetLimit(ctx, Limit{ActivityType: "a", MaxConcurrent: 3}); err != nil {
		t.Fatal(err)
	}
	if err := client.SetLimit(ctx, Limit{ActivityType: "b", RateLimit: 2, RatePeriod: time.Minute}); err != nil {
		t.Fatal(err)
	}
	list, err := client.ListLimits(ctx)
	if err != nil || len(list) != 2 || list[0].MaxConcurrent != 3 || list[1].RatePeriod != time.Minute {
		t.Fatalf("ListLimits = %+v, %v", list, err)
	}
	var se *storage.StorageError
	if err := client.SetLimit(ctx, Limit{ActivityType: "c"}); !errors.As(err, &se) || se.Kind != storage.ErrInvalidArgument {
		t.Fatalf("SetLimit without a limit = %v", err)
	}
	if err := client.DeleteLimit(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetLimit(ctx, "a"); !isNotFound(err) {
		t.Fatalf("GetLimit after delete = %v", err)
	}
	if err := NewClient(&batchStorageStub{}).SetLimit(ctx, Limit{ActivityType: "a", MaxConcurrent: 1}); err == nil {
		t.Fatal("a backend without limits accepted one")
	}
}

func isNotFound(err error) bool {
	var se *storage.StorageError
	return errors.As(err, &se) && se.Kind == storage.ErrNotFound
}
