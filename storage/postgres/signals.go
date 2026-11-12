package postgres

// Cross-process wake-ups over LISTEN/NOTIFY; the tables stay the source of truth.
//
//  1. NOTIFY never runs inside a data transaction: Postgres serializes every
//     NOTIFY-carrying commit on one global lock, capping cluster-wide commit
//     throughput. Hot paths report AFTER commit and one goroutine per backend
//     flushes small batched notifications outside any transaction.
//  2. Waiters may live in another process than the producer (e.g. a server
//     calling GetResult with only a backend handle), so wake-up state lives in
//     the backend (one LISTEN connection + in-process fan-out), not the engine.
//  3. Notifications are hints: connections drop and signals can be lost
//     between a waiter's check and its park, so every wait path re-checks the
//     tables on a slow fallback interval.
//
// Channels (queue names are capped at 48 chars, keeping these under the 63-byte
// identifier limit):
//
//	rq_w_<queue> — edge trigger: runnable work was committed
//	rq_r_<queue> — payload: comma-joined activity IDs whose results committed

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/runnerq/runnerq-go/internal/spec"
	"github.com/runnerq/runnerq-go/storage"
)

const (
	// Bounds the per-process NOTIFY rate at ~20/sec/channel regardless of
	// throughput, and the wake-up latency it adds.
	signalFlushInterval = 50 * time.Millisecond

	// Covers lost work signals and scheduled/retrying rows coming due, which
	// emit no signal.
	workWaitProbe = 2 * time.Second

	resultWaitFallback = 5 * time.Second

	// Bounds a claim statement that no longer follows its caller's context.
	claimStatementTimeout = 30 * time.Second

	// Keeps payloads under the 8KB NOTIFY limit (36-byte UUIDs plus separators).
	resultIDsPerNotify = 200
)

func (b *PostgresBackend) workChannel() string   { return spec.WorkChannelPrefix + b.queueName }
func (b *PostgresBackend) resultChannel() string { return spec.ResultChannelPrefix + b.queueName }

type signalKind int

const (
	sigWork signalKind = iota
	sigResult
)

type signalMsg struct {
	kind signalKind
	id   uuid.UUID
}

type signaler struct {
	b      *PostgresBackend
	ch     chan signalMsg
	cancel context.CancelFunc
	done   chan struct{}
}

func newSignaler(b *PostgresBackend) *signaler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &signaler{
		b:      b,
		ch:     make(chan signalMsg, 4096),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go s.run(ctx)
	return s
}

func (s *signaler) stop() {
	s.cancel()
	<-s.done
}

// send never blocks the hot path: a full buffer drops the signal and
// receivers recover via their fallback probes.
func (s *signaler) send(m signalMsg) {
	select {
	case s.ch <- m:
	default:
	}
}

// run arms a timer only when there is something to send, so an idle backend
// runs no timer.
func (s *signaler) run(ctx context.Context) {
	defer close(s.done)
	var (
		workPending bool
		resultIDs   = make(map[uuid.UUID]struct{})
		lastFlush   time.Time
		timer       = time.NewTimer(0)
		due         <-chan time.Time // non-nil while a flush is scheduled
	)
	timer.Stop()
	defer timer.Stop()

	flush := func() {
		if !workPending && len(resultIDs) == 0 {
			return
		}
		batch := &pgx.Batch{}
		if workPending {
			batch.Queue(`SELECT pg_notify($1, '')`, s.b.workChannel())
		}
		if len(resultIDs) > 0 {
			ids := make([]string, 0, len(resultIDs))
			for id := range resultIDs {
				ids = append(ids, id.String())
			}
			for start := 0; start < len(ids); start += resultIDsPerNotify {
				end := min(start+resultIDsPerNotify, len(ids))
				batch.Queue(`SELECT pg_notify($1, $2)`, s.b.resultChannel(), strings.Join(ids[start:end], ","))
			}
		}
		// A failed flush is harmless: receivers re-probe.
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := s.b.pool.SendBatch(flushCtx, batch).Close()
		cancel()
		if err != nil {
			slog.Debug("runnerq: signal flush failed; receivers fall back to polling", "error", err)
		}
		workPending = false
		clear(resultIDs)
		lastFlush = time.Now()
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case m := <-s.ch:
			switch m.kind {
			case sigWork:
				workPending = true
			case sigResult:
				resultIDs[m.id] = struct{}{}
			}
			if due == nil {
				timer.Reset(max(signalFlushInterval-time.Since(lastFlush), 0))
				due = timer.C
			}
		case <-due:
			due = nil
			flush()
		}
	}
}

// Call signal helpers ONLY after commit, so a woken waiter's re-query sees
// the data.
func (b *PostgresBackend) signalWork() { b.sig.send(signalMsg{kind: sigWork}) }
func (b *PostgresBackend) signalResult(id uuid.UUID) {
	b.sig.send(signalMsg{kind: sigResult, id: id})
}

type watcher struct {
	b      *PostgresBackend
	cancel context.CancelFunc
	done   chan struct{}

	mu            sync.Mutex
	workWaiters   map[chan struct{}]struct{}
	resultWaiters map[uuid.UUID]map[chan struct{}]struct{}
}

// Lazy so backends that never block (pure producers, read-only tools) don't
// hold a LISTEN connection.
func (b *PostgresBackend) getWatcher() *watcher {
	b.watcherMu.Lock()
	defer b.watcherMu.Unlock()
	if b.watch == nil {
		ctx, cancel := context.WithCancel(context.Background())
		w := &watcher{
			b:             b,
			cancel:        cancel,
			done:          make(chan struct{}),
			workWaiters:   make(map[chan struct{}]struct{}),
			resultWaiters: make(map[uuid.UUID]map[chan struct{}]struct{}),
		}
		go w.run(ctx)
		b.watch = w
	}
	return b.watch
}

func (w *watcher) stop() {
	w.cancel()
	<-w.done
}

func (w *watcher) run(ctx context.Context) {
	defer close(w.done)

	for ctx.Err() == nil {
		if err := w.listenOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("runnerq: notification listener error; reconnecting", "error", err)
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
			}
		}
	}
}

// Waiters parked during an outage are woken by their fallback probes, so a
// dropped LISTEN connection degrades latency, not correctness.
func (w *watcher) listenOnce(ctx context.Context) error {
	conn, err := w.b.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	for _, ch := range []string{w.b.workChannel(), w.b.resultChannel()} {
		if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{ch}.Sanitize()); err != nil {
			return err
		}
	}

	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		switch n.Channel {
		case w.b.workChannel():
			w.wakeWorkWaiters()
		case w.b.resultChannel():
			w.wakeResultWaiters(n.Payload)
		}
	}
}

func (w *watcher) registerWork() chan struct{} {
	ch := make(chan struct{}, 1)
	w.mu.Lock()
	w.workWaiters[ch] = struct{}{}
	w.mu.Unlock()
	return ch
}

func (w *watcher) unregisterWork(ch chan struct{}) {
	w.mu.Lock()
	delete(w.workWaiters, ch)
	w.mu.Unlock()
}

func (w *watcher) wakeWorkWaiters() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for ch := range w.workWaiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (w *watcher) registerResult(id uuid.UUID) chan struct{} {
	ch := make(chan struct{}, 1)
	w.mu.Lock()
	m := w.resultWaiters[id]
	if m == nil {
		m = make(map[chan struct{}]struct{})
		w.resultWaiters[id] = m
	}
	m[ch] = struct{}{}
	w.mu.Unlock()
	return ch
}

func (w *watcher) unregisterResult(id uuid.UUID, ch chan struct{}) {
	w.mu.Lock()
	if m := w.resultWaiters[id]; m != nil {
		delete(m, ch)
		if len(m) == 0 {
			delete(w.resultWaiters, id)
		}
	}
	w.mu.Unlock()
}

func (w *watcher) wakeResultWaiters(payload string) {
	if payload == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.resultWaiters) == 0 {
		return
	}
	for part := range strings.SplitSeq(payload, ",") {
		id, err := uuid.Parse(part)
		if err != nil {
			continue
		}
		for ch := range w.resultWaiters[id] {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
}

// NotifyResult hints when a result for id is stored, by any process on the
// database (storage.ResultNotifier).
func (b *PostgresBackend) NotifyResult(id uuid.UUID) (<-chan struct{}, func()) {
	w := b.getWatcher()
	ch := w.registerResult(id)
	return ch, sync.OnceFunc(func() { w.unregisterResult(id, ch) })
}

var _ storage.ResultNotifier = (*PostgresBackend)(nil)

// WaitForResult blocks until the activity's result exists, ctx is done, or a
// storage error occurs. It works across processes: the producer and the waiter
// only need backends on the same database. Idle cost is one point-SELECT per
// waiter every 5s.
func (b *PostgresBackend) WaitForResult(ctx context.Context, activityID uuid.UUID) (*storage.ActivityResult, error) {
	w := b.getWatcher()
	ch := w.registerResult(activityID)
	defer w.unregisterResult(activityID, ch)
	fallback := time.NewTimer(resultWaitFallback)
	defer fallback.Stop()

	for {
		// Checked after registering so a result committed before the park
		// still signals ch.
		res, err := b.GetResult(ctx, activityID)
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}

		fallback.Reset(resultWaitFallback)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		case <-fallback.C:
		}
	}
}
