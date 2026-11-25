package runnerq

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/runnerq/runnerq-go/storage"
)

// Limit caps one activity type of a queue across every worker
// (see storage.Limit).
type Limit = storage.Limit

// RegisterOption configures a registered activity type; pass it to
// RegisterActivity or RegisterActivityWithName.
type RegisterOption func(*registration)

type registration struct {
	limit     storage.Limit
	perWorker int
}

// MaxConcurrent caps how many activities of the type run at once across
// every worker of the queue. Start stores it, replacing the type's stored
// limit; the Client changes it at runtime. Activities parked on a sleep, a
// signal, a child or a retry's backoff don't count.
func MaxConcurrent(n int) RegisterOption {
	if n <= 0 {
		panic("runnerq: MaxConcurrent must be positive")
	}
	return func(r *registration) { r.limit.MaxConcurrent = n }
}

// RateLimit caps how many activities of the type are claimed per period
// across every worker of the queue, with bursts of up to n: RateLimit(5,
// time.Second) starts at most 5 a second. Start stores it with
// MaxConcurrent, if any.
func RateLimit(n int, per time.Duration) RegisterOption {
	if n <= 0 || per.Round(time.Millisecond) <= 0 {
		panic("runnerq: RateLimit needs a positive count and a period of at least a millisecond")
	}
	return func(r *registration) { r.limit.RateLimit, r.limit.RatePeriod = n, per }
}

// MaxConcurrentPerWorker caps how many activities of the type this engine
// runs at once, within its MaxWorkers. Nothing is stored: other workers
// don't see it.
func MaxConcurrentPerWorker(n int) RegisterOption {
	if n <= 0 {
		panic("runnerq: MaxConcurrentPerWorker must be positive")
	}
	return func(r *registration) { r.perWorker = n }
}

// SetLimit creates or replaces the limit of an activity type in the queue,
// for every worker. A worker that starts with a declared limit for the type
// replaces it again. Needs a backend implementing storage.LimitStorage.
func (c *Client) SetLimit(ctx context.Context, limit Limit) error {
	ls, err := c.limits()
	if err != nil {
		return err
	}
	return ls.SetLimit(ctx, limit)
}

// GetLimit returns the type's limit; a not-found error when it has none.
func (c *Client) GetLimit(ctx context.Context, activityType string) (*Limit, error) {
	ls, err := c.limits()
	if err != nil {
		return nil, err
	}
	return ls.GetLimit(ctx, activityType)
}

// ListLimits returns the queue's limits by activity type.
func (c *Client) ListLimits(ctx context.Context) ([]Limit, error) {
	ls, err := c.limits()
	if err != nil {
		return nil, err
	}
	return ls.ListLimits(ctx)
}

// DeleteLimit removes the type's limit, so it runs unlimited; a not-found
// error when it has none.
func (c *Client) DeleteLimit(ctx context.Context, activityType string) error {
	ls, err := c.limits()
	if err != nil {
		return err
	}
	return ls.DeleteLimit(ctx, activityType)
}

func (c *Client) limits() (storage.LimitStorage, error) {
	ls, ok := c.backend.(storage.LimitStorage)
	if !ok {
		return nil, &WorkerError{Kind: ErrConfiguration, Message: fmt.Sprintf("storage %T does not support limits", c.backend)}
	}
	return ls, nil
}

// storeLimits upserts the declared limits.
func (e *WorkerEngine) storeLimits(ctx context.Context, limits []storage.Limit) error {
	if len(limits) == 0 {
		return nil
	}
	ls, ok := e.backend.(storage.LimitStorage)
	if !ok {
		return &WorkerError{Kind: ErrConfiguration, Message: fmt.Sprintf("storage %T does not support limits", e.backend)}
	}
	for _, l := range limits {
		if err := ls.SetLimit(ctx, l); err != nil {
			return fmt.Errorf("store limit of %q: %w", l.ActivityType, err)
		}
	}
	return nil
}

// localLimits enforces MaxConcurrentPerWorker: per-type caps on this
// engine's running activities. A claim leaves out the types at their cap and
// never asks for more of a capped type than it has room for.
type localLimits struct {
	mu      sync.Mutex
	caps    map[string]int
	running map[string]int
	freed   chan struct{} // closed, then replaced, when a capped type frees a slot
}

func newLocalLimits(caps map[string]int) *localLimits {
	if len(caps) == 0 {
		return nil
	}
	return &localLimits{caps: caps, running: map[string]int{}, freed: make(chan struct{})}
}

// plan narrows a claim of up to n activities of types to what the caps
// allow. freed is non-nil when a cap narrowed it: it closes when a slot of a
// capped type frees, so a blocked claim can start over with more.
func (l *localLimits) plan(types []string, n int) (claim []string, limit int, freed <-chan struct{}) {
	if l == nil {
		return types, n, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	limit = n
	narrowed := false
	for _, t := range types {
		c, capped := l.caps[t]
		if !capped {
			claim = append(claim, t)
			continue
		}
		room := c - l.running[t]
		if room <= 0 {
			narrowed = true
			continue
		}
		if room < limit {
			limit, narrowed = room, true
		}
		claim = append(claim, t)
	}
	if narrowed {
		freed = l.freed
	}
	return claim, limit, freed
}

// reserve takes one slot of every capped type with room, for a single-row
// claim of types; release returns the ones the claim didn't use.
func (l *localLimits) reserve(types []string) (claim, reserved []string, freed <-chan struct{}) {
	if l == nil {
		return types, nil, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, t := range types {
		c, capped := l.caps[t]
		switch {
		case !capped:
			claim = append(claim, t)
		case l.running[t] < c:
			l.running[t]++
			reserved = append(reserved, t)
			claim = append(claim, t)
		}
	}
	return claim, reserved, l.freed
}

// release returns the reserved slots except keep's.
func (l *localLimits) release(reserved []string, keep string) {
	if l == nil || len(reserved) == 0 {
		return
	}
	for _, t := range reserved {
		if t == keep {
			keep = ""
			continue
		}
		l.done(t)
	}
}

func (l *localLimits) start(typ string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, capped := l.caps[typ]; capped {
		l.running[typ]++
	}
}

func (l *localLimits) done(typ string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, capped := l.caps[typ]; capped {
		l.running[typ]--
		close(l.freed)
		l.freed = make(chan struct{})
	}
}

// sortedLimits returns the declared limits by activity type.
func sortedLimits(m map[string]registration) []storage.Limit {
	var out []storage.Limit
	for t, r := range m {
		if r.limit.MaxConcurrent > 0 || r.limit.RateLimit > 0 {
			l := r.limit
			l.ActivityType = t
			out = append(out, l)
		}
	}
	slices.SortFunc(out, func(a, b storage.Limit) int { return cmp.Compare(a.ActivityType, b.ActivityType) })
	return out
}
