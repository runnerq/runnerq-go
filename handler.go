package runnerq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// ActivityContext is what a handler gets about the activity it runs.
type ActivityContext struct {
	ActivityID   uuid.UUID
	ActivityType string

	// RetryCount is the attempt number minus one (0 on the first attempt).
	RetryCount uint32

	Metadata map[string]string

	// Ctx ends at the activity's timeout, and earlier if this execution loses
	// its claim to another worker; context.Cause(Ctx) is then a storage
	// ErrClaimLost error. Handlers must stop when it ends: the engine cannot
	// interrupt one that ignores it, and its side effects would overlap the
	// replacement execution's.
	Ctx context.Context

	// ActivityExecutor spawns children of this activity.
	ActivityExecutor *ActivityExecutor

	ParentActivityID *uuid.UUID // nil for a root
	RootActivityID   uuid.UUID  // ActivityID for a root
	Depth            uint16     // 0 for a root

	// queue is the checkpoint storage behind Run, Sleep and WaitForSignal;
	// nil in a hand-built context (handler unit tests), where Run just calls
	// fn and Sleep waits in-process.
	queue activityQueue
}

// checkpointID is the stable id of a named checkpoint: every process derives
// the same one, so a retried handler finds a previous attempt's value and an
// external process can address a signal at a waiting activity.
func (c ActivityContext) checkpointID(kind, name string) uuid.UUID {
	return storage.CheckpointID(c.ActivityID, kind, name)
}

// Run executes fn as a named, checkpointed step: a retried handler reaching
// the same step gets the stored outcome without re-running fn. Use it for
// side effects that must not repeat across retries (payments, emails).
//
//   - success: the result is stored and returned by later attempts.
//   - NonRetryError: the failure is stored and returned by later attempts.
//   - retryable error: nothing is stored; fn runs again next attempt.
//   - a crash after fn but before the result commits: fn runs again. Make fn
//     as idempotent as the external system allows (e.g. pass it an
//     idempotency key).
//
// Step names must be stable across retries and unique within the handler.
func (c ActivityContext) Run(name string, fn func() (json.RawMessage, error)) (json.RawMessage, error) {
	if name == "" {
		return nil, NewNonRetryError("Run requires a non-empty step name")
	}
	if c.queue == nil {
		return fn()
	}
	checkID := c.checkpointID("run", name)

	if stored, err := c.queue.GetResult(c.Ctx, checkID); err != nil {
		return nil, err
	} else if stored != nil {
		if stored.State == ResultOk {
			return stored.Data, nil
		}
		var failure struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(stored.Data, &failure)
		return nil, NewNonRetryError(failure.Error)
	}

	out, fnErr := fn()
	if fnErr != nil {
		if !retryableError(fnErr) {
			// Checkpoint a permanent failure so a retry of the handler (for
			// another reason) doesn't re-run the step.
			failureJSON, _ := json.Marshal(map[string]string{"error": fnErr.Error()})
			if err := c.queue.StoreResult(c.Ctx, checkID, c.ActivityID, activityResult{Data: failureJSON, State: ResultErr}, "run:"+name); err != nil {
				return nil, err
			}
		}
		return nil, fnErr
	}

	if err := c.queue.StoreResult(c.Ctx, checkID, c.ActivityID, activityResult{Data: out, State: ResultOk}, "run:"+name); err != nil {
		// Wrapped, not replaced, so the error keeps its classification.
		return nil, fmt.Errorf("step %q ran but checkpoint failed: %w", name, err)
	}
	return out, nil
}

// Step is a typed unit of work for RunStep. Its context derives from the
// activity's, so the activity timeout still bounds it.
type Step[R any] func(ctx context.Context) (R, error)

// RunStep is the typed form of ActivityContext.Run: fn's result is stored as
// JSON and a replaying attempt decodes it into R, so R must round-trip through
// encoding/json. Semantics and step-name rules are Run's.
//
//	receipt, err := runnerq.RunStep(ctx, "charge", func(c context.Context) (Receipt, error) {
//	    return payments.Charge(c, orderID, amount)
//	})
//
// A stored result that no longer decodes into R (the type changed between
// deploys) is a NonRetryError. So is an encode failure, which is stored as a
// permanent step failure because the side effect has already happened.
//
// On Go 1.27 and later, ctx.RunStep(name, fn) is the same call as a method.
func RunStep[R any](c ActivityContext, name string, fn Step[R]) (R, error) {
	var zero R
	raw, err := c.Run(name, func() (json.RawMessage, error) {
		parent := c.Ctx
		if parent == nil {
			parent = context.Background()
		}
		stepCtx, cancel := context.WithCancel(parent)
		defer cancel()
		out, err := fn(stepCtx)
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(out)
		if err != nil {
			return nil, NewNonRetryError(fmt.Sprintf("step %q: cannot encode %T result: %v", name, out, err))
		}
		return data, nil
	})
	if err != nil {
		return zero, err
	}
	var out R
	if err := json.Unmarshal(raw, &out); err != nil {
		return zero, NewNonRetryError(fmt.Sprintf("step %q: stored result does not decode into %T: %v", name, out, err))
	}
	return out, nil
}

// yieldMargin is the headroom before the handler deadline a wait needs to
// happen in-process; otherwise it yields rather than end in a timeout retry.
const yieldMargin = 2 * time.Second

// yieldPark is the sentinel error of a yielding wait (Sleep, WaitForSignal,
// in-handler GetResult). The engine parks the activity as 'waiting' until
// wakeAt without counting a retry; what it waits for wakes it early.
//
// recheck names the awaited result: one that commits between the handler's
// last check and the park wakes nothing (the row wasn't 'waiting' yet), so
// the engine re-checks it after the park commits.
type yieldPark struct {
	wakeAt  time.Time
	kind    string // "sleep", "signal" or "await", for the Yielded event
	step    string
	recheck uuid.UUID // uuid.Nil: none
}

func (y *yieldPark) Error() string {
	return fmt.Sprintf("durable wait %q yields until %s", y.step, y.wakeAt.Format(time.RFC3339))
}

// Sleep is a durable timer: the wake time is stored on first execution, so a
// handler that crashes or is redeployed mid-sleep waits only the remainder,
// and a replay past it returns at once.
//
// A wait that fits in the activity's timeout happens in-process. Otherwise
// Sleep YIELDS: it returns a sentinel error the caller MUST propagate
// unchanged, the activity is parked until the wake time without consuming a
// retry, and the handler then replays (earlier checkpoints fast-forward) to
// a Sleep that returns nil.
//
// Step names must be stable across retries and unique within the handler.
func (c ActivityContext) Sleep(name string, d time.Duration) error {
	if name == "" {
		return NewNonRetryError("Sleep requires a non-empty step name")
	}
	if c.queue == nil {
		select {
		case <-time.After(d):
			return nil
		case <-c.Ctx.Done():
			return c.Ctx.Err()
		}
	}
	checkID := c.checkpointID("sleep", name)

	var wakeAt time.Time
	if stored, err := c.queue.GetResult(c.Ctx, checkID); err != nil {
		return err
	} else if stored != nil {
		var cp struct {
			WakeAt time.Time `json:"wake_at"`
		}
		if err := json.Unmarshal(stored.Data, &cp); err != nil {
			return NewNonRetryError(fmt.Sprintf("corrupt sleep checkpoint %q: %v", name, err))
		}
		wakeAt = cp.WakeAt
	} else {
		wakeAt = time.Now().UTC().Add(d)
		cpJSON, _ := json.Marshal(map[string]time.Time{"wake_at": wakeAt})
		// Stored before waiting, so a crash mid-sleep resumes the remainder.
		if err := c.queue.StoreResult(c.Ctx, checkID, c.ActivityID, activityResult{Data: cpJSON, State: ResultOk}, "sleep:"+name); err != nil {
			return err
		}
	}

	remaining := time.Until(wakeAt)
	if remaining <= 0 {
		return nil
	}

	// Yield unless the wake comfortably precedes the handler deadline. The
	// margin is capped at half the remaining budget so short-timeout handlers
	// can still take short sleeps in-process.
	if deadline, ok := c.Ctx.Deadline(); ok {
		margin := max(min(yieldMargin, time.Until(deadline)/2), 0)
		if wakeAt.After(deadline.Add(-margin)) {
			return &yieldPark{wakeAt: wakeAt, kind: "sleep", step: name}
		}
	}

	select {
	case <-time.After(remaining):
		return nil
	case <-c.Ctx.Done():
		return c.Ctx.Err()
	}
}

// signalParkHorizon is the park deadline of a wait with no timeout:
// effectively until woken.
const signalParkHorizon = 100 * 365 * 24 * time.Hour

// WaitForSignal waits for the signal named name to be delivered to this
// activity (WorkerEngine.Signal or SignalActivity, from any process sharing
// the database) and returns its payload. timeout runs from the first attempt
// to reach this call, not from each replay; 0 waits forever. On timeout it
// returns a non-retryable error (IsSignalTimeout).
//
// Signals are buffered: one delivered earlier, even before the activity
// started, is returned at once. A repeated signal overwrites the payload.
//
// Like Sleep, a wait that doesn't fit the timeout YIELDS: propagate the
// sentinel error unchanged. Signal names must be stable across retries and
// unique within the handler.
func (c ActivityContext) WaitForSignal(name string, timeout time.Duration) (json.RawMessage, error) {
	if name == "" {
		return nil, NewNonRetryError("WaitForSignal requires a non-empty signal name")
	}
	if timeout < 0 {
		return nil, NewNonRetryError("WaitForSignal timeout must be >= 0 (0 = wait forever)")
	}
	if c.queue == nil {
		return nil, NewNonRetryError("WaitForSignal requires engine checkpoint storage; it cannot run on a hand-constructed ActivityContext")
	}
	sigID := c.checkpointID("signal", name)

	// The deadline is stored on first arrival so replays don't restart it;
	// nil is forever.
	var deadline *time.Time
	waitID := c.checkpointID("signalwait", name)
	if stored, err := c.queue.GetResult(c.Ctx, waitID); err != nil {
		return nil, err
	} else if stored != nil {
		var cp struct {
			Deadline *time.Time `json:"deadline"`
		}
		if err := json.Unmarshal(stored.Data, &cp); err != nil {
			return nil, NewNonRetryError(fmt.Sprintf("corrupt signal-wait checkpoint %q: %v", name, err))
		}
		deadline = cp.Deadline
	} else {
		if timeout > 0 {
			d := time.Now().UTC().Add(timeout)
			deadline = &d
		}
		cpJSON, _ := json.Marshal(map[string]*time.Time{"deadline": deadline})
		if err := c.queue.StoreResult(c.Ctx, waitID, c.ActivityID, activityResult{Data: cpJSON, State: ResultOk}, ""); err != nil {
			return nil, err
		}
	}

	for {
		stored, err := c.queue.GetResult(c.Ctx, sigID)
		if err != nil {
			return nil, err
		}
		if stored != nil {
			return stored.Data, nil
		}

		if deadline != nil && !time.Now().Before(*deadline) {
			// Check once more before timing out: ties go to the signal.
			if stored, err := c.queue.GetResult(c.Ctx, sigID); err != nil {
				return nil, err
			} else if stored != nil {
				return stored.Data, nil
			}
			return nil, &WorkerError{
				Kind:    ErrSignalTimeoutW,
				Message: fmt.Sprintf("signal %q was not delivered within the wait deadline", name),
			}
		}

		wake := time.Now().UTC().Add(signalParkHorizon)
		if deadline != nil {
			wake = *deadline
		}

		// Same yield policy as Sleep; delivery wakes the parked activity.
		if ctxDeadline, ok := c.Ctx.Deadline(); ok {
			margin := max(min(yieldMargin, time.Until(ctxDeadline)/2), 0)
			if wake.After(ctxDeadline.Add(-margin)) {
				return nil, &yieldPark{wakeAt: wake, kind: "signal", step: name, recheck: sigID}
			}
		}

		// Wait in-process, bounded by the deadline so the timeout above fires.
		stored, err = c.waitForCheckpoint(sigID, wake)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && c.Ctx.Err() == nil {
				continue // deadline reached: re-check, then time out
			}
			return nil, err
		}
		return stored.Data, nil
	}
}

func (c ActivityContext) waitForCheckpoint(id uuid.UUID, wake time.Time) (*activityResult, error) {
	waitCtx, cancel := context.WithDeadline(c.Ctx, wake)
	defer cancel()
	return c.queue.WaitForResult(waitCtx, id)
}

// ActivityHandler runs activities of the type it is registered under
// (RegisterActivity, RegisterActivityWithName). It must be safe for
// concurrent use.
type ActivityHandler interface {
	// Handle runs one attempt. It returns (result, nil) on success (result
	// may be nil), a NonRetryError to fail for good, or any other error to
	// retry.
	Handle(ctx ActivityContext, payload json.RawMessage) (json.RawMessage, error)

	// OnDeadLetter is called when an attempt fails for the last time; embed
	// DefaultDeadLetterHandler for a no-op.
	OnDeadLetter(ctx ActivityContext, payload json.RawMessage, errorMsg string)
}

// DefaultDeadLetterHandler is a no-op OnDeadLetter to embed in handlers.
type DefaultDeadLetterHandler struct{}

func (DefaultDeadLetterHandler) OnDeadLetter(_ ActivityContext, _ json.RawMessage, _ string) {}
