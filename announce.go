package runnerq

import (
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/executor"
)

// announcer holds an engine's executor.Announcer. Its methods are no-ops
// when none is set, or on a nil announcer (an ActivityExecutor built outside
// an engine).
type announcer struct {
	to atomic.Pointer[executor.Announcer]
}

func (a *announcer) set(to executor.Announcer) {
	if to == nil {
		a.to.Store(nil)
		return
	}
	a.to.Store(&to)
}

func (a *announcer) send(c executor.Change) {
	if a == nil {
		return
	}
	if to := a.to.Load(); to != nil {
		(*to).Announce(c)
	}
}

func (a *announcer) change(kind executor.ChangeKind, act *activity, at time.Time) {
	if a == nil || a.to.Load() == nil {
		return
	}
	a.send(executor.Change{Kind: kind, ActivityID: act.ID, ActivityType: act.ActivityType,
		RootID: rootOf(act), Attempt: int(act.RetryCount) + 1, At: at})
}

func (a *announcer) submitted(act *activity) {
	if a == nil || a.to.Load() == nil {
		return
	}
	kind := executor.Created
	if act.ScheduledAt != nil && act.ScheduledAt.After(act.CreatedAt) {
		kind = executor.Scheduled
	}
	a.send(executor.Change{Kind: kind, ActivityID: act.ID, ActivityType: act.ActivityType,
		RootID: rootOf(act), At: act.CreatedAt})
}

func rootOf(act *activity) uuid.UUID {
	if act.RootActivityID == (uuid.UUID{}) {
		return act.ID
	}
	return act.RootActivityID
}
