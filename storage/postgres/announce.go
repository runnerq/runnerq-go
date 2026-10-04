package postgres

import (
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/executor"
	"github.com/alob-mtc/runnerq-go/storage"
)

// SetAnnouncer sends a every lifecycle change this backend commits from now
// on, whoever asked for it: new submissions, claims and successes. nil stops.
// A storage service uses it to announce what its remote workers do (RunnerQ
// Cloud's hosted stores); a worker process with an agent uses
// WorkerEngine.Announce instead. a runs on the caller's goroutine, after the
// commit, and must not block.
func (b *PostgresBackend) SetAnnouncer(a executor.Announcer) {
	if a == nil {
		b.announcer.Store(nil)
		return
	}
	b.announcer.Store(&a)
}

// announce builds and sends a change, building it only when someone listens.
func (b *PostgresBackend) announce(change func() executor.Change) {
	if a := b.announcer.Load(); a != nil {
		(*a).Announce(change())
	}
}

func (b *PostgresBackend) announceClaims(token string, claims ...storage.DequeuedActivity) {
	if b.announcer.Load() == nil {
		return
	}
	now := time.Now().UTC()
	for _, c := range claims {
		a := c.Activity
		b.announce(func() executor.Change {
			return executor.Change{Kind: executor.AttemptStarted, ActivityID: a.ID, ActivityType: a.ActivityType,
				RootID: rootOf(&a), Attempt: int(a.RetryCount) + 1, At: now, ExecutorID: executorOf(token)}
		})
	}
}

func rootOf(a *storage.QueuedActivity) uuid.UUID {
	if a.RootActivityID == (uuid.UUID{}) {
		return a.ID
	}
	return a.RootActivityID
}

// executorOf is the executor a claim token names: the part before its first
// colon, as queries' executor_id reads it.
func executorOf(token string) string {
	executor, _, _ := strings.Cut(token, ":")
	return executor
}
