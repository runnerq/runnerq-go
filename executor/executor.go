// Package executor describes a running engine (an executor): who it is,
// what it is doing and what it has done. The engine produces these
// snapshots; the Cloud agent, the cloud storage adapter and metrics
// exporters read them on their own schedules.
package executor

import (
	"os"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
)

// Info identifies an executor; it is fixed for the run.
type Info struct {
	// ID is unique per run and is the executor id in RunnerQ Cloud.
	ID             string
	Queue          string
	ActivityTypes  []string
	MaxConcurrency int
	StartedAt      time.Time
	Hostname       string
	SDK            SDK
	// Labels are tags (region, deploy version) from WorkerConfig.Labels.
	Labels map[string]string
}

// SDK names the RunnerQ SDK the executor runs.
type SDK struct {
	Name, Version, Language string
}

// State is what an executor is doing now.
type State struct {
	Running []Running // oldest first
	// Draining means a shutdown has begun: intake has stopped and in-flight
	// activities are finishing.
	Draining bool
}

// Running is an activity an executor is executing.
type Running struct {
	ID        uuid.UUID
	Type      string
	Attempt   int
	StartedAt time.Time
}

// Counters are what an executor has done since it started.
type Counters struct {
	// Claimed counts activities that started executing here.
	Claimed uint64
	// Succeeded completed; Retried asked for another attempt; Failed failed
	// for good; TimedOut ran past their timeout; DeadLettered ran out of
	// attempts.
	Succeeded, Retried, Failed, TimedOut, DeadLettered uint64
	// ClaimsLost counts activities another worker took over from here.
	ClaimsLost uint64
	// HeartbeatFailures counts failed claim renewals.
	HeartbeatFailures uint64
	// LastClaimLag is how long the last claimed activity waited after it
	// was due.
	LastClaimLag time.Duration
}

// Snapshot is an executor at a moment.
type Snapshot struct {
	Info     Info
	State    State
	Counters Counters
	At       time.Time
}

// Source provides a running executor's snapshots.
type Source interface {
	Snapshot() Snapshot
}

// Observer hears an executor start and stop and reads the Source on its own
// schedule. Both calls must return promptly. An engine notifies every
// Observer registered with WorkerEngine.Observe, and its storage backend if
// that is an Observer (how RunnerQ Cloud's adapter reports hosted workers).
type Observer interface {
	ExecutorStarted(src Source)
	ExecutorStopped(id string)
}

// ChangeKind is a lifecycle change no event is stored for: the activity's
// own times say it, so it can only be announced as it happens.
type ChangeKind int

const (
	Created          ChangeKind = iota // submitted, runnable now
	Scheduled                          // submitted to run later
	AttemptStarted                     // claimed by this executor
	AttemptSucceeded                   // completed by this executor
)

// Change is one lifecycle change this executor made.
type Change struct {
	Kind         ChangeKind
	ActivityID   uuid.UUID
	ActivityType string
	// RootID is the activity's workflow root: its own id for a root.
	RootID uuid.UUID
	// Attempt is set for AttemptStarted and AttemptSucceeded.
	Attempt int
	At      time.Time
	// ExecutorID is the executor that made the change, when the reporter
	// knows it from the claim (a storage backend does; an engine's own
	// changes leave it empty).
	ExecutorID string
}

// Announcer hears every Change an engine makes, as it makes it, from the
// goroutine that made it. Announce must not block.
type Announcer interface {
	Announce(Change)
}

// ThisSDK names the runnerq-go SDK in this binary; Version is "unknown"
// without build information.
func ThisSDK() SDK {
	const module = "github.com/alob-mtc/runnerq-go"
	sdk := SDK{Name: "runnerq-go", Version: "unknown", Language: "go"}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return sdk
	}
	if info.Main.Path == module {
		sdk.Version = info.Main.Version
		return sdk
	}
	for _, dep := range info.Deps {
		if dep.Path == module {
			sdk.Version = dep.Version
			if dep.Replace != nil {
				sdk.Version = dep.Replace.Version
			}
		}
	}
	return sdk
}

// Hostname is this machine's name, or empty.
func Hostname() string {
	h, _ := os.Hostname()
	return h
}
