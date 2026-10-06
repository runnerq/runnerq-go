package storage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// CommandStorage applies operator commands to the backend's own queue.
// Commands are idempotent by Command.ID: the backend keeps each applied
// command's result for at least 24 hours and replays it for the same ID; the
// same ID with a different Fingerprint is ErrConflict.
//
// Per-target problems (not found, wrong state) are reported per item and do
// not fail the command. A returned error means nothing was applied.
type CommandStorage interface {
	ApplyCommand(ctx context.Context, cmd Command) (*CommandResult, error)
}

type CommandKind string

const (
	CommandCancel      CommandKind = "cancel"
	CommandRetry       CommandKind = "retry"
	CommandRunNow      CommandKind = "run_now"
	CommandReschedule  CommandKind = "reschedule"
	CommandSetPriority CommandKind = "set_priority"
	CommandDelete      CommandKind = "delete"
	CommandSignal      CommandKind = "signal"
)

// CommandTarget selects what a command acts on: exactly one of IDs, a
// Filter bounded by Max, or an IdempotencyKey (the stored form, see
// BusinessIdempotencyKey).
type CommandTarget struct {
	IDs            []uuid.UUID
	Filter         *QueryFilter
	Max            int
	IdempotencyKey string
}

// Command is one command against the backend's queue.
type Command struct {
	// ID makes the command idempotent; empty disables the ledger.
	ID string
	// Fingerprint identifies the input, to detect a reused ID.
	Fingerprint string
	Kind        CommandKind
	Target      CommandTarget
	DryRun      bool
	Reason      string

	// CascadeChildren also cancels non-terminal descendants (cancel).
	CascadeChildren bool
	// ResetAttempts restores the full retry budget (retry).
	ResetAttempts bool
	// At is the new time (reschedule).
	At time.Time
	// Priority is the new priority (set_priority).
	Priority      ActivityPriority
	SignalName    string
	SignalPayload json.RawMessage
}

// CommandItem.Outcome values.
const (
	CommandApplied    = "applied"
	CommandSkipped    = "skipped"
	CommandWouldApply = "would_apply"
)

// CommandItem is the outcome for one target.
type CommandItem struct {
	ID      uuid.UUID
	Outcome string
	// Status is the canonical status after the command (current if skipped).
	Status string
	// ErrKind and ErrMessage explain a skip: ErrNotFound, or ErrConflict for
	// a target in the wrong state.
	ErrKind    StorageErrorKind
	ErrMessage string
}

// CommandResult is a command's outcome.
type CommandResult struct {
	Matched int
	Applied int
	// Cascaded counts descendants a cascading cancel also cancelled.
	Cascaded int
	// More is true when a filter target matched more than Max.
	More  bool
	Items []CommandItem
	// Replayed is true when the result came from the ledger.
	Replayed bool
}

// CheckpointID derives the id of an activity's named checkpoint (kind "run",
// "sleep", "signal", ...). Backends must use it so a signal they store is the
// one the handler's WaitForSignal reads.
func CheckpointID(activityID uuid.UUID, kind, name string) uuid.UUID {
	return uuid.NewSHA1(activityID, []byte(kind+":"+name))
}
