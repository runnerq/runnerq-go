package runnerq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// Client submits, signals, awaits, inspects and manages activities. It needs
// only a backend, no running engine, so any process sharing the database can
// use it: a web handler, a CLI, a cron job. WorkerEngine.Client returns one
// bound to the engine.
//
//	client := runnerq.NewClient(backend)
//	fut, _ := runnerq.Activity[SendEmail](client).Payload(p).Execute(ctx)
//	res, _ := client.Cancel(ctx, runnerq.IDs(fut.ActivityID()))
//
// Management commands (Cancel, Retry, RunNow, Reschedule, SetPriority,
// Delete) need a backend implementing storage.CommandStorage, and Get and
// List one implementing storage.QueryStorage; the Postgres backend
// implements both. All of them act on the backend's own queue.
type Client struct {
	backend   storage.Storage
	exec      *ActivityExecutor
	queue     string
	interrupt func(uuid.UUID) bool // the engine's Interrupt, for engine-bound clients
}

// NewClient returns a Client on backend.
func NewClient(backend storage.Storage, opts ...ClientOption) *Client {
	var o clientOptions
	for _, opt := range opts {
		opt(&o)
	}
	exec := newActivityExecutor(newBackendQueueAdapter(backend, nil), DefaultMaxActivityDepth, &announcer{}, nil)
	exec.version = o.version
	return &Client{
		backend: backend,
		exec:    exec,
		queue:   queueOf(backend),
	}
}

// ClientOption configures NewClient and NewSchedules.
type ClientOption func(*clientOptions)

type clientOptions struct {
	version string
}

// ClientVersion pins the roots the client submits, and the schedules it
// upserts, to a code version (docs/versions.md), as a worker's own client
// does with its WorkerConfig.Version. Without it they are unstamped, and
// any worker may run them.
func ClientVersion(v string) ClientOption {
	return func(o *clientOptions) { o.version = v }
}

// Client returns a Client on the engine's backend. Its submissions are
// announced like the engine's own, a spawn on a handler's context becomes
// that handler's child (as with GetActivityExecutor), and a Cancel stops a
// handler this engine is running at once.
func (e *WorkerEngine) Client() *Client {
	return &Client{
		backend:   e.backend,
		exec:      e.GetActivityExecutor(),
		queue:     queueOf(e.backend),
		interrupt: e.Interrupt,
	}
}

func queueOf(backend storage.Storage) string {
	if q, ok := backend.(interface{ QueueName() string }); ok {
		return q.QueueName()
	}
	return ""
}

// Backend is the client's backend.
func (c *Client) Backend() storage.Storage { return c.backend }

// ActivityNamed starts a submission of activityType by name.
func (c *Client) ActivityNamed(activityType string) *ActivityBuilder {
	return c.exec.ActivityNamed(activityType)
}

// Future returns the future of an activity submitted earlier, here or in
// another process.
func (c *Client) Future(activityID uuid.UUID) *ActivityFuture {
	return &ActivityFuture{queue: c.exec.queue, activityID: activityID}
}

// Result waits for the activity's result (ActivityFuture.GetResult). A
// cancelled activity's result is an error for which IsCancelled is true.
func (c *Client) Result(ctx context.Context, activityID uuid.UUID) (json.RawMessage, error) {
	return c.Future(activityID).GetResult(ctx)
}

// Signal delivers a signal to an activity (SignalActivity).
func (c *Client) Signal(ctx context.Context, activityID uuid.UUID, name string, payload json.RawMessage) error {
	return SignalActivity(ctx, c.backend, activityID, name, payload)
}

// SignalByKey delivers a signal to the activity of activityType that owns
// idempotencyKey (SignalActivityByKey).
func (c *Client) SignalByKey(ctx context.Context, activityType, idempotencyKey, name string, payload json.RawMessage) error {
	return SignalActivityByKey(ctx, c.backend, activityType, idempotencyKey, name, payload)
}

// Get returns an activity with its payload, result and last error. It fails
// with IsActivityNotFound when the queue has no such activity.
func (c *Client) Get(ctx context.Context, activityID uuid.UUID) (*storage.ActivityRecord, error) {
	qs, err := c.queries()
	if err != nil {
		return nil, err
	}
	page, err := qs.QueryActivities(ctx, storage.ActivityQuery{
		Filter:  c.scope(&storage.QueryFilter{Field: "id", Op: storage.OpEq, Value: activityID.String()}),
		Include: storage.RecordInclude{Payload: true, Result: true, LastError: true},
		Limit:   1,
	})
	if err != nil {
		return nil, clientError(err)
	}
	if len(page.Items) == 0 {
		return nil, &WorkerError{Kind: ErrActivityNotFoundW, Message: fmt.Sprintf("activity %s not found", activityID)}
	}
	return &page.Items[0], nil
}

// ListOptions selects and orders the activities List returns. The zero
// value lists the newest 50 of every status.
type ListOptions struct {
	// Statuses keeps activities in any of these statuses.
	Statuses []ActivityStatus
	// Type keeps activities of this type.
	Type string
	// RootsOnly leaves out child activities.
	RootsOnly bool
	// Filter adds conditions in the query model (storage.QueryFilter).
	Filter *storage.QueryFilter
	// Sort orders the page; nil is newest first.
	Sort *storage.QuerySort
	// Include fetches heavy fields, left out by default.
	Include storage.RecordInclude
	// Limit is the page size: 0 is 50, at most 1000.
	Limit int
	// Cursor continues from a previous page's NextCursor.
	Cursor string
}

// List returns a page of the queue's activities.
func (c *Client) List(ctx context.Context, opts ListOptions) (*storage.ActivityRecordPage, error) {
	qs, err := c.queries()
	if err != nil {
		return nil, err
	}
	var terms []storage.QueryFilter
	if len(opts.Statuses) > 0 {
		values := make([]any, len(opts.Statuses))
		for i, st := range opts.Statuses {
			values[i] = string(st)
		}
		terms = append(terms, storage.QueryFilter{Field: "status", Op: storage.OpIn, Value: values})
	}
	if opts.Type != "" {
		terms = append(terms, storage.QueryFilter{Field: "type", Op: storage.OpEq, Value: opts.Type})
	}
	if opts.RootsOnly {
		terms = append(terms, storage.QueryFilter{Field: "parent_id", Op: storage.OpExists, Value: false})
	}
	if opts.Filter != nil {
		terms = append(terms, *opts.Filter)
	}
	var filter *storage.QueryFilter
	switch len(terms) {
	case 0:
	case 1:
		filter = &terms[0]
	default:
		filter = &storage.QueryFilter{And: terms}
	}
	page, err := qs.QueryActivities(ctx, storage.ActivityQuery{
		Filter: c.scope(filter), Sort: opts.Sort, Include: opts.Include, Limit: opts.Limit, Cursor: opts.Cursor,
	})
	if err != nil {
		return nil, clientError(err)
	}
	return page, nil
}

// clientError reports bad input (a malformed filter or target) as a
// configuration error, and other storage errors as WorkerErrorFromStorage does.
func clientError(err error) error {
	if se, ok := storage.IsStorageError(err); ok && (se.Kind == storage.ErrInvalidArgument || se.Kind == storage.ErrUnsupported) {
		return &WorkerError{Kind: ErrConfiguration, Message: se.Error(), Cause: err}
	}
	return WorkerErrorFromStorage(err)
}

// scope narrows a query to the client's queue, which commands act on.
func (c *Client) scope(f *storage.QueryFilter) *storage.QueryFilter {
	if c.queue == "" {
		return f
	}
	q := storage.QueryFilter{Field: "queue", Op: storage.OpEq, Value: c.queue}
	if f == nil {
		return &q
	}
	return &storage.QueryFilter{And: []storage.QueryFilter{q, *f}}
}

func (c *Client) queries() (storage.QueryStorage, error) {
	qs, ok := c.backend.(storage.QueryStorage)
	if !ok {
		return nil, &WorkerError{Kind: ErrConfiguration, Message: fmt.Sprintf("backend %T cannot be queried (storage.QueryStorage)", c.backend)}
	}
	return qs, nil
}

// Target selects the activities a command acts on: IDs, or a filter.
type Target struct {
	// IDs names activities; at most 1000.
	IDs []uuid.UUID
	// Filter selects, oldest first, up to Max activities the command can act
	// on (cancel skips finished ones, retry takes only failed ones, ...), so
	// repeating a bounded command makes progress.
	Filter *storage.QueryFilter
	// Max bounds a filter target: 1 to 10000.
	Max int
}

// IDs targets the activities with these ids.
func IDs(ids ...uuid.UUID) Target { return Target{IDs: ids} }

// Matching targets up to max activities matching filter that the command
// can act on.
func Matching(filter storage.QueryFilter, max int) Target {
	return Target{Filter: &filter, Max: max}
}

// CommandOption configures a management command.
type CommandOption func(*commandConfig)

type commandConfig struct {
	id     string
	reason string
	dryRun bool
	noCasc bool
	reset  bool
}

// WithCommandID makes the command idempotent under id: sending the same
// command again with it (a retry after a timeout, say) replays the first
// result, reported with Replayed set, for at least 24 hours. The same id with
// different input fails (IsCommandConflict). Without it each call gets a
// fresh id.
func WithCommandID(id string) CommandOption {
	return func(c *commandConfig) { c.id = id }
}

// WithReason records why, in each activity's history (and a cancelled
// activity's error).
func WithReason(reason string) CommandOption {
	return func(c *commandConfig) { c.reason = reason }
}

// DryRun reports what the command would do (outcome WouldApply) and changes
// nothing.
func DryRun() CommandOption {
	return func(c *commandConfig) { c.dryRun = true }
}

// NoCascade cancels only the targets, leaving their unfinished descendants
// running (Cancel only). By default a cancel cancels the whole subtree.
func NoCascade() CommandOption {
	return func(c *commandConfig) { c.noCasc = true }
}

// ResetAttempts gives a retried activity its full attempt budget again
// (Retry only). By default it keeps the attempts it used.
func ResetAttempts() CommandOption {
	return func(c *commandConfig) { c.reset = true }
}

// CommandOutcome is what a command did to one target.
type CommandOutcome string

const (
	// Applied: the command changed the activity.
	Applied CommandOutcome = "applied"
	// WouldApply: a dry run would have changed it.
	WouldApply CommandOutcome = "would_apply"
	// NotEligible: the activity's status doesn't allow the command (cancel
	// of a finished activity, retry of one that didn't fail, ...).
	NotEligible CommandOutcome = "not_eligible"
	// NotFound: the queue has no such activity.
	NotFound CommandOutcome = "not_found"
)

// CommandItem is a command's outcome for one activity.
type CommandItem struct {
	ID      uuid.UUID
	Outcome CommandOutcome
	// Status is the activity's status after the command (its current one
	// when not eligible); empty when deleted or not found.
	Status ActivityStatus
	// Reason says why the command didn't apply.
	Reason string
}

// CommandResult is a command's outcome.
type CommandResult struct {
	// CommandID is the command's id: WithCommandID's, or a generated one.
	CommandID string
	// Items has one entry per target activity: per id given, in order, or
	// per activity a filter selected.
	Items []CommandItem
	// Applied counts the activities the command changed.
	Applied int
	// Cascaded counts descendants a cancel also cancelled.
	Cascaded int
	// More is true when a filter matched more than Max activities.
	More bool
	// Replayed is true when the result is the recorded one of an earlier
	// call with the same command id; nothing was applied this time.
	Replayed bool
}

// Item returns the outcome for id.
func (r *CommandResult) Item(id uuid.UUID) (CommandItem, bool) {
	for _, it := range r.Items {
		if it.ID == id {
			return it, true
		}
	}
	return CommandItem{}, false
}

// Cancel cancels unfinished activities and, unless NoCascade, their
// unfinished descendants. Each stores a cancellation error as its result, so
// awaiting parents and Result callers see IsCancelled. A running handler's
// context is cancelled promptly, on whichever worker holds it.
func (c *Client) Cancel(ctx context.Context, target Target, opts ...CommandOption) (*CommandResult, error) {
	return c.apply(ctx, storage.Command{Kind: storage.CommandCancel}, target, opts)
}

// Retry makes failed, dead-lettered or cancelled activities pending again.
// Completed steps replay from their checkpoints. Pass ResetAttempts to
// restore the full attempt budget.
func (c *Client) Retry(ctx context.Context, target Target, opts ...CommandOption) (*CommandResult, error) {
	return c.apply(ctx, storage.Command{Kind: storage.CommandRetry}, target, opts)
}

// RunNow makes scheduled activities (delayed, or waiting out a retry
// backoff) and waiting ones (parked on a sleep, signal or child) runnable
// now. A woken waiter replays its handler and parks again if what it waits
// for isn't there.
func (c *Client) RunNow(ctx context.Context, target Target, opts ...CommandOption) (*CommandResult, error) {
	return c.apply(ctx, storage.Command{Kind: storage.CommandRunNow}, target, opts)
}

// Reschedule moves scheduled activities to run at at.
func (c *Client) Reschedule(ctx context.Context, target Target, at time.Time, opts ...CommandOption) (*CommandResult, error) {
	return c.apply(ctx, storage.Command{Kind: storage.CommandReschedule, At: at}, target, opts)
}

// SetPriority changes unfinished activities' priority.
func (c *Client) SetPriority(ctx context.Context, target Target, priority ActivityPriority, opts ...CommandOption) (*CommandResult, error) {
	return c.apply(ctx, storage.Command{Kind: storage.CommandSetPriority, Priority: storage.ActivityPriority(priority)}, target, opts)
}

// Repin moves unfinished activities to another code version, for when old
// work is known to be safe on new code (docs/versions.md). Only workers
// serving version claim them from then on; a running attempt finishes where
// it runs. Activities already on version, and finished ones, are not
// eligible. Children spawned later take their root's version, so repin a
// workflow by its tree (filter on root_id) or a whole version (filter on
// version).
func (c *Client) Repin(ctx context.Context, target Target, version string, opts ...CommandOption) (*CommandResult, error) {
	if err := validateVersion(version); err != nil {
		return nil, err
	}
	return c.apply(ctx, storage.Command{Kind: storage.CommandRepin, Version: version}, target, opts)
}

// Delete removes finished root activities with their whole tree: children,
// events, results and idempotency keys. Children can't be deleted alone, and
// an unfinished tree must be cancelled first.
func (c *Client) Delete(ctx context.Context, target Target, opts ...CommandOption) (*CommandResult, error) {
	return c.apply(ctx, storage.Command{Kind: storage.CommandDelete}, target, opts)
}

func (c *Client) apply(ctx context.Context, cmd storage.Command, target Target, opts []CommandOption) (*CommandResult, error) {
	cs, ok := c.backend.(storage.CommandStorage)
	if !ok {
		return nil, &WorkerError{Kind: ErrConfiguration, Message: fmt.Sprintf("backend %T does not take commands (storage.CommandStorage)", c.backend)}
	}
	var cfg commandConfig
	for _, o := range opts {
		o(&cfg)
	}
	switch {
	case cfg.noCasc && cmd.Kind != storage.CommandCancel:
		return nil, &WorkerError{Kind: ErrConfiguration, Message: "NoCascade applies to Cancel only"}
	case cfg.reset && cmd.Kind != storage.CommandRetry:
		return nil, &WorkerError{Kind: ErrConfiguration, Message: "ResetAttempts applies to Retry only"}
	}
	cmd.ID = cfg.id
	if cmd.ID == "" {
		cmd.ID = uuid.NewString()
	}
	cmd.Target = storage.CommandTarget{IDs: target.IDs, Filter: target.Filter, Max: target.Max}
	cmd.DryRun, cmd.Reason = cfg.dryRun, cfg.reason
	cmd.CascadeChildren = cmd.Kind == storage.CommandCancel && !cfg.noCasc
	cmd.ResetAttempts = cfg.reset
	cmd.Fingerprint = storage.CommandFingerprint(cmd)

	res, err := cs.ApplyCommand(ctx, cmd)
	if err != nil {
		if se, ok := storage.IsStorageError(err); ok && se.Kind == storage.ErrConflict {
			return nil, &WorkerError{Kind: ErrCommandConflict, Message: se.Message, Cause: err}
		}
		return nil, clientError(err)
	}
	out := &CommandResult{CommandID: cmd.ID, Applied: res.Applied, Cascaded: res.Cascaded, More: res.More,
		Replayed: res.Replayed, Items: make([]CommandItem, len(res.Items))}
	for i, it := range res.Items {
		item := CommandItem{ID: it.ID, Status: ActivityStatus(it.Status), Reason: it.ErrMessage}
		switch {
		case it.Outcome == storage.CommandApplied:
			item.Outcome = Applied
		case it.Outcome == storage.CommandWouldApply:
			item.Outcome = WouldApply
		case it.ErrKind == storage.ErrNotFound:
			item.Outcome = NotFound
		default:
			item.Outcome = NotEligible
		}
		out.Items[i] = item
		// The heartbeat would notice within an interval; this is immediate.
		if c.interrupt != nil && cmd.Kind == storage.CommandCancel && item.Outcome == Applied && !res.Replayed {
			c.interrupt(it.ID)
		}
	}
	return out, nil
}
