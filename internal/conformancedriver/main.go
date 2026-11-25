// Command conformancedriver runs runnerq-spec's conformance scenarios'
// storage operations on the Postgres backend, speaking the spec's driver
// protocol (conformance/README.md) on stdin and stdout.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
	"github.com/runnerq/runnerq-go/storage/postgres"
)

func main() {
	d := &driver{}
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 16<<20)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		var req struct {
			Op   string          `json:"op"`
			Args json.RawMessage `json:"args"`
		}
		if err := json.Unmarshal(in.Bytes(), &req); err != nil {
			fmt.Fprintln(os.Stderr, "conformancedriver:", err)
			os.Exit(1)
		}
		res, err := d.do(context.Background(), req.Op, req.Args)
		if err != nil {
			_ = out.Encode(map[string]any{"error": map[string]string{"kind": kindOf(err), "message": err.Error()}})
			continue
		}
		if res == nil {
			res = struct{}{}
		}
		_ = out.Encode(map[string]any{"ok": res})
	}
	if d.b != nil {
		d.b.Close()
	}
}

type driver struct {
	b *postgres.PostgresBackend
}

type fence struct {
	ID    uuid.UUID `json:"id"`
	Token string    `json:"token"`
}

type args struct {
	DSN   string `json:"dsn"`
	Queue string `json:"queue"`

	ID          uuid.UUID         `json:"id"`
	Type        string            `json:"type"`
	Payload     json.RawMessage   `json:"payload"`
	Priority    *int              `json:"priority"`
	MaxAttempts *uint32           `json:"max_attempts"`
	TimeoutS    *uint64           `json:"timeout_s"`
	DelayS      float64           `json:"delay_s"`
	Metadata    map[string]string `json:"metadata"`
	Parent      *uuid.UUID        `json:"parent"`
	Root        *uuid.UUID        `json:"root"`
	Depth       uint16            `json:"depth"`
	// Key is {key, on_duplicate} for submit, an encoded key for lookup_key.
	Key   json.RawMessage `json:"key"`
	Fence *fence          `json:"fence"`

	Types   []string `json:"types"`
	Limit   int      `json:"limit"`
	LeaseMS int64    `json:"lease_ms"`

	Claim     fence           `json:"claim"`
	Value     json.RawMessage `json:"value"`
	Reason    string          `json:"reason"`
	Retryable bool            `json:"retryable"`
	ResultID  uuid.UUID       `json:"result_id"`
	State     string          `json:"state"`
	Data      json.RawMessage `json:"data"`
	Step      string          `json:"step"`
	Producer  *uuid.UUID      `json:"producer"`
	Kind      string          `json:"kind"`
	WakeAt    time.Time       `json:"wake_at"`
	Target    uuid.UUID       `json:"target"`
	Name      string          `json:"name"`

	CompletedS float64 `json:"completed_s"`
	FailedS    float64 `json:"failed_s"`

	EventsS float64 `json:"events_s"`
	Batch   int     `json:"batch"`

	CommandID     string               `json:"command_id"`
	Fingerprint   string               `json:"fingerprint"`
	IDs           []uuid.UUID          `json:"ids"`
	Filter        *storage.QueryFilter `json:"filter"`
	Max           int                  `json:"max"`
	DryRun        bool                 `json:"dry_run"`
	Cascade       bool                 `json:"cascade"`
	ResetAttempts bool                 `json:"reset_attempts"`
	At            time.Time            `json:"at"`

	Cron         string `json:"cron"`
	Timezone     string `json:"timezone"`
	CatchUp      string `json:"catch_up"`
	CatchUpLimit int    `json:"catch_up_limit"`

	MaxConcurrent int     `json:"max_concurrent"`
	RateTokens    int     `json:"rate_tokens"`
	RatePerS      float64 `json:"rate_per_s"`
}

var behaviors = map[string]storage.IdempotencyBehavior{
	"allow_reuse":            storage.BehaviorAllowReuse,
	"return_existing":        storage.BehaviorReturnExisting,
	"allow_reuse_on_failure": storage.BehaviorAllowReuseOnFailure,
	"no_reuse":               storage.BehaviorNoReuse,
}

func (d *driver) do(ctx context.Context, op string, raw json.RawMessage) (any, error) {
	var a args
	if err := json.Unmarshal(raw, &a); err != nil {
		return nil, &storage.StorageError{Kind: storage.ErrInvalidArgument, Message: err.Error()}
	}
	if op != "open" && d.b == nil {
		return nil, errors.New("open first")
	}
	switch op {
	case "open":
		if d.b != nil {
			d.b.Close()
		}
		b, err := postgres.WithConfig(ctx, a.DSN, a.Queue, 30_000, 4)
		d.b = b
		return nil, err
	case "submit":
		return d.submit(ctx, a)
	case "claim":
		lease := a.LeaseMS
		if lease == 0 {
			lease = 30_000
		}
		d.b.SetLeaseMS(lease)
		limit := max(a.Limit, 1)
		got, err := d.b.DequeueBatch(ctx, "conformance:"+uuid.NewString(), limit, 0, a.Types)
		claims := make([]fence, len(got))
		for i, c := range got {
			claims[i] = fence{ID: c.Activity.ID, Token: c.LeaseID}
		}
		return map[string]any{"claims": claims}, err
	case "renew":
		ok, err := d.b.ExtendLeaseForWorker(ctx, a.Claim.ID, a.Claim.Token, time.Duration(a.LeaseMS)*time.Millisecond)
		return map[string]any{"renewed": ok}, err
	case "complete":
		return nil, d.b.AckSuccess(ctx, a.Claim.ID, a.Value, a.Claim.Token)
	case "fail":
		dead, err := d.b.AckFailure(ctx, a.Claim.ID, storage.FailureKind{Retryable: a.Retryable, Reason: a.Reason}, a.Claim.Token)
		outcome := "failed"
		if a.Retryable && dead {
			outcome = "dead_letter"
		} else if a.Retryable {
			outcome = "retrying"
		}
		return map[string]any{"outcome": outcome}, err
	case "checkpoint":
		return nil, d.b.StoreCheckpoint(ctx, a.ResultID, a.Claim.ID, a.Claim.Token, result(a.State, a.Data), a.Step)
	case "register_dependency":
		return nil, d.b.RegisterDependency(ctx, a.Claim.ID, *a.Producer, a.Claim.Token)
	case "park":
		if a.ResultID == uuid.Nil {
			return nil, d.b.Yield(ctx, a.Claim.ID, a.WakeAt, a.Claim.Token, a.Kind, a.Step)
		}
		return nil, d.b.YieldForResult(ctx, a.Claim.ID, a.ResultID, a.Producer, a.WakeAt, a.Claim.Token, a.Kind, a.Step)
	case "signal":
		return nil, d.b.SignalActivity(ctx, a.Target, storage.CheckpointID(a.Target, "signal", a.Name), a.Name, a.Payload)
	case "lookup_key":
		var key string
		_ = json.Unmarshal(a.Key, &key)
		id, err := d.b.LookupIdempotencyActivityID(ctx, key)
		return map[string]any{"id": id}, err
	case "reap":
		n, err := d.b.RequeueExpired(ctx, max(a.Limit, 1))
		return map[string]any{"count": n}, err
	case "cleanup":
		n, err := d.b.CleanupExpired(ctx, storage.RetentionPolicy{
			Completed: time.Duration(a.CompletedS * float64(time.Second)),
			Failed:    time.Duration(a.FailedS * float64(time.Second)),
			Events:    time.Duration(a.EventsS * float64(time.Second)),
		}, max(a.Batch, 1))
		return map[string]any{"count": n}, err
	case "upsert_schedule":
		o := storage.ScheduleOptions{Priority: storage.PriorityNormal, MaxRetries: 3, TimeoutSeconds: 30, RetryDelaySeconds: 1, Metadata: a.Metadata}
		if a.Priority != nil {
			o.Priority = storage.ActivityPriority(*a.Priority)
		}
		if a.MaxAttempts != nil {
			o.MaxRetries = *a.MaxAttempts
		}
		if a.TimeoutS != nil {
			o.TimeoutSeconds = *a.TimeoutS
		}
		return nil, d.b.UpsertSchedule(ctx, storage.Schedule{
			Name: a.Name, Cron: a.Cron, Timezone: a.Timezone, ActivityType: a.Type, Payload: a.Payload,
			Options: o, CatchUp: storage.CatchUp(a.CatchUp), CatchUpLimit: a.CatchUpLimit,
		})
	case "pause_schedule":
		return nil, d.b.PauseSchedule(ctx, a.Name)
	case "resume_schedule":
		return nil, d.b.ResumeSchedule(ctx, a.Name)
	case "delete_schedule":
		return nil, d.b.DeleteSchedule(ctx, a.Name)
	case "trigger_schedule":
		id, err := d.b.TriggerSchedule(ctx, a.Name)
		return map[string]any{"id": id}, err
	case "list_schedules":
		list, err := d.b.ListSchedules(ctx)
		names := []string{}
		for _, s := range list {
			names = append(names, s.Name)
		}
		return map[string]any{"names": names}, err
	case "fire_schedules":
		n, err := d.b.FireDueSchedules(ctx, max(a.Limit, 100))
		return map[string]any{"count": n}, err
	case "set_limit":
		return nil, d.b.SetLimit(ctx, storage.Limit{ActivityType: a.Type, MaxConcurrent: a.MaxConcurrent,
			RateLimit: a.RateTokens, RatePeriod: time.Duration(a.RatePerS * float64(time.Second))})
	case "get_limit":
		l, err := d.b.GetLimit(ctx, a.Type)
		if kindOf(err) == "not_found" {
			return map[string]any{"limit": nil}, nil
		}
		if err != nil {
			return nil, err
		}
		out := map[string]any{"max_concurrent": nil, "rate_tokens": nil, "rate_per_s": nil}
		if l.MaxConcurrent > 0 {
			out["max_concurrent"] = l.MaxConcurrent
		}
		if l.RateLimit > 0 {
			out["rate_tokens"], out["rate_per_s"] = l.RateLimit, l.RatePeriod.Seconds()
		}
		return map[string]any{"limit": out}, nil
	case "delete_limit":
		return nil, d.b.DeleteLimit(ctx, a.Type)
	case "list_limits":
		list, err := d.b.ListLimits(ctx)
		types := []string{}
		for _, l := range list {
			types = append(types, l.ActivityType)
		}
		return map[string]any{"types": types}, err
	case "get_result":
		r, err := d.b.GetResult(ctx, a.ID)
		if err != nil || r == nil {
			return map[string]any{"result": nil}, err
		}
		state := "ok"
		if r.State == storage.ResultErr {
			state = "error"
		}
		return map[string]any{"result": map[string]any{"state": state, "data": r.Data, "serialization": r.Serialization}}, nil
	case "command":
		return d.command(ctx, a)
	}
	return nil, &storage.StorageError{Kind: storage.ErrUnsupported, Message: "unknown op " + op}
}

func (d *driver) command(ctx context.Context, a args) (any, error) {
	cmd := storage.Command{
		ID: a.CommandID, Fingerprint: a.Fingerprint, Kind: storage.CommandKind(a.Kind),
		Target: storage.CommandTarget{IDs: a.IDs, Filter: a.Filter, Max: a.Max},
		DryRun: a.DryRun, Reason: a.Reason, CascadeChildren: a.Cascade, ResetAttempts: a.ResetAttempts,
		At: a.At, SignalName: a.Name, SignalPayload: a.Payload,
	}
	if a.Priority != nil {
		cmd.Priority = storage.ActivityPriority(*a.Priority)
	}
	if cmd.Fingerprint == "" {
		cmd.Fingerprint = storage.CommandFingerprint(cmd)
	}
	res, err := d.b.ApplyCommand(ctx, cmd)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, len(res.Items))
	for i, it := range res.Items {
		item := map[string]any{"id": it.ID, "outcome": it.Outcome}
		if it.Status != "" {
			item["status"] = it.Status
		}
		if it.ErrMessage != "" {
			item["error"] = kindOf(&storage.StorageError{Kind: it.ErrKind})
		}
		items[i] = item
	}
	return map[string]any{"matched": res.Matched, "applied": res.Applied, "cascaded": res.Cascaded,
		"more": res.More, "replayed": res.Replayed, "items": items}, nil
}

func (d *driver) submit(ctx context.Context, a args) (any, error) {
	q := storage.QueuedActivity{
		ID: a.ID, ActivityType: a.Type, Payload: a.Payload,
		Priority: storage.PriorityNormal, MaxRetries: 3, TimeoutSeconds: 30,
		RetryDelaySeconds: 1, Metadata: a.Metadata, CreatedAt: time.Now().UTC(),
		ParentActivityID: a.Parent, RootActivityID: a.ID, Depth: a.Depth,
	}
	if a.Priority != nil {
		q.Priority = storage.ActivityPriority(*a.Priority)
	}
	if a.MaxAttempts != nil {
		q.MaxRetries = *a.MaxAttempts
	}
	if a.TimeoutS != nil {
		q.TimeoutSeconds = *a.TimeoutS
	}
	if a.Root != nil {
		q.RootActivityID = *a.Root
	}
	if a.DelayS > 0 {
		at := time.Now().UTC().Add(time.Duration(a.DelayS * float64(time.Second)))
		q.ScheduledAt = &at
	}
	var key *struct {
		Key         string `json:"key"`
		OnDuplicate string `json:"on_duplicate"`
	}
	if err := json.Unmarshal(a.Key, &key); len(a.Key) > 0 && err != nil {
		return nil, &storage.StorageError{Kind: storage.ErrInvalidArgument, Message: err.Error()}
	}
	if key == nil {
		if a.Fence != nil {
			return nil, d.b.EnqueueForWorker(ctx, q, a.Fence.ID, a.Fence.Token)
		}
		return nil, d.b.Enqueue(ctx, q)
	}
	behavior, ok := behaviors[key.OnDuplicate]
	if !ok {
		return nil, &storage.StorageError{Kind: storage.ErrInvalidArgument, Message: "unknown on_duplicate " + key.OnDuplicate}
	}
	q.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: storage.BusinessIdempotencyKey(key.Key, a.Type), Behavior: behavior}
	var res *storage.IdempotencyResult
	var err error
	if a.Fence != nil {
		res, err = d.b.EnqueueIdempotentForWorker(ctx, &q, a.Fence.ID, a.Fence.Token)
	} else {
		res, err = d.b.EnqueueIdempotent(ctx, &q)
	}
	if err != nil || res == nil {
		return nil, err
	}
	return map[string]any{"existing": res.ExistingID}, nil
}

func result(state string, data json.RawMessage) storage.ActivityResult {
	if state == "error" {
		return storage.ActivityResult{State: storage.ResultErr, Data: data}
	}
	return storage.ActivityResult{State: storage.ResultOk, Data: data}
}

// kindOf is a storage error's kind name (runnerq-spec constants.json
// storage_error_kind).
func kindOf(err error) string {
	var se *storage.StorageError
	if !errors.As(err, &se) {
		return "internal"
	}
	names := map[storage.StorageErrorKind]string{
		storage.ErrUnavailable: "unavailable", storage.ErrConflict: "conflict", storage.ErrNotFound: "not_found",
		storage.ErrInternal: "internal", storage.ErrSerialization: "serialization", storage.ErrConfiguration: "configuration",
		storage.ErrTimeout: "timeout", storage.ErrDuplicateActivity: "duplicate_activity",
		storage.ErrIdempotencyConflict: "idempotency_conflict", storage.ErrClaimLost: "claim_lost",
		storage.ErrCheckpointConflict: "checkpoint_conflict", storage.ErrInvalidArgument: "invalid_argument",
		storage.ErrUnsupported: "unsupported",
	}
	if n, ok := names[se.Kind]; ok {
		return n
	}
	return "internal"
}
