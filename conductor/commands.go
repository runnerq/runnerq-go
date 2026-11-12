package conductor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/conductor/internal/wire"
	"github.com/runnerq/runnerq-go/storage"
)

var commandKinds = map[string]storage.CommandKind{
	wire.TypeActivitiesCancel:      storage.CommandCancel,
	wire.TypeActivitiesRetry:       storage.CommandRetry,
	wire.TypeActivitiesRunNow:      storage.CommandRunNow,
	wire.TypeActivitiesReschedule:  storage.CommandReschedule,
	wire.TypeActivitiesSetPriority: storage.CommandSetPriority,
	wire.TypeActivitiesDelete:      storage.CommandDelete,
	wire.TypeActivitiesSignal:      storage.CommandSignal,
}

func (h *handlers) addCommands(t map[string]handlerFunc) {
	if h.cs == nil || !h.allowControl {
		return
	}
	for msgType, kind := range commandKinds {
		t[msgType] = h.command(kind)
	}
}

func (h *handlers) commandCapabilities(caps map[string]wire.Capability) {
	if h.cs == nil || !h.allowControl {
		return
	}
	for msgType, kind := range commandKinds {
		targets := []wire.TargetKind{wire.TargetFilter, wire.TargetIDs}
		if kind == storage.CommandSignal {
			targets = []wire.TargetKind{wire.TargetFilter, wire.TargetIdempotencyKey, wire.TargetIDs}
		}
		caps[msgType] = wire.Capability{V: 1, Targets: targets}
	}
}

// fingerprint identifies a command's input independent of JSON key order.
func fingerprint(data json.RawMessage) string {
	var v any
	if json.Unmarshal(data, &v) == nil {
		if canon, err := json.Marshal(v); err == nil {
			data = canon
		}
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (h *handlers) command(kind storage.CommandKind) handlerFunc {
	return func(ctx context.Context, data json.RawMessage) (any, error) {
		cmd, target, err := toCommand(kind, data)
		if err != nil {
			return nil, err
		}
		if q := target.Queue; q != "" && q != h.queue {
			return nil, fieldError(wire.CodeFailedPrecondition, "target.queue", "this executor serves queue %q, not %q", h.queue, q)
		}
		badIDs, err := toTarget(&cmd, target)
		if err != nil {
			return nil, err
		}
		cmd.Fingerprint = fingerprint(data)

		res := &storage.CommandResult{}
		if len(target.IDs) == 0 || len(cmd.Target.IDs) > 0 {
			res, err = h.cs.ApplyCommand(ctx, cmd)
		} // else every id was foreign
		if err != nil {
			if se, ok := storage.IsStorageError(err); ok && se.Kind == storage.ErrConflict {
				return nil, errorf(wire.CodeConflict, "%s", se.Message)
			}
			return nil, err
		}
		// Stop a cancelled activity running here now, not at its next heartbeat.
		if kind == storage.CommandCancel && !cmd.DryRun && !res.Replayed && h.engine != nil {
			for _, it := range res.Items {
				if it.Outcome == storage.CommandApplied {
					h.engine.Interrupt(it.ID)
				}
			}
		}

		out := wire.CommandResult{Matched: res.Matched, Applied: res.Applied, Cascaded: res.Cascaded,
			More: res.More, Replayed: res.Replayed, Results: make([]wire.CommandItem, 0, len(res.Items)+len(badIDs))}
		for _, it := range res.Items {
			ci := wire.CommandItem{ID: it.ID.String(), Outcome: wire.CommandOutcome(it.Outcome), Status: wire.ActivityStatus(it.Status)}
			switch {
			case it.ErrMessage == "":
			case it.ErrKind == storage.ErrNotFound:
				ci.Error = (*wire.Error)(errorf(wire.CodeNotFound, "%s", it.ErrMessage))
			default:
				ci.Error = (*wire.Error)(errorf(wire.CodeFailedPrecondition, "%s", it.ErrMessage))
				if it.Status != "" {
					ci.Error.Details = map[string]any{"status": it.Status}
				}
			}
			out.Results = append(out.Results, ci)
		}
		// Ids this backend could not have issued do not exist.
		for _, id := range badIDs {
			out.Results = append(out.Results, wire.CommandItem{ID: id, Outcome: wire.OutcomeSkipped,
				Error: (*wire.Error)(errorf(wire.CodeNotFound, "no such activity"))})
		}
		return out, nil
	}
}

// toCommand decodes the command's own request type strictly, so each command
// accepts only its own fields.
func toCommand(kind storage.CommandKind, data json.RawMessage) (storage.Command, wire.Target, error) {
	cmd := storage.Command{Kind: kind}
	var target wire.Target
	common := func(id string, t wire.Target, dryRun bool, reason string) {
		cmd.ID, target, cmd.DryRun, cmd.Reason = id, t, dryRun, reason
	}
	switch kind {
	case storage.CommandCancel:
		req, err := decode[wire.CancelRequest](data)
		if err != nil {
			return cmd, target, err
		}
		common(req.CommandID, req.Target, req.DryRun, req.Reason)
		switch req.Cascade {
		case "", wire.CascadeChildren:
			cmd.CascadeChildren = true // cascading is the default
		case wire.CascadeNone:
		default:
			return cmd, target, fieldError(wire.CodeInvalidArgument, "cascade", "cascade must be children or none")
		}
	case storage.CommandRetry:
		req, err := decode[wire.RetryRequest](data)
		if err != nil {
			return cmd, target, err
		}
		common(req.CommandID, req.Target, req.DryRun, req.Reason)
		cmd.ResetAttempts = req.ResetAttempts
	case storage.CommandRunNow:
		req, err := decode[wire.RunNowRequest](data)
		if err != nil {
			return cmd, target, err
		}
		common(req.CommandID, req.Target, req.DryRun, req.Reason)
	case storage.CommandReschedule:
		req, err := decode[wire.RescheduleRequest](data)
		if err != nil {
			return cmd, target, err
		}
		common(req.CommandID, req.Target, req.DryRun, req.Reason)
		at, err := time.Parse(time.RFC3339Nano, req.At)
		if err != nil {
			return cmd, target, fieldError(wire.CodeInvalidArgument, "at", "at must be an RFC 3339 timestamp")
		}
		cmd.At = at
	case storage.CommandSetPriority:
		req, err := decode[wire.SetPriorityRequest](data)
		if err != nil {
			return cmd, target, err
		}
		common(req.CommandID, req.Target, req.DryRun, req.Reason)
		cmd.Priority = storage.ActivityPriority(req.Priority)
	case storage.CommandDelete:
		req, err := decode[wire.DeleteRequest](data)
		if err != nil {
			return cmd, target, err
		}
		common(req.CommandID, req.Target, req.DryRun, req.Reason)
		if req.Cascade != "" && req.Cascade != wire.CascadeTree {
			return cmd, target, fieldError(wire.CodeInvalidArgument, "cascade", `delete removes the whole tree: cascade must be "tree"`)
		}
	case storage.CommandSignal:
		req, err := decode[wire.SignalRequest](data)
		if err != nil {
			return cmd, target, err
		}
		common(req.CommandID, req.Target, req.DryRun, req.Reason)
		cmd.SignalName, cmd.SignalPayload = req.Name, req.Payload
	}
	return cmd, target, nil
}

// toTarget sets cmd's target and returns the target ids this backend could
// not have issued.
func toTarget(cmd *storage.Command, t wire.Target) ([]string, error) {
	var badIDs []string
	switch {
	case len(t.IDs) > 0:
		for _, s := range t.IDs {
			if id, err := uuid.Parse(s); err == nil {
				cmd.Target.IDs = append(cmd.Target.IDs, id)
			} else {
				badIDs = append(badIDs, s)
			}
		}
	case t.Filter != nil:
		f, err := toStorageFilter(t.Filter)
		if err != nil {
			return nil, err
		}
		cmd.Target.Filter, cmd.Target.Max = f, t.Max
	case t.IdempotencyKey != "":
		if t.Type == "" {
			return nil, fieldError(wire.CodeInvalidArgument, "target.type", "an idempotency_key target needs the activity type")
		}
		cmd.Target.IdempotencyKey = storage.BusinessIdempotencyKey(t.IdempotencyKey, t.Type)
	}
	return badIDs, nil
}
