package conductor

import (
	"context"
	"encoding/json"

	"github.com/runnerq/runnerq-go/conductor/internal/wire"
	"github.com/runnerq/runnerq-go/storage"
)

// Handler serves the Cloud protocol's queries and commands from a storage
// backend, as an agent does, but without a worker engine or a connection.
//
// Reads cover whatever the backend's queries cover (for Postgres, every
// queue in its schema); commands act on the backend's own queue. Handler
// checks no caller identity and applies no deadline or size limit: the
// caller authorizes requests, scopes the backend to what they may see, and
// sets deadlines and bounds answer sizes.
type Handler struct {
	h     *handlers
	table map[string]handlerFunc
}

// HandlerConfig configures a Handler.
type HandlerConfig struct {
	// AllowControl serves commands (cancel, retry, run now, reschedule, set
	// priority, delete, signal) when the backend supports them and has a
	// QueueName() string method; targets naming another queue fail.
	AllowControl bool
}

type queueNamer interface{ QueueName() string }

// Error is a failed request as sent on the wire.
type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// NewHandler returns a Handler for backend. It serves queries when backend
// implements storage.QueryStorage, and commands when it implements
// storage.CommandStorage, reports its queue and cfg allows them.
func NewHandler(backend storage.Storage, cfg HandlerConfig) *Handler {
	qs, _ := backend.(storage.QueryStorage)
	cs, _ := backend.(storage.CommandStorage)
	// Commands are checked against the backend's own queue, never a name
	// from elsewhere: without one, there are no commands.
	named, ok := backend.(queueNamer)
	if !ok {
		cs = nil
	}
	h := &handlers{backend: backend, qs: qs, cs: cs, allowControl: cfg.AllowControl}
	if ok {
		h.queue = named.QueueName()
	}
	t := h.table()
	delete(t, wire.TypeExecutorDescribe)
	return &Handler{h: h, table: t}
}

// Serve answers one request (msgType and data from its envelope) with the
// response data or the error to send instead. Handler panics become errors.
func (h *Handler) Serve(ctx context.Context, msgType string, data json.RawMessage) (res json.RawMessage, werr *Error) {
	defer func() {
		if p := recover(); p != nil {
			res, werr = nil, public(errorf(wire.CodeInternal, "handler panicked"))
		}
	}()
	serve, ok := h.table[msgType]
	if !ok {
		return nil, public(errorf(wire.CodeUnsupported, "this handler does not serve %q", msgType))
	}
	out, err := serve(ctx, data)
	if err != nil {
		return nil, public(toWireError(err))
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, public(errorf(wire.CodeInternal, "encode response: %v", err))
	}
	return encoded, nil
}

// Capabilities describes what the handler serves as a JSON object keyed by
// message type, as in an agent's hello.
func (h *Handler) Capabilities() json.RawMessage {
	caps := h.h.capabilities()
	delete(caps, wire.TypeExecutorDescribe)
	// Live events need a connection, which a Handler doesn't have.
	delete(caps, wire.TypeEventsSubscribe)
	delete(caps, wire.TypeEventsUnsubscribe)
	b, _ := json.Marshal(caps)
	return b
}

func public(e *wireError) *Error {
	return &Error{Code: string(e.Code), Message: e.Message, Details: e.Details}
}
