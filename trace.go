package runnerq

import (
	"context"
	"strings"
)

// Tracer starts the spans the engine reports: one per enqueue, attempt,
// executed step, durable sleep, signal wait and signal sent, named by the
// Span* constants and described by Attr* attributes (runnerq-spec
// observability/tracing.json). The module github.com/runnerq/runnerq-go/otel
// implements it with OpenTelemetry; the SDK itself depends on no tracing
// library. Implementations must be safe for concurrent use.
//
// Whether or not a Tracer is set, every enqueue stores a W3C traceparent in
// the activity's metadata (MetadataTraceparent) when there is one to store:
// the caller's own, the enqueue span's, or the enqueueing handler's activity's.
// An attempt continues the stored one, so retries and the attempts that
// resume after a sleep or signal wait join the enqueue's trace.
type Tracer interface {
	// Start starts the span name as a child of parent, a W3C traceparent, or
	// of ctx's current span when parent is "". It returns ctx carrying the
	// new span and the function that ends it.
	Start(ctx context.Context, name, parent string, attrs ...TraceAttribute) (context.Context, EndSpan)
	// Traceparent returns ctx's current span as a W3C traceparent, or "" when
	// ctx carries none.
	Traceparent(ctx context.Context) string
}

// TraceAttribute is a span attribute keyed by an Attr* constant; Value is a
// string or an int64.
type TraceAttribute struct {
	Key   string
	Value any
}

// EndSpan ends a span with its outcome (an Outcome* constant), the error that
// caused it if any, and attributes learnt meanwhile.
type EndSpan func(outcome string, err error, attrs ...TraceAttribute)

func noEnd(string, error, ...TraceAttribute) {}

// traceparentKey carries the running activity's stored traceparent on its
// handler's context, for children enqueued without a tracer.
type traceparentKey struct{}

// startSpan starts a span when a tracer is set; attrs is only called then.
func startSpan(ctx context.Context, t Tracer, name, parent string, attrs func() []TraceAttribute) (context.Context, EndSpan) {
	if t == nil {
		return ctx, noEnd
	}
	return t.Start(ctx, name, parent, attrs()...)
}

// setTraceparent stores the traceparent an enqueue on ctx should carry in
// metadata (runnerq-spec vectors/traceparent.json) and returns metadata,
// allocated if needed.
func setTraceparent(ctx context.Context, t Tracer, metadata map[string]string) map[string]string {
	if _, explicit := metadata[MetadataTraceparent]; explicit {
		return metadata
	}
	var active string
	if t != nil {
		active = t.Traceparent(ctx)
	}
	inherited, _ := ctx.Value(traceparentKey{}).(string)
	tp, ok := storedTraceparent(nil, active, inherited)
	if !ok {
		return metadata
	}
	if metadata == nil {
		metadata = make(map[string]string, 1)
	}
	metadata[MetadataTraceparent] = tp
	return metadata
}

// storedTraceparent is runnerq-spec vectors/traceparent.json: the caller's
// value as given, else the first valid of active and inherited.
func storedTraceparent(explicit *string, active, inherited string) (string, bool) {
	if explicit != nil {
		return *explicit, true
	}
	for _, tp := range []string{active, inherited} {
		if ValidTraceparent(tp) {
			return tp, true
		}
	}
	return "", false
}

// ValidTraceparent reports whether s is a W3C trace context traceparent this
// SDK continues: lowercase hex version (not ff), a non-zero 32-digit trace id,
// a non-zero 16-digit parent id and flags; version 00 has nothing after them.
func ValidTraceparent(s string) bool {
	if len(s) < 55 || (len(s) > 55 && (s[:2] == "00" || s[55] != '-')) {
		return false
	}
	f := strings.Split(s[:55], "-")
	return len(f) == 4 && lowerHex(f[0], 2) && f[0] != "ff" &&
		lowerHex(f[1], 32) && strings.Trim(f[1], "0") != "" &&
		lowerHex(f[2], 16) && strings.Trim(f[2], "0") != "" &&
		lowerHex(f[3], 2)
}

func lowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := range len(s) {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
