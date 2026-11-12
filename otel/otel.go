// Package otel reports a RunnerQ engine's traces and metrics through
// OpenTelemetry. It is its own module so the SDK depends on no OpenTelemetry
// package.
//
//	engine, err := runnerq.Builder().
//	    Backend(backend).
//	    Tracer(otel.NewTracer()).
//	    LabeledMetrics(otel.MustNewMetrics()).
//	    GaugeInterval(30 * time.Second).
//	    Build()
//
// Spans: one per enqueue, attempt, executed step, durable sleep, signal wait
// and signal sent (runnerq-spec observability/tracing.json). Every attempt is
// a child of the enqueue span through the traceparent stored with the
// activity, so retries and the attempts that resume after a sleep or signal
// form one trace with the enqueue and every child activity.
//
// Metrics: every metric of runnerq.MetricCatalog as an instrument named
// "runnerq." plus its name, with the catalog's unit and labels as attributes.
package otel

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/runnerq/runnerq-go"
)

// ScopeName is the instrumentation scope of the tracer and meter.
const ScopeName = "github.com/runnerq/runnerq-go"

// Prefix is prepended to every metric name.
const Prefix = "runnerq."

// Option configures NewTracer and NewMetrics.
type Option func(*options)

type options struct {
	tp trace.TracerProvider
	mp metric.MeterProvider
}

// WithTracerProvider sets the provider spans come from (default: the global
// one, otel.GetTracerProvider).
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.tp = tp }
}

// WithMeterProvider sets the provider instruments come from (default: the
// global one, otel.GetMeterProvider).
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *options) { o.mp = mp }
}

func resolve(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.tp == nil {
		o.tp = otel.GetTracerProvider()
	}
	if o.mp == nil {
		o.mp = otel.GetMeterProvider()
	}
	return o
}

// NewTracer returns a runnerq.Tracer that starts OpenTelemetry spans.
func NewTracer(opts ...Option) runnerq.Tracer {
	return &tracer{t: resolve(opts).tp.Tracer(ScopeName)}
}

type tracer struct{ t trace.Tracer }

var traceContext propagation.TraceContext

var spanKinds = map[string]trace.SpanKind{
	runnerq.SpanEnqueue:    trace.SpanKindProducer,
	runnerq.SpanSignalSend: trace.SpanKindProducer,
	runnerq.SpanAttempt:    trace.SpanKindConsumer,
}

func (t *tracer) Start(ctx context.Context, name, parent string, attrs ...runnerq.TraceAttribute) (context.Context, runnerq.EndSpan) {
	if parent != "" {
		ctx = traceContext.Extract(ctx, propagation.MapCarrier{"traceparent": parent})
	}
	kind, ok := spanKinds[name]
	if !ok {
		kind = trace.SpanKindInternal
	}
	ctx, span := t.t.Start(ctx, name, trace.WithSpanKind(kind), trace.WithAttributes(attributes(attrs)...))
	return ctx, func(outcome string, err error, attrs ...runnerq.TraceAttribute) {
		span.SetAttributes(attribute.String(runnerq.AttrOutcome, outcome))
		span.SetAttributes(attributes(attrs)...)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func (t *tracer) Traceparent(ctx context.Context) string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ""
	}
	carrier := propagation.MapCarrier{}
	traceContext.Inject(ctx, carrier)
	return carrier["traceparent"]
}

func attributes(attrs []runnerq.TraceAttribute) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		switch v := a.Value.(type) {
		case string:
			out = append(out, attribute.String(a.Key, v))
		case int64:
			out = append(out, attribute.Int64(a.Key, v))
		case int:
			out = append(out, attribute.Int(a.Key, v))
		case bool:
			out = append(out, attribute.Bool(a.Key, v))
		}
	}
	return out
}

// NewMetrics returns a runnerq.LabeledMetricsSink that records every metric
// of runnerq.MetricCatalog into OpenTelemetry instruments.
func NewMetrics(opts ...Option) (runnerq.LabeledMetricsSink, error) {
	meter := resolve(opts).mp.Meter(ScopeName)
	m := &metrics{
		counters:   map[string]metric.Int64Counter{},
		histograms: map[string]metric.Float64Histogram{},
		gauges:     map[string]metric.Float64Gauge{},
	}
	for _, info := range runnerq.MetricCatalog {
		name := Prefix + info.Name
		desc, unit := metric.WithDescription(info.Description), metric.WithUnit(info.Unit)
		var err error
		switch info.Kind {
		case runnerq.MetricCounter:
			m.counters[info.Name], err = meter.Int64Counter(name, desc, unit)
		case runnerq.MetricHistogram:
			m.histograms[info.Name], err = meter.Float64Histogram(name, desc, unit)
		case runnerq.MetricGauge:
			m.gauges[info.Name], err = meter.Float64Gauge(name, desc, unit)
		}
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}

// MustNewMetrics is NewMetrics, panicking on an error.
func MustNewMetrics(opts ...Option) runnerq.LabeledMetricsSink {
	m, err := NewMetrics(opts...)
	if err != nil {
		panic(err)
	}
	return m
}

type metrics struct {
	counters   map[string]metric.Int64Counter
	histograms map[string]metric.Float64Histogram
	gauges     map[string]metric.Float64Gauge
	// sets caches attribute sets by labels: the engine reuses a handful.
	sets sync.Map // runnerq.MetricLabels -> metric.MeasurementOption
}

func (m *metrics) labels(l runnerq.MetricLabels) metric.MeasurementOption {
	if o, ok := m.sets.Load(l); ok {
		return o.(metric.MeasurementOption)
	}
	kv := make([]attribute.KeyValue, 0, 4)
	if l.Queue != "" {
		kv = append(kv, attribute.String(runnerq.LabelQueue, l.Queue))
	}
	if l.ActivityType != "" {
		kv = append(kv, attribute.String(runnerq.LabelActivityType, l.ActivityType))
	}
	if l.Status != "" {
		kv = append(kv, attribute.String(runnerq.LabelStatus, l.Status))
	}
	if l.Version != "" {
		kv = append(kv, attribute.String(runnerq.LabelVersion, l.Version))
	}
	o := metric.WithAttributeSet(attribute.NewSet(kv...))
	m.sets.Store(l, o)
	return o
}

func (m *metrics) AddCounter(name string, value uint64, l runnerq.MetricLabels) {
	if c, ok := m.counters[name]; ok {
		c.Add(context.Background(), int64(value), m.labels(l))
	}
}

func (m *metrics) RecordHistogram(name string, value float64, l runnerq.MetricLabels) {
	if h, ok := m.histograms[name]; ok {
		h.Record(context.Background(), value, m.labels(l))
	}
}

func (m *metrics) SetGauge(name string, value float64, l runnerq.MetricLabels) {
	if g, ok := m.gauges[name]; ok {
		g.Record(context.Background(), value, m.labels(l))
	}
}
