package otel_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/runnerq/runnerq-go"
	rqotel "github.com/runnerq/runnerq-go/otel"
	"github.com/runnerq/runnerq-go/storage"
	"github.com/runnerq/runnerq-go/storage/postgres"
)

type handler struct {
	runnerq.DefaultDeadLetterHandler
	fn func(runnerq.ActivityContext, json.RawMessage) (json.RawMessage, error)
}

func (h handler) Handle(c runnerq.ActivityContext, p json.RawMessage) (json.RawMessage, error) {
	return h.fn(c, p)
}

// One connected trace from the enqueue to the last step, across a retry, a
// durable sleep that parks the activity, and a child activity.
func TestOneTraceAcrossRetryAndSleep(t *testing.T) {
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping integration test")
	}
	queue := "o_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	backend, err := postgres.New(context.Background(), dsn, queue)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	spans := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	sink, err := rqotel.NewMetrics(rqotel.WithMeterProvider(mp))
	if err != nil {
		t.Fatal(err)
	}

	engine, err := runnerq.Builder().Backend(backend).QueueName(queue).MaxWorkers(2).
		Tracer(rqotel.NewTracer(rqotel.WithTracerProvider(tp))).LabeledMetrics(sink).Build()
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	engine.RegisterActivityWithName("order", handler{fn: func(c runnerq.ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		if _, err := c.RunStep("reserve", func(context.Context) (string, error) { return "r1", nil }); err != nil {
			return nil, err
		}
		if attempts.Add(1) == 1 {
			return nil, runnerq.NewRetryError("payment gateway busy")
		}
		// Longer than the timeout allows in-process: the activity parks.
		if err := c.Sleep("cool-off", 2*time.Second); err != nil {
			return nil, err
		}
		fut, err := c.ActivityExecutor.ActivityNamed("email").Step("email").Payload(json.RawMessage(`{}`)).Execute(c.Ctx)
		if err != nil {
			return nil, err
		}
		if _, err := fut.GetResult(c.Ctx); err != nil {
			return nil, err
		}
		out, err := c.RunStep("ship", func(context.Context) (string, error) { return "shipped", nil })
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}})
	engine.RegisterActivityWithName("email", handler{fn: func(runnerq.ActivityContext, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`"sent"`), nil
	}})
	done := make(chan error, 1)
	go func() { done <- engine.Start(context.Background()) }()
	defer func() { engine.Stop(); <-done }()

	// The caller's own span is the root of the trace.
	ctx, caller := tp.Tracer("test").Start(context.Background(), "checkout")
	fut, err := engine.GetActivityExecutor().ActivityNamed("order").Payload(json.RawMessage(`{}`)).
		MaxRetries(3).Timeout(3 * time.Second).Execute(ctx)
	caller.End()
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if res, err := backend.WaitForResult(waitCtx, fut.ActivityID()); err != nil || res.State != storage.ResultOk {
		t.Fatalf("result %v %v", res, err)
	}
	// The final attempt's span ends just after the result commits.
	deadline := time.Now().Add(5 * time.Second)
	var last tracetest.SpanStub
	for {
		got := spans.GetSpans()
		if i := lastStep(got, "ship"); i >= 0 && completedAttempt(got, "order") {
			last = got[i]
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("spans so far: %v", names(got))
		}
		time.Sleep(20 * time.Millisecond)
	}

	all := spans.GetSpans()
	traceID := caller.SpanContext().TraceID()
	byID := map[trace.SpanID]tracetest.SpanStub{}
	for _, s := range all {
		if s.SpanContext.TraceID() != traceID {
			t.Errorf("%s is in trace %s, want %s", s.Name, s.SpanContext.TraceID(), traceID)
		}
		byID[s.SpanContext.SpanID()] = s
	}
	// Walk from the last step to the caller: step → attempt → enqueue → checkout.
	var chain []string
	for s, ok := last, true; ok; s, ok = byID[s.Parent.SpanID()] {
		chain = append(chain, s.Name)
	}
	if want := []string{runnerq.SpanStep, runnerq.SpanAttempt, runnerq.SpanEnqueue, "checkout"}; strings.Join(chain, ",") != strings.Join(want, ",") {
		t.Fatalf("chain from the last step %v, want %v", chain, want)
	}
	enqueue := byID[byID[last.Parent.SpanID()].Parent.SpanID()]
	outcomes := map[string]bool{}
	for _, s := range all {
		if s.Name == runnerq.SpanAttempt && attr(s, runnerq.AttrActivityType) == "order" {
			if s.Parent.SpanID() != enqueue.SpanContext.SpanID() {
				t.Errorf("attempt %d's parent is not the enqueue span", attr(s, runnerq.AttrAttempt))
			}
			outcomes[attr(s, runnerq.AttrOutcome).(string)] = true
		}
	}
	for _, o := range []string{runnerq.OutcomeRetry, runnerq.OutcomeYielded, runnerq.OutcomeCompleted} {
		if !outcomes[o] {
			t.Errorf("no order attempt ended %s: %v", o, outcomes)
		}
	}
	for _, s := range all {
		if s.Name == runnerq.SpanSleep && attr(s, runnerq.AttrOutcome) == runnerq.OutcomeParked && s.SpanKind != trace.SpanKindInternal {
			t.Errorf("sleep span kind %v", s.SpanKind)
		}
		if s.Name == runnerq.SpanAttempt && attr(s, runnerq.AttrActivityType) == "email" &&
			byID[byID[s.Parent.SpanID()].Parent.SpanID()].Name != runnerq.SpanAttempt {
			t.Errorf("the child's attempt does not hang under the parent's attempt")
		}
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	completed := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != rqotel.Prefix+runnerq.MetricActivityCompleted {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				if v, _ := dp.Attributes.Value(attribute.Key(runnerq.LabelActivityType)); v.AsString() == "order" && dp.Value == 1 {
					q, _ := dp.Attributes.Value(attribute.Key(runnerq.LabelQueue))
					completed = q.AsString() == queue
				}
			}
		}
	}
	if !completed {
		t.Errorf("no runnerq.activity_completed{queue, activity_type=order} = 1 in %+v", rm.ScopeMetrics)
	}
}

func attr(s tracetest.SpanStub, key string) any {
	for _, kv := range s.Attributes {
		if string(kv.Key) == key {
			return kv.Value.AsInterface()
		}
	}
	return nil
}

func lastStep(spans tracetest.SpanStubs, name string) int {
	for i, s := range spans {
		if s.Name == runnerq.SpanStep && attr(s, runnerq.AttrStepName) == name {
			return i
		}
	}
	return -1
}

func completedAttempt(spans tracetest.SpanStubs, activityType string) bool {
	for _, s := range spans {
		if s.Name == runnerq.SpanAttempt && attr(s, runnerq.AttrActivityType) == activityType && attr(s, runnerq.AttrOutcome) == runnerq.OutcomeCompleted {
			return true
		}
	}
	return false
}

func names(spans tracetest.SpanStubs) []string {
	var out []string
	for _, s := range spans {
		out = append(out, s.Name)
	}
	return out
}
