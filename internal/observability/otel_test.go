package observability

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestNoopAndSetupEmptyEndpoint(t *testing.T) {
	tel, err := Setup(context.Background(), "")
	if err != nil || tel == nil {
		t.Fatalf("Setup(\"\") = %v, %v", tel, err)
	}
	_, span := tel.Tracer.Start(context.Background(), "x")
	span.End()
	tel.Metrics.E2EMs.Record(context.Background(), 1)
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestInstrumentsRecord(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tel := New(tp, mp)
	ctx, span := tel.Tracer.Start(context.Background(), "session")
	_, child := tel.Tracer.Start(ctx, "response")
	child.End()
	span.End()
	tel.Metrics.LLMTTFTMs.Record(context.Background(), 12.5)
	tel.Metrics.SessionsEnded.Add(context.Background(), 1)

	spans := exp.GetSpans()
	if len(spans) != 2 || spans[0].Name != "response" || spans[0].Parent.SpanID() != spans[1].SpanContext.SpanID() {
		t.Fatalf("spans = %+v", spans)
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names[m.Name] = true
		}
	}
	if !names["cascade.llm.ttft_ms"] || !names["cascade.sessions.ended"] {
		t.Fatalf("metrics = %v", names)
	}
}
