package session

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/rasonyang/cascade-realtime-gateway/internal/observability"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider/mock"
)

// TestTelemetrySpanTreeAndMetrics runs a VAD turn plus an interrupt with
// in-memory exporters and checks the span tree (session → response →
// llm/tts, nothing finer) and the core metrics.
func TestTelemetrySpanTreeAndMetrics(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tel := observability.New(tp, mp)

	sc := defaultScripts()
	sc.asr.PartialAfterMs = 100
	sc.tts.FirstChunkDelay = mock.Duration(300 * time.Millisecond) // keeps response 2 in flight until the cancel lands
	h := newHarness(t, sc, chain(serverVAD(true, true), func(o *Options) { o.Telemetry = tel }))
	h.speak(300)
	h.silence(600)
	h.waitResponseDone(1)

	// Second response cancelled by the client while TTS is in flight.
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "again"}})
	h.post(CmdCreateResponse{})
	h.waitFor("second delta", func(e Event) bool { d, ok := e.(EvOutputAudioTranscriptDelta); return ok && d.Resp == 2 })
	h.post(CmdCancelResponse{})
	if d := h.waitResponseDone(2); d.Status != ResponseCancelled {
		t.Fatalf("second response = %+v", d)
	}
	h.s.Close(CloseClient)
	<-h.s.Done()

	spans := exp.GetSpans()
	byName := map[string][]tracetest.SpanStub{}
	for _, s := range spans {
		byName[s.Name] = append(byName[s.Name], s)
	}
	if len(byName["session"]) != 1 || len(byName["response"]) != 2 || len(byName["llm"]) != 2 || len(byName["tts"]) < 1 {
		t.Fatalf("span counts: %v", names(spans))
	}
	if len(spans) != 1+2+2+len(byName["tts"]) {
		t.Fatalf("unexpected extra spans (frame-level?): %v", names(spans))
	}
	sessionID := byName["session"][0].SpanContext.SpanID()
	for _, r := range byName["response"] {
		if r.Parent.SpanID() != sessionID {
			t.Fatalf("response span parent is not the session span")
		}
	}
	responseIDs := map[trace.SpanID]bool{}
	for _, r := range byName["response"] {
		responseIDs[r.SpanContext.SpanID()] = true
	}
	for _, n := range []string{"llm", "tts"} {
		for _, s := range byName[n] {
			if !responseIDs[s.Parent.SpanID()] {
				t.Fatalf("%s span parent is not a response span", n)
			}
		}
	}
	statuses := map[string]bool{}
	for _, r := range byName["response"] {
		for _, a := range r.Attributes {
			if a.Key == observability.KeyStatus {
				statuses[a.Value.AsString()] = true
			}
		}
	}
	if !statuses["completed"] || !statuses["cancelled"] {
		t.Fatalf("response statuses = %v", statuses)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			got[m.Name] = true
		}
	}
	for _, want := range []string{
		"cascade.asr.first_transcript_ms", "cascade.asr.final_transcript_ms", "cascade.asr.commit_to_final_ms",
		"cascade.llm.ttft_ms", "cascade.tts.first_audio_ms", "cascade.e2e_ms", "cascade.interrupt_ms",
		"cascade.sessions.ended", "cascade.sessions.active",
	} {
		if !got[want] {
			t.Errorf("metric %s not recorded (have %v)", want, got)
		}
	}
	if got["cascade.provider.errors"] {
		t.Error("no provider error expected")
	}
}

func names(spans tracetest.SpanStubs) []string {
	out := make([]string, len(spans))
	for i, s := range spans {
		out[i] = s.Name
	}
	return out
}
