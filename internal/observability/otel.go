package observability

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// ServiceName is the OTel service.name resource attribute.
const ServiceName = "cascade"

const instrumentationName = "github.com/rasonyang/cascade-realtime-gateway"

// Metrics are the core latency histograms and counters. Spans stop at the
// response level; everything at frame granularity is a metric only.
type Metrics struct {
	ASRFirstTranscriptMs metric.Float64Histogram // speech start (or commit) → first partial
	ASRFinalTranscriptMs metric.Float64Histogram // speech stop (or commit) → final
	CommitToFinalMs      metric.Float64Histogram // commit → final
	LLMTTFTMs            metric.Float64Histogram // pipeline start → first text delta
	TTSFirstAudioMs      metric.Float64Histogram // first sentence handed to TTS → first audio delta
	E2EMs                metric.Float64Histogram // commit (or response.create) → first audio/text delta
	InterruptMs          metric.Float64Histogram // interrupt trigger → response.done{cancelled}
	ProviderErrors       metric.Int64Counter     // attr provider=asr|llm|tts
	SessionsEnded        metric.Int64Counter     // attr reason
	SessionsActive       metric.Int64UpDownCounter
	AdminConfigWrites    metric.Int64Counter // attr result=ok|invalid|conflict|persist_error
}

// Telemetry bundles the tracer and instruments a session uses.
type Telemetry struct {
	Tracer  trace.Tracer
	Metrics Metrics

	shutdown []func(context.Context) error
}

// Attribute keys shared by spans and metrics.
var (
	KeySessionID  = attribute.Key("cascade.session_id")
	KeyResponseID = attribute.Key("cascade.response_id")
	KeyProvider   = attribute.Key("cascade.provider")
	KeyReason     = attribute.Key("cascade.reason")
	KeyStatus     = attribute.Key("cascade.status")
	KeyModalities = attribute.Key("cascade.output_modalities")
	KeyResult     = attribute.Key("cascade.result")
	KeyProfile    = attribute.Key("cascade.profile")
	KeyASR        = attribute.Key("cascade.asr")
	KeyLLM        = attribute.Key("cascade.llm")
	KeyTTS        = attribute.Key("cascade.tts")
)

// Setup builds exporters for an OTLP/HTTP endpoint ("host:port"); an empty
// endpoint yields no-op telemetry so the session code never branches.
func Setup(ctx context.Context, endpoint string) (*Telemetry, error) {
	if endpoint == "" {
		return Noop(), nil
	}
	res := resource.NewSchemaless(attribute.String("service.name", ServiceName))
	traceExp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(endpoint), otlptracehttp.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("otlp trace exporter: %w", err)
	}
	metricExp, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpoint(endpoint), otlpmetrichttp.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("otlp metric exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExp), sdktrace.WithResource(res))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)), sdkmetric.WithResource(res))
	t := New(tp, mp)
	t.shutdown = []func(context.Context) error{tp.Shutdown, mp.Shutdown}
	return t, nil
}

// New builds Telemetry from arbitrary providers (tests use in-memory ones).
func New(tp trace.TracerProvider, mp metric.MeterProvider) *Telemetry {
	meter := mp.Meter(instrumentationName)
	hist := func(name, desc string) metric.Float64Histogram {
		h, err := meter.Float64Histogram(name, metric.WithUnit("ms"), metric.WithDescription(desc))
		if err != nil {
			panic("observability: " + err.Error())
		}
		return h
	}
	counter := func(name, desc string) metric.Int64Counter {
		c, err := meter.Int64Counter(name, metric.WithDescription(desc))
		if err != nil {
			panic("observability: " + err.Error())
		}
		return c
	}
	active, err := meter.Int64UpDownCounter("cascade.sessions.active", metric.WithDescription("live sessions"))
	if err != nil {
		panic("observability: " + err.Error())
	}
	return &Telemetry{
		Tracer: tp.Tracer(instrumentationName),
		Metrics: Metrics{
			ASRFirstTranscriptMs: hist("cascade.asr.first_transcript_ms", "speech start (or commit) to first partial transcript"),
			ASRFinalTranscriptMs: hist("cascade.asr.final_transcript_ms", "speech stop (or commit) to final transcript"),
			CommitToFinalMs:      hist("cascade.asr.commit_to_final_ms", "buffer commit to final transcript"),
			LLMTTFTMs:            hist("cascade.llm.ttft_ms", "pipeline start to first LLM text delta"),
			TTSFirstAudioMs:      hist("cascade.tts.first_audio_ms", "first sentence handed to TTS to first audio delta"),
			E2EMs:                hist("cascade.e2e_ms", "commit (or response.create) to first output delta"),
			InterruptMs:          hist("cascade.interrupt_ms", "interrupt trigger to response.done{cancelled}"),
			ProviderErrors:       counter("cascade.provider.errors", "provider failures by provider"),
			SessionsEnded:        counter("cascade.sessions.ended", "sessions ended by reason"),
			SessionsActive:       active,
			AdminConfigWrites:    counter("cascade.admin.config_writes", "admin runtime configuration writes by result"),
		},
	}
}

// Noop returns telemetry that records nothing.
func Noop() *Telemetry {
	return New(tracenoop.NewTracerProvider(), metricnoop.NewMeterProvider())
}

// Shutdown flushes and stops the exporters.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	var errs []error
	for _, fn := range t.shutdown {
		if err := fn(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
