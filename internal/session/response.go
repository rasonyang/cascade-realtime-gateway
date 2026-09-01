package session

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// response is the actor-private FSM for one generation. Status is one of the
// five protocol states; pipeline progress is the orthogonal flag set.
type response struct {
	ref    ResponseRef
	gen    Generation
	status ResponseStatus

	awaiting     bool // created, waiting for the user item's transcript Final
	llmDone      bool
	ttsDone      bool
	audioFlushed bool

	cancel    context.CancelFunc // nil until the pipeline starts
	item      ItemRef            // output item, 0 until the pipeline starts
	dependsOn ItemRef            // user item whose transcript gates the LLM

	textOnly     bool
	instructions string
	maxTokens    int // 0 = unlimited
	voice        string
	failCode     string
	finish       provider.FinishReason
	usage        Usage
	tag          string

	// Observability: the response span and its llm / tts children, plus the
	// timestamps the core latency metrics are derived from.
	ctx          context.Context // response span context; the pipeline ctx derives from it
	span         trace.Span
	llmSpan      trace.Span
	ttsSpan      trace.Span
	anchorAt     time.Time // commit of the user turn, or response.create for text turns
	startedAt    time.Time // pipeline start (LLM request issued)
	ttsStartedAt time.Time // first sentence handed to TTS
	firstTextAt  time.Time
	firstAudioAt time.Time
}

// terminal reports whether the response has left in_progress.
func (r *response) terminal() bool { return r.status != ResponseInProgress }

// outcome maps the completed pipeline onto completed / incomplete.
func (r *response) outcome() (ResponseStatus, StatusReason) {
	switch r.finish {
	case provider.FinishLength:
		return ResponseIncomplete, ReasonMaxOutputTokens
	case provider.FinishContentFilter:
		return ResponseIncomplete, ReasonContentFilter
	}
	return ResponseCompleted, ReasonNone
}

// done reports whether every progress flag required by the modality is set.
func (r *response) done() bool {
	if !r.llmDone {
		return false
	}
	return r.textOnly || (r.ttsDone && r.audioFlushed)
}

// ---- pipeline → actor progress facts (internal, generation-stamped) --------

// stamped is implemented by every event that crosses respEvents.
type stamped interface{ generation() Generation }

func (e EvOutputTextDelta) generation() Generation            { return e.Gen }
func (e EvOutputAudioTranscriptDelta) generation() Generation { return e.Gen }
func (e EvOutputAudioDelta) generation() Generation           { return e.Gen }

type pipeLLMDone struct {
	Gen    Generation
	Finish provider.FinishReason
	Usage  Usage
}

type pipeTTSDone struct{ Gen Generation }

// pipeTTSStart marks the first sentence being handed to the TTS provider.
type pipeTTSStart struct{ Gen Generation }

type pipeSegment struct {
	Gen Generation
	Seg audioSegment
}

type pipeAlignment struct {
	Gen     Generation
	Timings []provider.CharTiming
}

type pipeError struct {
	Gen   Generation
	Stage string
	Err   error
}

func (pipeLLMDone) isEvent()   {}
func (pipeTTSDone) isEvent()   {}
func (pipeTTSStart) isEvent()  {}
func (pipeSegment) isEvent()   {}
func (pipeAlignment) isEvent() {}
func (pipeError) isEvent()     {}

func (e pipeLLMDone) generation() Generation   { return e.Gen }
func (e pipeTTSDone) generation() Generation   { return e.Gen }
func (e pipeTTSStart) generation() Generation  { return e.Gen }
func (e pipeSegment) generation() Generation   { return e.Gen }
func (e pipeAlignment) generation() Generation { return e.Gen }
func (e pipeError) generation() Generation     { return e.Gen }
