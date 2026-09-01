// Package provider defines the three narrow interfaces the session pipeline
// consumes (ASR, LLM, TTS), the shared value types that cross them, a plain
// registry map, and the normalized provider error.
//
// Layering: provider must not import session or protocol. Provider-specific
// wire shapes never leave a provider's own package; everything here is
// already normalized. ctx is the only cancellation mechanism: cancelling it
// must promptly close streams and connections.
package provider

import (
	"context"
	"encoding/json"
	"fmt"
)

// Kind identifies which of the three provider roles an implementation fills.
type Kind string

const (
	KindASR Kind = "asr"
	KindLLM Kind = "llm"
	KindTTS Kind = "tts"
)

// Provider is implemented by every ASR, LLM and TTS implementation.
type Provider interface {
	Kind() Kind
}

// ---- ASR -------------------------------------------------------------------

// ASRConfig is the per-session configuration for an ASR stream.
type ASRConfig struct {
	SampleRate int
	Language   string
	Prompt     string
	Model      string
}

// ASR opens one session-scoped, long-lived transcription stream.
type ASR interface {
	Provider
	OpenStream(ctx context.Context, cfg ASRConfig) (ASRStream, error)
}

// ASRStream carries audio in and transcript facts out.
//
// PushAudio must not block on the network: implementations buffer internally
// and surface failures through Events. Finalize means "the current utterance
// has ended, produce a Final as soon as possible"; implementations without a
// native flush may no-op but MUST still guarantee a subsequent Final or an
// explicit EndOfTurn. Events is closed once the stream is finished, whether
// by Close or by ctx cancellation.
type ASRStream interface {
	PushAudio(pcm []byte) error
	Finalize() error
	Events() <-chan ASREvent
	Close() error
}

// ASREventKind enumerates ASR facts.
type ASREventKind int

const (
	ASRPartial ASREventKind = iota + 1
	ASRFinal
	ASREndOfTurn
	ASRError
)

func (k ASREventKind) String() string {
	switch k {
	case ASRPartial:
		return "partial"
	case ASRFinal:
		return "final"
	case ASREndOfTurn:
		return "end_of_turn"
	case ASRError:
		return "error"
	}
	return "unknown"
}

// ASREvent is one fact from the stream. Partial carries the current
// interim text for the utterance in progress; Final carries finalized text
// for a segment with its offsets on the stream's audio timeline; EndOfTurn
// signals the provider's own end-of-utterance judgment.
type ASREvent struct {
	Kind    ASREventKind
	Text    string
	StartMs int
	EndMs   int
	Err     error
}

// ---- LLM -------------------------------------------------------------------

// Role is a chat message role.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	// RoleTool carries the result of a tool the client executed. ToolCallID
	// names the call it answers.
	RoleTool Role = "tool"
)

// ToolDef is one function the model may call. Parameters is an opaque JSON
// Schema object, passed through verbatim; Cascade does not interpret it. An
// empty Parameters means a no-argument tool.
type ToolDef struct {
	Name        string
	Description string
	Parameters  json.RawMessage
}

// Tool choice modes. ToolChoiceFunction forces the function named by
// ToolChoice.Name.
const (
	ToolChoiceAuto     = "auto"
	ToolChoiceNone     = "none"
	ToolChoiceRequired = "required"
	ToolChoiceFunction = "function"
)

// ToolChoice mirrors the GA field. Cascade's session model always resolves it
// to an explicit value, so adapters never inherit a provider default; the
// zero value is treated as ToolChoiceAuto.
type ToolChoice struct {
	Mode string
	Name string // set only when Mode == ToolChoiceFunction
}

// ToolCall is one complete call the model asked for. Arguments is the raw
// JSON string the model produced; Cascade never parses it. Adapters
// accumulate streamed argument fragments internally and only ever surface a
// complete call.
type ToolCall struct {
	ID        string
	Name      string
	Arguments string
}

// Message is one turn of conversation context. An assistant turn that spoke
// and called a tool carries both Content and ToolCalls; a RoleTool message
// carries the tool result in Content and the call it answers in ToolCallID.
type Message struct {
	Role       Role
	Content    string
	ToolCalls  []ToolCall
	ToolCallID string
}

// ChatRequest is a single, request-scoped generation. Temperature is nil when
// the caller wants the provider's own default; 0 is a meaningful value, so the
// field is a pointer rather than a sentinel. ToolChoice is a value, not a
// pointer: when Tools is non-empty the effective choice is always sent.
type ChatRequest struct {
	Instructions    string
	Messages        []Message
	MaxOutputTokens int // 0 means no limit
	Temperature     *float64
	Tools           []ToolDef
	ToolChoice      ToolChoice
}

// FinishReason mirrors the OpenAI finish reasons Cascade maps.
type FinishReason string

const (
	FinishStop          FinishReason = "stop"
	FinishLength        FinishReason = "length"
	FinishContentFilter FinishReason = "content_filter"
)

// Usage is token accounting reported by the LLM.
type Usage struct {
	InputTokens  int
	OutputTokens int
}

// LLMChunkKind enumerates LLM stream items.
type LLMChunkKind int

const (
	LLMTextDelta LLMChunkKind = iota + 1
	// LLMToolCall carries one complete tool call. At most one is emitted per
	// generation, always before LLMDone.
	LLMToolCall
	LLMDone
	LLMError
)

// LLMChunk is one item of the generation stream. The channel is closed after
// Done or Error.
type LLMChunk struct {
	Kind         LLMChunkKind
	Text         string
	ToolCall     ToolCall
	FinishReason FinishReason
	Usage        Usage
	Err          error
}

// LLM starts one unidirectional generation stream per request.
type LLM interface {
	Provider
	Chat(ctx context.Context, req ChatRequest) (<-chan LLMChunk, error)
}

// ---- TTS -------------------------------------------------------------------

// TTSConfig is the per-stream synthesis configuration.
type TTSConfig struct {
	Voice      string
	Speed      float64
	SampleRate int
}

// TTSCaps declares optional capabilities. IncrementalText means text can be
// appended while audio is already streaming; Alignment means AudioChunk
// carries character timings.
type TTSCaps struct {
	IncrementalText bool
	Alignment       bool
}

// CharTiming maps one character of the text written to a stream to the
// moment its audio starts. CharIndex is a rune index into the concatenation
// of all text written to the stream; StartMs is relative to the stream's
// first audio byte.
type CharTiming struct {
	CharIndex int
	StartMs   int
}

// AudioChunk is one piece of synthesized audio; Alignment may be nil.
type AudioChunk struct {
	PCM       []byte
	Alignment []CharTiming
}

// TTSStream turns text into audio. ReadAudio returns io.EOF when all audio
// for the written text has been delivered after EndInput, and must return an
// error promptly once ctx is cancelled.
type TTSStream interface {
	WriteText(s string) error
	EndInput() error
	ReadAudio() (AudioChunk, error)
	Close() error
}

// TTS opens synthesis streams. Streaming audio output is mandatory.
type TTS interface {
	Provider
	Synthesize(ctx context.Context, cfg TTSConfig) (TTSStream, error)
	Capabilities() TTSCaps
}

// ---- errors ----------------------------------------------------------------

// ErrorKind classifies provider failures for metrics and retry policy.
type ErrorKind int

const (
	ErrAuth ErrorKind = iota + 1
	ErrRateLimit
	ErrTransient
	ErrFatal
)

func (k ErrorKind) String() string {
	switch k {
	case ErrAuth:
		return "auth"
	case ErrRateLimit:
		return "rate_limit"
	case ErrTransient:
		return "transient"
	case ErrFatal:
		return "fatal"
	}
	return "unknown"
}

// Error is the normalized provider error.
type Error struct {
	Provider string
	Kind     ErrorKind
	Err      error
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s: %v", e.Provider, e.Kind, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// ---- registry --------------------------------------------------------------

// Factory builds a provider from its API key and its own opaque options.
type Factory func(apiKey string, options json.RawMessage) (Provider, error)

var registry = map[Kind]map[string]Factory{}

// Register adds a factory under kind/name. It is meant to be called from
// init functions; registering the same name twice panics.
func Register(kind Kind, name string, f Factory) {
	if registry[kind] == nil {
		registry[kind] = map[string]Factory{}
	}
	if _, dup := registry[kind][name]; dup {
		panic(fmt.Sprintf("provider: duplicate registration %s/%s", kind, name))
	}
	registry[kind][name] = f
}

// Known reports whether kind/name is registered. Its signature matches
// config.ProviderLookup.
func Known(kind, name string) bool {
	_, ok := registry[Kind(kind)][name]
	return ok
}

// New builds the provider registered under kind/name.
func New(kind Kind, name, apiKey string, options json.RawMessage) (Provider, error) {
	f, ok := registry[kind][name]
	if !ok {
		return nil, fmt.Errorf("provider: unknown %s provider %q", kind, name)
	}
	p, err := f(apiKey, options)
	if err != nil {
		return nil, err
	}
	if p.Kind() != kind {
		return nil, fmt.Errorf("provider: %q registered as %s but reports kind %s", name, kind, p.Kind())
	}
	return p, nil
}

// ErrorFromStatus classifies an HTTP status into an Error for provider name.
// body is included in the message, truncated, and must not contain secrets.
func ErrorFromStatus(name string, status int, body string) *Error {
	kind := ErrFatal
	switch {
	case status == 401 || status == 403:
		kind = ErrAuth
	case status == 429:
		kind = ErrRateLimit
	case status >= 500 || status == 408:
		kind = ErrTransient
	}
	const maxBody = 200
	if len(body) > maxBody {
		body = body[:maxBody] + "…"
	}
	return &Error{Provider: name, Kind: kind, Err: fmt.Errorf("http %d: %s", status, body)}
}
