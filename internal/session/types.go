package session

import (
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// ItemRef is an opaque handle to a conversation item. Zero means "none".
type ItemRef uint64

// ResponseRef is an opaque handle to a response. Zero means "none".
type ResponseRef uint64

// Generation stamps events produced by pipeline goroutines; the actor drops
// events whose generation is not the active one.
type Generation uint64

// CloseReason states why a session ended.
type CloseReason string

const (
	CloseClient           CloseReason = "client_close"
	CloseInputOverflow    CloseReason = "input_queue_overflow"
	CloseBufferOverflow   CloseReason = "input_audio_buffer_overflow"
	CloseProviderError    CloseReason = "provider_error"
	CloseContextCancelled CloseReason = "context_cancelled"
)

// Role is a conversation item role.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
)

// ContentKind distinguishes text items from audio items.
type ContentKind int

const (
	ContentText ContentKind = iota + 1
	ContentAudio
)

// ItemStatus follows the protocol item statuses.
type ItemStatus string

const (
	ItemInProgress ItemStatus = "in_progress"
	ItemCompleted  ItemStatus = "completed"
	ItemIncomplete ItemStatus = "incomplete"
)

// Item is a conversation entry as seen from outside the actor. Audio items
// carry their transcript in Text; raw user audio is not retained. ClientID is
// an opaque label supplied at creation that the session never interprets.
type Item struct {
	Ref            ItemRef
	ClientID       string
	Role           Role
	Content        ContentKind
	Status         ItemStatus
	Text           string
	AudioMs        int
	TranscriptDone bool
}

// ItemSpec describes a client-created text item.
type ItemSpec struct {
	Role     Role
	Text     string
	ClientID string
}

// ResponseStatus is the five-state protocol-aligned FSM.
type ResponseStatus string

const (
	ResponseInProgress ResponseStatus = "in_progress"
	ResponseCompleted  ResponseStatus = "completed"
	ResponseCancelled  ResponseStatus = "cancelled"
	ResponseIncomplete ResponseStatus = "incomplete"
	ResponseFailed     ResponseStatus = "failed"
)

// StatusReason refines a terminal status.
// PROTOCOL-VERIFY: cancelled/incomplete reason strings are copied from the GA
// response.status_details reference and re-checked in Phase 2.
type StatusReason string

const (
	ReasonNone            StatusReason = ""
	ReasonTurnDetected    StatusReason = "turn_detected"
	ReasonClientCancelled StatusReason = "client_cancelled"
	ReasonMaxOutputTokens StatusReason = "max_output_tokens"
	ReasonContentFilter   StatusReason = "content_filter"
)

// Usage is token accounting for a response.
type Usage = provider.Usage

// Nullable carries a tri-state patch value for nullable session fields:
// Set=false leaves the field unchanged; Set=true with Value=nil sets null.
type Nullable[T any] struct {
	Set   bool
	Value *T
}

// SessionPatch is the internal form of session.update: nil means unchanged.
type SessionPatch struct {
	Instructions     *string
	OutputModalities []string
	Voice            *string
	Speed            *float64
	MaxOutputTokens  *config.MaxOutputTokens
	Transcription    Nullable[config.Transcription]
	TurnDetection    Nullable[config.TurnDetection]
}

// ResponseOverrides is the internal form of response.create's per-response
// overrides; nil means "use the session value". Metadata is opaque and only
// echoed back on EvResponseCreated.
type ResponseOverrides struct {
	Instructions     *string
	OutputModalities []string
	MaxOutputTokens  *config.MaxOutputTokens
	Voice            *string
	Metadata         map[string]string
}

// ---- Commands (into the actor, express intent) -----------------------------

// Command is the sealed inbound type set. Tag is an opaque correlation value
// set by the caller (the protocol layer's client event_id) and echoed on any
// EvError the command produces.
type Command interface {
	isCommand()
	CommandTag() string
}

// Meta is embedded by every command.
type Meta struct{ Tag string }

func (Meta) isCommand()           {}
func (m Meta) CommandTag() string { return m.Tag }

type CmdUpdateSession struct {
	Meta
	Patch SessionPatch
}

type CmdCommitAudio struct{ Meta }

type CmdClearAudio struct{ Meta }

// CmdCreateItem inserts a text item. PreviousItem 0 with AtRoot=false
// appends; AtRoot inserts at the beginning; otherwise after PreviousItem.
type CmdCreateItem struct {
	Meta
	Item         ItemSpec
	PreviousItem ItemRef
	AtRoot       bool
}

type CmdDeleteItem struct {
	Meta
	ID ItemRef
}

type CmdTruncateItem struct {
	Meta
	ID           ItemRef
	ContentIndex int
	AudioEndMs   int
}

type CmdCreateResponse struct {
	Meta
	Overrides ResponseOverrides
}

// CmdCancelResponse cancels the active response. Resp 0 means "whatever is
// active"; a non-zero Resp that is not active is a no-op per GA semantics.
type CmdCancelResponse struct {
	Meta
	Resp ResponseRef
}

type CmdClose struct {
	Meta
	Reason CloseReason
}

// ---- Events (out of the actor, state facts) --------------------------------

// Event is the sealed outbound type set.
type Event interface{ isEvent() }

type EvSessionUpdated struct{ Config config.SessionDefaults }

// EvError reports a non-fatal or fatal problem. Tag echoes the command tag
// when the error was caused by a command; Param names the offending field
// (dotted path) when there is one.
type EvError struct {
	Tag     string
	Code    string
	Message string
	Param   string
	Fatal   bool
}

type EvAudioBufferCommitted struct{ Item, PreviousItem ItemRef }

type EvAudioBufferCleared struct{}

type EvSpeechStarted struct {
	AudioStartMs int
	Item         ItemRef
}

type EvSpeechStopped struct {
	AudioEndMs int
	Item       ItemRef
}

type EvItemAdded struct {
	Item         Item
	PreviousItem ItemRef
}

type EvItemDone struct{ Item Item }

type EvItemDeleted struct{ ID ItemRef }

type EvItemTruncated struct {
	ID           ItemRef
	ContentIndex int
	AudioEndMs   int
}

type EvInputTranscriptDelta struct {
	Item  ItemRef
	Delta string
}

type EvInputTranscriptDone struct {
	Item ItemRef
	Text string
}

// EvResponseCreated carries the effective per-response parameters so the
// protocol layer can echo the response object without guessing.
type EvResponseCreated struct {
	Resp             ResponseRef
	OutputModalities []string
	Voice            string
	MaxOutputTokens  config.MaxOutputTokens
	Metadata         map[string]string
}

type EvOutputItemAdded struct {
	Resp         ResponseRef
	Item         Item
	PreviousItem ItemRef
}

// Delta events are produced by pipeline goroutines and therefore carry Gen.
type EvOutputTextDelta struct {
	Resp  ResponseRef
	Item  ItemRef
	Gen   Generation
	Delta string
}

type EvOutputAudioTranscriptDelta struct {
	Resp  ResponseRef
	Item  ItemRef
	Gen   Generation
	Delta string
}

type EvOutputAudioDelta struct {
	Resp ResponseRef
	Item ItemRef
	Gen  Generation
	PCM  []byte
}

type EvOutputTextDone struct {
	Resp ResponseRef
	Item ItemRef
	Text string
}

type EvOutputAudioTranscriptDone struct {
	Resp ResponseRef
	Item ItemRef
	Text string
}

type EvOutputAudioDone struct {
	Resp ResponseRef
	Item ItemRef
}

type EvOutputItemDone struct {
	Resp ResponseRef
	Item Item
}

type EvResponseDone struct {
	Resp    ResponseRef
	Status  ResponseStatus
	Reason  StatusReason
	ErrCode string // Cascade error code when Status is failed
	Err     error
	Usage   Usage
	Output  []Item
}

type EvSessionClosed struct{ Reason CloseReason }

func (EvSessionUpdated) isEvent()             {}
func (EvError) isEvent()                      {}
func (EvAudioBufferCommitted) isEvent()       {}
func (EvAudioBufferCleared) isEvent()         {}
func (EvSpeechStarted) isEvent()              {}
func (EvSpeechStopped) isEvent()              {}
func (EvItemAdded) isEvent()                  {}
func (EvItemDone) isEvent()                   {}
func (EvItemDeleted) isEvent()                {}
func (EvItemTruncated) isEvent()              {}
func (EvInputTranscriptDelta) isEvent()       {}
func (EvInputTranscriptDone) isEvent()        {}
func (EvResponseCreated) isEvent()            {}
func (EvOutputItemAdded) isEvent()            {}
func (EvOutputTextDelta) isEvent()            {}
func (EvOutputAudioTranscriptDelta) isEvent() {}
func (EvOutputAudioDelta) isEvent()           {}
func (EvOutputTextDone) isEvent()             {}
func (EvOutputAudioTranscriptDone) isEvent()  {}
func (EvOutputAudioDone) isEvent()            {}
func (EvOutputItemDone) isEvent()             {}
func (EvResponseDone) isEvent()               {}
func (EvSessionClosed) isEvent()              {}

// Error codes emitted by the actor.
// PROTOCOL-VERIFY: codes are internal identifiers; the protocol layer maps
// them onto GA error.code values in Phase 2.
const (
	ErrCodeInvalidSession          = "invalid_session_update"
	ErrCodeBufferEmpty             = "input_audio_buffer_commit_empty"
	ErrCodeBufferOverflow          = "input_audio_buffer_overflow"
	ErrCodeInputQueueOverflow      = "input_queue_overflow"
	ErrCodeItemNotFound            = "item_not_found"
	ErrCodeItemNotTruncatable      = "item_not_truncatable"
	ErrCodeInvalidItem             = "invalid_item"
	ErrCodeResponseInProgress      = "response_in_progress"
	ErrCodeResponseFailed          = "response_failed"
	ErrCodeProviderError           = "provider_error"
	ErrCodeInvalidOverrides        = "invalid_response_overrides"
	ErrCodeTranscriptTimeout       = "transcript_timeout"
	ErrCodeResponseCancelNotActive = "response_cancel_not_active"
	ErrCodeTruncateOutOfRange      = "item_truncate_out_of_range"
	ErrCodeVoiceLocked             = "voice_locked"
)
