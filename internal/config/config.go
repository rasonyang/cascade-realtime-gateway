package config

import (
	"encoding/json"
	"fmt"
	"time"
)

// Protocol constants fixed by the v1 scope. They are not configurable.
const (
	AudioFormatPCM  = "audio/pcm"
	AudioSampleRate = 24000

	TurnDetectionServerVAD   = "server_vad"
	TurnDetectionSemanticVAD = "semantic_vad"

	ModalityAudio = "audio"
	ModalityText  = "text"

	// SpeedMin and SpeedMax are the documented GA range for audio.output.speed.
	// PROTOCOL-VERIFY: re-check when the Phase 2 protocol profile is frozen.
	SpeedMin = 0.25
	SpeedMax = 1.5

	// MaxOutputTokensLimit is the GA upper bound for an integer max_output_tokens.
	// PROTOCOL-VERIFY: re-check when the Phase 2 protocol profile is frozen.
	MaxOutputTokensLimit = 4096
)

// Config is the complete static Cascade configuration: everything that is
// fixed for the lifetime of the process. Providers, profiles and settings are
// runtime configuration owned by the Admin API (see RuntimeConfig) and appear
// here only as the optional Bootstrap seed. Config is an immutable snapshot
// after Load; nothing mutates it at runtime.
type Config struct {
	Listen        string        `json:"listen"`
	Auth          Auth          `json:"auth"`
	Admin         Admin         `json:"admin"`
	Limits        Limits        `json:"limits"`
	Observability Observability `json:"observability"`
	// Bootstrap seeds the runtime configuration the first time the gateway
	// starts with no state file. It is kept raw so that {env.NAME}
	// placeholders inside it are stored verbatim rather than expanded into
	// the state file, and so that profile defaults are applied by the same
	// decoder the Admin API uses.
	Bootstrap json.RawMessage `json:"bootstrap,omitempty"`
}

// Auth holds the gateway's static Bearer tokens. APIKey guards
// /v1/realtime; AdminAPIKey guards the Admin API and, when empty, disables
// the Admin API entirely. The two must differ.
type Auth struct {
	APIKey      string `json:"api_key"`
	AdminAPIKey string `json:"admin_api_key"`
}

// Admin configures the Admin API listener and its state file. Bind Listen to
// localhost or a private network: the Admin API changes what every new call
// runs on.
type Admin struct {
	Listen    string `json:"listen"`
	StateFile string `json:"state_file"`
}

// SessionDefaults is a subset of the GA session object. Field names and JSON
// tags mirror the wire shape so the protocol layer can echo them without
// translation.
type SessionDefaults struct {
	Instructions     string          `json:"instructions"`
	OutputModalities []string        `json:"output_modalities"`
	Audio            Audio           `json:"audio"`
	MaxOutputTokens  MaxOutputTokens `json:"max_output_tokens"`
	// Tools is empty unless a session.update declares one; tool_choice always
	// carries an effective value (default "auto") so no provider default is
	// ever inherited.
	Tools      []Tool     `json:"tools"`
	ToolChoice ToolChoice `json:"tool_choice"`
}

// Tool is one function the model may call, in the flat GA session shape.
// Parameters is an opaque JSON Schema object passed to the LLM provider
// verbatim; Cascade never interprets it, and an absent Parameters means a
// no-argument tool.
type Tool struct {
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// ToolTypeFunction is the only tool type in the compatibility profile; MCP
// tools are rejected.
const ToolTypeFunction = "function"

// Tool choice modes. ToolChoiceFunction is the object form and forces the
// function named by ToolChoice.Name.
const (
	ToolChoiceAuto     = "auto"
	ToolChoiceNone     = "none"
	ToolChoiceRequired = "required"
	ToolChoiceFunction = "function"
)

// ToolChoice is the GA union: one of the three mode strings, or a
// {"type":"function","name":…} object. It round-trips to the form it names,
// so the echoed session object matches what the client sent.
type ToolChoice struct {
	Mode string
	Name string // set only when Mode is ToolChoiceFunction
}

// MarshalJSON renders the mode string, or the forced-function object.
func (tc ToolChoice) MarshalJSON() ([]byte, error) {
	if tc.Mode == ToolChoiceFunction {
		return json.Marshal(struct {
			Type string `json:"type"`
			Name string `json:"name"`
		}{Type: ToolChoiceFunction, Name: tc.Name})
	}
	mode := tc.Mode
	if mode == "" {
		mode = ToolChoiceAuto
	}
	return json.Marshal(mode)
}

// UnmarshalJSON accepts either form. Values are checked by Validate, not
// here, so the error carries a dotted field path.
func (tc *ToolChoice) UnmarshalJSON(b []byte) error {
	var mode string
	if err := json.Unmarshal(b, &mode); err == nil {
		*tc = ToolChoice{Mode: mode}
		return nil
	}
	var obj struct {
		Type string `json:"type"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return fmt.Errorf(`tool_choice must be a string or a {"type":"function","name":…} object`)
	}
	*tc = ToolChoice{Mode: obj.Type, Name: obj.Name}
	return nil
}

// Audio mirrors session.audio.
type Audio struct {
	Input  AudioInput  `json:"input"`
	Output AudioOutput `json:"output"`
}

// AudioInput mirrors session.audio.input.
type AudioInput struct {
	Format AudioFormat `json:"format"`
	// Transcription controls only whether conversation.item.input_audio_transcription.*
	// events are exposed to the client. Cascade's internal ASR always runs for the
	// ASR → LLM pipeline regardless of this field. nil (absent or null) = disabled.
	// PROTOCOL-VERIFY: exact GA field subset is frozen in Phase 2 (docs/protocol-profile.md).
	Transcription *Transcription `json:"transcription"`
	// TurnDetection nil (absent or null) = manual mode; VAD is not instantiated.
	TurnDetection *TurnDetection `json:"turn_detection"`
}

// AudioOutput mirrors session.audio.output.
type AudioOutput struct {
	Format AudioFormat `json:"format"`
	Voice  string      `json:"voice"`
	// Speed is validated against [SpeedMin, SpeedMax].
	Speed float64 `json:"speed"`
}

// AudioFormat mirrors the GA audio format object. Only audio/pcm at 24 kHz is
// accepted in v1.
type AudioFormat struct {
	Type string `json:"type"`
	Rate int    `json:"rate"`
}

// Transcription is the GA-shaped input transcription object. An empty object
// {} is valid and means "enabled with defaults".
// PROTOCOL-VERIFY: the GA object carries additional model-specific fields;
// Cascade's exact supported subset is frozen in Phase 2.
type Transcription struct {
	Model    string `json:"model,omitempty"`
	Language string `json:"language,omitempty"`
	Prompt   string `json:"prompt,omitempty"`
}

// TurnDetection mirrors session.audio.input.turn_detection. Tuning fields are
// pointers so that presence can be validated per mode: threshold,
// prefix_padding_ms and silence_duration_ms belong to server_vad only;
// eagerness belongs to semantic_vad only. After Load, the fields that apply to
// the configured type are always non-nil.
type TurnDetection struct {
	Type              string   `json:"type"`
	Threshold         *float64 `json:"threshold,omitempty"`
	PrefixPaddingMs   *int     `json:"prefix_padding_ms,omitempty"`
	SilenceDurationMs *int     `json:"silence_duration_ms,omitempty"`
	Eagerness         *string  `json:"eagerness,omitempty"`
	CreateResponse    bool     `json:"create_response"`
	InterruptResponse bool     `json:"interrupt_response"`
}

// Limits holds every queue size, buffer cap and timeout used by the gateway.
type Limits struct {
	MaxSessions           int      `json:"max_sessions"`
	SessionTimeout        Duration `json:"session_timeout"`
	InputAudioQueueFrames int      `json:"input_audio_queue_frames"`
	InputAudioBufferMaxMs int      `json:"input_audio_buffer_max_ms"`
	ASRFinalTimeout       Duration `json:"asr_final_timeout"`
	OutputEventQueue      int      `json:"output_event_queue"`
	ClientWriteTimeout    Duration `json:"client_write_timeout"`
	ClientMaxMessageBytes int      `json:"client_max_message_bytes"`
}

// Observability configures logging and tracing export.
type Observability struct {
	LogLevel     string `json:"log_level"`
	OTelEndpoint string `json:"otel_endpoint"`
}

// Server-side VAD defaults, applied when a server_vad block omits them. They
// are exported because semantic_vad exposes no acoustic tuning fields, so the
// session's acoustic VAD falls back to them in that mode.
const (
	DefaultVADThreshold         = 0.5
	DefaultVADPrefixPaddingMs   = 300
	DefaultVADSilenceDurationMs = 500
	defaultSemanticEagerness    = "auto"
)

// DefaultConfig returns the configuration used as the base that a loaded file
// overlays. Every default value in Cascade is defined here.
func DefaultConfig() *Config {
	return &Config{
		Listen: ":8080",
		Admin: Admin{
			Listen:    "127.0.0.1:8081",
			StateFile: "cascade-state.json",
		},
		Limits: Limits{
			MaxSessions:           100,
			SessionTimeout:        Duration(30 * time.Minute),
			InputAudioQueueFrames: 200,
			InputAudioBufferMaxMs: 60000,
			ASRFinalTimeout:       Duration(2 * time.Second),
			OutputEventQueue:      256,
			ClientWriteTimeout:    Duration(10 * time.Second),
			ClientMaxMessageBytes: 16 << 20,
		},
		Observability: Observability{
			LogLevel: "info",
		},
	}
}

// ApplyDefaults fills the mode-specific tuning fields that were omitted.
// Fields belonging to the other mode are left untouched so that Validate can
// reject them.
func (td *TurnDetection) ApplyDefaults() {
	switch td.Type {
	case TurnDetectionServerVAD:
		if td.Threshold == nil {
			v := DefaultVADThreshold
			td.Threshold = &v
		}
		if td.PrefixPaddingMs == nil {
			v := DefaultVADPrefixPaddingMs
			td.PrefixPaddingMs = &v
		}
		if td.SilenceDurationMs == nil {
			v := DefaultVADSilenceDurationMs
			td.SilenceDurationMs = &v
		}
	case TurnDetectionSemanticVAD:
		if td.Eagerness == nil {
			v := defaultSemanticEagerness
			td.Eagerness = &v
		}
	}
}

// Clone returns a deep copy so a session can hold an immutable snapshot.
func (s SessionDefaults) Clone() SessionDefaults {
	out := s
	out.OutputModalities = append([]string(nil), s.OutputModalities...)
	out.Tools = CloneTools(s.Tools)
	if s.Audio.Input.Transcription != nil {
		t := *s.Audio.Input.Transcription
		out.Audio.Input.Transcription = &t
	}
	if s.Audio.Input.TurnDetection != nil {
		out.Audio.Input.TurnDetection = s.Audio.Input.TurnDetection.Clone()
	}
	return out
}

// CloneTools returns a deep copy of a tool list. Parameters is opaque JSON,
// so its bytes are copied rather than shared.
func CloneTools(in []Tool) []Tool {
	if in == nil {
		return nil
	}
	out := make([]Tool, len(in))
	for i, t := range in {
		out[i] = t
		out[i].Parameters = append(json.RawMessage(nil), t.Parameters...)
	}
	return out
}

// Clone returns a deep copy.
func (td *TurnDetection) Clone() *TurnDetection {
	if td == nil {
		return nil
	}
	out := *td
	clone := func(p *float64) *float64 {
		if p == nil {
			return nil
		}
		v := *p
		return &v
	}
	cloneInt := func(p *int) *int {
		if p == nil {
			return nil
		}
		v := *p
		return &v
	}
	out.Threshold = clone(td.Threshold)
	out.PrefixPaddingMs = cloneInt(td.PrefixPaddingMs)
	out.SilenceDurationMs = cloneInt(td.SilenceDurationMs)
	if td.Eagerness != nil {
		v := *td.Eagerness
		out.Eagerness = &v
	}
	return &out
}
