package config

import (
	"encoding/json"
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

// Config is the complete Cascade configuration. It is an immutable snapshot
// after Load; nothing mutates it at runtime.
type Config struct {
	Listen          string          `json:"listen"`
	Auth            Auth            `json:"auth"`
	Providers       Providers       `json:"providers"`
	SessionDefaults SessionDefaults `json:"session_defaults"`
	Limits          Limits          `json:"limits"`
	Observability   Observability   `json:"observability"`
}

// Auth holds the gateway's static Bearer token.
type Auth struct {
	APIKey string `json:"api_key"`
}

// Providers selects one provider per kind.
type Providers struct {
	ASR Provider `json:"asr"`
	LLM Provider `json:"llm"`
	TTS Provider `json:"tts"`
}

// Provider names a provider implementation and carries its opaque options,
// which are parsed only inside the provider's own package.
type Provider struct {
	Type    string          `json:"type"`
	APIKey  string          `json:"api_key"`
	Options json.RawMessage `json:"options"`
}

// SessionDefaults is a subset of the GA session object. Field names and JSON
// tags mirror the wire shape so the protocol layer can echo them without
// translation.
type SessionDefaults struct {
	Instructions     string          `json:"instructions"`
	OutputModalities []string        `json:"output_modalities"`
	Audio            Audio           `json:"audio"`
	MaxOutputTokens  MaxOutputTokens `json:"max_output_tokens"`
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
}

// Observability configures logging and tracing export.
type Observability struct {
	LogLevel     string `json:"log_level"`
	OTelEndpoint string `json:"otel_endpoint"`
}

// Server-side VAD defaults, applied when a server_vad block omits them.
const (
	defaultVADThreshold         = 0.5
	defaultVADPrefixPaddingMs   = 300
	defaultVADSilenceDurationMs = 500
	defaultSemanticEagerness    = "auto"
)

// DefaultConfig returns the configuration used as the base that a loaded file
// overlays. Every default value in Cascade is defined here.
func DefaultConfig() *Config {
	return &Config{
		Listen: ":8080",
		SessionDefaults: SessionDefaults{
			Instructions:     "You are a helpful assistant.",
			OutputModalities: []string{ModalityAudio},
			Audio: Audio{
				Input: AudioInput{
					Format:        AudioFormat{Type: AudioFormatPCM, Rate: AudioSampleRate},
					Transcription: &Transcription{},
					TurnDetection: &TurnDetection{
						Type:              TurnDetectionServerVAD,
						CreateResponse:    true,
						InterruptResponse: true,
					},
				},
				Output: AudioOutput{
					Format: AudioFormat{Type: AudioFormatPCM, Rate: AudioSampleRate},
					Voice:  "alloy",
					Speed:  1.0,
				},
			},
			MaxOutputTokens: MaxOutputTokens{Inf: true},
		},
		Limits: Limits{
			MaxSessions:           100,
			SessionTimeout:        Duration(30 * time.Minute),
			InputAudioQueueFrames: 200,
			InputAudioBufferMaxMs: 60000,
			ASRFinalTimeout:       Duration(2 * time.Second),
			OutputEventQueue:      256,
			ClientWriteTimeout:    Duration(10 * time.Second),
		},
		Observability: Observability{
			LogLevel: "info",
		},
	}
}

// applyTurnDetectionDefaults fills the mode-specific tuning fields that the
// file omitted. Fields belonging to the other mode are left untouched so that
// Validate can reject them.
func (td *TurnDetection) applyDefaults() {
	switch td.Type {
	case TurnDetectionServerVAD:
		if td.Threshold == nil {
			v := defaultVADThreshold
			td.Threshold = &v
		}
		if td.PrefixPaddingMs == nil {
			v := defaultVADPrefixPaddingMs
			td.PrefixPaddingMs = &v
		}
		if td.SilenceDurationMs == nil {
			v := defaultVADSilenceDurationMs
			td.SilenceDurationMs = &v
		}
	case TurnDetectionSemanticVAD:
		if td.Eagerness == nil {
			v := defaultSemanticEagerness
			td.Eagerness = &v
		}
	}
}
