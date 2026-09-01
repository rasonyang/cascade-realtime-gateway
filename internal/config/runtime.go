package config

import (
	"bytes"
	"encoding/json"
	"maps"
	"slices"
)

// RuntimeConfig is the part of the configuration that the Admin API owns:
// provider instances, profiles and the settings that pick among them. It is
// never read from the main configuration file (only seeded from `bootstrap`);
// it lives in the Admin state file and in memory as an immutable snapshot.
type RuntimeConfig struct {
	Providers map[string]*ProviderInstance `json:"providers"`
	Profiles  map[string]*Profile          `json:"profiles"`
	Settings  Settings                     `json:"settings"`
}

// ProviderInstance is one configured provider. Type names a registered
// provider implementation; Options is the provider's own opaque block, parsed
// and validated only inside that provider's package. APIKey holds either a
// literal secret or an unexpanded {env.NAME} placeholder: the state file keeps
// whatever was written, and expansion happens when the provider is built.
type ProviderInstance struct {
	Name    string          `json:"name"`
	Type    string          `json:"type"`
	APIKey  string          `json:"api_key"`
	Options json.RawMessage `json:"options,omitempty"`
}

// Profile is one named set of session parameters plus the provider instances
// that serve it. Its JSON shape is flat by design: it is the Admin API's
// resource model, not the GA session object. SessionDefaults derives the
// GA-shaped snapshot the session and protocol layers consume.
type Profile struct {
	Name string `json:"name"`
	ASR  string `json:"asr"`
	LLM  string `json:"llm"`
	TTS  string `json:"tts"`

	Instructions string `json:"instructions"`
	// Temperature nil (absent or null) leaves sampling to the LLM provider's
	// own default. Some models reject the parameter outright, so Cascade
	// sends it only when a profile asks for it.
	Temperature      *float64        `json:"temperature,omitempty"`
	MaxOutputTokens  MaxOutputTokens `json:"max_output_tokens"`
	OutputModalities []string        `json:"output_modalities"`
	Voice            string          `json:"voice"`
	Speed            float64         `json:"speed"`

	// ASRLanguage and ASRModel configure the ASR stream itself. They are the
	// single source of truth for those two knobs; Transcription carries only
	// the protocol-visible toggle and prompt (see Validate).
	ASRLanguage string `json:"asr_language"`
	ASRModel    string `json:"asr_model"`

	// Transcription nil (absent or null) disables the
	// conversation.item.input_audio_transcription.* events; the internal ASR
	// runs either way.
	Transcription *Transcription `json:"transcription"`
	// TurnDetection nil (absent or null) is manual mode.
	TurnDetection *TurnDetection `json:"turn_detection"`
}

// Settings holds the runtime knobs that are not per-profile.
type Settings struct {
	// DefaultProfile may be empty: the gateway then starts but refuses new
	// realtime connections until a profile is selected.
	DefaultProfile string `json:"default_profile"`
}

// TemperatureMin and TemperatureMax bound Profile.Temperature.
const (
	TemperatureMin = 0.0
	TemperatureMax = 2.0
)

// DefaultProfile returns the base a decoded profile overlays: every profile
// default in Cascade is defined here.
func DefaultProfile() *Profile {
	return &Profile{
		Instructions:     "You are a helpful assistant.",
		MaxOutputTokens:  MaxOutputTokens{Inf: true},
		OutputModalities: []string{ModalityAudio},
		Voice:            "alloy",
		Speed:            1.0,
		Transcription:    &Transcription{},
		TurnDetection: &TurnDetection{
			Type:              TurnDetectionServerVAD,
			CreateResponse:    true,
			InterruptResponse: true,
		},
	}
}

// SessionDefaults projects the profile onto the GA-shaped session snapshot the
// session actor and the protocol adapter consume. Audio formats are fixed by
// the v1 scope and are not configurable.
func (p *Profile) SessionDefaults() SessionDefaults {
	s := SessionDefaults{
		Instructions:     p.Instructions,
		OutputModalities: slices.Clone(p.OutputModalities),
		MaxOutputTokens:  p.MaxOutputTokens,
		Audio: Audio{
			Input: AudioInput{
				Format:        AudioFormat{Type: AudioFormatPCM, Rate: AudioSampleRate},
				TurnDetection: p.TurnDetection.Clone(),
			},
			Output: AudioOutput{
				Format: AudioFormat{Type: AudioFormatPCM, Rate: AudioSampleRate},
				Voice:  p.Voice,
				Speed:  p.Speed,
			},
		},
	}
	if p.Transcription != nil {
		// The echoed transcription object reports the ASR knobs that are
		// actually in force, so the projection is truthful.
		s.Audio.Input.Transcription = &Transcription{
			Model:    p.ASRModel,
			Language: p.ASRLanguage,
			Prompt:   p.Transcription.Prompt,
		}
	}
	return s
}

// Clone returns a deep copy so a caller can mutate a candidate configuration
// without touching the live snapshot.
func (c *RuntimeConfig) Clone() *RuntimeConfig {
	out := &RuntimeConfig{Settings: c.Settings}
	if c.Providers != nil {
		out.Providers = make(map[string]*ProviderInstance, len(c.Providers))
		for k, v := range c.Providers {
			out.Providers[k] = v.Clone()
		}
	}
	if c.Profiles != nil {
		out.Profiles = make(map[string]*Profile, len(c.Profiles))
		for k, v := range c.Profiles {
			out.Profiles[k] = v.Clone()
		}
	}
	return out
}

// Clone returns a deep copy.
func (p *ProviderInstance) Clone() *ProviderInstance {
	if p == nil {
		return nil
	}
	out := *p
	out.Options = bytes.Clone(p.Options)
	return &out
}

// Clone returns a deep copy.
func (p *Profile) Clone() *Profile {
	if p == nil {
		return nil
	}
	out := *p
	out.OutputModalities = slices.Clone(p.OutputModalities)
	if p.Temperature != nil {
		v := *p.Temperature
		out.Temperature = &v
	}
	if p.Transcription != nil {
		t := *p.Transcription
		out.Transcription = &t
	}
	out.TurnDetection = p.TurnDetection.Clone()
	return &out
}

// ProviderNames returns the provider instance names in sorted order.
func (c *RuntimeConfig) ProviderNames() []string { return slices.Sorted(maps.Keys(c.Providers)) }

// ProfileNames returns the profile names in sorted order.
func (c *RuntimeConfig) ProfileNames() []string { return slices.Sorted(maps.Keys(c.Profiles)) }
