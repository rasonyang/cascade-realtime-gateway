package config

import (
	"slices"
	"strings"
)

// ProviderLookup reports whether a provider named typ is registered for kind
// ("asr", "llm" or "tts"). It is injected by the caller so this package never
// imports the provider registry. A nil lookup skips the existence check.
type ProviderLookup func(kind, typ string) bool

var logLevels = []string{"debug", "info", "warn", "error"}

var eagernessValues = []string{"low", "medium", "high", "auto"}

// Validate checks the whole configuration and returns the first problem as a
// *FieldError naming the offending field.
func (c *Config) Validate(known ProviderLookup) error {
	if strings.TrimSpace(c.Listen) == "" {
		return fieldErrorf("listen", "must not be empty")
	}
	if c.Auth.APIKey == "" {
		return fieldErrorf("auth.api_key", "must not be empty")
	}
	for _, p := range []struct {
		kind string
		cfg  Provider
	}{
		{"asr", c.Providers.ASR},
		{"llm", c.Providers.LLM},
		{"tts", c.Providers.TTS},
	} {
		field := "providers." + p.kind + ".type"
		if p.cfg.Type == "" {
			return fieldErrorf(field, "must not be empty")
		}
		if known != nil && !known(p.kind, p.cfg.Type) {
			return fieldErrorf(field, "unknown provider %q", p.cfg.Type)
		}
	}
	if err := c.SessionDefaults.Validate("session_defaults"); err != nil {
		return err
	}
	if err := c.Limits.validate("limits"); err != nil {
		return err
	}
	if !slices.Contains(logLevels, c.Observability.LogLevel) {
		return fieldErrorf("observability.log_level", "must be one of %s, got %q",
			strings.Join(logLevels, "|"), c.Observability.LogLevel)
	}
	return nil
}

// Validate checks a session object; prefix is prepended to field paths so the
// same checks serve both the config file and protocol-level session updates.
func (s *SessionDefaults) Validate(prefix string) error {
	mods := s.OutputModalities
	if len(mods) != 1 || (mods[0] != ModalityAudio && mods[0] != ModalityText) {
		return fieldErrorf(prefix+".output_modalities", `must be exactly ["audio"] or ["text"]`)
	}
	if err := s.Audio.Input.Format.validate(prefix + ".audio.input.format"); err != nil {
		return err
	}
	if err := s.Audio.Output.Format.validate(prefix + ".audio.output.format"); err != nil {
		return err
	}
	if td := s.Audio.Input.TurnDetection; td != nil {
		if err := td.validate(prefix + ".audio.input.turn_detection"); err != nil {
			return err
		}
	}
	if sp := s.Audio.Output.Speed; sp < SpeedMin || sp > SpeedMax {
		return fieldErrorf(prefix+".audio.output.speed", "must be within [%g, %g], got %g", SpeedMin, SpeedMax, sp)
	}
	if !s.MaxOutputTokens.Inf && (s.MaxOutputTokens.N < 1 || s.MaxOutputTokens.N > MaxOutputTokensLimit) {
		return fieldErrorf(prefix+".max_output_tokens", `must be "inf" or within [1, %d], got %d`, MaxOutputTokensLimit, s.MaxOutputTokens.N)
	}
	return nil
}

func (f *AudioFormat) validate(prefix string) error {
	if f.Type != AudioFormatPCM {
		return fieldErrorf(prefix+".type", "only %q is supported, got %q", AudioFormatPCM, f.Type)
	}
	if f.Rate != AudioSampleRate {
		return fieldErrorf(prefix+".rate", "only %d is supported, got %d", AudioSampleRate, f.Rate)
	}
	return nil
}

func (td *TurnDetection) validate(prefix string) error {
	switch td.Type {
	case TurnDetectionServerVAD:
		if td.Eagerness != nil {
			return fieldErrorf(prefix+".eagerness", "only allowed for %s", TurnDetectionSemanticVAD)
		}
		if td.Threshold == nil || *td.Threshold < 0 || *td.Threshold > 1 {
			return fieldErrorf(prefix+".threshold", "must be within [0, 1]")
		}
		if td.PrefixPaddingMs == nil || *td.PrefixPaddingMs < 0 {
			return fieldErrorf(prefix+".prefix_padding_ms", "must be >= 0")
		}
		if td.SilenceDurationMs == nil || *td.SilenceDurationMs <= 0 {
			return fieldErrorf(prefix+".silence_duration_ms", "must be > 0")
		}
	case TurnDetectionSemanticVAD:
		switch {
		case td.Threshold != nil:
			return fieldErrorf(prefix+".threshold", "only allowed for %s", TurnDetectionServerVAD)
		case td.PrefixPaddingMs != nil:
			return fieldErrorf(prefix+".prefix_padding_ms", "only allowed for %s", TurnDetectionServerVAD)
		case td.SilenceDurationMs != nil:
			return fieldErrorf(prefix+".silence_duration_ms", "only allowed for %s", TurnDetectionServerVAD)
		}
		if td.Eagerness == nil || !slices.Contains(eagernessValues, *td.Eagerness) {
			got := "<nil>"
			if td.Eagerness != nil {
				got = *td.Eagerness
			}
			return fieldErrorf(prefix+".eagerness", "must be one of %s, got %q", strings.Join(eagernessValues, "|"), got)
		}
	default:
		return fieldErrorf(prefix+".type", "must be %s or %s (use null for manual mode), got %q",
			TurnDetectionServerVAD, TurnDetectionSemanticVAD, td.Type)
	}
	return nil
}

func (l *Limits) validate(prefix string) error {
	ints := []struct {
		name string
		v    int
	}{
		{"max_sessions", l.MaxSessions},
		{"input_audio_queue_frames", l.InputAudioQueueFrames},
		{"input_audio_buffer_max_ms", l.InputAudioBufferMaxMs},
		{"output_event_queue", l.OutputEventQueue},
		{"client_max_message_bytes", l.ClientMaxMessageBytes},
	}
	for _, f := range ints {
		if f.v <= 0 {
			return fieldErrorf(prefix+"."+f.name, "must be > 0, got %d", f.v)
		}
	}
	durs := []struct {
		name string
		v    Duration
	}{
		{"session_timeout", l.SessionTimeout},
		{"asr_final_timeout", l.ASRFinalTimeout},
		{"client_write_timeout", l.ClientWriteTimeout},
	}
	for _, f := range durs {
		if f.v <= 0 {
			return fieldErrorf(prefix+"."+f.name, "must be > 0, got %s", f.v)
		}
	}
	return nil
}
