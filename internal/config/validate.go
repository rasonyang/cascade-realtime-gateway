package config

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// ProviderLookup reports whether a provider named typ is registered for kind
// ("asr", "llm" or "tts"). It is injected by the caller so this package never
// imports the provider registry. A nil lookup skips the existence check.
type ProviderLookup func(kind, typ string) bool

// providerKinds are the three roles a provider instance can fill.
var providerKinds = []string{"asr", "llm", "tts"}

var logLevels = []string{"debug", "info", "warn", "error"}

var eagernessValues = []string{"low", "medium", "high", "auto"}

// Validate checks the whole static configuration and returns the first problem
// as a *FieldError naming the offending field. Runtime configuration
// (providers, profiles, settings) is validated by RuntimeConfig.Validate; the
// optional bootstrap block is checked here so a bad seed fails at startup.
func (c *Config) Validate(known ProviderLookup) error {
	if strings.TrimSpace(c.Listen) == "" {
		return fieldErrorf("listen", "must not be empty")
	}
	if c.Auth.APIKey == "" {
		return fieldErrorf("auth.api_key", "must not be empty")
	}
	if c.Auth.AdminAPIKey != "" {
		if c.Auth.AdminAPIKey == c.Auth.APIKey {
			return fieldErrorf("auth.admin_api_key", "must differ from auth.api_key")
		}
		if strings.TrimSpace(c.Admin.Listen) == "" {
			return fieldErrorf("admin.listen", "must not be empty when auth.admin_api_key is set")
		}
		if strings.TrimSpace(c.Admin.StateFile) == "" {
			return fieldErrorf("admin.state_file", "must not be empty when auth.admin_api_key is set")
		}
	}
	if len(c.Bootstrap) > 0 {
		rc, err := DecodeRuntimeConfig(c.Bootstrap, "bootstrap")
		if err != nil {
			return err
		}
		if err := rc.validate("bootstrap.", known); err != nil {
			return err
		}
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

// Validate checks a whole runtime configuration: every provider instance, every
// profile, the cross-references between them, and the settings. It returns the
// first problem as a *FieldError with the resource's dotted field path.
func (c *RuntimeConfig) Validate(known ProviderLookup) error { return c.validate("", known) }

func (c *RuntimeConfig) validate(prefix string, known ProviderLookup) error {
	for _, name := range c.ProviderNames() {
		if err := c.Providers[name].validate(prefix+"providers."+name, name, known); err != nil {
			return err
		}
	}
	for _, name := range c.ProfileNames() {
		p := c.Profiles[name]
		field := prefix + "profiles." + name
		if err := p.validate(field, name); err != nil {
			return err
		}
		for _, ref := range []struct {
			kind, instance string
		}{{"asr", p.ASR}, {"llm", p.LLM}, {"tts", p.TTS}} {
			if err := c.validateRef(field+"."+ref.kind, ref.kind, ref.instance, known); err != nil {
				return err
			}
		}
	}
	if d := c.Settings.DefaultProfile; d != "" {
		if _, ok := c.Profiles[d]; !ok {
			return fieldErrorf(prefix+"settings.default_profile", "unknown profile %q", d)
		}
	}
	return nil
}

// validateRef checks that a profile's provider reference exists and that the
// referenced instance's type is registered for that role.
func (c *RuntimeConfig) validateRef(field, kind, instance string, known ProviderLookup) error {
	if instance == "" {
		return fieldErrorf(field, "must not be empty")
	}
	inst, ok := c.Providers[instance]
	if !ok {
		return fieldErrorf(field, "unknown provider instance %q", instance)
	}
	if known != nil && !known(kind, inst.Type) {
		return fieldErrorf(field, "provider %q does not support %s", inst.Type, kind)
	}
	return nil
}

func (p *ProviderInstance) validate(field, key string, known ProviderLookup) error {
	if p.Name != key {
		return fieldErrorf(field+".name", "must equal the resource name %q, got %q", key, p.Name)
	}
	if p.Type == "" {
		return fieldErrorf(field+".type", "must not be empty")
	}
	if known != nil && !slices.ContainsFunc(providerKinds, func(k string) bool { return known(k, p.Type) }) {
		return fieldErrorf(field+".type", "unknown provider type %q", p.Type)
	}
	if p.APIKey == "" {
		return fieldErrorf(field+".api_key", "must not be empty")
	}
	return nil
}

func (p *Profile) validate(field, key string) error {
	if p.Name != key {
		return fieldErrorf(field+".name", "must equal the resource name %q, got %q", key, p.Name)
	}
	if err := validateModalities(field+".output_modalities", p.OutputModalities); err != nil {
		return err
	}
	if err := validateMaxOutputTokens(field+".max_output_tokens", p.MaxOutputTokens); err != nil {
		return err
	}
	if err := validateSpeed(field+".speed", p.Speed); err != nil {
		return err
	}
	if p.Voice == "" {
		return fieldErrorf(field+".voice", "must not be empty")
	}
	switch p.ToolChoice.Mode {
	case ToolChoiceAuto, ToolChoiceNone, ToolChoiceRequired:
	case ToolChoiceFunction:
		// A profile declares no tools, so a forced function could never name
		// one; tools arrive through session.update.
		return fieldErrorf(field+".tool_choice", "a forced function is not configurable on a profile; tools are declared per session")
	default:
		return fieldErrorf(field+".tool_choice", `must be "auto", "none" or "required", got %q`, p.ToolChoice.Mode)
	}
	if t := p.Temperature; t != nil && (*t < TemperatureMin || *t > TemperatureMax) {
		return fieldErrorf(field+".temperature", "must be within [%g, %g], got %g",
			TemperatureMin, TemperatureMax, *t)
	}
	if t := p.Transcription; t != nil {
		// asr_language / asr_model are the single source of truth; accepting
		// them here too would leave two places to look.
		if t.Language != "" {
			return fieldErrorf(field+".transcription.language", "not configurable here; use %s.asr_language", field)
		}
		if t.Model != "" {
			return fieldErrorf(field+".transcription.model", "not configurable here; use %s.asr_model", field)
		}
	}
	if td := p.TurnDetection; td != nil {
		if err := td.validate(field + ".turn_detection"); err != nil {
			return err
		}
	}
	return nil
}

// Validate checks a session object; prefix is prepended to field paths so the
// same checks serve both a profile projection and protocol-level session
// updates.
func (s *SessionDefaults) Validate(prefix string) error {
	if err := validateModalities(prefix+".output_modalities", s.OutputModalities); err != nil {
		return err
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
	if err := validateSpeed(prefix+".audio.output.speed", s.Audio.Output.Speed); err != nil {
		return err
	}
	if err := validateTools(prefix+".tools", s.Tools); err != nil {
		return err
	}
	if err := validateToolChoice(prefix+".tool_choice", s.ToolChoice, s.Tools); err != nil {
		return err
	}
	return validateMaxOutputTokens(prefix+".max_output_tokens", s.MaxOutputTokens)
}

// validateTools checks the tool array. Parameters is opaque, but it must be a
// JSON object: everything else would be rejected by the provider anyway, and
// far later.
func validateTools(field string, tools []Tool) error {
	seen := make(map[string]bool, len(tools))
	for i, t := range tools {
		at := fmt.Sprintf("%s[%d]", field, i)
		if t.Type != ToolTypeFunction {
			return fieldErrorf(at+".type", "only %q is supported, got %q", ToolTypeFunction, t.Type)
		}
		if t.Name == "" {
			return fieldErrorf(at+".name", "must not be empty")
		}
		if seen[t.Name] {
			return fieldErrorf(at+".name", "duplicate tool name %q", t.Name)
		}
		seen[t.Name] = true
		if len(t.Parameters) > 0 && !isJSONObject(t.Parameters) {
			return fieldErrorf(at+".parameters", "must be a JSON Schema object")
		}
	}
	return nil
}

// validateToolChoice checks the union. A forced function must name a tool
// that was actually declared, so the model can never be pointed at nothing.
func validateToolChoice(field string, tc ToolChoice, tools []Tool) error {
	switch tc.Mode {
	case ToolChoiceAuto, ToolChoiceNone, ToolChoiceRequired, "":
		return nil
	case ToolChoiceFunction:
		if !slices.ContainsFunc(tools, func(t Tool) bool { return t.Name == tc.Name }) {
			return fieldErrorf(field+".name", "no tool named %q is declared", tc.Name)
		}
		return nil
	}
	return fieldErrorf(field, `must be "auto", "none", "required" or a {"type":"function","name":…} object, got %q`, tc.Mode)
}

// isJSONObject reports whether raw is a JSON object, ignoring leading space.
func isJSONObject(raw json.RawMessage) bool {
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return false
	}
	_, ok := v.(map[string]any)
	return ok
}

func validateModalities(field string, mods []string) error {
	if len(mods) != 1 || (mods[0] != ModalityAudio && mods[0] != ModalityText) {
		return fieldErrorf(field, `must be exactly ["audio"] or ["text"]`)
	}
	return nil
}

func validateSpeed(field string, speed float64) error {
	if speed < SpeedMin || speed > SpeedMax {
		return fieldErrorf(field, "must be within [%g, %g], got %g", SpeedMin, SpeedMax, speed)
	}
	return nil
}

func validateMaxOutputTokens(field string, m MaxOutputTokens) error {
	if !m.Inf && (m.N < 1 || m.N > MaxOutputTokensLimit) {
		return fieldErrorf(field, `must be "inf" or within [1, %d], got %d`, MaxOutputTokensLimit, m.N)
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
