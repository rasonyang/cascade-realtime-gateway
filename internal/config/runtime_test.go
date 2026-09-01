package config

import (
	"encoding/json"
	"strings"
	"testing"
)

// knownAll accepts the "mock" provider for every role and "asronly" for ASR.
func knownAll(kind, typ string) bool {
	switch typ {
	case "mock":
		return true
	case "asronly":
		return kind == "asr"
	}
	return false
}

const runtimeDoc = `{
  "providers": {
    "m": { "name": "m", "type": "mock", "api_key": "{env.X}" }
  },
  "profiles": {
    "default": { "name": "default", "asr": "m", "llm": "m", "tts": "m" }
  },
  "settings": { "default_profile": "default" }
}`

func mustDecode(t *testing.T, src string) *RuntimeConfig {
	t.Helper()
	rc, err := DecodeRuntimeConfig([]byte(src), "")
	if err != nil {
		t.Fatalf("DecodeRuntimeConfig: %v", err)
	}
	return rc
}

func TestDecodeProfileAppliesDefaults(t *testing.T) {
	rc := mustDecode(t, runtimeDoc)
	if err := rc.Validate(knownAll); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	p := rc.Profiles["default"]
	if p.Instructions == "" || p.Voice != "alloy" || p.Speed != 1.0 {
		t.Fatalf("profile defaults not applied: %+v", p)
	}
	// Sampling is left to the provider unless a profile asks for it.
	if p.Temperature != nil {
		t.Fatalf("temperature should default to unset, got %g", *p.Temperature)
	}
	if !p.MaxOutputTokens.Inf || len(p.OutputModalities) != 1 || p.OutputModalities[0] != ModalityAudio {
		t.Fatalf("profile defaults not applied: %+v", p)
	}
	if p.Transcription == nil {
		t.Fatal("transcription default should be enabled ({})")
	}
	td := p.TurnDetection
	if td == nil || td.Type != TurnDetectionServerVAD || *td.Threshold != DefaultVADThreshold ||
		*td.PrefixPaddingMs != DefaultVADPrefixPaddingMs || *td.SilenceDurationMs != DefaultVADSilenceDurationMs {
		t.Fatalf("server_vad defaults not applied: %+v", td)
	}
	if !td.CreateResponse || !td.InterruptResponse || td.Eagerness != nil {
		t.Fatalf("server_vad flags wrong: %+v", td)
	}
	// The secret is stored exactly as written.
	if rc.Providers["m"].APIKey != "{env.X}" {
		t.Fatalf("api_key rewritten: %q", rc.Providers["m"].APIKey)
	}
}

func TestProfileSessionDefaultsProjection(t *testing.T) {
	rc := mustDecode(t, `{
	  "providers": { "m": { "name": "m", "type": "mock", "api_key": "k" } },
	  "profiles": { "p": { "name": "p", "asr": "m", "llm": "m", "tts": "m",
	    "instructions": "be brief", "voice": "verse", "speed": 1.2,
	    "asr_language": "zh", "asr_model": "big", "max_output_tokens": 128,
	    "output_modalities": ["text"] } },
	  "settings": { "default_profile": "p" }
	}`)
	if err := rc.Validate(knownAll); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	s := rc.Profiles["p"].SessionDefaults()
	if s.Instructions != "be brief" || s.Audio.Output.Voice != "verse" || s.Audio.Output.Speed != 1.2 {
		t.Fatalf("projection wrong: %+v", s)
	}
	if s.Audio.Input.Format.Type != AudioFormatPCM || s.Audio.Input.Format.Rate != AudioSampleRate ||
		s.Audio.Output.Format.Type != AudioFormatPCM || s.Audio.Output.Format.Rate != AudioSampleRate {
		t.Fatalf("formats are fixed constants: %+v", s.Audio)
	}
	// The echoed transcription object reports the ASR knobs in force.
	if tr := s.Audio.Input.Transcription; tr == nil || tr.Language != "zh" || tr.Model != "big" {
		t.Fatalf("transcription projection wrong: %+v", tr)
	}
	if s.MaxOutputTokens.Inf || s.MaxOutputTokens.N != 128 {
		t.Fatalf("max_output_tokens = %+v", s.MaxOutputTokens)
	}
	if err := s.Validate("session"); err != nil {
		t.Fatalf("projection must satisfy the session validation: %v", err)
	}
	// Mutating the projection must not touch the profile.
	s.Audio.Input.TurnDetection.Type = "mutated"
	if rc.Profiles["p"].TurnDetection.Type != TurnDetectionServerVAD {
		t.Fatal("projection shares state with the profile")
	}
}

func TestTranscriptionAndTurnDetectionNull(t *testing.T) {
	rc := mustDecode(t, `{
	  "providers": { "m": { "name": "m", "type": "mock", "api_key": "k" } },
	  "profiles": { "p": { "name": "p", "asr": "m", "llm": "m", "tts": "m",
	    "transcription": null, "turn_detection": null } },
	  "settings": { "default_profile": "p" }
	}`)
	if err := rc.Validate(knownAll); err != nil {
		t.Fatalf("manual mode must validate: %v", err)
	}
	p := rc.Profiles["p"]
	if p.Transcription != nil || p.TurnDetection != nil {
		t.Fatalf("null should disable: %+v", p)
	}
	s := p.SessionDefaults()
	if s.Audio.Input.Transcription != nil || s.Audio.Input.TurnDetection != nil {
		t.Fatalf("projection kept the disabled blocks: %+v", s.Audio.Input)
	}
}

func TestSemanticVADInProfile(t *testing.T) {
	rc := mustDecode(t, `{
	  "providers": { "m": { "name": "m", "type": "mock", "api_key": "k" } },
	  "profiles": { "p": { "name": "p", "asr": "m", "llm": "m", "tts": "m",
	    "turn_detection": { "type": "semantic_vad" } } },
	  "settings": { "default_profile": "p" }
	}`)
	if err := rc.Validate(knownAll); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	td := rc.Profiles["p"].TurnDetection
	if td.Eagerness == nil || *td.Eagerness != "auto" {
		t.Fatalf("eagerness default not applied: %+v", td)
	}
	if td.Threshold != nil || td.PrefixPaddingMs != nil || td.SilenceDurationMs != nil {
		t.Fatalf("server_vad fields leaked into semantic_vad: %+v", td)
	}
}

func TestProfileValidationFieldPaths(t *testing.T) {
	cases := []struct {
		name, body, field, msg string
	}{
		{"name mismatch", `"name": "other"`, "name", "must equal the resource name"},
		{"modalities both", `"output_modalities": ["audio", "text"]`, "output_modalities", "exactly"},
		{"modalities empty", `"output_modalities": []`, "output_modalities", "exactly"},
		{"max tokens zero", `"max_output_tokens": 0`, "max_output_tokens", "[1, 4096]"},
		{"max tokens big", `"max_output_tokens": 5000`, "max_output_tokens", "[1, 4096]"},
		{"speed low", `"speed": 0.2`, "speed", "[0.25, 1.5]"},
		{"speed high", `"speed": 1.51`, "speed", "[0.25, 1.5]"},
		{"voice empty", `"voice": ""`, "voice", "empty"},
		{"temperature high", `"temperature": 2.5`, "temperature", "[0, 2]"},
		{"temperature low", `"temperature": -0.1`, "temperature", "[0, 2]"},
		{"transcription language", `"transcription": { "language": "en" }`, "transcription.language", "asr_language"},
		{"transcription model", `"transcription": { "model": "x" }`, "transcription.model", "asr_model"},
		{"asr empty", `"asr": ""`, "asr", "empty"},
		{"asr unknown instance", `"asr": "nope"`, "asr", `unknown provider instance "nope"`},
		{"semantic+threshold", `"turn_detection": { "type": "semantic_vad", "threshold": 0.3 }`, "turn_detection.threshold", "only allowed for server_vad"},
		{"server+eagerness", `"turn_detection": { "type": "server_vad", "eagerness": "high" }`, "turn_detection.eagerness", "only allowed for semantic_vad"},
		{"server threshold >1", `"turn_detection": { "threshold": 1.5 }`, "turn_detection.threshold", "[0, 1]"},
		{"unknown vad type", `"turn_detection": { "type": "client_vad" }`, "turn_detection.type", "server_vad or semantic_vad"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := `{
			  "providers": { "m": { "name": "m", "type": "mock", "api_key": "k" } },
			  "profiles": { "p": { "name": "p", "asr": "m", "llm": "m", "tts": "m", ` + tc.body + ` } }
			}`
			rc := mustDecode(t, src)
			wantFieldError(t, rc.Validate(knownAll), "profiles.p."+tc.field, tc.msg)
		})
	}
}

func TestProviderInstanceValidation(t *testing.T) {
	cases := []struct {
		name, body, field, msg string
	}{
		{"name mismatch", `{ "name": "other", "type": "mock", "api_key": "k" }`, "providers.m.name", "must equal the resource name"},
		{"type empty", `{ "name": "m", "api_key": "k" }`, "providers.m.type", "empty"},
		{"type unknown", `{ "name": "m", "type": "nope", "api_key": "k" }`, "providers.m.type", `unknown provider type "nope"`},
		{"api key empty", `{ "name": "m", "type": "mock" }`, "providers.m.api_key", "empty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rc := mustDecode(t, `{ "providers": { "m": `+tc.body+` } }`)
			wantFieldError(t, rc.Validate(knownAll), tc.field, tc.msg)
		})
	}
}

func TestProviderRoleSupport(t *testing.T) {
	rc := mustDecode(t, `{
	  "providers": { "a": { "name": "a", "type": "asronly", "api_key": "k" } },
	  "profiles": { "p": { "name": "p", "asr": "a", "llm": "a", "tts": "a" } }
	}`)
	wantFieldError(t, rc.Validate(knownAll), "profiles.p.llm", `provider "asronly" does not support llm`)
}

func TestDefaultProfileReference(t *testing.T) {
	rc := mustDecode(t, `{ "settings": { "default_profile": "nope" } }`)
	wantFieldError(t, rc.Validate(knownAll), "settings.default_profile", `unknown profile "nope"`)

	// An empty runtime configuration is valid: the gateway starts and refuses
	// realtime connections until a profile is selected.
	if err := mustDecode(t, `{}`).Validate(knownAll); err != nil {
		t.Fatalf("empty runtime config must validate: %v", err)
	}
}

func TestRuntimeConfigUnknownFieldNamesPath(t *testing.T) {
	_, err := DecodeRuntimeConfig([]byte(`{ "providers": { "m": { "name": "m", "typ": "mock" } } }`), "")
	wantFieldError(t, err, "providers.m.typ", "unknown field")

	_, err = DecodeRuntimeConfig([]byte(`{ "profiles": { "p": { "temp": 1 } } }`), "")
	wantFieldError(t, err, "profiles.p.temp", "unknown field")

	_, err = DecodeRuntimeConfig([]byte(`{ "settings": { "default": "p" } }`), "bootstrap")
	wantFieldError(t, err, "bootstrap.settings.default", "unknown field")

	_, err = DecodeRuntimeConfig([]byte(`[]`), "")
	wantFieldError(t, err, "(root)", "must be an object")
}

func TestRuntimeConfigRoundTrip(t *testing.T) {
	rc := mustDecode(t, runtimeDoc)
	body, err := MarshalRuntimeConfig(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "{env.X}") {
		t.Fatalf("secret not stored verbatim: %s", body)
	}
	back, err := DecodeRuntimeConfig(body, "")
	if err != nil {
		t.Fatalf("re-decode: %v", err)
	}
	again, err := MarshalRuntimeConfig(back)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(body) {
		t.Fatalf("round trip is not stable:\n%s\nvs\n%s", body, again)
	}
}

func TestRuntimeConfigCloneIsDeep(t *testing.T) {
	rc := mustDecode(t, runtimeDoc)
	rc.Providers["m"].Options = json.RawMessage(`{"a":1}`)
	temp := 1.5
	rc.Profiles["default"].Temperature = &temp

	c := rc.Clone()
	c.Providers["m"].APIKey = "changed"
	c.Providers["m"].Options[2] = 'b'
	c.Profiles["default"].Voice = "changed"
	*c.Profiles["default"].Temperature = 0.1
	c.Profiles["default"].TurnDetection.Type = "changed"
	c.Profiles["default"].OutputModalities[0] = "changed"
	c.Settings.DefaultProfile = "changed"
	delete(c.Profiles, "default")

	if rc.Providers["m"].APIKey != "{env.X}" || string(rc.Providers["m"].Options) != `{"a":1}` {
		t.Fatalf("provider aliased: %+v", rc.Providers["m"])
	}
	p := rc.Profiles["default"]
	if p == nil || p.Voice != "alloy" || p.TurnDetection.Type != TurnDetectionServerVAD || p.OutputModalities[0] != ModalityAudio {
		t.Fatalf("profile aliased: %+v", p)
	}
	if *p.Temperature != 1.5 {
		t.Fatalf("temperature aliased: %g", *p.Temperature)
	}
	if rc.Settings.DefaultProfile != "default" {
		t.Fatalf("settings aliased: %+v", rc.Settings)
	}
}
