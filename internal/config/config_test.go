package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// minimal is the smallest file that passes Validate with a nil lookup.
const minimal = `{
  "auth": { "api_key": "{env.REALTIME_API_KEY}" },
  "providers": {
    "asr": { "type": "mock" },
    "llm": { "type": "mock" },
    "tts": { "type": "mock" }
  }
}`

var testEnv = map[string]string{
	"REALTIME_API_KEY": "rt-secret",
	"DEEPGRAM_API_KEY": "dg-secret",
	"OPENAI_API_KEY":   "oa-secret",
}

func lookup(name string) (string, bool) {
	v, ok := testEnv[name]
	return v, ok
}

func parse(t *testing.T, src string) (*Config, error) {
	t.Helper()
	return Parse([]byte(src), lookup)
}

func mustParse(t *testing.T, src string) *Config {
	t.Helper()
	cfg, err := parse(t, src)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return cfg
}

// wantFieldError asserts err is a *FieldError for field and that its message
// contains msg.
func wantFieldError(t *testing.T, err error, field, msg string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error on field %q, got nil", field)
	}
	var fe *FieldError
	if !errors.As(err, &fe) {
		t.Fatalf("expected *FieldError, got %T: %v", err, err)
	}
	if fe.Field != field {
		t.Fatalf("error field = %q, want %q (err: %v)", fe.Field, field, err)
	}
	if !strings.Contains(fe.Msg, msg) {
		t.Fatalf("error %q does not mention %q", err, msg)
	}
	if !strings.HasPrefix(err.Error(), `field "`+field+`": `) {
		t.Fatalf("error format %q is not `field \"<path>\": <reason>`", err)
	}
}

func TestLoadMinimalAppliesDefaults(t *testing.T) {
	cfg := mustParse(t, minimal)
	if err := cfg.Validate(nil); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if cfg.Auth.APIKey != "rt-secret" {
		t.Fatalf("env not expanded: %q", cfg.Auth.APIKey)
	}
	if cfg.Listen != ":8080" {
		t.Fatalf("listen default = %q", cfg.Listen)
	}
	if cfg.Limits.SessionTimeout.Std() != 30*time.Minute || cfg.Limits.OutputEventQueue != 256 {
		t.Fatalf("limits defaults not applied: %+v", cfg.Limits)
	}
	td := cfg.SessionDefaults.Audio.Input.TurnDetection
	if td == nil || td.Type != TurnDetectionServerVAD {
		t.Fatalf("turn_detection default = %+v", td)
	}
	if td.Threshold == nil || *td.Threshold != 0.5 || *td.PrefixPaddingMs != 300 || *td.SilenceDurationMs != 500 {
		t.Fatalf("server_vad defaults not applied: %+v", td)
	}
	if td.Eagerness != nil || !td.CreateResponse || !td.InterruptResponse {
		t.Fatalf("server_vad flags wrong: %+v", td)
	}
	if cfg.SessionDefaults.Audio.Input.Transcription == nil {
		t.Fatal("transcription default should be enabled ({})")
	}
	if cfg.SessionDefaults.Audio.Output.Speed != 1.0 || cfg.SessionDefaults.Audio.Output.Voice != "alloy" {
		t.Fatalf("output defaults wrong: %+v", cfg.SessionDefaults.Audio.Output)
	}
	if !cfg.SessionDefaults.MaxOutputTokens.Inf {
		t.Fatalf("max_output_tokens default = %+v", cfg.SessionDefaults.MaxOutputTokens)
	}
}

func TestExampleConfigLoadsAndValidates(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "config.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := Parse(raw, lookup)
	if err != nil {
		t.Fatalf("Parse example: %v", err)
	}
	known := func(kind, typ string) bool {
		return map[string]string{"asr": "deepgram", "llm": "openai", "tts": "openai"}[kind] == typ
	}
	if err := cfg.Validate(known); err != nil {
		t.Fatalf("Validate example: %v", err)
	}
	if cfg.Providers.LLM.APIKey != "oa-secret" || string(cfg.Providers.LLM.Options) == "" {
		t.Fatalf("provider block not loaded: %+v", cfg.Providers.LLM)
	}
	if got := cfg.SessionDefaults.Audio.Output.Speed; got != 1.0 {
		t.Fatalf("speed = %v", got)
	}
}

func TestLoadFromFileAndMissingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.json")
	if err := os.WriteFile(path, []byte(minimal), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REALTIME_API_KEY", "from-env")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Auth.APIKey != "from-env" {
		t.Fatalf("api_key = %q", cfg.Auth.APIKey)
	}
	if _, err := Load(filepath.Join(dir, "nope.json")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestMissingEnvNamesFieldPath(t *testing.T) {
	src := strings.Replace(minimal, `"llm": { "type": "mock" }`, `"llm": { "type": "mock", "api_key": "{env.NOT_SET_ANYWHERE}" }`, 1)
	_, err := parse(t, src)
	wantFieldError(t, err, "providers.llm.api_key", "NOT_SET_ANYWHERE")
}

func TestEnvExpansionInsideOptionsAndArrays(t *testing.T) {
	src := strings.Replace(minimal, `"tts": { "type": "mock" }`,
		`"tts": { "type": "mock", "options": { "keys": ["{env.OPENAI_API_KEY}-x"] } }`, 1)
	cfg := mustParse(t, src)
	if !strings.Contains(string(cfg.Providers.TTS.Options), "oa-secret-x") {
		t.Fatalf("placeholder inside options not expanded: %s", cfg.Providers.TTS.Options)
	}
	src = strings.Replace(minimal, `"tts": { "type": "mock" }`,
		`"tts": { "type": "mock", "options": { "keys": ["ok", "{env.MISSING_IN_ARRAY}"] } }`, 1)
	_, err := parse(t, src)
	wantFieldError(t, err, "providers.tts.options.keys[1]", "MISSING_IN_ARRAY")
}

func TestUnknownFieldNamesPath(t *testing.T) {
	src := strings.Replace(minimal, `"providers"`, `"listenn": ":1", "providers"`, 1)
	_, err := parse(t, src)
	wantFieldError(t, err, "listenn", "unknown field")

	src = minimal[:len(minimal)-1] + `, "session_defaults": { "audio": { "input": { "noise_reduction": null } } } }`
	_, err = parse(t, src)
	wantFieldError(t, err, "session_defaults.audio.input.noise_reduction", "unknown field")
}

func TestWrongTypeNamesPath(t *testing.T) {
	src := minimal[:len(minimal)-1] + `, "limits": { "max_sessions": "many" } }`
	_, err := parse(t, src)
	wantFieldError(t, err, "limits.max_sessions", "expected int")
}

func TestMalformedJSON(t *testing.T) {
	if _, err := parse(t, `{"listen": }`); err == nil {
		t.Fatal("expected parse error")
	}
	if _, err := parse(t, `[]`); err == nil {
		t.Fatal("expected error for non-object root")
	}
	if _, err := parse(t, `{} {}`); err == nil {
		t.Fatal("expected error for trailing data")
	}
}

func withSessionDefaults(body string) string {
	return minimal[:len(minimal)-1] + `, "session_defaults": ` + body + ` }`
}

func TestTurnDetectionNullIsManual(t *testing.T) {
	cfg := mustParse(t, withSessionDefaults(`{ "audio": { "input": { "turn_detection": null } } }`))
	if cfg.SessionDefaults.Audio.Input.TurnDetection != nil {
		t.Fatal("turn_detection: null should yield nil (manual mode)")
	}
	if err := cfg.Validate(nil); err != nil {
		t.Fatalf("manual mode must validate: %v", err)
	}
}

func TestTranscriptionNullDisables(t *testing.T) {
	cfg := mustParse(t, withSessionDefaults(`{ "audio": { "input": { "transcription": null } } }`))
	if cfg.SessionDefaults.Audio.Input.Transcription != nil {
		t.Fatal("transcription: null should yield nil")
	}
	cfg = mustParse(t, withSessionDefaults(`{ "audio": { "input": { "transcription": { "language": "en" } } } }`))
	if tr := cfg.SessionDefaults.Audio.Input.Transcription; tr == nil || tr.Language != "en" {
		t.Fatalf("transcription object not loaded: %+v", tr)
	}
}

func TestTurnDetectionOverlayKeepsDefaults(t *testing.T) {
	cfg := mustParse(t, withSessionDefaults(`{ "audio": { "input": { "turn_detection": { "threshold": 0.8, "create_response": false } } } }`))
	td := cfg.SessionDefaults.Audio.Input.TurnDetection
	if td.Type != TurnDetectionServerVAD || *td.Threshold != 0.8 || *td.PrefixPaddingMs != 300 {
		t.Fatalf("overlay wrong: %+v", td)
	}
	if td.CreateResponse || !td.InterruptResponse {
		t.Fatalf("flags wrong: %+v", td)
	}
	if err := cfg.Validate(nil); err != nil {
		t.Fatal(err)
	}
}

func TestSemanticVADDefaultsAndTypeSpecificFields(t *testing.T) {
	cfg := mustParse(t, withSessionDefaults(`{ "audio": { "input": { "turn_detection": { "type": "semantic_vad" } } } }`))
	td := cfg.SessionDefaults.Audio.Input.TurnDetection
	if td.Eagerness == nil || *td.Eagerness != "auto" {
		t.Fatalf("eagerness default not applied: %+v", td)
	}
	if td.Threshold != nil || td.PrefixPaddingMs != nil || td.SilenceDurationMs != nil {
		t.Fatalf("server_vad fields leaked into semantic_vad: %+v", td)
	}
	if err := cfg.Validate(nil); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name, body, field, msg string
	}{
		{"semantic+threshold", `{ "type": "semantic_vad", "threshold": 0.3 }`, "threshold", "only allowed for server_vad"},
		{"semantic+prefix", `{ "type": "semantic_vad", "prefix_padding_ms": 1 }`, "prefix_padding_ms", "only allowed for server_vad"},
		{"semantic+silence", `{ "type": "semantic_vad", "silence_duration_ms": 1 }`, "silence_duration_ms", "only allowed for server_vad"},
		{"semantic bad eagerness", `{ "type": "semantic_vad", "eagerness": "asap" }`, "eagerness", "must be one of"},
		{"server+eagerness", `{ "type": "server_vad", "eagerness": "high" }`, "eagerness", "only allowed for semantic_vad"},
		{"server threshold >1", `{ "type": "server_vad", "threshold": 1.5 }`, "threshold", "[0, 1]"},
		{"server prefix <0", `{ "type": "server_vad", "prefix_padding_ms": -1 }`, "prefix_padding_ms", ">= 0"},
		{"server silence 0", `{ "type": "server_vad", "silence_duration_ms": 0 }`, "silence_duration_ms", "> 0"},
		{"unknown type", `{ "type": "client_vad" }`, "type", "server_vad or semantic_vad"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := mustParse(t, withSessionDefaults(`{ "audio": { "input": { "turn_detection": `+tc.body+` } } }`))
			wantFieldError(t, cfg.Validate(nil), "session_defaults.audio.input.turn_detection."+tc.field, tc.msg)
		})
	}
}

func TestSessionDefaultsValidation(t *testing.T) {
	cases := []struct {
		name, body, field, msg string
	}{
		{"modalities both", `{ "output_modalities": ["audio", "text"] }`, "output_modalities", "exactly"},
		{"modalities empty", `{ "output_modalities": [] }`, "output_modalities", "exactly"},
		{"modalities unknown", `{ "output_modalities": ["video"] }`, "output_modalities", "exactly"},
		{"input format type", `{ "audio": { "input": { "format": { "type": "audio/pcmu" } } } }`, "audio.input.format.type", "audio/pcm"},
		{"input rate", `{ "audio": { "input": { "format": { "rate": 16000 } } } }`, "audio.input.format.rate", "24000"},
		{"output rate", `{ "audio": { "output": { "format": { "rate": 8000 } } } }`, "audio.output.format.rate", "24000"},
		{"speed low", `{ "audio": { "output": { "speed": 0.2 } } }`, "audio.output.speed", "[0.25, 1.5]"},
		{"speed high", `{ "audio": { "output": { "speed": 1.51 } } }`, "audio.output.speed", "[0.25, 1.5]"},
		{"max tokens zero", `{ "max_output_tokens": 0 }`, "max_output_tokens", "[1, 4096]"},
		{"max tokens big", `{ "max_output_tokens": 5000 }`, "max_output_tokens", "[1, 4096]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := mustParse(t, withSessionDefaults(tc.body))
			wantFieldError(t, cfg.Validate(nil), "session_defaults."+tc.field, tc.msg)
		})
	}
	for _, ok := range []string{`{ "output_modalities": ["text"] }`, `{ "audio": { "output": { "speed": 0.25 } } }`, `{ "audio": { "output": { "speed": 1.5 } } }`, `{ "max_output_tokens": 4096 }`, `{ "max_output_tokens": 1 }`} {
		if err := mustParse(t, withSessionDefaults(ok)).Validate(nil); err != nil {
			t.Fatalf("%s should validate: %v", ok, err)
		}
	}
}

func TestMaxOutputTokensDecoding(t *testing.T) {
	cfg := mustParse(t, withSessionDefaults(`{ "max_output_tokens": 128 }`))
	if m := cfg.SessionDefaults.MaxOutputTokens; m.Inf || m.N != 128 {
		t.Fatalf("got %+v", m)
	}
	for _, bad := range []string{`"infinite"`, `1.5`, `true`} {
		if _, err := parse(t, withSessionDefaults(`{ "max_output_tokens": `+bad+` }`)); err == nil {
			t.Fatalf("max_output_tokens %s should fail", bad)
		}
	}
}

func TestDurationDecoding(t *testing.T) {
	cfg := mustParse(t, minimal[:len(minimal)-1]+`, "limits": { "asr_final_timeout": "1500ms" } }`)
	if cfg.Limits.ASRFinalTimeout.Std() != 1500*time.Millisecond {
		t.Fatalf("got %s", cfg.Limits.ASRFinalTimeout)
	}
	for _, bad := range []string{`5`, `"5"`, `"soon"`} {
		if _, err := parse(t, minimal[:len(minimal)-1]+`, "limits": { "asr_final_timeout": `+bad+` } }`); err == nil {
			t.Fatalf("duration %s should fail", bad)
		}
	}
}

func TestTopLevelValidation(t *testing.T) {
	cases := []struct {
		name, src, field, msg string
	}{
		{"listen empty", strings.Replace(minimal, `"auth"`, `"listen": " ", "auth"`, 1), "listen", "empty"},
		{"api key empty", strings.Replace(minimal, `"{env.REALTIME_API_KEY}"`, `""`, 1), "auth.api_key", "empty"},
		{"asr type empty", strings.Replace(minimal, `"asr": { "type": "mock" }`, `"asr": {}`, 1), "providers.asr.type", "empty"},
		{"limit zero", minimal[:len(minimal)-1] + `, "limits": { "input_audio_queue_frames": 0 } }`, "limits.input_audio_queue_frames", "> 0"},
		{"timeout zero", minimal[:len(minimal)-1] + `, "limits": { "client_write_timeout": "0s" } }`, "limits.client_write_timeout", "> 0"},
		{"log level", minimal[:len(minimal)-1] + `, "observability": { "log_level": "trace" } }`, "observability.log_level", "must be one of"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := mustParse(t, tc.src)
			wantFieldError(t, cfg.Validate(nil), tc.field, tc.msg)
		})
	}
}

func TestProviderLookup(t *testing.T) {
	cfg := mustParse(t, strings.Replace(minimal, `"llm": { "type": "mock" }`, `"llm": { "type": "foo" }`, 1))
	known := func(kind, typ string) bool { return typ == "mock" }
	err := cfg.Validate(known)
	wantFieldError(t, err, "providers.llm.type", `unknown provider "foo"`)
	if err.Error() != `field "providers.llm.type": unknown provider "foo"` {
		t.Fatalf("unexpected message: %q", err)
	}
	if err := mustParse(t, minimal).Validate(known); err != nil {
		t.Fatalf("known providers rejected: %v", err)
	}
}

func TestDefaultConfigFailsValidationWithoutSecrets(t *testing.T) {
	wantFieldError(t, DefaultConfig().Validate(nil), "auth.api_key", "empty")
}
