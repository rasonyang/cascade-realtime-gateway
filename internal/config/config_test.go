package config

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// minimal is the smallest file that passes Validate with a nil lookup.
const minimal = `{
  "auth": { "api_key": "{env.REALTIME_API_KEY}" }
}`

var testEnv = map[string]string{
	"REALTIME_API_KEY": "rt-secret",
	"ADMIN_API_KEY":    "admin-secret",
	"DEEPGRAM_API_KEY": "dg-secret",
	"OPENAI_API_KEY":   "oa-secret",
	"ALIYUN_API_KEY":   "qw-secret",
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

// withBlock appends a top-level block to the minimal document.
func withBlock(body string) string { return minimal[:len(minimal)-1] + ", " + body + " }" }

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
	if cfg.Admin.Listen != "127.0.0.1:8081" || cfg.Admin.StateFile != "cascade-state.json" {
		t.Fatalf("admin defaults = %+v", cfg.Admin)
	}
	if cfg.Auth.AdminAPIKey != "" {
		t.Fatalf("admin api key should default to empty (Admin API disabled), got %q", cfg.Auth.AdminAPIKey)
	}
	if len(cfg.Bootstrap) != 0 {
		t.Fatalf("bootstrap should default to absent, got %s", cfg.Bootstrap)
	}
	if cfg.Limits.SessionTimeout.Std() != 30*time.Minute || cfg.Limits.OutputEventQueue != 256 || cfg.Limits.ClientMaxMessageBytes != 16<<20 {
		t.Fatalf("limits defaults not applied: %+v", cfg.Limits)
	}
}

func TestExampleConfigsLoadAndValidate(t *testing.T) {
	known := func(kind, typ string) bool {
		switch typ {
		case "deepgram":
			return kind == "asr"
		case "openai":
			return kind == "llm" || kind == "tts"
		case "qwen":
			return true
		}
		return false
	}
	for _, name := range []string{"config.example.json", "config.qwen.example.json"} {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("..", "..", name))
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := Parse(raw, lookup)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if err := cfg.Validate(known); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			rc, err := DecodeRuntimeConfig(cfg.Bootstrap, "bootstrap")
			if err != nil {
				t.Fatalf("DecodeRuntimeConfig: %v", err)
			}
			if rc.Settings.DefaultProfile != "default" {
				t.Fatalf("default_profile = %q", rc.Settings.DefaultProfile)
			}
			for _, p := range rc.Providers {
				if !strings.HasPrefix(p.APIKey, "{env.") {
					t.Fatalf("bootstrap secret was expanded at load: %q", p.APIKey)
				}
			}
		})
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
	_, err := parse(t, `{ "auth": { "api_key": "{env.NOT_SET_ANYWHERE}" } }`)
	wantFieldError(t, err, "auth.api_key", "NOT_SET_ANYWHERE")
}

// The bootstrap block is runtime configuration: it is stored verbatim so the
// Admin state file keeps {env.NAME} rather than the resolved secret.
func TestBootstrapIsNotEnvExpanded(t *testing.T) {
	cfg := mustParse(t, withBlock(`"bootstrap": {
		"providers": { "m": { "name": "m", "type": "mock", "api_key": "{env.OPENAI_API_KEY}" } },
		"profiles": { "p": { "name": "p", "asr": "m", "llm": "m", "tts": "m" } },
		"settings": { "default_profile": "p" }
	}`))
	if strings.Contains(string(cfg.Bootstrap), "oa-secret") {
		t.Fatalf("bootstrap was expanded: %s", cfg.Bootstrap)
	}
	if !strings.Contains(string(cfg.Bootstrap), "{env.OPENAI_API_KEY}") {
		t.Fatalf("placeholder lost: %s", cfg.Bootstrap)
	}
	// An unset variable inside bootstrap is likewise not an error at load.
	cfg = mustParse(t, withBlock(`"bootstrap": { "providers": { "m": { "name": "m", "type": "mock", "api_key": "{env.NEVER_SET}" } } }`))
	if !strings.Contains(string(cfg.Bootstrap), "{env.NEVER_SET}") {
		t.Fatalf("placeholder lost: %s", cfg.Bootstrap)
	}
}

// Bootstrap is decoded and validated at startup so a bad seed fails fast.
func TestBootstrapValidatedAtLoad(t *testing.T) {
	cfg := mustParse(t, withBlock(`"bootstrap": { "profiles": { "p": { "name": "p", "asr": "nope", "llm": "nope", "tts": "nope" } } }`))
	wantFieldError(t, cfg.Validate(nil), "bootstrap.profiles.p.asr", `unknown provider instance "nope"`)

	cfg = mustParse(t, withBlock(`"bootstrap": { "profiles": { "p": { "name": "p", "voice": 3 } } }`))
	wantFieldError(t, cfg.Validate(nil), "bootstrap.profiles.p.voice", "expected string")
}

func TestExpandEnvString(t *testing.T) {
	got, err := ExpandEnv("providers.x.api_key", "{env.OPENAI_API_KEY}", lookup)
	if err != nil || got != "oa-secret" {
		t.Fatalf("ExpandEnv = %q, %v", got, err)
	}
	if got, err := ExpandEnv("providers.x.api_key", "literal", lookup); err != nil || got != "literal" {
		t.Fatalf("literal secret changed: %q, %v", got, err)
	}
	_, err = ExpandEnv("providers.x.api_key", "{env.NOT_SET_ANYWHERE}", lookup)
	wantFieldError(t, err, "providers.x.api_key", "NOT_SET_ANYWHERE")
}

func TestUnknownFieldNamesPath(t *testing.T) {
	src := strings.Replace(minimal, `"auth"`, `"listenn": ":1", "auth"`, 1)
	_, err := parse(t, src)
	wantFieldError(t, err, "listenn", "unknown field")

	_, err = parse(t, withBlock(`"limits": { "max_turns": 3 }`))
	wantFieldError(t, err, "limits.max_turns", "unknown field")
}

func TestWrongTypeNamesPath(t *testing.T) {
	_, err := parse(t, withBlock(`"limits": { "max_sessions": "many" }`))
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

func TestDurationDecoding(t *testing.T) {
	cfg := mustParse(t, withBlock(`"limits": { "asr_final_timeout": "1500ms" }`))
	if cfg.Limits.ASRFinalTimeout.Std() != 1500*time.Millisecond {
		t.Fatalf("got %s", cfg.Limits.ASRFinalTimeout)
	}
	for _, bad := range []string{`5`, `"5"`, `"soon"`} {
		if _, err := parse(t, withBlock(`"limits": { "asr_final_timeout": `+bad+` }`)); err == nil {
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
		{"same keys", `{ "auth": { "api_key": "k", "admin_api_key": "k" } }`, "auth.admin_api_key", "must differ"},
		{"admin listen empty", `{ "auth": { "api_key": "k", "admin_api_key": "a" }, "admin": { "listen": "" } }`, "admin.listen", "empty"},
		{"admin state empty", `{ "auth": { "api_key": "k", "admin_api_key": "a" }, "admin": { "state_file": "" } }`, "admin.state_file", "empty"},
		{"limit zero", withBlock(`"limits": { "input_audio_queue_frames": 0 }`), "limits.input_audio_queue_frames", "> 0"},
		{"max message zero", withBlock(`"limits": { "client_max_message_bytes": 0 }`), "limits.client_max_message_bytes", "> 0"},
		{"timeout zero", withBlock(`"limits": { "client_write_timeout": "0s" }`), "limits.client_write_timeout", "> 0"},
		{"log level", withBlock(`"observability": { "log_level": "trace" }`), "observability.log_level", "must be one of"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := mustParse(t, tc.src)
			wantFieldError(t, cfg.Validate(nil), tc.field, tc.msg)
		})
	}
}

func TestAdminKeyEnablesAdminWithDefaults(t *testing.T) {
	cfg := mustParse(t, `{ "auth": { "api_key": "{env.REALTIME_API_KEY}", "admin_api_key": "{env.ADMIN_API_KEY}" } }`)
	if err := cfg.Validate(nil); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if cfg.Auth.AdminAPIKey != "admin-secret" {
		t.Fatalf("admin key = %q", cfg.Auth.AdminAPIKey)
	}
}

func TestDefaultConfigFailsValidationWithoutSecrets(t *testing.T) {
	wantFieldError(t, DefaultConfig().Validate(nil), "auth.api_key", "empty")
}

// TestToolChoiceJSONRoundTrip: the GA union round-trips to the form it names,
// so the echoed session object matches what the client sent.
func TestToolChoiceJSONRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		json string
		want ToolChoice
	}{
		{`"auto"`, ToolChoice{Mode: ToolChoiceAuto}},
		{`"none"`, ToolChoice{Mode: ToolChoiceNone}},
		{`"required"`, ToolChoice{Mode: ToolChoiceRequired}},
		{`{"type":"function","name":"hangup"}`, ToolChoice{Mode: ToolChoiceFunction, Name: "hangup"}},
	} {
		var got ToolChoice
		if err := json.Unmarshal([]byte(tc.json), &got); err != nil {
			t.Fatalf("%s: %v", tc.json, err)
		}
		if got != tc.want {
			t.Errorf("%s decoded to %+v, want %+v", tc.json, got, tc.want)
		}
		back, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		if string(back) != tc.json {
			t.Errorf("%s re-encoded as %s", tc.json, back)
		}
	}
	// The zero value echoes the default rather than an empty string.
	back, _ := json.Marshal(ToolChoice{})
	if string(back) != `"auto"` {
		t.Errorf("zero value encoded as %s, want \"auto\"", back)
	}
	if err := json.Unmarshal([]byte(`42`), new(ToolChoice)); err == nil {
		t.Error("a number was accepted as tool_choice")
	}
}

// TestSessionDefaultsToolValidation: the dotted field paths the protocol
// layer reports back to the client.
func TestSessionDefaultsToolValidation(t *testing.T) {
	base := func() SessionDefaults {
		s := DefaultProfile().SessionDefaults()
		s.Audio.Input.TurnDetection.ApplyDefaults()
		return s
	}
	fn := func(name string) Tool { return Tool{Type: ToolTypeFunction, Name: name} }
	for _, tc := range []struct {
		name  string
		mut   func(*SessionDefaults)
		field string
	}{
		{"valid", func(s *SessionDefaults) { s.Tools = []Tool{fn("a")} }, ""},
		{"valid with schema", func(s *SessionDefaults) {
			t := fn("a")
			t.Parameters = json.RawMessage(`{"type":"object"}`)
			s.Tools = []Tool{t}
		}, ""},
		{"bad type", func(s *SessionDefaults) { s.Tools = []Tool{{Type: "mcp", Name: "a"}} }, "session.tools[0].type"},
		{"empty name", func(s *SessionDefaults) { s.Tools = []Tool{{Type: ToolTypeFunction}} }, "session.tools[0].name"},
		{"duplicate name", func(s *SessionDefaults) { s.Tools = []Tool{fn("a"), fn("a")} }, "session.tools[1].name"},
		{"array parameters", func(s *SessionDefaults) {
			t := fn("a")
			t.Parameters = json.RawMessage(`[]`)
			s.Tools = []Tool{t}
		}, "session.tools[0].parameters"},
		{"bad mode", func(s *SessionDefaults) { s.ToolChoice = ToolChoice{Mode: "maybe"} }, "session.tool_choice"},
		{"forced unknown function", func(s *SessionDefaults) {
			s.Tools = []Tool{fn("a")}
			s.ToolChoice = ToolChoice{Mode: ToolChoiceFunction, Name: "b"}
		}, "session.tool_choice.name"},
		{"forced known function", func(s *SessionDefaults) {
			s.Tools = []Tool{fn("a")}
			s.ToolChoice = ToolChoice{Mode: ToolChoiceFunction, Name: "a"}
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := base()
			tc.mut(&s)
			err := s.Validate("session")
			if tc.field == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			var fe *FieldError
			if !errors.As(err, &fe) {
				t.Fatalf("error = %v, want a FieldError", err)
			}
			if fe.Field != tc.field {
				t.Errorf("field = %q, want %q", fe.Field, tc.field)
			}
		})
	}
}

// TestProfileToolChoice: the default is "auto" and a profile may not force a
// function, because it declares no tools to force.
func TestProfileToolChoice(t *testing.T) {
	if got := DefaultProfile().ToolChoice; got != (ToolChoice{Mode: ToolChoiceAuto}) {
		t.Fatalf("default tool_choice = %+v", got)
	}
	if got := DefaultProfile().SessionDefaults().ToolChoice; got != (ToolChoice{Mode: ToolChoiceAuto}) {
		t.Fatalf("projected tool_choice = %+v", got)
	}
	p := DefaultProfile()
	p.Name = "p"
	p.TurnDetection.ApplyDefaults()
	p.ToolChoice = ToolChoice{Mode: ToolChoiceFunction, Name: "hangup"}
	err := p.validate("profiles.p", "p")
	var fe *FieldError
	if !errors.As(err, &fe) || fe.Field != "profiles.p.tool_choice" {
		t.Fatalf("error = %v, want profiles.p.tool_choice", err)
	}
	p.ToolChoice = ToolChoice{Mode: ToolChoiceNone}
	if err := p.validate("profiles.p", "p"); err != nil {
		t.Fatalf(`"none" rejected on a profile: %v`, err)
	}
}

// TestSessionDefaultsCloneTools: a snapshot must not share tool state with
// the session it was copied from.
func TestSessionDefaultsCloneTools(t *testing.T) {
	s := DefaultProfile().SessionDefaults()
	s.Tools = []Tool{{Type: ToolTypeFunction, Name: "a", Parameters: json.RawMessage(`{"type":"object"}`)}}
	c := s.Clone()
	c.Tools[0].Name = "b"
	c.Tools[0].Parameters[1] = 'X'
	if s.Tools[0].Name != "a" || string(s.Tools[0].Parameters) != `{"type":"object"}` {
		t.Fatalf("clone shares state: %+v", s.Tools[0])
	}
}
