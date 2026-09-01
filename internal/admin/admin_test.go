package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/observability"

	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"

	// The mock provider registers itself for all three roles; deepgram
	// registers for ASR only, which the role-mismatch test relies on.
	_ "github.com/rasonyang/cascade-realtime-gateway/internal/provider/deepgram"
	_ "github.com/rasonyang/cascade-realtime-gateway/internal/provider/mock"
)

const adminKey = "admin-test-key"

// seed is a complete runtime configuration built on the mock provider.
const seed = `{
  "providers": {
    "m": { "name": "m", "type": "mock", "api_key": "{env.MOCK_KEY}" }
  },
  "profiles": {
    "default": { "name": "default", "asr": "m", "llm": "m", "tts": "m", "voice": "alloy" }
  },
  "settings": { "default_profile": "default" }
}`

func env(name string) (string, bool) {
	if name == "MOCK_KEY" {
		return "resolved-secret", true
	}
	return "", false
}

func newStore(t *testing.T, bootstrap string) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	return openStore(t, path, bootstrap), path
}

func openStore(t *testing.T, path, bootstrap string) *Store {
	t.Helper()
	var boot json.RawMessage
	if bootstrap != "" {
		boot = json.RawMessage(bootstrap)
	}
	s, err := Open(Options{
		StateFile: path, Bootstrap: boot, Known: provider.Known, LookupEnv: env,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

// do issues one Admin request and returns the status and body.
func do(t *testing.T, s *Store, method, path, token, body string) (int, []byte) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, r)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	s.Handler(adminKey).ServeHTTP(w, req)
	return w.Code, w.Body.Bytes()
}

// wantParam asserts the response is an error naming field.
func wantParam(t *testing.T, body []byte, field string) {
	t.Helper()
	var e struct {
		Error struct {
			Param   string `json:"param"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("decode error body %s: %v", body, err)
	}
	if e.Error.Param != field {
		t.Fatalf("param = %q, want %q (message: %s)", e.Error.Param, field, e.Error.Message)
	}
}

func stateBytes(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read state file: %v", err)
	}
	return string(b)
}

// ---- bootstrap and persistence --------------------------------------------

func TestBootstrapAppliedOnceAndPersisted(t *testing.T) {
	s, path := newStore(t, seed)
	if _, err := s.Resolve(); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	// The state file keeps the placeholder, not the resolved secret.
	body := stateBytes(t, path)
	if !strings.Contains(body, "{env.MOCK_KEY}") || strings.Contains(body, "resolved-secret") {
		t.Fatalf("state file does not store the secret verbatim: %s", body)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("state file mode = %v (%v)", fi.Mode().Perm(), err)
	}

	// A later start reads the state file; the bootstrap is not applied again.
	if code, b := do(t, s, "PUT", BasePath+"/settings", adminKey, `{"default_profile":""}`); code != http.StatusOK {
		t.Fatalf("PUT settings = %d %s", code, b)
	}
	s2 := openStore(t, path, seed)
	if _, err := s2.Resolve(); err != ErrNoDefaultProfile {
		t.Fatalf("bootstrap was re-applied over the state file: %v", err)
	}
}

func TestRestartRoundTrip(t *testing.T) {
	s, path := newStore(t, seed)
	put := `{"name":"fast","asr":"m","llm":"m","tts":"m","voice":"verse","speed":1.25,
	         "temperature":0.2,"asr_language":"zh","instructions":"be quick"}`
	if code, b := do(t, s, "PUT", BasePath+"/profiles/fast", adminKey, put); code != http.StatusOK {
		t.Fatalf("PUT profile = %d %s", code, b)
	}
	if code, b := do(t, s, "PUT", BasePath+"/settings", adminKey, `{"default_profile":"fast"}`); code != http.StatusOK {
		t.Fatalf("PUT settings = %d %s", code, b)
	}
	before := s.Config()

	s2 := openStore(t, path, "")
	after := s2.Config()
	b1, _ := config.MarshalRuntimeConfig(before)
	b2, _ := config.MarshalRuntimeConfig(after)
	if string(b1) != string(b2) {
		t.Fatalf("restart changed the configuration:\n%s\nvs\n%s", b1, b2)
	}
	rt, err := s2.Resolve()
	if err != nil {
		t.Fatalf("Resolve after restart: %v", err)
	}
	if rt.Profile != "fast" || rt.Session.Audio.Output.Voice != "verse" || rt.ASRLanguage != "zh" ||
		rt.Temperature == nil || *rt.Temperature != 0.2 {
		t.Fatalf("resolved runtime wrong: %+v", rt)
	}
}

func TestNoStateFileAndNoBootstrapStartsEmpty(t *testing.T) {
	s, path := newStore(t, "")
	if _, err := s.Resolve(); err != ErrNoDefaultProfile {
		t.Fatalf("Resolve = %v, want ErrNoDefaultProfile", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("an empty start must not write a state file: %v", err)
	}
}

func TestCorruptStateFileFailsFast(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte(`{"providers": {"m": {"name":"m","type":"nope","api_key":"k"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{StateFile: path, Bootstrap: json.RawMessage(seed), Known: provider.Known, LookupEnv: env}); err == nil {
		t.Fatal("an invalid state file must fail startup, never fall back to bootstrap")
	}
}

// A failing persist must leave both the in-memory snapshot and the file alone.
func TestPersistFailureLeavesMemoryAndFileUnchanged(t *testing.T) {
	s, path := newStore(t, seed)
	before := stateBytes(t, path)

	// Make the directory read-only so the temporary file cannot be created.
	dir := filepath.Dir(path)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Skipf("cannot make the directory read-only: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	code, body := do(t, s, "PUT", BasePath+"/settings", adminKey, `{"default_profile":""}`)
	if code != http.StatusInternalServerError {
		t.Fatalf("PUT settings = %d %s, want 500", code, body)
	}
	if got := s.Config().Settings.DefaultProfile; got != "default" {
		t.Fatalf("memory changed despite a failed persist: %q", got)
	}
	if _, err := s.Resolve(); err != nil {
		t.Fatalf("live snapshot broken: %v", err)
	}
	if got := stateBytes(t, path); got != before {
		t.Fatalf("state file changed despite a failed persist:\n%s", got)
	}
}

// A rejected write must leave both memory and the file alone.
func TestValidationFailureLeavesMemoryAndFileUnchanged(t *testing.T) {
	s, path := newStore(t, seed)
	before := stateBytes(t, path)
	code, body := do(t, s, "PUT", BasePath+"/profiles/default", adminKey,
		`{"name":"default","asr":"m","llm":"m","tts":"m","speed":9}`)
	if code != http.StatusBadRequest {
		t.Fatalf("PUT = %d %s, want 400", code, body)
	}
	wantParam(t, body, "profiles.default.speed")
	if got := stateBytes(t, path); got != before {
		t.Fatalf("state file changed on a rejected write:\n%s", got)
	}
	if s.Config().Profiles["default"].Speed != 1.0 {
		t.Fatal("memory changed on a rejected write")
	}
}

// ---- validation over the API ----------------------------------------------

func TestValidationFieldPaths(t *testing.T) {
	s, _ := newStore(t, seed)
	cases := []struct {
		name, method, path, body, param string
	}{
		{"unknown provider type", "PUT", "/providers/x", `{"name":"x","type":"nope","api_key":"k"}`, "providers.x.type"},
		{"provider name mismatch", "PUT", "/providers/x", `{"name":"y","type":"mock","api_key":"k"}`, "providers.x.name"},
		{"provider unknown field", "PUT", "/providers/x", `{"name":"x","type":"mock","api_key":"k","url":"u"}`, "providers.x.url"},
		{"profile unknown instance", "PUT", "/profiles/p", `{"name":"p","asr":"nope","llm":"m","tts":"m"}`, "profiles.p.asr"},
		{"profile temperature", "PUT", "/profiles/p", `{"name":"p","asr":"m","llm":"m","tts":"m","temperature":9}`, "profiles.p.temperature"},
		{"profile transcription language", "PUT", "/profiles/p", `{"name":"p","asr":"m","llm":"m","tts":"m","transcription":{"language":"en"}}`, "profiles.p.transcription.language"},
		{"settings unknown profile", "PUT", "/settings", `{"default_profile":"nope"}`, "settings.default_profile"},
		{"settings unknown field", "PUT", "/settings", `{"default":"x"}`, "settings.default"},
		{"config bad profile", "PUT", "/config", `{"profiles":{"p":{"name":"p","speed":3}}}`, "profiles.p.speed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, body := do(t, s, tc.method, BasePath+tc.path, adminKey, tc.body)
			if code != http.StatusBadRequest {
				t.Fatalf("status = %d %s, want 400", code, body)
			}
			wantParam(t, body, tc.param)
		})
	}
}

// deepgram is registered for ASR only: naming it as the LLM must be rejected
// with the profile's field path, not with "unknown provider type".
func TestProviderRoleMismatch(t *testing.T) {
	s, _ := newStore(t, seed)
	if code, body := do(t, s, "PUT", BasePath+"/providers/dg", adminKey,
		`{"name":"dg","type":"deepgram","api_key":"k"}`); code != http.StatusOK {
		t.Fatalf("PUT provider = %d %s", code, body)
	}
	code, body := do(t, s, "PUT", BasePath+"/profiles/mixed", adminKey,
		`{"name":"mixed","asr":"dg","llm":"dg","tts":"m"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("PUT profile = %d %s, want 400", code, body)
	}
	wantParam(t, body, "profiles.mixed.llm")
	if !strings.Contains(string(body), "does not support llm") {
		t.Fatalf("message should name the role: %s", body)
	}
}

func TestReferentialDelete(t *testing.T) {
	s, _ := newStore(t, seed)
	code, body := do(t, s, "DELETE", BasePath+"/providers/m", adminKey, "")
	if code != http.StatusConflict {
		t.Fatalf("DELETE referenced provider = %d %s, want 409", code, body)
	}
	if !strings.Contains(string(body), "default") {
		t.Fatalf("409 should name the referencing profile: %s", body)
	}
	code, body = do(t, s, "DELETE", BasePath+"/profiles/default", adminKey, "")
	if code != http.StatusConflict {
		t.Fatalf("DELETE default profile = %d %s, want 409", code, body)
	}
	if code, body := do(t, s, "DELETE", BasePath+"/providers/nope", adminKey, ""); code != http.StatusNotFound {
		t.Fatalf("DELETE missing provider = %d %s, want 404", code, body)
	}

	// Clearing the reference first makes both deletions succeed.
	if code, b := do(t, s, "PUT", BasePath+"/settings", adminKey, `{"default_profile":""}`); code != http.StatusOK {
		t.Fatalf("PUT settings = %d %s", code, b)
	}
	if code, b := do(t, s, "DELETE", BasePath+"/profiles/default", adminKey, ""); code != http.StatusNoContent {
		t.Fatalf("DELETE profile = %d %s", code, b)
	}
	if code, b := do(t, s, "DELETE", BasePath+"/providers/m", adminKey, ""); code != http.StatusNoContent {
		t.Fatalf("DELETE provider = %d %s", code, b)
	}
	if len(s.Config().Providers) != 0 || len(s.Config().Profiles) != 0 {
		t.Fatalf("resources survived deletion: %+v", s.Config())
	}
}

// ---- secrets ---------------------------------------------------------------

func TestSecretsAreMaskedOnReadAndKeptOnWrite(t *testing.T) {
	s, path := newStore(t, seed)
	for _, p := range []string{"/config", "/providers", "/providers/m"} {
		code, body := do(t, s, "GET", BasePath+p, adminKey, "")
		if code != http.StatusOK {
			t.Fatalf("GET %s = %d %s", p, code, body)
		}
		if strings.Contains(string(body), "{env.MOCK_KEY}") || strings.Contains(string(body), "resolved-secret") {
			t.Fatalf("GET %s leaked the secret: %s", p, body)
		}
		if !strings.Contains(string(body), SecretMask) {
			t.Fatalf("GET %s did not mask the secret: %s", p, body)
		}
	}

	// Writing the mask back keeps the stored secret.
	code, body := do(t, s, "PUT", BasePath+"/providers/m", adminKey,
		`{"name":"m","type":"mock","api_key":"***","options":{"MsPerRune":5}}`)
	if code != http.StatusOK {
		t.Fatalf("PUT = %d %s", code, body)
	}
	if got := s.Config().Providers["m"].APIKey; got != "{env.MOCK_KEY}" {
		t.Fatalf("secret not preserved: %q", got)
	}
	if !strings.Contains(stateBytes(t, path), "{env.MOCK_KEY}") {
		t.Fatalf("state file lost the secret: %s", stateBytes(t, path))
	}

	// The mask on an instance that does not exist has nothing to keep.
	code, body = do(t, s, "PUT", BasePath+"/providers/new", adminKey, `{"name":"new","type":"mock","api_key":"***"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("PUT new with mask = %d %s, want 400", code, body)
	}
	wantParam(t, body, "providers.new.api_key")

	// A literal secret is stored as written.
	if code, body := do(t, s, "PUT", BasePath+"/providers/lit", adminKey,
		`{"name":"lit","type":"mock","api_key":"sk-literal"}`); code != http.StatusOK {
		t.Fatalf("PUT literal = %d %s", code, body)
	}
	if got := s.Config().Providers["lit"].APIKey; got != "sk-literal" {
		t.Fatalf("literal secret = %q", got)
	}
}

func TestWholeConfigPutKeepsMaskedSecrets(t *testing.T) {
	s, _ := newStore(t, seed)
	_, body := do(t, s, "GET", BasePath+"/config", adminKey, "")
	// Feed the redacted document straight back: nothing should change.
	if code, b := do(t, s, "PUT", BasePath+"/config", adminKey, string(body)); code != http.StatusOK {
		t.Fatalf("PUT config = %d %s", code, b)
	}
	if got := s.Config().Providers["m"].APIKey; got != "{env.MOCK_KEY}" {
		t.Fatalf("secret not preserved through a whole-config round trip: %q", got)
	}
}

func TestAdminWriteLoggingOmitsBody(t *testing.T) {
	var buf bytes.Buffer
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(Options{StateFile: path, Bootstrap: json.RawMessage(seed), Known: provider.Known,
		LookupEnv: env, Logger: slog.New(slog.NewTextHandler(&buf, nil))})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("PUT", BasePath+"/providers/m",
		strings.NewReader(`{"name":"m","type":"mock","api_key":"sk-super-secret"}`))
	req.Header.Set("Authorization", "Bearer "+adminKey)
	req.Header.Set("X-Request-ID", "req-42")
	w := httptest.NewRecorder()
	s.Handler(adminKey).ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT = %d %s", w.Code, w.Body)
	}
	logged := buf.String()
	if strings.Contains(logged, "sk-super-secret") {
		t.Fatalf("the request body reached the log: %s", logged)
	}
	for _, want := range []string{"PUT", BasePath + "/providers/m", "req-42", "provider m"} {
		if !strings.Contains(logged, want) {
			t.Fatalf("log does not mention %q: %s", want, logged)
		}
	}
}

// ---- auth ------------------------------------------------------------------

func TestAdminAuth(t *testing.T) {
	s, _ := newStore(t, seed)
	for _, tc := range []struct{ name, token string }{
		{"no token", ""},
		{"realtime key", "realtime-key"},
		{"wrong key", adminKey + "x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if code, body := do(t, s, "GET", BasePath+"/config", tc.token, ""); code != http.StatusUnauthorized {
				t.Fatalf("GET = %d %s, want 401", code, body)
			}
			if code, body := do(t, s, "PUT", BasePath+"/settings", tc.token, `{"default_profile":""}`); code != http.StatusUnauthorized {
				t.Fatalf("PUT = %d %s, want 401", code, body)
			}
		})
	}
	if code, _ := do(t, s, "GET", BasePath+"/config", adminKey, ""); code != http.StatusOK {
		t.Fatal("the admin key must be accepted")
	}
}

// ---- reads -----------------------------------------------------------------

func TestReadsAndNotFound(t *testing.T) {
	s, _ := newStore(t, seed)
	code, body := do(t, s, "GET", BasePath+"/profiles/default", adminKey, "")
	if code != http.StatusOK {
		t.Fatalf("GET profile = %d %s", code, body)
	}
	var p config.Profile
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("decode profile: %v", err)
	}
	if p.Name != "default" || p.ASR != "m" || p.Voice != "alloy" {
		t.Fatalf("profile = %+v", p)
	}
	code, body = do(t, s, "GET", BasePath+"/settings", adminKey, "")
	if code != http.StatusOK || !strings.Contains(string(body), `"default_profile": "default"`) {
		t.Fatalf("GET settings = %d %s", code, body)
	}
	for _, p := range []string{"/providers/nope", "/profiles/nope"} {
		if code, body := do(t, s, "GET", BasePath+p, adminKey, ""); code != http.StatusNotFound {
			t.Fatalf("GET %s = %d %s, want 404", p, code, body)
		}
	}
}

// ---- metrics ---------------------------------------------------------------

func TestAdminWriteMetricByResult(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	tel := observability.New(tracenoop.NewTracerProvider(), sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(Options{StateFile: path, Bootstrap: json.RawMessage(seed), Known: provider.Known,
		LookupEnv: env, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Telemetry: tel})
	if err != nil {
		t.Fatal(err)
	}
	// one ok, one invalid, one conflict, one not_found
	do(t, s, "PUT", BasePath+"/profiles/extra", adminKey, `{"name":"extra","asr":"m","llm":"m","tts":"m"}`)
	do(t, s, "PUT", BasePath+"/profiles/extra", adminKey, `{"name":"extra","asr":"m","llm":"m","tts":"m","speed":9}`)
	do(t, s, "DELETE", BasePath+"/providers/m", adminKey, "")
	do(t, s, "DELETE", BasePath+"/profiles/nope", adminKey, "")

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "cascade.admin.config_writes" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("unexpected data type %T", m.Data)
			}
			for _, dp := range sum.DataPoints {
				v, _ := dp.Attributes.Value(observability.KeyResult)
				counts[v.AsString()] += dp.Value
			}
		}
	}
	for _, want := range []string{"ok", "invalid", "conflict", "not_found"} {
		if counts[want] != 1 {
			t.Fatalf("result %q = %d, want 1 (all: %v)", want, counts[want], counts)
		}
	}
}
