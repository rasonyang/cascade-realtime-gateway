package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/rasonyang/cascade-realtime-gateway/internal/admin"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"

	// The mock provider registers itself for all three roles.
	_ "github.com/rasonyang/cascade-realtime-gateway/internal/provider/mock"
)

const adminKey = "admin-key"

// adminFixture wires a real Admin store to a real gateway, which is what the
// binary does: the runtime configuration decides whether /v1/realtime serves.
type adminFixture struct {
	t     *testing.T
	srv   *Server
	http  *httptest.Server
	admin http.Handler
	store *admin.Store
}

func newAdminFixture(t *testing.T, bootstrap string) *adminFixture {
	t.Helper()
	var boot json.RawMessage
	if bootstrap != "" {
		boot = json.RawMessage(bootstrap)
	}
	store, err := admin.Open(admin.Options{
		StateFile: filepath.Join(t.TempDir(), "state.json"),
		Bootstrap: boot,
		Known:     provider.Known,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("admin.Open: %v", err)
	}
	cfg := config.DefaultConfig()
	cfg.Auth.APIKey = apiKey
	cfg.Auth.AdminAPIKey = adminKey
	cfg.Limits.ClientWriteTimeout = config.Duration(300 * time.Millisecond)
	cfg.Limits.ASRFinalTimeout = config.Duration(time.Second)
	srv := New(Options{Config: cfg, Resolver: store, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()
		_ = srv.Shutdown(ctx)
		hs.Close()
	})
	return &adminFixture{t: t, srv: srv, http: hs, admin: store.Handler(adminKey), store: store}
}

func (f *adminFixture) do(method, path, token, body string) (int, []byte) {
	f.t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, r)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	f.admin.ServeHTTP(w, req)
	return w.Code, w.Body.Bytes()
}

func (f *adminFixture) dial() (*client, *http.Response, error) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	h := http.Header{"Authorization": []string{"Bearer " + apiKey}}
	url := "ws" + strings.TrimPrefix(f.http.URL, "http") + RealtimePath
	conn, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: h, Subprotocols: []string{Subprotocol}})
	if err != nil {
		return nil, resp, err
	}
	f.t.Cleanup(func() { conn.CloseNow() })
	return &client{t: f.t, conn: conn}, resp, nil
}

const mockProvider = `{"name":"m","type":"mock","api_key":"k","options":{"Utterances":["hello there"],"Tokens":["Hi."],"MsPerRune":10}}`

// With no default profile the gateway runs but refuses realtime connections;
// configuring one through the Admin API makes the next connection succeed.
func TestRealtimeRefusedUntilDefaultProfile(t *testing.T) {
	f := newAdminFixture(t, "")
	_, resp, err := f.dial()
	if err == nil {
		t.Fatal("dial should fail without a default profile")
	}
	if resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %v", resp)
	}

	if code, b := f.do("PUT", admin.BasePath+"/providers/m", adminKey, mockProvider); code != http.StatusOK {
		t.Fatalf("PUT provider = %d %s", code, b)
	}
	if code, b := f.do("PUT", admin.BasePath+"/profiles/p", adminKey,
		`{"name":"p","asr":"m","llm":"m","tts":"m"}`); code != http.StatusOK {
		t.Fatalf("PUT profile = %d %s", code, b)
	}
	// A profile alone is not enough: the default still has to be selected.
	if _, _, err := f.dial(); err == nil {
		t.Fatal("dial should still fail before default_profile is set")
	}
	if code, b := f.do("PUT", admin.BasePath+"/settings", adminKey, `{"default_profile":"p"}`); code != http.StatusOK {
		t.Fatalf("PUT settings = %d %s", code, b)
	}
	c, _, err := f.dial()
	if err != nil {
		t.Fatalf("dial after configuration: %v", err)
	}
	ev := c.readUntil("session.created")
	if ev["session"].(map[string]any)["instructions"] == nil {
		t.Fatalf("session object missing: %v", ev)
	}
}

// A session keeps the configuration it was started with; an Admin write in
// flight only affects connections opened afterwards.
func TestConfigSnapshotIsolatesSessions(t *testing.T) {
	boot := `{
	  "providers": { "m": ` + mockProvider + ` },
	  "profiles": { "p": { "name": "p", "asr": "m", "llm": "m", "tts": "m", "voice": "alloy" } },
	  "settings": { "default_profile": "p" }
	}`
	f := newAdminFixture(t, boot)

	old, _, err := f.dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if got := sessionVoice(t, old.readUntil("session.created")); got != "alloy" {
		t.Fatalf("old session voice = %q", got)
	}

	if code, b := f.do("PUT", admin.BasePath+"/profiles/p", adminKey,
		`{"name":"p","asr":"m","llm":"m","tts":"m","voice":"verse"}`); code != http.StatusOK {
		t.Fatalf("PUT profile = %d %s", code, b)
	}

	fresh, _, err := f.dial()
	if err != nil {
		t.Fatalf("dial after write: %v", err)
	}
	if got := sessionVoice(t, fresh.readUntil("session.created")); got != "verse" {
		t.Fatalf("new session voice = %q, want the updated profile", got)
	}
	// The live session is untouched: it still reports its own snapshot.
	old.send(`{"type":"session.update","event_id":"e1","session":{"type":"realtime"}}`)
	if got := sessionVoice(t, old.readUntil("session.updated")); got != "alloy" {
		t.Fatalf("live session voice changed to %q", got)
	}
}

func sessionVoice(t *testing.T, ev map[string]any) string {
	t.Helper()
	sess, ok := ev["session"].(map[string]any)
	if !ok {
		t.Fatalf("no session object: %v", ev)
	}
	audio, ok := sess["audio"].(map[string]any)
	if !ok {
		t.Fatalf("no audio object: %v", sess)
	}
	out, ok := audio["output"].(map[string]any)
	if !ok {
		t.Fatalf("no audio.output object: %v", audio)
	}
	v, _ := out["voice"].(string)
	return v
}

// The two credentials are separate: neither opens the other's door.
func TestAdminAndRealtimeKeysAreIsolated(t *testing.T) {
	boot := `{
	  "providers": { "m": ` + mockProvider + ` },
	  "profiles": { "p": { "name": "p", "asr": "m", "llm": "m", "tts": "m" } },
	  "settings": { "default_profile": "p" }
	}`
	f := newAdminFixture(t, boot)

	if code, body := f.do("GET", admin.BasePath+"/config", apiKey, ""); code != http.StatusUnauthorized {
		t.Fatalf("realtime key on the Admin API = %d %s, want 401", code, body)
	}

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	url := "ws" + strings.TrimPrefix(f.http.URL, "http") + RealtimePath
	h := http.Header{"Authorization": []string{"Bearer " + adminKey}}
	conn, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: h, Subprotocols: []string{Subprotocol}})
	if err == nil {
		conn.CloseNow()
		t.Fatal("the admin key must not open a realtime connection")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %v", resp)
	}
}
