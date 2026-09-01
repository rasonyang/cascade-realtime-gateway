package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/rasonyang/cascade-realtime-gateway/internal/admin"
	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider/mock"
)

const apiKey = "test-key"

const waitTimeout = 5 * time.Second

type fixture struct {
	t    *testing.T
	srv  *Server
	http *httptest.Server
	llm  *mock.LLM
}

// stubResolver serves one fixed runtime configuration. The Admin store is
// exercised in internal/admin and in admin_test.go; these tests need direct
// handles on the mock providers instead.
type stubResolver struct{ rt admin.Runtime }

func (s stubResolver) Resolve() (admin.Runtime, error) { return s.rt, nil }

func newFixture(t *testing.T, llmScript mock.LLMScript, tweak func(*config.Config, *config.Profile)) *fixture {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Auth.APIKey = apiKey
	cfg.Limits.ClientWriteTimeout = config.Duration(300 * time.Millisecond)
	cfg.Limits.ASRFinalTimeout = config.Duration(time.Second)
	prof := config.DefaultProfile()
	prof.Name, prof.ASR, prof.LLM, prof.TTS = "test", "asr", "llm", "tts"
	if tweak != nil {
		tweak(cfg, prof)
	}
	llm := mock.NewLLM(llmScript)
	rt := admin.Runtime{
		Profile: prof.Name, ASRName: prof.ASR, LLMName: prof.LLM, TTSName: prof.TTS,
		Session:     prof.SessionDefaults(),
		Temperature: prof.Temperature,
		ASR:         mock.NewASR(mock.ASRScript{Utterances: []string{"hello there"}}),
		LLM:         llm,
		TTS:         mock.NewTTS(mock.TTSScript{MsPerRune: 10}),
	}
	srv := New(Options{Config: cfg, Resolver: stubResolver{rt}})
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
		hs.Close()
	})
	return &fixture{t: t, srv: srv, http: hs, llm: llm}
}

func defaultLLM() mock.LLMScript {
	return mock.LLMScript{Tokens: []string{"Hello", " world.", " Second", " sentence", " here."}}
}

func (f *fixture) wsURL(query string) string {
	return "ws" + strings.TrimPrefix(f.http.URL, "http") + RealtimePath + query
}

type client struct {
	t       *testing.T
	conn    *websocket.Conn
	resp    *http.Response
	timeout time.Duration // per-read timeout; zero means waitTimeout
}

func (f *fixture) dial(query, token string) (*client, *http.Response, error) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	h := http.Header{}
	if token != "" {
		h.Set("Authorization", "Bearer "+token)
	}
	conn, resp, err := websocket.Dial(ctx, f.wsURL(query), &websocket.DialOptions{HTTPHeader: h, Subprotocols: []string{Subprotocol}})
	if err != nil {
		return nil, resp, err
	}
	conn.SetReadLimit(1 << 24)
	return &client{t: f.t, conn: conn, resp: resp}, resp, nil
}

func (f *fixture) connect() *client {
	f.t.Helper()
	c, _, err := f.dial("", apiKey)
	if err != nil {
		f.t.Fatalf("dial: %v", err)
	}
	f.t.Cleanup(func() { c.conn.CloseNow() })
	return c
}

func (c *client) send(js string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if err := c.conn.Write(ctx, websocket.MessageText, []byte(js)); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

// read returns the next frame; ok=false on close, with the close error.
func (c *client) read() (map[string]any, error) {
	d := c.timeout
	if d == 0 {
		d = waitTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	_, data, err := c.conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	var f map[string]any
	if err := json.Unmarshal(data, &f); err != nil {
		c.t.Fatalf("bad frame %s: %v", data, err)
	}
	return f, nil
}

func (c *client) readUntil(typ string) map[string]any {
	c.t.Helper()
	for {
		f, err := c.read()
		if err != nil {
			c.t.Fatalf("connection ended while waiting for %s: %v", typ, err)
		}
		if f["type"] == typ {
			return f
		}
	}
}

func (c *client) expectClose() (websocket.StatusCode, string) {
	c.t.Helper()
	for {
		_, err := c.read()
		if err == nil {
			continue
		}
		var ce websocket.CloseError
		if !asCloseError(err, &ce) {
			c.t.Fatalf("expected a close frame, got %v", err)
		}
		return ce.Code, ce.Reason
	}
}

func asCloseError(err error, ce *websocket.CloseError) bool {
	code := websocket.CloseStatus(err)
	if code == -1 {
		return false
	}
	ce.Code = code
	if e, ok := err.(websocket.CloseError); ok {
		ce.Reason = e.Reason
	} else {
		var inner websocket.CloseError
		if errorsAs(err, &inner) {
			ce.Reason = inner.Reason
		}
	}
	return true
}

func errorsAs(err error, target *websocket.CloseError) bool {
	for err != nil {
		if e, ok := err.(websocket.CloseError); ok {
			*target = e
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func eventually(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", desc)
}

// ---- tests ---------------------------------------------------------------------

func TestAuthAndUpgrade(t *testing.T) {
	f := newFixture(t, defaultLLM(), nil)
	for _, token := range []string{"", "wrong"} {
		_, resp, err := f.dial("", token)
		if err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("token %q: err=%v status=%v", token, err, resp)
		}
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), "invalid_request_error") || resp.Header.Get("WWW-Authenticate") == "" {
			t.Fatalf("401 body/header: %s %v", body, resp.Header)
		}
	}
	resp, err := http.Post(f.http.URL+RealtimePath, "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST: %v %v", err, resp)
	}
	c, resp, err := f.dial("?model=gpt-realtime", apiKey)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.conn.CloseNow()
	if resp.StatusCode != http.StatusSwitchingProtocols || c.conn.Subprotocol() != Subprotocol {
		t.Fatalf("upgrade: %v subprotocol=%q", resp.StatusCode, c.conn.Subprotocol())
	}
	created := c.readUntil("session.created")
	sess := created["session"].(map[string]any)
	if sess["model"] != "gpt-realtime" || !strings.HasPrefix(sess["id"].(string), "sess_") || sess["type"] != "realtime" {
		t.Fatalf("session.created = %v", sess)
	}
	c.readUntil("conversation.created")
	eventually(t, "active=1", func() bool { return f.srv.ActiveSessions() == 1 })
}

func TestTextConversationOverWebSocket(t *testing.T) {
	f := newFixture(t, defaultLLM(), nil)
	c := f.connect()
	c.readUntil("conversation.created")
	c.send(`{"type":"session.update","event_id":"u1","session":{"type":"realtime","output_modalities":["text"],"instructions":"Be terse."}}`)
	u := c.readUntil("session.updated")
	if u["session"].(map[string]any)["instructions"] != "Be terse." {
		t.Fatalf("session.updated = %v", u)
	}
	c.send(`{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"Hello"}]}}`)
	c.readUntil("conversation.item.done")
	c.send(`{"type":"response.create","event_id":"r1"}`)
	var text strings.Builder
	for {
		fr, err := c.read()
		if err != nil {
			t.Fatal(err)
		}
		switch fr["type"] {
		case "response.output_text.delta":
			text.WriteString(fr["delta"].(string))
		case "response.done":
			r := fr["response"].(map[string]any)
			if r["status"] != "completed" || text.String() != "Hello world. Second sentence here." {
				t.Fatalf("done = %v text=%q", r, text.String())
			}
			c.send(`{"type":"nope","event_id":"bad"}`)
			e := c.readUntil("error")
			if e["error"].(map[string]any)["event_id"] != "bad" {
				t.Fatalf("error = %v", e)
			}
			return
		}
	}
}

func TestBinaryFrameRejected(t *testing.T) {
	f := newFixture(t, defaultLLM(), nil)
	c := f.connect()
	c.readUntil("conversation.created")
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if err := c.conn.Write(ctx, websocket.MessageBinary, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	e := c.readUntil("error")
	if e["error"].(map[string]any)["code"] != "invalid_event" {
		t.Fatalf("error = %v", e)
	}
}

func TestMaxSessions(t *testing.T) {
	f := newFixture(t, defaultLLM(), func(c *config.Config, _ *config.Profile) { c.Limits.MaxSessions = 1 })
	c1 := f.connect()
	c1.readUntil("conversation.created")
	_, resp, err := f.dial("", apiKey)
	if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second connection: err=%v resp=%v", err, resp)
	}
	c1.conn.Close(websocket.StatusNormalClosure, "")
	eventually(t, "slot released", func() bool { return f.srv.ActiveSessions() == 0 })
	c2 := f.connect()
	c2.readUntil("session.created")
}

func TestSessionTimeoutCloses(t *testing.T) {
	f := newFixture(t, defaultLLM(), func(c *config.Config, _ *config.Profile) {
		c.Limits.SessionTimeout = config.Duration(200 * time.Millisecond)
	})
	c := f.connect()
	c.readUntil("conversation.created")
	code, reason := c.expectClose()
	if code != websocket.StatusNormalClosure || reason != "session_timeout" {
		t.Fatalf("close = %d %q", code, reason)
	}
	eventually(t, "session gone", func() bool { return f.srv.ActiveSessions() == 0 })
}

func TestFatalErrorClosesWith1011(t *testing.T) {
	f := newFixture(t, defaultLLM(), func(c *config.Config, p *config.Profile) {
		c.Limits.InputAudioBufferMaxMs = 40
		p.TurnDetection = nil
	})
	c := f.connect()
	c.readUntil("conversation.created")
	pcm := base64.StdEncoding.EncodeToString(make([]byte, audio.MsToBytes(60)))
	c.send(fmt.Sprintf(`{"type":"input_audio_buffer.append","audio":%q}`, pcm))
	e := c.readUntil("error")
	if e["error"].(map[string]any)["code"] != "input_audio_buffer_overflow" {
		t.Fatalf("error = %v", e)
	}
	code, reason := c.expectClose()
	if code != websocket.StatusInternalError || reason != "input_audio_buffer_overflow" {
		t.Fatalf("close = %d %q", code, reason)
	}
}

func TestReadLimitCloses(t *testing.T) {
	f := newFixture(t, defaultLLM(), func(c *config.Config, _ *config.Profile) { c.Limits.ClientMaxMessageBytes = 256 })
	c := f.connect()
	c.readUntil("conversation.created")
	c.send(`{"type":"input_audio_buffer.append","audio":"` + strings.Repeat("A", 1024) + `"}`)
	code, _ := c.expectClose()
	if code != websocket.StatusMessageTooBig {
		t.Fatalf("close = %d", code)
	}
	eventually(t, "session gone", func() bool { return f.srv.ActiveSessions() == 0 })
}

func TestSlowClientIsDisconnected(t *testing.T) {
	big := mock.LLMScript{Tokens: make([]string, 4000)}
	for i := range big.Tokens {
		big.Tokens[i] = strings.Repeat("x", 250)
	}
	f := newFixture(t, big, func(c *config.Config, _ *config.Profile) { c.Limits.OutputEventQueue = 8 })
	c := f.connect()
	c.readUntil("conversation.created")
	c.send(`{"type":"session.update","session":{"type":"realtime","output_modalities":["text"]}}`)
	c.send(`{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"go"}]}}`)
	c.send(`{"type":"response.create"}`)
	// Never read: the server's per-write timeout must drop us.
	eventually(t, "slow client disconnected", func() bool { return f.srv.ActiveSessions() == 0 })
}

func TestShutdownClosesClientsGoingAway(t *testing.T) {
	f := newFixture(t, defaultLLM(), nil)
	c := f.connect()
	c.readUntil("conversation.created")
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- f.srv.Shutdown(ctx) }()
	// A live client answers the close handshake; Shutdown completes then.
	code, reason := c.expectClose()
	if code != websocket.StatusGoingAway || reason != "shutting_down" {
		t.Fatalf("close = %d %q", code, reason)
	}
	if err := <-shutdownErr; err != nil {
		t.Fatal(err)
	}
	if _, resp, err := f.dial("", apiKey); err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("connect after shutdown: %v %v", err, resp)
	}
}

func TestNoConnectionLeak(t *testing.T) {
	f := newFixture(t, defaultLLM(), nil)
	baseline := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		c, _, err := f.dial("", apiKey)
		if err != nil {
			t.Fatal(err)
		}
		c.readUntil("conversation.created")
		if i%2 == 0 {
			c.conn.Close(websocket.StatusNormalClosure, "bye")
		} else {
			c.conn.CloseNow() // abrupt
		}
	}
	eventually(t, "active=0", func() bool { return f.srv.ActiveSessions() == 0 })
	eventually(t, "goroutines back to baseline", func() bool { return runtime.NumGoroutine() <= baseline+2 })
}

// TestConcurrentInterrupts runs 100 sessions, each interrupting a blocked
// response 10 times (1000 interrupts), and checks goroutines return to the
// baseline afterwards.
func TestConcurrentInterrupts(t *testing.T) {
	sc := mock.LLMScript{Tokens: []string{"Hello", " again"}, Block: true, BlockAfter: 1}
	f := newFixture(t, sc, func(_ *config.Config, p *config.Profile) { p.TurnDetection = nil })
	baseline := runtime.NumGoroutine()
	const sessions, rounds = 100, 10
	var wg sync.WaitGroup
	errs := make(chan error, sessions)
	for i := 0; i < sessions; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, _, err := f.dial("", apiKey)
			if err != nil {
				errs <- err
				return
			}
			defer c.conn.CloseNow()
			c.readUntil("conversation.created")
			c.send(`{"type":"session.update","session":{"type":"realtime","output_modalities":["text"]}}`)
			c.readUntil("session.updated")
			c.send(`{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}}`)
			c.readUntil("conversation.item.done")
			for r := 0; r < rounds; r++ {
				c.send(`{"type":"response.create"}`)
				c.readUntil("response.output_text.delta")
				c.send(`{"type":"response.cancel"}`)
				done := c.readUntil("response.done")
				if done["response"].(map[string]any)["status"] != "cancelled" {
					errs <- fmt.Errorf("round %d: %v", r, done)
					return
				}
			}
			c.conn.Close(websocket.StatusNormalClosure, "")
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	eventually(t, "active=0", func() bool { return f.srv.ActiveSessions() == 0 })
	eventually(t, "goroutines back to baseline", func() bool { return runtime.NumGoroutine() <= baseline+2 })
	if f.llm.Active() != 0 {
		t.Fatalf("%d LLM generations still running", f.llm.Active())
	}
}
