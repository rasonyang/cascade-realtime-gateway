package openai

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// fakeRealtime is a minimal Realtime server: it records what the adapter
// sends and answers every response.create with two audio deltas filled with
// the segment number, then response.done (or whatever fail dictates).
type fakeRealtime struct {
	t *testing.T

	mu        sync.Mutex
	auth      string
	query     string
	update    map[string]any
	creates   []map[string]any
	inflight  int
	maxFlight int
	closed    chan struct{} // closed when the server side sees the socket go away

	// sessionError, if set, answers session.update with an error event.
	sessionError bool
	// failSegment (1-based) answers that response.create with an error event.
	failSegment int
	// stallSegment (1-based) sends one delta for that segment and then hangs.
	stallSegment int
	// doneStatus overrides the status of response.done.
	doneStatus string
}

func (f *fakeRealtime) server() *httptest.Server {
	f.closed = make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.auth, f.query = r.Header.Get("Authorization"), r.URL.RawQuery
		f.mu.Unlock()
		if r.URL.Path != "/v1/realtime" {
			f.t.Errorf("path = %s", r.URL.Path)
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer close(f.closed)
		defer c.CloseNow()
		ctx := r.Context()
		send := func(v any) bool {
			b, _ := json.Marshal(v)
			return c.Write(ctx, websocket.MessageText, b) == nil
		}
		send(map[string]any{"type": "session.created"})
		for {
			_, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			var m map[string]any
			json.Unmarshal(data, &m)
			switch m["type"] {
			case "session.update":
				f.mu.Lock()
				f.update = m
				f.mu.Unlock()
				if f.sessionError {
					send(map[string]any{"type": "error", "error": map[string]any{"type": "invalid_request_error", "code": "invalid_value", "message": "bad voice"}})
					continue
				}
				send(map[string]any{"type": "session.updated"})
			case "response.create":
				f.mu.Lock()
				f.creates = append(f.creates, m)
				n := len(f.creates)
				f.inflight++
				f.maxFlight = max(f.maxFlight, f.inflight)
				f.mu.Unlock()
				ok := f.respond(func() { c.Read(ctx) }, send, n)
				f.mu.Lock()
				f.inflight--
				f.mu.Unlock()
				if !ok {
					return
				}
			}
		}
	}))
	return srv
}

func (f *fakeRealtime) respond(hold func(), send func(any) bool, n int) bool {
	delta := func(size int) any {
		pcm := make([]byte, size)
		for i := range pcm {
			pcm[i] = byte(n)
		}
		return map[string]any{"type": "response.output_audio.delta", "delta": base64.StdEncoding.EncodeToString(pcm)}
	}
	send(map[string]any{"type": "response.created"})
	if n == f.failSegment {
		return send(map[string]any{"type": "error", "error": map[string]any{"type": "server_error", "code": "boom", "message": "oops"}})
	}
	// A transcript event the adapter must ignore, and an odd-sized first
	// delta so alignment carry across deltas is exercised.
	send(map[string]any{"type": "response.output_audio_transcript.delta", "delta": "x"})
	if !send(delta(1001)) {
		return false
	}
	if n == f.stallSegment {
		hold() // returns once the client closes the socket
		return false
	}
	if !send(delta(1001)) {
		return false
	}
	status := f.doneStatus
	if status == "" {
		status = "completed"
	}
	return send(map[string]any{"type": "response.done", "response": map[string]any{"status": status,
		"status_details": map[string]any{"reason": "max_output_tokens"}}})
}

func (f *fakeRealtime) snapshot() (update map[string]any, creates []map[string]any, maxFlight int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.update, append([]map[string]any(nil), f.creates...), f.maxFlight
}

func newRealtimeTTS(srv *httptest.Server) *TTS {
	return NewTTS("sk-rt", TTSOptions{httpOptions: opts(srv.URL + "/v1"), Model: "gpt-realtime-2.1-mini"})
}

func TestRealtimeTTSDispatch(t *testing.T) {
	if got := NewTTS("k", TTSOptions{}).opts.Model; got != "gpt-realtime-2.1-mini" {
		t.Fatalf("default model = %q", got)
	}
	if !NewTTS("k", TTSOptions{}).Capabilities().IncrementalText {
		t.Fatal("realtime path must report IncrementalText")
	}
	if NewTTS("k", TTSOptions{Model: "tts-1"}).Capabilities().IncrementalText {
		t.Fatal("speech path must not report IncrementalText")
	}
	if got := realtimeURL("https://api.openai.com/v1", "gpt-realtime-2.1-mini"); got != "wss://api.openai.com/v1/realtime?model=gpt-realtime-2.1-mini" {
		t.Fatalf("url = %s", got)
	}
	if got := realtimeURL("http://127.0.0.1:1/v1", "m"); got != "ws://127.0.0.1:1/v1/realtime?model=m" {
		t.Fatalf("url = %s", got)
	}
}

func TestRealtimeTTSSegmentsAreSerializedAndOrdered(t *testing.T) {
	f := &fakeRealtime{t: t}
	srv := f.server()
	defer srv.Close()
	s, err := newRealtimeTTS(srv).Synthesize(context.Background(), provider.TTSConfig{Voice: "coral", Speed: 1.25})
	if err != nil {
		t.Fatal(err)
	}
	// All text is queued before the handshake can have finished.
	for _, seg := range []string{"First.", "Second.", "  ", "Third."} {
		if err := s.WriteText(seg); err != nil {
			t.Fatal(err)
		}
	}
	s.EndInput()
	var pcm []byte
	var firstOf []byte
	for {
		c, err := s.ReadAudio()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(c.PCM)%2 != 0 {
			t.Fatalf("unaligned chunk of %d bytes", len(c.PCM))
		}
		pcm = append(pcm, c.PCM...)
		if len(firstOf) == 0 || firstOf[len(firstOf)-1] != c.PCM[0] {
			firstOf = append(firstOf, c.PCM[0])
		}
	}
	// 3 segments x 2002 bytes in, in segment order; the silence gate works in
	// whole 20 ms frames and drops the 82-byte sub-frame remainder of each.
	if len(pcm) != 3*2*gateFrame || string(firstOf) != "\x01\x02\x03" {
		t.Fatalf("pcm len=%d order=%v", len(pcm), firstOf)
	}
	if _, err := s.ReadAudio(); !errors.Is(err, io.EOF) {
		t.Fatalf("read after EOF: %v", err)
	}
	if err := s.WriteText("late"); err == nil {
		t.Fatal("write after EndInput must fail")
	}
	s.Close()
	s.Close() // idempotent

	update, creates, maxFlight := f.snapshot()
	if f.auth != "Bearer sk-rt" || f.query != "model=gpt-realtime-2.1-mini" {
		t.Fatalf("auth=%q query=%q", f.auth, f.query)
	}
	sess, _ := update["session"].(map[string]any)
	out := sess["audio"].(map[string]any)["output"].(map[string]any)
	format := out["format"].(map[string]any)
	if sess["type"] != "realtime" || out["voice"] != "coral" || out["speed"] != 1.25 ||
		format["type"] != "audio/pcm" || format["rate"] != 24000.0 {
		t.Fatalf("session = %v", sess)
	}
	if mods, _ := sess["output_modalities"].([]any); len(mods) != 1 || mods[0] != "audio" {
		t.Fatalf("output_modalities = %v", sess["output_modalities"])
	}
	if sess["reasoning"].(map[string]any)["effort"] != "none" {
		t.Fatalf("reasoning = %v", sess["reasoning"])
	}
	if instr, _ := sess["instructions"].(string); !strings.Contains(instr, "text-to-speech engine") {
		t.Fatalf("instructions = %q", instr)
	}
	if len(creates) != 3 || maxFlight != 1 {
		t.Fatalf("creates=%d maxFlight=%d", len(creates), maxFlight)
	}
	r := creates[0]["response"].(map[string]any)
	if r["conversation"] != "none" || len(r["input"].([]any)) != 0 ||
		r["instructions"] != "Say exactly the following, verbatim, and nothing else:\nFirst." {
		t.Fatalf("response = %v", r)
	}
	select {
	case <-f.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("socket not closed after the stream finished")
	}
}

func TestRealtimeTTSDefaultVoiceAndNoSpeed(t *testing.T) {
	f := &fakeRealtime{t: t}
	srv := f.server()
	defer srv.Close()
	s, _ := newRealtimeTTS(srv).Synthesize(context.Background(), provider.TTSConfig{Speed: 1})
	s.EndInput()
	if _, err := readAll(t, s); err != nil {
		t.Fatal(err)
	}
	update, creates, _ := f.snapshot()
	out := update["session"].(map[string]any)["audio"].(map[string]any)["output"].(map[string]any)
	if out["voice"] != "marin" {
		t.Fatalf("voice = %v", out["voice"])
	}
	if _, ok := out["speed"]; ok {
		t.Fatal("speed 1 must be omitted")
	}
	if len(creates) != 0 {
		t.Fatalf("no text, no responses; got %d", len(creates))
	}
}

func TestRealtimeTTSErrors(t *testing.T) {
	kindOf := func(err error) provider.ErrorKind {
		t.Helper()
		var pe *provider.Error
		if !errors.As(err, &pe) {
			t.Fatalf("err = %v, want provider.Error", err)
		}
		return pe.Kind
	}
	run := func(f *fakeRealtime) error {
		srv := f.server()
		defer srv.Close()
		s, _ := newRealtimeTTS(srv).Synthesize(context.Background(), provider.TTSConfig{})
		defer s.Close()
		s.WriteText("a")
		s.WriteText("b")
		s.EndInput()
		_, err := readAll(t, s)
		return err
	}
	if k := kindOf(run(&fakeRealtime{t: t, failSegment: 1})); k != provider.ErrTransient {
		t.Fatalf("server_error kind = %v", k)
	}
	if k := kindOf(run(&fakeRealtime{t: t, sessionError: true})); k != provider.ErrFatal {
		t.Fatalf("session error kind = %v", k)
	}
	if k := kindOf(run(&fakeRealtime{t: t, doneStatus: "incomplete"})); k != provider.ErrFatal {
		t.Fatalf("incomplete kind = %v", k)
	}
	// A failure on the second segment still delivers the first one's audio.
	f := &fakeRealtime{t: t, failSegment: 2}
	srv := f.server()
	defer srv.Close()
	s, _ := newRealtimeTTS(srv).Synthesize(context.Background(), provider.TTSConfig{})
	defer s.Close()
	s.WriteText("a")
	s.WriteText("b")
	s.EndInput()
	out, err := readAll(t, s)
	if len(out) != 2*gateFrame || err == nil {
		t.Fatalf("len=%d err=%v", len(out), err)
	}

	// HTTP-level rejection of the upgrade is classified by status.
	deny := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }))
	defer deny.Close()
	s2, _ := newRealtimeTTS(deny).Synthesize(context.Background(), provider.TTSConfig{})
	defer s2.Close()
	s2.EndInput()
	if _, err := readAll(t, s2); kindOf(err) != provider.ErrAuth {
		t.Fatalf("401 err = %v", err)
	}
}

func TestRealtimeTTSCancelIsPromptAndClosesSocket(t *testing.T) {
	before := runtime.NumGoroutine()
	f := &fakeRealtime{t: t, stallSegment: 1}
	srv := f.server()
	ctx, cancel := context.WithCancel(context.Background())
	s, _ := newRealtimeTTS(srv).Synthesize(ctx, provider.TTSConfig{})
	s.WriteText("hang")
	if _, err := s.ReadAudio(); err != nil {
		t.Fatal(err)
	}
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	start := time.Now()
	if _, err := s.ReadAudio(); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("ReadAudio did not return promptly after cancel")
	}
	select {
	case <-f.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("socket not closed after cancel")
	}
	s.Close()
	srv.Close()
	// Close (without a ctx cancel) must also tear the stream down.
	f2 := &fakeRealtime{t: t, stallSegment: 1}
	srv2 := f2.server()
	s2, _ := newRealtimeTTS(srv2).Synthesize(context.Background(), provider.TTSConfig{})
	s2.WriteText("hang")
	s2.ReadAudio()
	s2.Close()
	select {
	case <-f2.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("socket not closed after Close")
	}
	srv2.Close()

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("goroutines: %d before, %d after", before, n)
	}
}

func TestSilenceGateTrimsPadding(t *testing.T) {
	frame := func(loud bool, n int) []byte {
		b := make([]byte, n*gateFrame)
		if loud {
			for i := 0; i < len(b)/2; i++ {
				b[2*i], b[2*i+1] = 0xe8, 0x03 // 1000
			}
		}
		return b
	}
	// 10 silent, 4 speech, 40 silent, 4 speech, 30 silent frames, pushed in
	// uneven slices so frame boundaries do not line up with the pushes.
	var in []byte
	in = append(in, frame(false, 10)...)
	in = append(in, frame(true, 4)...)
	in = append(in, frame(false, 40)...)
	in = append(in, frame(true, 4)...)
	in = append(in, frame(false, 30)...)
	var g silenceGate
	var out []byte
	for i := 0; i < len(in); i += 777 {
		out = append(out, g.push(in[i:min(i+777, len(in))])...)
	}
	out = append(out, g.finish()...)
	want := (gateLeadKeep + 4 + gateGapKeep + 4 + gateTailKeep) * gateFrame
	if len(out) != want {
		t.Fatalf("out = %d bytes, want %d", len(out), want)
	}
	// An all-silent segment yields nothing, and the gate is reusable.
	if got := g.push(frame(false, 50)); len(got) != 0 {
		t.Fatalf("silent push emitted %d bytes", len(got))
	}
	if got := g.finish(); len(got) != 0 {
		t.Fatalf("silent segment emitted %d bytes", len(got))
	}
	if got := g.push(frame(true, 2)); len(got) != 2*gateFrame {
		t.Fatalf("gate not reset: %d bytes", len(got))
	}
}
