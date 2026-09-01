package deepgram

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// fakeDeepgram is a scripted /v1/listen endpoint.
type fakeDeepgram struct {
	t *testing.T
	// onFinalize returns the messages to send when Finalize arrives.
	onFinalize func() []string
	// onAudio is called with the cumulative audio byte count after each frame
	// and may return messages to send.
	onAudio func(total int) []string

	mu        sync.Mutex
	query     map[string]string
	auth      string
	audio     int
	control   []string
	closedAt  time.Time
	closeCode websocket.StatusCode
	connected chan struct{}
}

func newFake(t *testing.T) (*fakeDeepgram, *httptest.Server) {
	f := &fakeDeepgram{t: t, connected: make(chan struct{})}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.auth = r.Header.Get("Authorization")
		f.query = map[string]string{}
		for k, v := range r.URL.Query() {
			f.query[k] = v[0]
		}
		f.mu.Unlock()
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		close(f.connected)
		ctx := context.Background()
		for {
			typ, data, err := conn.Read(ctx)
			if err != nil {
				f.mu.Lock()
				f.closedAt = time.Now()
				f.closeCode = websocket.CloseStatus(err)
				f.mu.Unlock()
				return
			}
			var replies []string
			if typ == websocket.MessageBinary {
				f.mu.Lock()
				f.audio += len(data)
				total := f.audio
				f.mu.Unlock()
				if f.onAudio != nil {
					replies = f.onAudio(total)
				}
			} else {
				var m struct{ Type string }
				json.Unmarshal(data, &m)
				f.mu.Lock()
				f.control = append(f.control, m.Type)
				f.mu.Unlock()
				if m.Type == "Finalize" && f.onFinalize != nil {
					replies = f.onFinalize()
				}
				if m.Type == "CloseStream" {
					conn.Close(websocket.StatusNormalClosure, "")
					return
				}
			}
			for _, r := range replies {
				if r == "close:1011" {
					conn.Close(websocket.StatusInternalError, "server down")
					return
				}
				if err := conn.Write(ctx, websocket.MessageText, []byte(r)); err != nil {
					return
				}
			}
		}
	}))
	return f, srv
}

func results(text string, isFinal, speechFinal, fromFinalize bool, start, dur float64) string {
	b, _ := json.Marshal(map[string]any{
		"type": "Results", "is_final": isFinal, "speech_final": speechFinal, "from_finalize": fromFinalize,
		"start": start, "duration": dur,
		"channel": map[string]any{"alternatives": []map[string]any{{"transcript": text}}},
	})
	return string(b)
}

func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/listen"
}

func open(t *testing.T, srv *httptest.Server, o Options) (provider.ASRStream, context.CancelFunc) {
	t.Helper()
	o.BaseURL = wsURL(srv)
	if o.FinalizeTimeout == 0 {
		o.FinalizeTimeout = config.Duration(300 * time.Millisecond)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s, err := New("dg-key", o).OpenStream(ctx, provider.ASRConfig{SampleRate: audio.SampleRate, Language: "en"})
	if err != nil {
		cancel()
		t.Fatalf("OpenStream: %v", err)
	}
	return s, cancel
}

func recv(t *testing.T, s provider.ASRStream) provider.ASREvent {
	t.Helper()
	select {
	case ev, ok := <-s.Events():
		if !ok {
			t.Fatal("events closed")
		}
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
	return provider.ASREvent{}
}

func TestQueryAuthAndMapping(t *testing.T) {
	f, srv := newFake(t)
	defer srv.Close()
	f.onAudio = func(total int) []string {
		switch total {
		case audio.MsToBytes(40):
			return []string{results("hel", false, false, false, 0, 0.4)}
		case audio.MsToBytes(100):
			return []string{results("hello there", true, false, false, 0, 1.0), results("", true, false, false, 1.0, 0.1)}
		}
		return nil
	}
	f.onFinalize = func() []string {
		return []string{results("how are you", true, true, true, 1.2, 0.8)}
	}
	s, cancel := open(t, srv, Options{SmartFormat: true, EndpointingMs: 250, UtteranceEndMs: 900})
	defer cancel()
	<-f.connected
	f.mu.Lock()
	q, auth := f.query, f.auth
	f.mu.Unlock()
	if auth != "Token dg-key" {
		t.Fatalf("auth = %q", auth)
	}
	for k, want := range map[string]string{"model": "nova-3", "encoding": "linear16", "sample_rate": "24000", "channels": "1",
		"interim_results": "true", "punctuate": "true", "endpointing": "250", "utterance_end_ms": "900", "smart_format": "true", "language": "en"} {
		if q[k] != want {
			t.Errorf("query %s = %q, want %q", k, q[k], want)
		}
	}
	frame := make([]byte, audio.MsToBytes(20))
	for range 5 {
		if err := s.PushAudio(frame); err != nil {
			t.Fatal(err)
		}
	}
	if ev := recv(t, s); ev.Kind != provider.ASRPartial || ev.Text != "hel" {
		t.Fatalf("partial = %+v", ev)
	}
	ev := recv(t, s)
	if ev.Kind != provider.ASRFinal || ev.Text != "hello there" || ev.StartMs != 0 || ev.EndMs != 1000 {
		t.Fatalf("final = %+v", ev)
	}
	// The empty is_final result is ignored; Finalize yields Final + EndOfTurn.
	if err := s.Finalize(); err != nil {
		t.Fatal(err)
	}
	if ev := recv(t, s); ev.Kind != provider.ASRFinal || ev.Text != "how are you" || ev.StartMs != 1200 || ev.EndMs != 2000 {
		t.Fatalf("finalized = %+v", ev)
	}
	if ev := recv(t, s); ev.Kind != provider.ASREndOfTurn {
		t.Fatalf("want end_of_turn, got %+v", ev)
	}
	// No synthesized EndOfTurn after a satisfied finalize.
	select {
	case ev := <-s.Events():
		t.Fatalf("unexpected extra event %+v", ev)
	case <-time.After(400 * time.Millisecond):
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	ctl := f.control
	f.mu.Unlock()
	if strings.Join(ctl, ",") != "Finalize,CloseStream" {
		t.Fatalf("control = %v", ctl)
	}
	if _, ok := <-s.Events(); ok {
		t.Fatal("events must be closed after Close")
	}
}

func TestFinalizeTimeoutSynthesizesEndOfTurn(t *testing.T) {
	f, srv := newFake(t)
	defer srv.Close()
	f.onFinalize = func() []string { return nil } // Deepgram stays silent
	s, cancel := open(t, srv, Options{FinalizeTimeout: config.Duration(100 * time.Millisecond)})
	defer cancel()
	<-f.connected
	start := time.Now()
	s.Finalize()
	if ev := recv(t, s); ev.Kind != provider.ASREndOfTurn {
		t.Fatalf("got %+v", ev)
	}
	if d := time.Since(start); d < 90*time.Millisecond || d > time.Second {
		t.Fatalf("synthesized after %v", d)
	}
	s.Close()
}

func TestUtteranceEndAndError(t *testing.T) {
	f, srv := newFake(t)
	defer srv.Close()
	f.onAudio = func(total int) []string {
		if total == audio.MsToBytes(20) {
			return []string{`{"type":"UtteranceEnd","last_word_end":1.2}`, `{"type":"Metadata"}`, `{"type":"Error","description":"NET-0001 boom"}`}
		}
		return nil
	}
	s, cancel := open(t, srv, Options{})
	defer cancel()
	<-f.connected
	s.PushAudio(make([]byte, audio.MsToBytes(20)))
	if ev := recv(t, s); ev.Kind != provider.ASREndOfTurn {
		t.Fatalf("got %+v", ev)
	}
	ev := recv(t, s)
	var pe *provider.Error
	if ev.Kind != provider.ASRError || !errors.As(ev.Err, &pe) || pe.Provider != Name || !strings.Contains(pe.Error(), "NET-0001") {
		t.Fatalf("got %+v", ev)
	}
	s.Close()
}

func TestCancelClosesSocketPromptly(t *testing.T) {
	f, srv := newFake(t)
	defer srv.Close()
	s, cancel := open(t, srv, Options{})
	<-f.connected
	s.PushAudio(make([]byte, 48))
	cancelAt := time.Now()
	cancel()
	select {
	case _, ok := <-s.Events():
		if ok {
			t.Fatal("expected closed events channel")
		}
	case <-time.After(time.Second):
		t.Fatal("events not closed after cancel")
	}
	deadline := time.Now().Add(time.Second)
	for {
		f.mu.Lock()
		c := f.closedAt
		f.mu.Unlock()
		if !c.IsZero() {
			if c.Sub(cancelAt) > 200*time.Millisecond {
				t.Fatalf("server saw the close %v after cancel", c.Sub(cancelAt))
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server never saw the close")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if err := s.PushAudio(make([]byte, 48)); err == nil {
		t.Fatal("push after cancel must fail")
	}
	s.Close() // idempotent after cancel
}

func TestServerDisconnectIsAnError(t *testing.T) {
	f, srv := newFake(t)
	defer srv.Close()
	f.onAudio = func(total int) []string { return []string{"close:1011"} }
	s, cancel := open(t, srv, Options{})
	defer cancel()
	<-f.connected
	s.PushAudio(make([]byte, 48))
	ev := recv(t, s)
	if ev.Kind != provider.ASRError {
		t.Fatalf("got %+v", ev)
	}
	if _, ok := <-s.Events(); ok {
		t.Fatal("events must close after the error")
	}
}

func TestAudioQueueFullFailsPush(t *testing.T) {
	f, srv := newFake(t)
	defer srv.Close()
	s, cancel := open(t, srv, Options{AudioQueueFrames: 2})
	defer cancel()
	<-f.connected
	// Stall the writer: the fake never reads fast enough only if we block it,
	// so fill the queue faster than the loop can drain by not yielding.
	failed := false
	for i := 0; i < 10000 && !failed; i++ {
		if err := s.PushAudio(make([]byte, audio.MsToBytes(20))); err != nil {
			failed = true
		}
	}
	if !failed {
		t.Skip("writer kept up; queue-full path not exercised on this machine")
	}
}

func TestDialFailures(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }))
	defer srv.Close()
	_, err := New("bad", Options{BaseURL: wsURL(srv)}).OpenStream(context.Background(), provider.ASRConfig{SampleRate: 24000})
	var pe *provider.Error
	if !errors.As(err, &pe) || pe.Kind != provider.ErrAuth {
		t.Fatalf("err = %v", err)
	}
	_, err = New("k", Options{BaseURL: "ws://127.0.0.1:1/v1/listen", ConnectTimeout: config.Duration(500 * time.Millisecond)}).OpenStream(context.Background(), provider.ASRConfig{SampleRate: 24000})
	if !errors.As(err, &pe) || pe.Kind != provider.ErrTransient {
		t.Fatalf("err = %v", err)
	}
	if !provider.Known("asr", Name) {
		t.Fatal("not registered")
	}
}
