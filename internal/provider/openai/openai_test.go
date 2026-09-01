package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

func opts(baseURL string) httpOptions {
	return httpOptions{BaseURL: baseURL, RequestTimeout: config.Duration(2 * time.Second), StreamIdleTimeout: config.Duration(2 * time.Second)}
}

func sse(w http.ResponseWriter, lines ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	f := w.(http.Flusher)
	for _, l := range lines {
		fmt.Fprintf(w, "data: %s\n\n", l)
		f.Flush()
	}
}

func drain(t *testing.T, ch <-chan provider.LLMChunk) []provider.LLMChunk {
	t.Helper()
	var out []provider.LLMChunk
	timeout := time.After(3 * time.Second)
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, c)
		case <-timeout:
			t.Fatalf("timeout draining, have %+v", out)
		}
	}
}

func TestLLMStreamsChunksAndUsage(t *testing.T) {
	var gotBody chatRequest
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path = %s", r.URL.Path)
		}
		json.NewDecoder(r.Body).Decode(&gotBody)
		sse(w,
			`{"choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}`,
			`{"choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}`,
			`{"choices":[{"index":0,"delta":{"content":" there"},"finish_reason":null}]}`,
			`{"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}`,
			`{"choices":[],"usage":{"prompt_tokens":12,"completion_tokens":3,"total_tokens":15}}`,
			`[DONE]`,
		)
	}))
	defer srv.Close()
	llm := NewLLM("sk-x", LLMOptions{httpOptions: opts(srv.URL + "/v1"), Model: "gpt-test"})
	ch, err := llm.Chat(context.Background(), provider.ChatRequest{
		Instructions: "Be kind.", MaxOutputTokens: 50,
		Messages: []provider.Message{{Role: provider.RoleUser, Content: "hi"}, {Role: provider.RoleAssistant, Content: "yo"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, ch)
	if len(got) != 3 || got[0].Text != "Hello" || got[1].Text != " there" || got[2].Kind != provider.LLMDone {
		t.Fatalf("chunks = %+v", got)
	}
	if got[2].FinishReason != provider.FinishLength || got[2].Usage != (provider.Usage{InputTokens: 12, OutputTokens: 3}) {
		t.Fatalf("done = %+v", got[2])
	}
	if gotAuth != "Bearer sk-x" || !gotBody.Stream || !gotBody.StreamOptions.IncludeUsage || gotBody.Model != "gpt-test" || gotBody.MaxCompletionTokens != 50 {
		t.Fatalf("request = %+v auth=%q", gotBody, gotAuth)
	}
	if len(gotBody.Messages) != 3 || gotBody.Messages[0].Role != "system" || gotBody.Messages[0].Content != "Be kind." || gotBody.Messages[2].Role != "assistant" {
		t.Fatalf("messages = %+v", gotBody.Messages)
	}
}

func TestLLMErrorClassification(t *testing.T) {
	for status, kind := range map[int]provider.ErrorKind{401: provider.ErrAuth, 429: provider.ErrRateLimit, 503: provider.ErrTransient, 400: provider.ErrFatal} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			io.WriteString(w, `{"error":{"message":"nope"}}`)
		}))
		llm := NewLLM("k", LLMOptions{httpOptions: opts(srv.URL)})
		ch, _ := llm.Chat(context.Background(), provider.ChatRequest{})
		got := drain(t, ch)
		srv.Close()
		var pe *provider.Error
		if len(got) != 1 || got[0].Kind != provider.LLMError || !errors.As(got[0].Err, &pe) || pe.Kind != kind || !strings.Contains(pe.Error(), "nope") {
			t.Fatalf("status %d: %+v", status, got)
		}
	}
}

func TestLLMCancelClosesPromptly(t *testing.T) {
	release := make(chan struct{})
	var closedAt time.Time
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sse(w, `{"choices":[{"index":0,"delta":{"content":"one"},"finish_reason":null}]}`)
		select {
		case <-r.Context().Done(): // client went away
			mu.Lock()
			closedAt = time.Now()
			mu.Unlock()
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release)
	llm := NewLLM("k", LLMOptions{httpOptions: opts(srv.URL)})
	ctx, cancel := context.WithCancel(context.Background())
	ch, _ := llm.Chat(ctx, provider.ChatRequest{})
	first := <-ch
	if first.Text != "one" {
		t.Fatalf("first = %+v", first)
	}
	cancelAt := time.Now()
	cancel()
	rest := drain(t, ch)
	if len(rest) != 0 {
		t.Fatalf("no chunks expected after cancel, got %+v", rest)
	}
	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		c := closedAt
		mu.Unlock()
		if !c.IsZero() {
			if c.Sub(cancelAt) > 200*time.Millisecond {
				t.Fatalf("server saw the close %v after cancel", c.Sub(cancelAt))
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("server never saw the connection close")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestLLMIdleTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sse(w, `{"choices":[{"index":0,"delta":{"content":"a"},"finish_reason":null}]}`)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release)
	o := opts(srv.URL)
	o.StreamIdleTimeout = config.Duration(100 * time.Millisecond)
	llm := NewLLM("k", LLMOptions{httpOptions: o})
	ch, _ := llm.Chat(context.Background(), provider.ChatRequest{})
	got := drain(t, ch)
	var pe *provider.Error
	if len(got) != 2 || got[1].Kind != provider.LLMError || !errors.As(got[1].Err, &pe) || pe.Kind != provider.ErrTransient {
		t.Fatalf("got %+v", got)
	}
}

func TestLLMStreamEndWithoutDone(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sse(w, `{"choices":[{"index":0,"delta":{"content":"a"},"finish_reason":"stop"}]}`)
	}))
	defer srv.Close()
	ch, _ := NewLLM("k", LLMOptions{httpOptions: opts(srv.URL)}).Chat(context.Background(), provider.ChatRequest{})
	got := drain(t, ch)
	if len(got) != 2 || got[1].Kind != provider.LLMDone || got[1].FinishReason != provider.FinishStop {
		t.Fatalf("got %+v", got)
	}
}

func readAll(t *testing.T, s provider.TTSStream) ([]byte, error) {
	t.Helper()
	var out []byte
	for {
		c, err := s.ReadAudio()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return out, err
		}
		out = append(out, c.PCM...)
	}
}

func TestTTSStreamsPCM(t *testing.T) {
	var got speechRequest
	pcm := make([]byte, audio.MsToBytes(250)+1) // odd trailing byte is dropped
	for i := range pcm {
		pcm[i] = byte(i)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/audio/speech" {
			t.Errorf("path = %s", r.URL.Path)
		}
		json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "audio/pcm")
		f := w.(http.Flusher)
		for off := 0; off < len(pcm); off += 1000 {
			w.Write(pcm[off:min(off+1000, len(pcm))])
			f.Flush()
		}
	}))
	defer srv.Close()
	tts := NewTTS("k", TTSOptions{httpOptions: opts(srv.URL + "/v1")})
	if caps := tts.Capabilities(); caps.IncrementalText || caps.Alignment {
		t.Fatal("openai tts must report no optional capabilities")
	}
	s, _ := tts.Synthesize(context.Background(), provider.TTSConfig{Voice: "coral", Speed: 1.2})
	s.WriteText("Hello ")
	s.WriteText("world.")
	s.EndInput()
	out, err := readAll(t, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != audio.MsToBytes(250) || out[5] != 5 || out[len(out)-1] != pcm[len(pcm)-2] {
		t.Fatalf("pcm len=%d", len(out))
	}
	if got.Input != "Hello world." || got.Voice != "coral" || got.ResponseFormat != "pcm" || got.Speed != 1.2 || got.Model != defaultTTSModel {
		t.Fatalf("request = %+v", got)
	}
	if _, err := readAll(t, s); !errors.Is(err, nil) {
		t.Fatalf("second read after EOF: %v", err)
	}
	if err := s.WriteText("x"); err == nil {
		t.Fatal("write after EndInput must fail")
	}
	s.Close()
}

func TestTTSErrorAndCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		io.WriteString(w, "slow down")
	}))
	tts := NewTTS("k", TTSOptions{httpOptions: opts(srv.URL)})
	s, _ := tts.Synthesize(context.Background(), provider.TTSConfig{})
	s.WriteText("x")
	s.EndInput()
	_, err := readAll(t, s)
	var pe *provider.Error
	if !errors.As(err, &pe) || pe.Kind != provider.ErrRateLimit {
		t.Fatalf("err = %v", err)
	}
	srv.Close()

	release := make(chan struct{})
	defer close(release)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(make([]byte, audio.MsToBytes(100)))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer slow.Close()
	ctx, cancel := context.WithCancel(context.Background())
	s2, _ := NewTTS("k", TTSOptions{httpOptions: opts(slow.URL)}).Synthesize(ctx, provider.TTSConfig{})
	s2.WriteText("x")
	s2.EndInput()
	if _, err := s2.ReadAudio(); err != nil {
		t.Fatal(err)
	}
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	start := time.Now()
	if _, err := s2.ReadAudio(); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("ReadAudio did not return promptly after cancel")
	}
}

func TestRegistered(t *testing.T) {
	if !provider.Known("llm", Name) || !provider.Known("tts", Name) {
		t.Fatal("openai providers not registered")
	}
	p, err := provider.New(provider.KindLLM, Name, "k", []byte(`{"model":"m","base_url":"http://x/","request_timeout":"1s"}`))
	if err != nil {
		t.Fatal(err)
	}
	if l := p.(*LLM); l.opts.Model != "m" || l.opts.BaseURL != "http://x" || l.opts.StreamIdleTimeout.Std() != defaultStreamIdleTimeout {
		t.Fatalf("opts = %+v", l.opts)
	}
	if _, err := provider.New(provider.KindTTS, Name, "k", []byte(`{"model":5}`)); err == nil {
		t.Fatal("bad options must fail")
	}
}
