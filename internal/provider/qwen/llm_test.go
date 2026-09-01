package qwen

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// fakeChat is a scripted /chat/completions endpoint that records the request
// body and streams a canned SSE script.
type fakeChat struct {
	mu     sync.Mutex
	body   map[string]any
	auth   string
	script []string
	status int
	// hold blocks the handler after the first event so cancellation can be
	// observed mid-stream.
	hold chan struct{}
}

func newFakeChat(t *testing.T, script ...string) (*fakeChat, *httptest.Server) {
	f := &fakeChat{script: script, status: http.StatusOK}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		json.Unmarshal(raw, &body)
		f.mu.Lock()
		f.body, f.auth = body, r.Header.Get("Authorization")
		status, script, hold := f.status, f.script, f.hold
		f.mu.Unlock()
		if status != http.StatusOK {
			w.WriteHeader(status)
			w.Write([]byte(`{"error":{"message":"scripted failure"}}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		for i, line := range script {
			w.Write([]byte(line + "\n\n"))
			flusher.Flush()
			if hold != nil && i == 0 {
				select {
				case <-hold:
				case <-r.Context().Done():
					return
				}
			}
		}
	}))
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeChat) request() map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.body
}

func newTestLLM(url string) *LLM {
	return NewLLM("test-key", LLMOptions{BaseURL: url, StreamIdleTimeout: config.Duration(2 * time.Second)})
}

func collect(t *testing.T, ch <-chan provider.LLMChunk) (string, provider.LLMChunk) {
	t.Helper()
	var text strings.Builder
	var last provider.LLMChunk
	deadline := time.After(5 * time.Second)
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				return text.String(), last
			}
			if c.Kind == provider.LLMTextDelta {
				text.WriteString(c.Text)
			}
			last = c
		case <-deadline:
			t.Fatal("timed out collecting LLM chunks")
		}
	}
}

func delta(content string) string {
	b, _ := json.Marshal(map[string]any{
		"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"content": content}}},
	})
	return "data: " + string(b)
}

// TestLLMEnableThinkingFalse: the model streams reasoning_content first
// unless enable_thinking is explicitly false, which costs seconds of
// time-to-first-token and produces text a voice turn must not speak.
func TestLLMEnableThinkingFalse(t *testing.T) {
	f, srv := newFakeChat(t, delta("hi"), "data: [DONE]")
	ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{
		Instructions: "be brief", Messages: []provider.Message{{Role: provider.RoleUser, Content: "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	collect(t, ch)

	body := f.request()
	got, present := body["enable_thinking"]
	if !present {
		t.Fatal("enable_thinking is absent from the request body")
	}
	if got != false {
		t.Errorf("enable_thinking = %v, want false", got)
	}
	if body["stream"] != true {
		t.Errorf("stream = %v, want true", body["stream"])
	}
	if body["model"] != defaultChatModel {
		t.Errorf("model = %v, want %q", body["model"], defaultChatModel)
	}
	msgs, _ := body["messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("messages = %v, want the system prompt plus the user turn", msgs)
	}
	first := msgs[0].(map[string]any)
	if first["role"] != "system" || first["content"] != "be brief" {
		t.Errorf("first message = %v, want the instructions as a system message", first)
	}
	f.mu.Lock()
	auth := f.auth
	f.mu.Unlock()
	if auth != "Bearer test-key" {
		t.Errorf("Authorization = %q", auth)
	}
}

func TestLLMStreaming(t *testing.T) {
	usage := `data: {"choices":[],"usage":{"prompt_tokens":14,"completion_tokens":9}}`
	stop := `data: {"choices":[{"index":0,"delta":{"content":""},"finish_reason":"stop"}]}`
	f, srv := newFakeChat(t, delta("The ocean "), delta("is vast"), delta("."), stop, usage, "data: [DONE]")
	_ = f
	ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{
		Messages: []provider.Message{{Role: provider.RoleUser, Content: "describe the ocean"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	text, last := collect(t, ch)
	if text != "The ocean is vast." {
		t.Errorf("text = %q", text)
	}
	if last.Kind != provider.LLMDone {
		t.Fatalf("last chunk = %+v, want done", last)
	}
	if last.FinishReason != provider.FinishStop {
		t.Errorf("finish reason = %q, want stop", last.FinishReason)
	}
	if last.Usage != (provider.Usage{InputTokens: 14, OutputTokens: 9}) {
		t.Errorf("usage = %+v", last.Usage)
	}
}

// TestLLMDropsReasoningContent: reasoning output must never reach the
// pipeline, even when the service sends it.
func TestLLMDropsReasoningContent(t *testing.T) {
	reasoning := `data: {"choices":[{"index":0,"delta":{"content":"","reasoning_content":"Thinking Process: the user wants…"}}]}`
	_, srv := newFakeChat(t, reasoning, reasoning, delta("Hello."), "data: [DONE]")
	ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	text, _ := collect(t, ch)
	if text != "Hello." {
		t.Errorf("text = %q, want only the answer with no reasoning content", text)
	}
	if strings.Contains(text, "Thinking") {
		t.Error("reasoning content leaked into the response")
	}
}

func TestLLMFinishReasons(t *testing.T) {
	for wire, want := range map[string]provider.FinishReason{
		"length":         provider.FinishLength,
		"content_filter": provider.FinishContentFilter,
		"stop":           provider.FinishStop,
	} {
		line := `data: {"choices":[{"index":0,"delta":{"content":"x"},"finish_reason":"` + wire + `"}]}`
		_, srv := newFakeChat(t, line, "data: [DONE]")
		ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{})
		if err != nil {
			t.Fatal(err)
		}
		_, last := collect(t, ch)
		if last.FinishReason != want {
			t.Errorf("%q → %q, want %q", wire, last.FinishReason, want)
		}
	}
}

// TestLLMEOFWithoutDone: a stream that ends without [DONE] still completes.
func TestLLMEOFWithoutDone(t *testing.T) {
	_, srv := newFakeChat(t, delta("partial answer"))
	ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	text, last := collect(t, ch)
	if text != "partial answer" || last.Kind != provider.LLMDone {
		t.Errorf("text=%q last=%+v, want the text and a done chunk", text, last)
	}
}

func TestLLMMalformedChunk(t *testing.T) {
	_, srv := newFakeChat(t, delta("ok "), "data: {not json}", "data: [DONE]")
	ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	_, last := collect(t, ch)
	if last.Kind != provider.LLMError {
		t.Fatalf("last chunk = %+v, want an error", last)
	}
	var perr *provider.Error
	if !errors.As(last.Err, &perr) || perr.Kind != provider.ErrFatal {
		t.Errorf("error = %v, want a fatal provider error", last.Err)
	}
}

func TestLLMHTTPErrorClassification(t *testing.T) {
	for status, want := range map[int]provider.ErrorKind{
		http.StatusUnauthorized:        provider.ErrAuth,
		http.StatusTooManyRequests:     provider.ErrRateLimit,
		http.StatusInternalServerError: provider.ErrTransient,
		http.StatusBadRequest:          provider.ErrFatal,
	} {
		f, srv := newFakeChat(t)
		f.mu.Lock()
		f.status = status
		f.mu.Unlock()
		ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{})
		if err != nil {
			t.Fatal(err)
		}
		_, last := collect(t, ch)
		if last.Kind != provider.LLMError {
			t.Fatalf("status %d: last chunk = %+v, want an error", status, last)
		}
		var perr *provider.Error
		if !errors.As(last.Err, &perr) {
			t.Fatalf("status %d: error = %v, want a *provider.Error", status, last.Err)
		}
		if perr.Kind != want {
			t.Errorf("status %d → %s, want %s", status, perr.Kind, want)
		}
	}
}

func TestLLMCancellation(t *testing.T) {
	f, srv := newFakeChat(t, delta("first "), delta("second"), "data: [DONE]")
	f.mu.Lock()
	f.hold = make(chan struct{})
	f.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := newTestLLM(srv.URL).Chat(ctx, provider.ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	c := <-ch
	if c.Kind != provider.LLMTextDelta {
		t.Fatalf("first chunk = %+v", c)
	}
	cancel()
	closed := make(chan struct{})
	go func() {
		for range ch {
		}
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("chunk channel not closed within 2s of cancellation")
	}
}

func TestLLMIdleTimeout(t *testing.T) {
	f, srv := newFakeChat(t, delta("first "), delta("second"), "data: [DONE]")
	f.mu.Lock()
	f.hold = make(chan struct{})
	f.mu.Unlock()
	l := NewLLM("k", LLMOptions{BaseURL: srv.URL, StreamIdleTimeout: config.Duration(200 * time.Millisecond)})
	ch, err := l.Chat(context.Background(), provider.ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	_, last := collect(t, ch)
	if last.Kind != provider.LLMError || !strings.Contains(last.Err.Error(), "idle") {
		t.Errorf("last chunk = %+v, want an idle timeout error", last)
	}
}

func TestLLMMaxOutputTokens(t *testing.T) {
	f, srv := newFakeChat(t, "data: [DONE]")
	ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{MaxOutputTokens: 64})
	if err != nil {
		t.Fatal(err)
	}
	collect(t, ch)
	if got := f.request()["max_tokens"]; got != float64(64) {
		t.Errorf("max_tokens = %v, want 64", got)
	}
}

func TestLLMOptionsDefaults(t *testing.T) {
	var o LLMOptions
	o.applyDefaults()
	if o.BaseURL != defaultLLMBaseURL {
		t.Errorf("base_url = %q, want %q", o.BaseURL, defaultLLMBaseURL)
	}
	if o.Model != defaultChatModel {
		t.Errorf("model = %q, want %q", o.Model, defaultChatModel)
	}
	if o.RequestTimeout == 0 || o.StreamIdleTimeout == 0 {
		t.Errorf("defaults left a zero timeout: %+v", o)
	}
	// A trailing slash must not produce a double slash in the URL.
	o2 := LLMOptions{BaseURL: "https://example.test/compatible-mode/v1/"}
	o2.applyDefaults()
	if o2.BaseURL != "https://example.test/compatible-mode/v1" {
		t.Errorf("base_url = %q", o2.BaseURL)
	}
}

func TestLLMRegisteredUnderQwen(t *testing.T) {
	if !provider.Known("llm", Name) {
		t.Fatal("llm provider not registered as qwen")
	}
	p, err := provider.New(provider.KindLLM, Name, "k", json.RawMessage(`{"model":"qwen3.6-flash"}`))
	if err != nil {
		t.Fatal(err)
	}
	if p.Kind() != provider.KindLLM {
		t.Errorf("kind = %s", p.Kind())
	}
	if _, err := provider.New(provider.KindLLM, Name, "k", json.RawMessage(`{"model":`)); err == nil {
		t.Error("malformed options accepted")
	}
}

func TestClassify(t *testing.T) {
	for code, want := range map[string]provider.ErrorKind{
		"Throttling.RateQuota":       provider.ErrRateLimit,
		"Throttling.AllocationQuota": provider.ErrRateLimit,
		"InvalidApiKey":              provider.ErrAuth,
		"AccessDenied":               provider.ErrAuth,
		"SERVER_ERROR":               provider.ErrTransient,
		"ServiceUnavailable":         provider.ErrTransient,
		"InvalidParameter":           provider.ErrFatal,
		"":                           provider.ErrFatal,
	} {
		if got := classify(code); got != want {
			t.Errorf("classify(%q) = %s, want %s", code, got, want)
		}
	}
}
