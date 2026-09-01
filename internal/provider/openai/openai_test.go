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
	llm := NewLLM("sk-x", LLMOptions{httpOptions: opts(srv.URL + "/v1"), Model: "gpt-test", ReasoningEffort: "minimal"})
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
	if gotAuth != "Bearer sk-x" || !gotBody.Stream || !gotBody.StreamOptions.IncludeUsage || gotBody.Model != "gpt-test" || gotBody.MaxCompletionTokens != 50 || gotBody.ReasoningEffort != "minimal" {
		t.Fatalf("request = %+v auth=%q", gotBody, gotAuth)
	}
	if len(gotBody.Messages) != 3 || gotBody.Messages[0].Role != "system" || gotBody.Messages[0].Content != "Be kind." || gotBody.Messages[2].Role != "assistant" {
		t.Fatalf("messages = %+v", gotBody.Messages)
	}
}

func TestLLMOmitsReasoningEffortByDefault(t *testing.T) {
	var raw map[string]json.RawMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&raw)
		sse(w, `[DONE]`)
	}))
	defer srv.Close()
	ch, _ := NewLLM("k", LLMOptions{httpOptions: opts(srv.URL)}).Chat(context.Background(), provider.ChatRequest{})
	drain(t, ch)
	if _, present := raw["reasoning_effort"]; present {
		t.Fatal("reasoning_effort must be omitted when not configured")
	}
	if _, present := raw["max_completion_tokens"]; present {
		t.Fatal("max_completion_tokens must be omitted when unlimited")
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

// ---- tool calls ------------------------------------------------------------

// toolFragment renders one delta.tool_calls fragment; id and name are sent
// only on a call's first fragment.
func toolFragment(index int, id, name, args string) string {
	call := map[string]any{"index": index, "function": map[string]any{"arguments": args}}
	if id != "" {
		call["id"] = id
		call["type"] = "function"
	}
	if name != "" {
		call["function"].(map[string]any)["name"] = name
	}
	b, _ := json.Marshal(map[string]any{
		"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"tool_calls": []any{call}}}},
	})
	return string(b)
}

const toolFinish = `{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`

// chatServer streams the given SSE payloads and records the request body.
func chatServer(t *testing.T, body *chatRequest, raw *map[string]any, lines ...string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf, _ := io.ReadAll(r.Body)
		if body != nil {
			json.Unmarshal(buf, body)
		}
		if raw != nil {
			json.Unmarshal(buf, raw)
		}
		sse(w, lines...)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func toolCallsOf(chunks []provider.LLMChunk) []provider.ToolCall {
	var out []provider.ToolCall
	for _, c := range chunks {
		if c.Kind == provider.LLMToolCall {
			out = append(out, c.ToolCall)
		}
	}
	return out
}

// TestLLMToolCallFragmentAccumulation: arguments split across fragments are
// reassembled inside the adapter and surfaced as one complete call, before
// Done.
func TestLLMToolCallFragmentAccumulation(t *testing.T) {
	srv := chatServer(t, nil, nil,
		toolFragment(0, "call_abc", "transfer_to_agent", `{"depart`),
		toolFragment(0, "", "", `ment":"sa`),
		toolFragment(0, "", "", `les"}`),
		toolFinish, `[DONE]`)
	llm := NewLLM("sk-x", LLMOptions{httpOptions: opts(srv.URL), Model: "gpt-test"})
	ch, err := llm.Chat(context.Background(), provider.ChatRequest{Tools: []provider.ToolDef{{Name: "transfer_to_agent"}}})
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, ch)
	if len(got) != 2 || got[0].Kind != provider.LLMToolCall || got[1].Kind != provider.LLMDone {
		t.Fatalf("chunks = %+v, want the call then done", got)
	}
	want := provider.ToolCall{ID: "call_abc", Name: "transfer_to_agent", Arguments: `{"department":"sales"}`}
	if got[0].ToolCall != want {
		t.Errorf("call = %+v, want %+v", got[0].ToolCall, want)
	}
	if got[1].FinishReason != provider.FinishStop {
		t.Errorf("finish reason = %q, want stop: a tool call is a completed turn", got[1].FinishReason)
	}
}

// TestLLMToolCallWithText: a spoken preamble and a call in one generation.
func TestLLMToolCallWithText(t *testing.T) {
	srv := chatServer(t, nil, nil,
		`{"choices":[{"index":0,"delta":{"content":"Let me check. "}}]}`,
		toolFragment(0, "call_1", "take_message", `{}`),
		toolFinish, `[DONE]`)
	llm := NewLLM("sk-x", LLMOptions{httpOptions: opts(srv.URL)})
	ch, err := llm.Chat(context.Background(), provider.ChatRequest{Tools: []provider.ToolDef{{Name: "take_message"}}})
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, ch)
	if len(got) != 3 || got[0].Kind != provider.LLMTextDelta || got[1].Kind != provider.LLMToolCall || got[2].Kind != provider.LLMDone {
		t.Fatalf("chunks = %+v, want text, call, done", got)
	}
}

// TestLLMToolCallCancelledMidArguments: a partial call never reaches the
// session (docs/protocol-profile.md §6).
func TestLLMToolCallCancelledMidArguments(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		fmt.Fprintf(w, "data: %s\n\n", toolFragment(0, "call_abc", "hangup", `{"reas`))
		f.Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	llm := NewLLM("sk-x", LLMOptions{httpOptions: opts(srv.URL)})
	ch, err := llm.Chat(ctx, provider.ChatRequest{Tools: []provider.ToolDef{{Name: "hangup"}}})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	if calls := toolCallsOf(drain(t, ch)); len(calls) != 0 {
		t.Fatalf("tool calls = %+v, want none after a mid-arguments cancel", calls)
	}
}

// TestLLMToolCallTruncatedArguments: max_output_tokens mid-arguments leaves
// the arguments unusable, so no call is delivered.
func TestLLMToolCallTruncatedArguments(t *testing.T) {
	srv := chatServer(t, nil, nil,
		toolFragment(0, "call_abc", "hangup", `{"reas`),
		`{"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}`,
		`[DONE]`)
	llm := NewLLM("sk-x", LLMOptions{httpOptions: opts(srv.URL)})
	ch, err := llm.Chat(context.Background(), provider.ChatRequest{Tools: []provider.ToolDef{{Name: "hangup"}}})
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, ch)
	if len(got) != 1 || got[0].Kind != provider.LLMDone || got[0].FinishReason != provider.FinishLength {
		t.Fatalf("chunks = %+v, want done/length with no call", got)
	}
}

// TestLLMMultipleToolCallsAreAnError: more than one call is reported, never
// silently dropped.
func TestLLMMultipleToolCallsAreAnError(t *testing.T) {
	srv := chatServer(t, nil, nil,
		toolFragment(0, "call_1", "hangup", `{}`),
		toolFragment(1, "call_2", "take_message", `{}`),
		toolFinish, `[DONE]`)
	llm := NewLLM("sk-x", LLMOptions{httpOptions: opts(srv.URL)})
	ch, err := llm.Chat(context.Background(), provider.ChatRequest{
		Tools: []provider.ToolDef{{Name: "hangup"}, {Name: "take_message"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, ch)
	if len(got) != 2 || got[0].Kind != provider.LLMToolCall || got[0].ToolCall.ID != "call_1" {
		t.Fatalf("chunks = %+v, want the first call then an error", got)
	}
	var perr *provider.Error
	if got[1].Kind != provider.LLMError || !errors.As(got[1].Err, &perr) {
		t.Fatalf("second chunk = %+v, want a provider.Error", got[1])
	}
}

// TestLLMToolsSerialization: the nested Chat Completions shape, an explicit
// tool_choice and parallel_tool_calls:false.
func TestLLMToolsSerialization(t *testing.T) {
	var raw map[string]any
	srv := chatServer(t, nil, &raw, `[DONE]`)
	llm := NewLLM("sk-x", LLMOptions{httpOptions: opts(srv.URL)})
	ch, err := llm.Chat(context.Background(), provider.ChatRequest{
		Messages: []provider.Message{{Role: provider.RoleUser, Content: "hi"}},
		Tools: []provider.ToolDef{{
			Name: "hangup", Description: "end the call",
			Parameters: json.RawMessage(`{"type":"object","properties":{}}`),
		}},
		ToolChoice: provider.ToolChoice{Mode: provider.ToolChoiceAuto},
	})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, ch)
	if raw["tool_choice"] != "auto" {
		t.Errorf("tool_choice = %v", raw["tool_choice"])
	}
	if raw["parallel_tool_calls"] != false {
		t.Errorf("parallel_tool_calls = %v, want false", raw["parallel_tool_calls"])
	}
	tools, _ := raw["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("tools = %v", raw["tools"])
	}
	tool := tools[0].(map[string]any)
	fn, _ := tool["function"].(map[string]any)
	if tool["type"] != "function" || fn["name"] != "hangup" || fn["description"] != "end the call" {
		t.Errorf("tools[0] = %v", tool)
	}
	if params, _ := fn["parameters"].(map[string]any); params["type"] != "object" {
		t.Errorf("parameters = %v, want the schema passed through verbatim", fn["parameters"])
	}
}

// TestLLMForcedToolChoice: the object form of tool_choice.
func TestLLMForcedToolChoice(t *testing.T) {
	var raw map[string]any
	srv := chatServer(t, nil, &raw, `[DONE]`)
	llm := NewLLM("sk-x", LLMOptions{httpOptions: opts(srv.URL)})
	ch, err := llm.Chat(context.Background(), provider.ChatRequest{
		Tools:      []provider.ToolDef{{Name: "hangup"}},
		ToolChoice: provider.ToolChoice{Mode: provider.ToolChoiceFunction, Name: "hangup"},
	})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, ch)
	choice, _ := raw["tool_choice"].(map[string]any)
	fn, _ := choice["function"].(map[string]any)
	if choice["type"] != "function" || fn["name"] != "hangup" {
		t.Fatalf("tool_choice = %v", raw["tool_choice"])
	}
}

// TestLLMNoToolFieldsWithoutTools: the API rejects tool_choice and
// parallel_tool_calls on a request that declares no tools.
func TestLLMNoToolFieldsWithoutTools(t *testing.T) {
	var raw map[string]any
	srv := chatServer(t, nil, &raw, `[DONE]`)
	llm := NewLLM("sk-x", LLMOptions{httpOptions: opts(srv.URL)})
	ch, err := llm.Chat(context.Background(), provider.ChatRequest{
		Messages:   []provider.Message{{Role: provider.RoleUser, Content: "hi"}},
		ToolChoice: provider.ToolChoice{Mode: provider.ToolChoiceAuto},
	})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, ch)
	for _, k := range []string{"tools", "tool_choice", "parallel_tool_calls"} {
		if _, present := raw[k]; present {
			t.Errorf("%s is present on a request that declares no tools", k)
		}
	}
}

// TestLLMToolHistoryReplay: an assistant turn that spoke and called, then the
// tool result, projected onto the Chat Completions shape.
func TestLLMToolHistoryReplay(t *testing.T) {
	var raw map[string]any
	srv := chatServer(t, nil, &raw, `[DONE]`)
	llm := NewLLM("sk-x", LLMOptions{httpOptions: opts(srv.URL)})
	ch, err := llm.Chat(context.Background(), provider.ChatRequest{
		Messages: []provider.Message{
			{Role: provider.RoleUser, Content: "put me through to sales"},
			{Role: provider.RoleAssistant, Content: "One moment.", ToolCalls: []provider.ToolCall{
				{ID: "call_abc", Name: "transfer_to_agent", Arguments: `{"department":"sales"}`},
			}},
			{Role: provider.RoleTool, ToolCallID: "call_abc", Content: "no agent available"},
		},
		Tools: []provider.ToolDef{{Name: "transfer_to_agent"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, ch)
	msgs, _ := raw["messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("messages = %v", msgs)
	}
	assistant := msgs[1].(map[string]any)
	tcs, _ := assistant["tool_calls"].([]any)
	if assistant["content"] != "One moment." || len(tcs) != 1 {
		t.Fatalf("assistant message = %v", assistant)
	}
	tc := tcs[0].(map[string]any)
	fn, _ := tc["function"].(map[string]any)
	if tc["id"] != "call_abc" || tc["type"] != "function" || fn["name"] != "transfer_to_agent" || fn["arguments"] != `{"department":"sales"}` {
		t.Errorf("tool_calls[0] = %v", tc)
	}
	result := msgs[2].(map[string]any)
	if result["role"] != "tool" || result["tool_call_id"] != "call_abc" || result["content"] != "no agent available" {
		t.Errorf("tool result message = %v", result)
	}
	if _, present := msgs[0].(map[string]any)["tool_calls"]; present {
		t.Error("a plain user message must not carry tool_calls")
	}
}

// TestLLMToolChoiceModes: the wire mapping of every tool_choice form.
func TestLLMToolChoiceModes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		choice provider.ToolChoice
		want   any
	}{
		{"zero value defaults to auto", provider.ToolChoice{}, "auto"},
		{"auto", provider.ToolChoice{Mode: provider.ToolChoiceAuto}, "auto"},
		{"none", provider.ToolChoice{Mode: provider.ToolChoiceNone}, "none"},
		{"required", provider.ToolChoice{Mode: provider.ToolChoiceRequired}, "required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var raw map[string]any
			srv := chatServer(t, nil, &raw, `[DONE]`)
			llm := NewLLM("sk-x", LLMOptions{httpOptions: opts(srv.URL)})
			ch, err := llm.Chat(context.Background(), provider.ChatRequest{
				Tools: []provider.ToolDef{{Name: "hangup"}}, ToolChoice: tc.choice,
			})
			if err != nil {
				t.Fatal(err)
			}
			drain(t, ch)
			if got := raw["tool_choice"]; got != tc.want {
				t.Errorf("tool_choice = %v, want %v", got, tc.want)
			}
		})
	}
}
