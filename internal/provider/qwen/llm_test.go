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

// ---- tool calls ------------------------------------------------------------

// toolFragment renders one delta.tool_calls fragment.
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
	return "data: " + string(b)
}

func toolFinish() string {
	return `data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`
}

// collectCalls returns every tool call chunk in arrival order alongside the
// full chunk list.
func collectCalls(t *testing.T, ch <-chan provider.LLMChunk) ([]provider.ToolCall, []provider.LLMChunk) {
	t.Helper()
	var calls []provider.ToolCall
	var all []provider.LLMChunk
	deadline := time.After(5 * time.Second)
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				return calls, all
			}
			if c.Kind == provider.LLMToolCall {
				calls = append(calls, c.ToolCall)
			}
			all = append(all, c)
		case <-deadline:
			t.Fatal("timed out collecting LLM chunks")
		}
	}
}

// TestLLMToolCallFragmentAccumulation: id and name arrive on the first
// fragment only and arguments are split; the session must see one complete
// call, before Done.
func TestLLMToolCallFragmentAccumulation(t *testing.T) {
	_, srv := newFakeChat(t,
		toolFragment(0, "call_abc", "transfer_to_agent", `{"dep`),
		toolFragment(0, "", "", `artment":"sa`),
		toolFragment(0, "", "", `les"}`),
		toolFinish(), "data: [DONE]")
	ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{
		Tools: []provider.ToolDef{{Name: "transfer_to_agent"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	calls, all := collectCalls(t, ch)
	if len(calls) != 1 {
		t.Fatalf("tool calls = %+v, want exactly one", calls)
	}
	want := provider.ToolCall{ID: "call_abc", Name: "transfer_to_agent", Arguments: `{"department":"sales"}`}
	if calls[0] != want {
		t.Errorf("call = %+v, want %+v", calls[0], want)
	}
	if len(all) != 2 || all[0].Kind != provider.LLMToolCall || all[1].Kind != provider.LLMDone {
		t.Fatalf("chunks = %+v, want the call then done", all)
	}
	if all[1].FinishReason != provider.FinishStop {
		t.Errorf("finish reason = %q, want stop: a tool call is a completed turn", all[1].FinishReason)
	}
}

// TestLLMToolCallWithText: a spoken preamble and a call in one generation.
func TestLLMToolCallWithText(t *testing.T) {
	_, srv := newFakeChat(t,
		delta("Let me check that. "),
		toolFragment(0, "call_1", "take_message", `{}`),
		toolFinish(), "data: [DONE]")
	ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{
		Tools: []provider.ToolDef{{Name: "take_message"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	calls, all := collectCalls(t, ch)
	if len(calls) != 1 || calls[0].Name != "take_message" {
		t.Fatalf("tool calls = %+v", calls)
	}
	if len(all) != 3 || all[0].Kind != provider.LLMTextDelta || all[1].Kind != provider.LLMToolCall || all[2].Kind != provider.LLMDone {
		t.Fatalf("chunks = %+v, want text, call, done", all)
	}
}

// TestLLMToolCallCancelledMidArguments: cancelling while arguments are still
// fragmented must yield no call at all — a partial call never reaches the
// session (docs/protocol-profile.md §6).
func TestLLMToolCallCancelledMidArguments(t *testing.T) {
	f, srv := newFakeChat(t,
		toolFragment(0, "call_abc", "hangup", `{"reas`),
		toolFragment(0, "", "", `on":"done"}`),
		toolFinish(), "data: [DONE]")
	f.mu.Lock()
	f.hold = make(chan struct{})
	f.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := newTestLLM(srv.URL).Chat(ctx, provider.ChatRequest{Tools: []provider.ToolDef{{Name: "hangup"}}})
	if err != nil {
		t.Fatal(err)
	}
	// The handler holds after the first fragment, so cancelling now lands
	// mid-arguments.
	time.Sleep(50 * time.Millisecond)
	cancel()
	calls, _ := collectCalls(t, ch)
	if len(calls) != 0 {
		t.Fatalf("tool calls = %+v, want none after a mid-arguments cancel", calls)
	}
}

// TestLLMToolCallTruncatedArguments: max_output_tokens mid-arguments leaves
// the arguments unusable, so no call is delivered.
func TestLLMToolCallTruncatedArguments(t *testing.T) {
	_, srv := newFakeChat(t,
		toolFragment(0, "call_abc", "hangup", `{"reas`),
		`data: {"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}`,
		"data: [DONE]")
	ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{Tools: []provider.ToolDef{{Name: "hangup"}}})
	if err != nil {
		t.Fatal(err)
	}
	calls, all := collectCalls(t, ch)
	if len(calls) != 0 {
		t.Fatalf("tool calls = %+v, want none for a truncated generation", calls)
	}
	if len(all) != 1 || all[0].Kind != provider.LLMDone || all[0].FinishReason != provider.FinishLength {
		t.Fatalf("chunks = %+v, want done/length", all)
	}
}

// TestLLMMultipleToolCallsAreAnError: more than one call is reported, never
// silently dropped.
func TestLLMMultipleToolCallsAreAnError(t *testing.T) {
	_, srv := newFakeChat(t,
		toolFragment(0, "call_1", "hangup", `{}`),
		toolFragment(1, "call_2", "take_message", `{}`),
		toolFinish(), "data: [DONE]")
	ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{
		Tools: []provider.ToolDef{{Name: "hangup"}, {Name: "take_message"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	calls, all := collectCalls(t, ch)
	if len(calls) != 1 || calls[0].ID != "call_1" {
		t.Fatalf("tool calls = %+v, want only the first", calls)
	}
	if len(all) != 2 || all[1].Kind != provider.LLMError {
		t.Fatalf("chunks = %+v, want the first call then an error", all)
	}
	var perr *provider.Error
	if !errors.As(all[1].Err, &perr) {
		t.Fatalf("error = %v, want a provider.Error", all[1].Err)
	}
}

// TestLLMToolsSerialization: the nested Chat Completions shape, an explicit
// tool_choice and parallel_tool_calls:false.
func TestLLMToolsSerialization(t *testing.T) {
	f, srv := newFakeChat(t, delta("ok"), "data: [DONE]")
	ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{
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
	collect(t, ch)
	body := f.request()
	if body["tool_choice"] != "auto" {
		t.Errorf("tool_choice = %v, want %q", body["tool_choice"], "auto")
	}
	if body["parallel_tool_calls"] != false {
		t.Errorf("parallel_tool_calls = %v, want false", body["parallel_tool_calls"])
	}
	tools, _ := body["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("tools = %v", body["tools"])
	}
	tool := tools[0].(map[string]any)
	if tool["type"] != "function" {
		t.Errorf("tools[0].type = %v", tool["type"])
	}
	fn, _ := tool["function"].(map[string]any)
	if fn["name"] != "hangup" || fn["description"] != "end the call" {
		t.Errorf("tools[0].function = %v", fn)
	}
	if params, _ := fn["parameters"].(map[string]any); params["type"] != "object" {
		t.Errorf("tools[0].function.parameters = %v, want the schema passed through verbatim", fn["parameters"])
	}
}

// TestLLMForcedToolChoice: the object form of tool_choice.
func TestLLMForcedToolChoice(t *testing.T) {
	f, srv := newFakeChat(t, delta("ok"), "data: [DONE]")
	ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{
		Tools:      []provider.ToolDef{{Name: "hangup"}},
		ToolChoice: provider.ToolChoice{Mode: provider.ToolChoiceFunction, Name: "hangup"},
	})
	if err != nil {
		t.Fatal(err)
	}
	collect(t, ch)
	choice, _ := f.request()["tool_choice"].(map[string]any)
	if choice["type"] != "function" {
		t.Fatalf("tool_choice = %v", f.request()["tool_choice"])
	}
	if fn, _ := choice["function"].(map[string]any); fn["name"] != "hangup" {
		t.Errorf("tool_choice.function = %v", choice["function"])
	}
}

// TestLLMNoToolFieldsWithoutTools: the API rejects tool_choice and
// parallel_tool_calls on a request that declares no tools, so a tool-less
// session must send neither.
func TestLLMNoToolFieldsWithoutTools(t *testing.T) {
	f, srv := newFakeChat(t, delta("ok"), "data: [DONE]")
	ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{
		Messages:   []provider.Message{{Role: provider.RoleUser, Content: "hi"}},
		ToolChoice: provider.ToolChoice{Mode: provider.ToolChoiceAuto},
	})
	if err != nil {
		t.Fatal(err)
	}
	collect(t, ch)
	for _, k := range []string{"tools", "tool_choice", "parallel_tool_calls"} {
		if _, present := f.request()[k]; present {
			t.Errorf("%s is present on a request that declares no tools", k)
		}
	}
}

// TestLLMToolHistoryReplay: an assistant turn that spoke and called, then the
// tool result, projected onto the Chat Completions shape.
func TestLLMToolHistoryReplay(t *testing.T) {
	f, srv := newFakeChat(t, delta("ok"), "data: [DONE]")
	ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{
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
	collect(t, ch)
	msgs, _ := f.request()["messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("messages = %v", msgs)
	}
	assistant := msgs[1].(map[string]any)
	if assistant["content"] != "One moment." {
		t.Errorf("assistant.content = %v", assistant["content"])
	}
	tcs, _ := assistant["tool_calls"].([]any)
	if len(tcs) != 1 {
		t.Fatalf("assistant.tool_calls = %v", assistant["tool_calls"])
	}
	tc := tcs[0].(map[string]any)
	if tc["id"] != "call_abc" || tc["type"] != "function" {
		t.Errorf("tool_calls[0] = %v", tc)
	}
	fn, _ := tc["function"].(map[string]any)
	if fn["name"] != "transfer_to_agent" || fn["arguments"] != `{"department":"sales"}` {
		t.Errorf("tool_calls[0].function = %v", fn)
	}
	result := msgs[2].(map[string]any)
	if result["role"] != "tool" || result["tool_call_id"] != "call_abc" || result["content"] != "no agent available" {
		t.Errorf("tool result message = %v", result)
	}
	if _, present := msgs[0].(map[string]any)["tool_calls"]; present {
		t.Error("a plain user message must not carry tool_calls")
	}
}

// TestLLMToolChoiceModes: the wire mapping of every tool_choice form. Only
// "auto" is verified against the live DashScope endpoint in Phase 7 (see
// TestRealLLMToolCall); the other three are covered here only.
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
			f, srv := newFakeChat(t, delta("ok"), "data: [DONE]")
			ch, err := newTestLLM(srv.URL).Chat(context.Background(), provider.ChatRequest{
				Tools: []provider.ToolDef{{Name: "hangup"}}, ToolChoice: tc.choice,
			})
			if err != nil {
				t.Fatal(err)
			}
			collect(t, ch)
			if got := f.request()["tool_choice"]; got != tc.want {
				t.Errorf("tool_choice = %v, want %v", got, tc.want)
			}
		})
	}
}
