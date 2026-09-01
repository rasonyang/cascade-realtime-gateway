package openai

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// LLMOptions is the "options" block of providers.llm for type "openai".
// ReasoningEffort is passed through verbatim when set ("none", "minimal",
// "low", …); supported values differ per model and non-reasoning models
// reject the field, so it has no default.
type LLMOptions struct {
	httpOptions
	Model           string `json:"model"`
	ReasoningEffort string `json:"reasoning_effort"`
}

const defaultChatModel = "gpt-4o-mini"

// chunkQueue bounds chunks buffered between the SSE reader and the consumer.
const chunkQueue = 16

func init() {
	provider.Register(provider.KindLLM, Name, func(apiKey string, raw json.RawMessage) (provider.Provider, error) {
		var o LLMOptions
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &o); err != nil {
				return nil, fmt.Errorf("openai llm options: %w", err)
			}
		}
		return NewLLM(apiKey, o), nil
	})
}

// LLM is the Chat Completions streaming client.
type LLM struct {
	apiKey string
	opts   LLMOptions
	client *http.Client
}

// NewLLM builds the provider with defaults applied.
func NewLLM(apiKey string, o LLMOptions) *LLM {
	o.applyDefaults()
	if o.Model == "" {
		o.Model = defaultChatModel
	}
	return &LLM{apiKey: apiKey, opts: o, client: newClient(o.httpOptions)}
}

// Kind implements provider.Provider.
func (l *LLM) Kind() provider.Kind { return provider.KindLLM }

// wire shapes (never exported)
type chatMessage struct {
	Role       string         `json:"role"`
	Content    string         `json:"content"`
	ToolCalls  []chatToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

// chatToolCall is the request-side shape of a call replayed from history;
// deltaToolCall is the streamed, fragmented response-side shape.
type chatToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function chatCallFunction `json:"function"`
}

type chatCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type chatTool struct {
	Type     string           `json:"type"`
	Function chatToolFunction `json:"function"`
}

type chatToolFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// chatForcedTool is the object form of tool_choice.
type chatForcedTool struct {
	Type     string           `json:"type"`
	Function chatForcedByName `json:"function"`
}

type chatForcedByName struct {
	Name string `json:"name"`
}

type chatRequest struct {
	Model               string        `json:"model"`
	Messages            []chatMessage `json:"messages"`
	Stream              bool          `json:"stream"`
	StreamOptions       streamOptions `json:"stream_options"`
	MaxCompletionTokens int           `json:"max_completion_tokens,omitempty"`
	ReasoningEffort     string        `json:"reasoning_effort,omitempty"`
	Temperature         *float64      `json:"temperature,omitempty"`
	// The three tool fields are serialized only together with a non-empty
	// tools array: the API rejects tool_choice and parallel_tool_calls on a
	// request that declares no tools.
	Tools             []chatTool `json:"tools,omitempty"`
	ToolChoice        any        `json:"tool_choice,omitempty"`
	ParallelToolCalls *bool      `json:"parallel_tool_calls,omitempty"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// deltaToolCall is one fragment of a streamed tool call: id and name arrive
// on the first fragment for an index, arguments across the following ones.
type deltaToolCall struct {
	Index    int              `json:"index"`
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function chatCallFunction `json:"function"`
}

type chatChunk struct {
	Choices []struct {
		Delta struct {
			Content   string          `json:"content"`
			ToolCalls []deltaToolCall `json:"tool_calls"`
		} `json:"delta"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

// Chat implements provider.LLM: it returns as soon as the request has been
// issued; the stream is consumed by a goroutine that exits on ctx.Done.
func (l *LLM) Chat(ctx context.Context, req provider.ChatRequest) (<-chan provider.LLMChunk, error) {
	body := chatRequest{Model: l.opts.Model, Stream: true, StreamOptions: streamOptions{IncludeUsage: true},
		MaxCompletionTokens: req.MaxOutputTokens, ReasoningEffort: l.opts.ReasoningEffort,
		Temperature: req.Temperature}
	if req.Instructions != "" {
		body.Messages = append(body.Messages, chatMessage{Role: "system", Content: req.Instructions})
	}
	for _, m := range req.Messages {
		body.Messages = append(body.Messages, chatMessageOf(m))
	}
	if len(req.Tools) > 0 {
		body.Tools = chatTools(req.Tools)
		body.ToolChoice = chatToolChoice(req.ToolChoice)
		body.ParallelToolCalls = new(bool) // false: at most one call per response
	}
	out := make(chan provider.LLMChunk, chunkQueue)
	go l.stream(ctx, body, out)
	return out, nil
}

// chatMessageOf projects one history message onto the Chat Completions
// shape: an assistant turn that called a tool carries tool_calls, and a tool
// result is a role:"tool" message keyed by tool_call_id.
func chatMessageOf(m provider.Message) chatMessage {
	cm := chatMessage{Role: string(m.Role), Content: m.Content, ToolCallID: m.ToolCallID}
	for _, tc := range m.ToolCalls {
		cm.ToolCalls = append(cm.ToolCalls, chatToolCall{
			ID: tc.ID, Type: "function",
			Function: chatCallFunction{Name: tc.Name, Arguments: tc.Arguments},
		})
	}
	return cm
}

func chatTools(defs []provider.ToolDef) []chatTool {
	out := make([]chatTool, 0, len(defs))
	for _, d := range defs {
		out = append(out, chatTool{Type: "function", Function: chatToolFunction{
			Name: d.Name, Description: d.Description, Parameters: d.Parameters,
		}})
	}
	return out
}

// chatToolChoice serializes the effective choice; the zero value means auto.
func chatToolChoice(tc provider.ToolChoice) any {
	switch tc.Mode {
	case provider.ToolChoiceFunction:
		return chatForcedTool{Type: "function", Function: chatForcedByName{Name: tc.Name}}
	case "":
		return provider.ToolChoiceAuto
	}
	return tc.Mode
}

func (l *LLM) stream(ctx context.Context, body chatRequest, out chan<- provider.LLMChunk) {
	defer close(out)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	started := time.Now()
	reason := "completed"
	defer func() {
		slog.Debug("openai chat stream closed", "provider", Name, "reason", reason, "lifetime_ms", time.Since(started).Milliseconds())
	}()
	send := func(c provider.LLMChunk) bool {
		select {
		case out <- c:
			return true
		case <-ctx.Done():
			return false
		}
	}
	resp, err := postJSON(ctx, l.client, l.apiKey, l.opts.BaseURL+"/chat/completions", body)
	if err != nil {
		reason = "error"
		if ctx.Err() != nil {
			reason = "cancelled"
		}
		send(provider.LLMChunk{Kind: provider.LLMError, Err: err})
		return
	}
	defer resp.Body.Close()
	// Cancelling ctx closes the body (through the request's context), which
	// unblocks the reader immediately; the watchdog reuses the same path.
	idle := false
	wd := newIdleWatchdog(l.opts.StreamIdleTimeout.Std(), func() { idle = true; cancel() })
	defer wd.stop()

	reader := bufio.NewReader(resp.Body)
	var finish provider.FinishReason
	var usage provider.Usage
	var calls toolCalls
	// done flushes any complete tool call ahead of the Done chunk, so the
	// session never sees a partial call.
	done := func() {
		if !calls.emit(send, finishOr(finish)) {
			return
		}
		send(provider.LLMChunk{Kind: provider.LLMDone, FinishReason: finishOr(finish), Usage: usage})
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if ctx.Err() != nil {
				reason = "cancelled"
				if idle {
					reason = "idle_timeout"
					// ctx is already cancelled by the watchdog; best-effort
					// delivery into the buffered channel.
					select {
					case out <- provider.LLMChunk{Kind: provider.LLMError, Err: &provider.Error{Provider: Name, Kind: provider.ErrTransient, Err: errors.New("stream idle timeout")}}:
					default:
					}
				}
				return
			}
			if errors.Is(err, io.EOF) {
				// Stream ended without [DONE]; treat what we have as complete.
				done()
				return
			}
			reason = "error"
			send(provider.LLMChunk{Kind: provider.LLMError, Err: &provider.Error{Provider: Name, Kind: provider.ErrTransient, Err: err}})
			return
		}
		wd.progress()
		line = strings.TrimRight(line, "\r\n")
		if !strings.HasPrefix(line, "data:") {
			continue // comments, event: lines, blank separators
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			done()
			return
		}
		var chunk chatChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			send(provider.LLMChunk{Kind: provider.LLMError, Err: fatalf("malformed stream chunk: %v", err)})
			return
		}
		if chunk.Usage != nil {
			usage = provider.Usage{InputTokens: chunk.Usage.PromptTokens, OutputTokens: chunk.Usage.CompletionTokens}
		}
		for _, c := range chunk.Choices {
			if c.Delta.Content != "" {
				if !send(provider.LLMChunk{Kind: provider.LLMTextDelta, Text: c.Delta.Content}) {
					return
				}
			}
			for _, tc := range c.Delta.ToolCalls {
				calls.push(tc)
			}
			if c.FinishReason != "" {
				finish = mapFinish(c.FinishReason)
			}
		}
	}
}

func finishOr(f provider.FinishReason) provider.FinishReason {
	if f == "" {
		return provider.FinishStop
	}
	return f
}

// mapFinish maps the finish reason. "tool_calls" is a completed turn, not a
// truncated one, so it maps to FinishStop.
func mapFinish(s string) provider.FinishReason {
	switch s {
	case "length":
		return provider.FinishLength
	case "content_filter":
		return provider.FinishContentFilter
	}
	return provider.FinishStop
}

// toolCalls accumulates streamed tool-call fragments by index. Accumulation
// lives here, at the provider boundary: internal/session only ever sees a
// complete call.
type toolCalls struct {
	byIndex map[int]*provider.ToolCall
	order   []int
}

func (t *toolCalls) push(d deltaToolCall) {
	if t.byIndex == nil {
		t.byIndex = map[int]*provider.ToolCall{}
	}
	c, ok := t.byIndex[d.Index]
	if !ok {
		c = &provider.ToolCall{}
		t.byIndex[d.Index] = c
		t.order = append(t.order, d.Index)
	}
	if d.ID != "" {
		c.ID = d.ID
	}
	if d.Function.Name != "" {
		c.Name = d.Function.Name
	}
	c.Arguments += d.Function.Arguments
}

// emit delivers at most one complete call and reports whether the stream may
// continue to its Done chunk. A truncated generation (length, content_filter)
// leaves the arguments unusable, so nothing is delivered; more than one call
// is an error rather than a silent drop (docs/protocol-profile.md §10).
func (t *toolCalls) emit(send func(provider.LLMChunk) bool, finish provider.FinishReason) bool {
	if len(t.order) == 0 || finish != provider.FinishStop {
		return true
	}
	if !send(provider.LLMChunk{Kind: provider.LLMToolCall, ToolCall: *t.byIndex[t.order[0]]}) {
		return false
	}
	if len(t.order) > 1 {
		send(provider.LLMChunk{Kind: provider.LLMError, Err: fatalf(
			"model returned %d tool calls; at most one per response is supported", len(t.order))})
		return false
	}
	return true
}
