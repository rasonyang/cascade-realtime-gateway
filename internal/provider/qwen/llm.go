package qwen

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// LLMOptions is the "options" block of providers.llm for type "qwen".
type LLMOptions struct {
	BaseURL           string          `json:"base_url"`
	Model             string          `json:"model"`
	RequestTimeout    config.Duration `json:"request_timeout"`     // until response headers arrive
	StreamIdleTimeout config.Duration `json:"stream_idle_timeout"` // max gap between streamed chunks
}

const (
	defaultChatModel         = "qwen3.6-flash"
	defaultRequestTimeout    = 30 * time.Second
	defaultStreamIdleTimeout = 30 * time.Second
	// chunkQueue bounds chunks buffered between the SSE reader and the consumer.
	chunkQueue = 16
)

func (o *LLMOptions) applyDefaults() {
	if o.BaseURL == "" {
		o.BaseURL = defaultLLMBaseURL
	}
	o.BaseURL = strings.TrimRight(o.BaseURL, "/")
	if o.Model == "" {
		o.Model = defaultChatModel
	}
	if o.RequestTimeout == 0 {
		o.RequestTimeout = config.Duration(defaultRequestTimeout)
	}
	if o.StreamIdleTimeout == 0 {
		o.StreamIdleTimeout = config.Duration(defaultStreamIdleTimeout)
	}
}

func init() {
	provider.Register(provider.KindLLM, Name, func(apiKey string, raw json.RawMessage) (provider.Provider, error) {
		var o LLMOptions
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &o); err != nil {
				return nil, fmt.Errorf("qwen llm options: %w", err)
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
	return &LLM{apiKey: apiKey, opts: o, client: &http.Client{Transport: &http.Transport{
		ResponseHeaderTimeout: o.RequestTimeout.Std(),
		ForceAttemptHTTP2:     true,
	}}}
}

// Kind implements provider.Provider.
func (l *LLM) Kind() provider.Kind { return provider.KindLLM }

// wire shapes (never exported)
type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// chatRequest carries Qwen's enable_thinking extension. It is always sent as
// false: when the field is omitted the model streams reasoning_content
// first, which adds seconds of time-to-first-token and produces text a voice
// turn must not speak.
type chatRequest struct {
	Model          string        `json:"model"`
	Messages       []chatMessage `json:"messages"`
	Stream         bool          `json:"stream"`
	StreamOptions  streamOptions `json:"stream_options"`
	MaxTokens      int           `json:"max_tokens,omitempty"`
	EnableThinking bool          `json:"enable_thinking"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// chatChunk deliberately has no field for delta.reasoning_content: reasoning
// output is never surfaced. Decoding drops it.
type chatChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

// Chat implements provider.LLM: it returns as soon as the request has been
// built; the stream is consumed by a goroutine that exits on ctx.Done.
func (l *LLM) Chat(ctx context.Context, req provider.ChatRequest) (<-chan provider.LLMChunk, error) {
	body := chatRequest{
		Model:          l.opts.Model,
		Stream:         true,
		StreamOptions:  streamOptions{IncludeUsage: true},
		MaxTokens:      req.MaxOutputTokens,
		EnableThinking: false,
	}
	if req.Instructions != "" {
		body.Messages = append(body.Messages, chatMessage{Role: "system", Content: req.Instructions})
	}
	for _, m := range req.Messages {
		body.Messages = append(body.Messages, chatMessage{Role: string(m.Role), Content: m.Content})
	}
	out := make(chan provider.LLMChunk, chunkQueue)
	go l.stream(ctx, body, out)
	return out, nil
}

func (l *LLM) stream(ctx context.Context, body chatRequest, out chan<- provider.LLMChunk) {
	defer close(out)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	started := time.Now()
	reason := "completed"
	defer func() {
		slog.Debug("qwen chat stream closed", "provider", Name, "reason", reason, "lifetime_ms", time.Since(started).Milliseconds())
	}()
	send := func(c provider.LLMChunk) bool {
		select {
		case out <- c:
			return true
		case <-ctx.Done():
			return false
		}
	}
	resp, err := l.post(ctx, body)
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
	wd := time.AfterFunc(l.opts.StreamIdleTimeout.Std(), func() { idle = true; cancel() })
	defer wd.Stop()

	reader := bufio.NewReader(resp.Body)
	var finish provider.FinishReason
	var usage provider.Usage
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
					case out <- provider.LLMChunk{Kind: provider.LLMError, Err: transientf("stream idle timeout")}:
					default:
					}
				}
				return
			}
			if errors.Is(err, io.EOF) {
				// Stream ended without [DONE]; treat what we have as complete.
				send(provider.LLMChunk{Kind: provider.LLMDone, FinishReason: finishOr(finish), Usage: usage})
				return
			}
			reason = "error"
			send(provider.LLMChunk{Kind: provider.LLMError, Err: transientf("read stream: %v", err)})
			return
		}
		wd.Reset(l.opts.StreamIdleTimeout.Std())
		line = strings.TrimRight(line, "\r\n")
		if !strings.HasPrefix(line, "data:") {
			continue // comments, event: lines, blank separators
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			send(provider.LLMChunk{Kind: provider.LLMDone, FinishReason: finishOr(finish), Usage: usage})
			return
		}
		var chunk chatChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			reason = "error"
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
			if c.FinishReason != "" {
				finish = mapFinish(c.FinishReason)
			}
		}
	}
}

// post issues the authenticated streaming request and classifies non-2xx
// statuses. The response body is left open for the caller to stream.
func (l *LLM) post(ctx context.Context, body chatRequest) (*http.Response, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, fatalf("encode request: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.opts.BaseURL+"/chat/completions", bytes.NewReader(buf))
	if err != nil {
		return nil, fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+l.apiKey)
	resp, err := l.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, transientf("request: %v", err)
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, provider.ErrorFromStatus(Name, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return resp, nil
}

func finishOr(f provider.FinishReason) provider.FinishReason {
	if f == "" {
		return provider.FinishStop
	}
	return f
}

func mapFinish(s string) provider.FinishReason {
	switch s {
	case "length":
		return provider.FinishLength
	case "content_filter":
		return provider.FinishContentFilter
	}
	return provider.FinishStop
}
