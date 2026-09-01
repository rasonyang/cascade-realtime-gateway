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
type LLMOptions struct {
	httpOptions
	Model string `json:"model"`
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
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model               string        `json:"model"`
	Messages            []chatMessage `json:"messages"`
	Stream              bool          `json:"stream"`
	StreamOptions       streamOptions `json:"stream_options"`
	MaxCompletionTokens int           `json:"max_completion_tokens,omitempty"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

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
// issued; the stream is consumed by a goroutine that exits on ctx.Done.
func (l *LLM) Chat(ctx context.Context, req provider.ChatRequest) (<-chan provider.LLMChunk, error) {
	body := chatRequest{Model: l.opts.Model, Stream: true, StreamOptions: streamOptions{IncludeUsage: true}, MaxCompletionTokens: req.MaxOutputTokens}
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
				send(provider.LLMChunk{Kind: provider.LLMDone, FinishReason: finishOr(finish), Usage: usage})
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
			send(provider.LLMChunk{Kind: provider.LLMDone, FinishReason: finishOr(finish), Usage: usage})
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

func mapFinish(s string) provider.FinishReason {
	switch s {
	case "length":
		return provider.FinishLength
	case "content_filter":
		return provider.FinishContentFilter
	}
	return provider.FinishStop
}
