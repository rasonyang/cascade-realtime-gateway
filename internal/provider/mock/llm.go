package mock

import (
	"context"
	"sync"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// LLMScript drives the mock generator: Tokens are emitted one per chunk,
// then Done with FinishReason and Usage. With Block set, the generation
// emits BlockAfter tokens and then blocks until ctx is cancelled. Err, when
// set, is emitted instead of Done after all tokens; ChatErr fails Chat itself.
type LLMScript struct {
	Tokens       []string              `json:"tokens"`
	FinishReason provider.FinishReason `json:"finish_reason"`
	Usage        provider.Usage        `json:"-"`
	TokenDelay   Duration              `json:"token_delay"`
	Block        bool                  `json:"block"`
	BlockAfter   int                   `json:"block_after"`
	Err          error                 `json:"-"`
	ChatErr      error                 `json:"-"`

	// ToolCall, when set, is emitted as one complete call after every token
	// and before Done — the shape a real adapter produces once it has
	// accumulated the argument fragments. An empty Tokens list therefore
	// scripts a call-only turn, and a non-empty one a text+call turn.
	ToolCall *provider.ToolCall `json:"-"`
	// BlockAfterToolCall blocks until ctx is cancelled once the call has been
	// delivered, which is how "cancelled after a delivered call" is scripted.
	// Block with BlockAfter inside Tokens covers the opposite case.
	BlockAfterToolCall bool `json:"-"`
}

// LLM is the mock provider. It records every request and the moment a
// blocked generation observed cancellation.
type LLM struct {
	script LLMScript

	// OnChat, when set before the first request, is called synchronously at
	// the top of Chat so tests can observe what had happened by then.
	OnChat func(provider.ChatRequest)

	mu          sync.Mutex
	requests    []provider.ChatRequest
	cancelledAt time.Time
	active      int
}

// NewLLM builds the provider.
func NewLLM(s LLMScript) *LLM {
	return &LLM{script: normalizeScript(s)}
}

func normalizeScript(s LLMScript) LLMScript {
	if s.FinishReason == "" {
		s.FinishReason = provider.FinishStop
	}
	return s
}

// SetScript replaces the script used by subsequent generations, so a test can
// script a second turn differently from the first — a tool call answered by a
// spoken reply, for instance.
func (l *LLM) SetScript(s LLMScript) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.script = normalizeScript(s)
}

// Kind implements provider.Provider.
func (l *LLM) Kind() provider.Kind { return provider.KindLLM }

// Requests returns every ChatRequest received so far.
func (l *LLM) Requests() []provider.ChatRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]provider.ChatRequest(nil), l.requests...)
}

// CancelledAt returns when a generation observed ctx cancellation.
func (l *LLM) CancelledAt() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cancelledAt
}

// Active returns the number of generations currently running.
func (l *LLM) Active() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.active
}

// Chat implements provider.LLM.
func (l *LLM) Chat(ctx context.Context, req provider.ChatRequest) (<-chan provider.LLMChunk, error) {
	if l.OnChat != nil {
		l.OnChat(req)
	}
	l.mu.Lock()
	chatErr := l.script.ChatErr
	l.mu.Unlock()
	if chatErr != nil {
		return nil, chatErr
	}
	l.mu.Lock()
	l.requests = append(l.requests, req)
	l.active++
	script := l.script
	l.mu.Unlock()
	out := make(chan provider.LLMChunk, llmChunkQueue)
	go l.generate(ctx, out, script)
	return out, nil
}

func (l *LLM) generate(ctx context.Context, out chan<- provider.LLMChunk, script LLMScript) {
	defer close(out)
	defer func() {
		l.mu.Lock()
		l.active--
		l.mu.Unlock()
	}()
	send := func(c provider.LLMChunk) bool {
		select {
		case out <- c:
			return true
		case <-ctx.Done():
			l.markCancelled()
			return false
		}
	}
	block := func() {
		<-ctx.Done()
		l.markCancelled()
		// Best-effort, non-blocking: the buffered channel normally has room.
		select {
		case out <- provider.LLMChunk{Kind: provider.LLMError, Err: ctx.Err()}:
		default:
		}
	}
	for i, tok := range script.Tokens {
		if script.Block && i == script.BlockAfter {
			block()
			return
		}
		if d := time.Duration(script.TokenDelay); d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				l.markCancelled()
				return
			}
		}
		if !send(provider.LLMChunk{Kind: provider.LLMTextDelta, Text: tok}) {
			return
		}
	}
	if script.Block && script.BlockAfter >= len(script.Tokens) {
		block()
		return
	}
	if tc := script.ToolCall; tc != nil {
		if !send(provider.LLMChunk{Kind: provider.LLMToolCall, ToolCall: *tc}) {
			return
		}
		if script.BlockAfterToolCall {
			block()
			return
		}
	}
	if script.Err != nil {
		send(provider.LLMChunk{Kind: provider.LLMError, Err: script.Err})
		return
	}
	send(provider.LLMChunk{Kind: provider.LLMDone, FinishReason: script.FinishReason, Usage: script.Usage})
}

func (l *LLM) markCancelled() {
	l.mu.Lock()
	if l.cancelledAt.IsZero() {
		l.cancelledAt = time.Now()
	}
	l.mu.Unlock()
}
