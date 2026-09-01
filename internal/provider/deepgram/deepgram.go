// Package deepgram implements the ASR provider on Deepgram's streaming
// speech-to-text WebSocket (/v1/listen), registered as "deepgram".
//
// Layering: deepgram imports provider, config and audio only. Deepgram's
// message shapes never leave the package. Mapping: interim Results →
// Partial; is_final Results → Final; speech_final / UtteranceEnd →
// EndOfTurn; Finalize sends Deepgram's Finalize control message and, because
// the from_finalize reply is not guaranteed by Deepgram, synthesizes
// EndOfTurn after finalize_timeout so the session's contract holds.
package deepgram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// Name is the registry name.
const Name = "deepgram"

// Options is the "options" block of providers.asr for type "deepgram".
type Options struct {
	BaseURL           string          `json:"base_url"`
	Model             string          `json:"model"`
	Language          string          `json:"language"`
	SmartFormat       bool            `json:"smart_format"`
	EndpointingMs     int             `json:"endpointing_ms"`
	UtteranceEndMs    int             `json:"utterance_end_ms"`
	ConnectTimeout    config.Duration `json:"connect_timeout"`
	FinalizeTimeout   config.Duration `json:"finalize_timeout"`
	KeepAliveInterval config.Duration `json:"keepalive_interval"`
	// AudioQueueFrames bounds audio buffered ahead of the socket writer; a
	// full queue makes PushAudio fail, which the session treats as fatal.
	AudioQueueFrames int `json:"audio_queue_frames"`
}

const (
	defaultBaseURL           = "wss://api.deepgram.com/v1/listen"
	defaultModel             = "nova-3"
	defaultEndpointingMs     = 300
	defaultUtteranceEndMs    = 1000
	defaultConnectTimeout    = 10 * time.Second
	defaultFinalizeTimeout   = 1500 * time.Millisecond
	defaultKeepAliveInterval = 3 * time.Second
	defaultAudioQueueFrames  = 500 // 10 s of 20 ms frames
	eventQueue               = 32
)

func (o *Options) applyDefaults() {
	if o.BaseURL == "" {
		o.BaseURL = defaultBaseURL
	}
	if o.Model == "" {
		o.Model = defaultModel
	}
	if o.EndpointingMs == 0 {
		o.EndpointingMs = defaultEndpointingMs
	}
	if o.UtteranceEndMs == 0 {
		o.UtteranceEndMs = defaultUtteranceEndMs
	}
	if o.ConnectTimeout == 0 {
		o.ConnectTimeout = config.Duration(defaultConnectTimeout)
	}
	if o.FinalizeTimeout == 0 {
		o.FinalizeTimeout = config.Duration(defaultFinalizeTimeout)
	}
	if o.KeepAliveInterval == 0 {
		o.KeepAliveInterval = config.Duration(defaultKeepAliveInterval)
	}
	if o.AudioQueueFrames == 0 {
		o.AudioQueueFrames = defaultAudioQueueFrames
	}
}

func init() {
	provider.Register(provider.KindASR, Name, func(apiKey string, raw json.RawMessage) (provider.Provider, error) {
		var o Options
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &o); err != nil {
				return nil, fmt.Errorf("deepgram options: %w", err)
			}
		}
		return New(apiKey, o), nil
	})
}

// ASR is the provider.
type ASR struct {
	apiKey string
	opts   Options
}

// New builds the provider with defaults applied.
func New(apiKey string, o Options) *ASR {
	o.applyDefaults()
	return &ASR{apiKey: apiKey, opts: o}
}

// Kind implements provider.Provider.
func (a *ASR) Kind() provider.Kind { return provider.KindASR }

// OpenStream implements provider.ASR: it dials Deepgram (bounded by
// connect_timeout) and starts the reader and writer goroutines.
func (a *ASR) OpenStream(ctx context.Context, cfg provider.ASRConfig) (provider.ASRStream, error) {
	u, err := url.Parse(a.opts.BaseURL)
	if err != nil {
		return nil, &provider.Error{Provider: Name, Kind: provider.ErrFatal, Err: err}
	}
	q := u.Query()
	q.Set("model", a.opts.Model)
	q.Set("encoding", "linear16")
	q.Set("sample_rate", strconv.Itoa(cfg.SampleRate))
	q.Set("channels", "1")
	q.Set("interim_results", "true")
	q.Set("punctuate", "true")
	q.Set("endpointing", strconv.Itoa(a.opts.EndpointingMs))
	q.Set("utterance_end_ms", strconv.Itoa(a.opts.UtteranceEndMs))
	if a.opts.SmartFormat {
		q.Set("smart_format", "true")
	}
	if lang := firstNonEmpty(cfg.Language, a.opts.Language); lang != "" {
		q.Set("language", lang)
	}
	u.RawQuery = q.Encode()

	dialCtx, cancel := context.WithTimeout(ctx, a.opts.ConnectTimeout.Std())
	defer cancel()
	conn, resp, err := websocket.Dial(dialCtx, u.String(), &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Token " + a.apiKey}},
	})
	if err != nil {
		if resp != nil {
			return nil, provider.ErrorFromStatus(Name, resp.StatusCode, err.Error())
		}
		return nil, &provider.Error{Provider: Name, Kind: provider.ErrTransient, Err: err}
	}
	sctx, scancel := context.WithCancel(ctx)
	s := &stream{
		opts:      a.opts,
		conn:      conn,
		ctx:       sctx,
		cancel:    scancel,
		in:        make(chan []byte, a.opts.AudioQueueFrames),
		ctl:       make(chan string, 4),
		events:    make(chan provider.ASREvent, eventQueue),
		done:      make(chan struct{}),
		closeSent: make(chan struct{}),
		openedAt:  time.Now(),
	}
	slog.Debug("deepgram stream opened", "provider", Name, "model", a.opts.Model)
	go s.reader()
	go s.writer()
	return s, nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// ---- stream ----------------------------------------------------------------

type stream struct {
	opts   Options
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	in     chan []byte
	ctl    chan string // control JSON messages
	events chan provider.ASREvent
	done   chan struct{}

	closeSent chan struct{} // closed by the writer once CloseStream is on the wire
	openedAt  time.Time

	mu            sync.Mutex
	finalizeTimer *time.Timer
	closed        bool
}

const closeStreamMsg = `{"type":"CloseStream"}`

// Deepgram messages (never exported).
type dgMessage struct {
	Type         string  `json:"type"`
	IsFinal      bool    `json:"is_final"`
	SpeechFinal  bool    `json:"speech_final"`
	FromFinalize bool    `json:"from_finalize"`
	Start        float64 `json:"start"`
	Duration     float64 `json:"duration"`
	Channel      struct {
		Alternatives []struct {
			Transcript string `json:"transcript"`
		} `json:"alternatives"`
	} `json:"channel"`
	Description string `json:"description"` // Error messages
	Message     string `json:"message"`
}

// PushAudio implements provider.ASRStream; it never blocks on the network.
func (s *stream) PushAudio(pcm []byte) error {
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	select {
	case s.in <- pcm:
		return nil
	default:
		return &provider.Error{Provider: Name, Kind: provider.ErrTransient, Err: errors.New("audio queue full: socket writer stalled")}
	}
}

// Finalize implements provider.ASRStream.
func (s *stream) Finalize() error {
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	s.mu.Lock()
	if s.finalizeTimer != nil {
		s.finalizeTimer.Stop()
	}
	s.finalizeTimer = time.AfterFunc(s.opts.FinalizeTimeout.Std(), s.finalizeTimedOut)
	s.mu.Unlock()
	select {
	case s.ctl <- `{"type":"Finalize"}`:
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
	return nil
}

// finalizeTimedOut honors the contract when Deepgram sends no from_finalize
// result (documented as not guaranteed when little audio is pending).
func (s *stream) finalizeTimedOut() {
	s.mu.Lock()
	s.finalizeTimer = nil
	s.mu.Unlock()
	s.emit(provider.ASREvent{Kind: provider.ASREndOfTurn})
}

func (s *stream) finalizeSatisfied() {
	s.mu.Lock()
	if s.finalizeTimer != nil {
		s.finalizeTimer.Stop()
		s.finalizeTimer = nil
	}
	s.mu.Unlock()
}

// Events implements provider.ASRStream.
func (s *stream) Events() <-chan provider.ASREvent { return s.events }

// Close implements provider.ASRStream: CloseStream is sent best-effort,
// then the socket is torn down and every goroutine reclaimed.
func (s *stream) Close() error {
	s.mu.Lock()
	already := s.closed
	s.closed = true
	if s.finalizeTimer != nil {
		s.finalizeTimer.Stop()
		s.finalizeTimer = nil
	}
	s.mu.Unlock()
	if !already {
		// Let the writer put CloseStream on the wire (bounded by
		// connect_timeout) before tearing the socket down.
		select {
		case s.ctl <- closeStreamMsg:
			select {
			case <-s.closeSent:
			case <-s.done:
			case <-time.After(s.opts.ConnectTimeout.Std()):
			}
		case <-s.done:
		}
		s.cancel()
	}
	<-s.done
	return nil
}

func (s *stream) emit(ev provider.ASREvent) bool {
	select {
	case s.events <- ev:
		return true
	case <-s.ctx.Done():
		return false
	}
}

// writer owns conn.Write: audio, control messages and keep-alives.
func (s *stream) writer() {
	defer s.cancel()
	keepalive := time.NewTicker(s.opts.KeepAliveInterval.Std())
	defer keepalive.Stop()
	idle := true
	for {
		select {
		case <-s.ctx.Done():
			return
		case pcm := <-s.in:
			idle = false
			if err := s.conn.Write(s.ctx, websocket.MessageBinary, pcm); err != nil {
				return
			}
		case msg := <-s.ctl:
			if err := s.conn.Write(s.ctx, websocket.MessageText, []byte(msg)); err != nil {
				return
			}
			if msg == closeStreamMsg {
				close(s.closeSent)
				return
			}
		case <-keepalive.C:
			if idle {
				if err := s.conn.Write(s.ctx, websocket.MessageText, []byte(`{"type":"KeepAlive"}`)); err != nil {
					return
				}
			}
			idle = true
		}
	}
}

// reader owns conn.Read and maps messages to ASR events. It closes the
// socket and the events channel on exit, so cancellation tears everything
// down immediately.
func (s *stream) reader() {
	defer close(s.done)
	defer close(s.events)
	defer s.conn.CloseNow() //nolint:errcheck
	defer s.cancel()
	reason := "cancelled"
	defer func() {
		slog.Debug("deepgram stream closed", "provider", Name, "reason", reason, "lifetime_ms", time.Since(s.openedAt).Milliseconds())
	}()
	for {
		typ, data, err := s.conn.Read(s.ctx)
		if err != nil {
			if s.ctx.Err() != nil {
				return // cancelled or closed by us
			}
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed || websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				reason = "closed"
				return
			}
			reason = "error"
			s.emit(provider.ASREvent{Kind: provider.ASRError, Err: &provider.Error{Provider: Name, Kind: provider.ErrTransient, Err: err}})
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var m dgMessage
		if err := json.Unmarshal(data, &m); err != nil {
			continue
		}
		if !s.handle(m) {
			return
		}
	}
}

func (s *stream) handle(m dgMessage) bool {
	switch m.Type {
	case "Results":
		text := ""
		if len(m.Channel.Alternatives) > 0 {
			text = m.Channel.Alternatives[0].Transcript
		}
		if m.FromFinalize {
			s.finalizeSatisfied()
		}
		switch {
		case !m.IsFinal:
			if text != "" {
				return s.emit(provider.ASREvent{Kind: provider.ASRPartial, Text: text})
			}
		case text != "":
			start := int(m.Start * 1000)
			ev := provider.ASREvent{Kind: provider.ASRFinal, Text: text, StartMs: start, EndMs: start + int(m.Duration*1000)}
			if !s.emit(ev) {
				return false
			}
			if m.SpeechFinal {
				return s.emit(provider.ASREvent{Kind: provider.ASREndOfTurn})
			}
		case m.FromFinalize:
			// Finalized with nothing pending: the utterance is over.
			return s.emit(provider.ASREvent{Kind: provider.ASREndOfTurn})
		}
	case "UtteranceEnd":
		return s.emit(provider.ASREvent{Kind: provider.ASREndOfTurn})
	case "Error":
		msg := firstNonEmpty(m.Description, m.Message)
		return s.emit(provider.ASREvent{Kind: provider.ASRError, Err: &provider.Error{Provider: Name, Kind: provider.ErrTransient, Err: errors.New(msg)}})
	}
	return true
}
