package openai

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// The Realtime transport of the TTS adapter. OpenAI retires /v1/audio/speech
// models (tts-1, tts-1-hd, gpt-4o-mini-tts and its snapshots) on 2027-01-06
// and names gpt-realtime-2.1-mini as the replacement, so a model whose ID
// contains "realtime" is synthesized over a Realtime WebSocket instead.
//
// The Realtime API is a conversational model, not a speech endpoint. It is
// turned into one by (a) replacing the session's persona with a read-aloud
// prompt and (b) sending every text segment as an out-of-band response
// (conversation "none", no input items) whose instructions carry the text.
// The model reads text verbatim in practice but may verbalize URLs and
// symbols ("example dot com slash help"); docs/decisions.md records this.
// It also pads its audio with silence: pauses of up to ~2 s at commas, and
// after the speech a tail of near-silent hiss that lasts seconds in roughly
// one segment in ten. silenceGate (below) trims that padding per segment.

const (
	defaultRealtimeVoice = "marin"
	realtimeTextQueue    = 32
	realtimeReadLimit    = 8 << 20
	// realtimeReadAloud replaces the default session instructions, which
	// describe a chatty assistant that answers and calls functions.
	realtimeReadAloud = "You are a text-to-speech engine. Read the text in the instructions aloud exactly as written, " +
		"verbatim, in the same language. Do not answer, translate, comment on, or add anything."
	// realtimeSayPrefix introduces each segment in response.instructions.
	realtimeSayPrefix = "Say exactly the following, verbatim, and nothing else:\n"

	// silenceGate limits (20 ms frames). Measured speech RMS is 1000-10000;
	// the padding sits below 100.
	gateFrame     = 20 * audio.BytesPerMs
	gateThreshold = 200 // RMS below this is silence
	gateLeadKeep  = 3   // 60 ms kept before the first speech of a segment
	gateGapKeep   = 15  // 300 ms kept for any silence inside a segment
	gateTailKeep  = 5   // 100 ms kept after the last speech of a segment
)

// isRealtimeModel reports whether model is served over the Realtime API.
func isRealtimeModel(model string) bool {
	return strings.Contains(strings.ToLower(model), "realtime")
}

// realtimeURL derives the Realtime WebSocket URL from the HTTP base URL.
func realtimeURL(baseURL, model string) string {
	u := baseURL
	switch {
	case strings.HasPrefix(u, "https://"):
		u = "wss://" + strings.TrimPrefix(u, "https://")
	case strings.HasPrefix(u, "http://"):
		u = "ws://" + strings.TrimPrefix(u, "http://")
	}
	return u + "/realtime?model=" + url.QueryEscape(model)
}

// ---- wire shapes (never exported) ------------------------------------------

type rtSessionUpdate struct {
	Type    string    `json:"type"`
	Session rtSession `json:"session"`
}

type rtSession struct {
	Type             string      `json:"type"`
	OutputModalities []string    `json:"output_modalities"`
	Instructions     string      `json:"instructions"`
	Reasoning        rtReasoning `json:"reasoning"`
	Audio            rtAudio     `json:"audio"`
}

type rtReasoning struct {
	Effort string `json:"effort"`
}

type rtAudio struct {
	Output rtAudioOutput `json:"output"`
}

type rtAudioOutput struct {
	Format rtFormat `json:"format"`
	Voice  string   `json:"voice"`
	Speed  float64  `json:"speed,omitempty"`
}

type rtFormat struct {
	Type string `json:"type"`
	Rate int    `json:"rate"`
}

type rtResponseCreate struct {
	Type     string     `json:"type"`
	Response rtResponse `json:"response"`
}

type rtResponse struct {
	Conversation     string   `json:"conversation"`
	Input            []any    `json:"input"`
	OutputModalities []string `json:"output_modalities"`
	Instructions     string   `json:"instructions"`
}

type rtError struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type rtEvent struct {
	Type     string   `json:"type"`
	Delta    string   `json:"delta"`
	Error    *rtError `json:"error"`
	Response struct {
		Status        string `json:"status"`
		StatusDetails struct {
			Reason string   `json:"reason"`
			Error  *rtError `json:"error"`
		} `json:"status_details"`
	} `json:"response"`
}

// rtProviderError classifies a Realtime error object.
func rtProviderError(e *rtError) *provider.Error {
	if e == nil {
		return &provider.Error{Provider: Name, Kind: provider.ErrTransient, Err: errors.New("realtime error without details")}
	}
	kind := provider.ErrFatal
	switch {
	case e.Code == "invalid_api_key" || e.Type == "authentication_error":
		kind = provider.ErrAuth
	case strings.Contains(e.Code, "rate_limit") || strings.Contains(e.Code, "quota") || strings.Contains(e.Type, "rate_limit"):
		kind = provider.ErrRateLimit
	case e.Type == "server_error":
		kind = provider.ErrTransient
	}
	return &provider.Error{Provider: Name, Kind: kind, Err: fmt.Errorf("realtime %s/%s: %s", e.Type, e.Code, e.Message)}
}

// silenceGate removes the model's silence padding from one segment's PCM:
// leading silence is cut to gateLeadKeep frames, a silent run inside the
// segment to gateGapKeep, and trailing silence to gateTailKeep. Silent frames
// are held back until speech resumes (or the segment ends), so memory is
// bounded by gateGapKeep frames. Dropping is lossless for speech and also
// makes first audio arrive sooner; the pipeline sees only the bytes pushed
// out, so its audio accounting stays consistent.
type silenceGate struct {
	part    []byte // incomplete frame
	held    []byte // silent frames waiting for the next speech frame
	started bool   // speech seen in this segment
}

func (g *silenceGate) keep() int {
	if !g.started {
		return gateLeadKeep
	}
	return gateGapKeep
}

// push consumes pcm (any length) and returns the bytes to forward.
func (g *silenceGate) push(pcm []byte) []byte {
	g.part = append(g.part, pcm...)
	var out []byte
	for len(g.part) >= gateFrame {
		f := g.part[:gateFrame]
		if frameRMS(f) >= gateThreshold {
			out = append(out, g.held...)
			out = append(out, f...)
			g.held = g.held[:0]
			g.started = true
		} else if len(g.held) < g.keep()*gateFrame {
			g.held = append(g.held, f...)
		} else if !g.started {
			// Leading silence: keep the most recent frames only.
			g.held = append(g.held[gateFrame:], f...)
		}
		g.part = g.part[gateFrame:]
	}
	g.part = append([]byte(nil), g.part...)
	return out
}

// finish ends the segment: the tail keeps at most gateTailKeep silent frames
// (nothing at all if the segment never held speech) and a sub-frame
// remainder is dropped.
func (g *silenceGate) finish() []byte {
	var out []byte
	if g.started {
		out = g.held[:min(len(g.held), gateTailKeep*gateFrame)]
	}
	*g = silenceGate{}
	return out
}

func frameRMS(f []byte) float64 {
	var sum float64
	n := len(f) / audio.BytesPerSample
	for i := 0; i < n; i++ {
		v := float64(audio.Sample(f, i))
		sum += v * v
	}
	return math.Sqrt(sum / float64(n))
}

// ---- stream ----------------------------------------------------------------

// synthesizeRealtime opens a stream without waiting for the handshake: the
// owning goroutine dials immediately, and WriteText only queues, so the
// 1-1.5 s connection cost overlaps whatever the caller does next (the
// session pipeline starts the stream when the response begins, while the LLM
// is still generating its first sentence). Dial and setup failures surface
// from ReadAudio.
func (t *TTS) synthesizeRealtime(ctx context.Context, cfg provider.TTSConfig) (provider.TTSStream, error) {
	sctx, cancel := context.WithCancel(ctx)
	s := &rtStream{
		tts:    t,
		voice:  firstNonEmpty(cfg.Voice, defaultRealtimeVoice),
		ctx:    sctx,
		cancel: cancel,
		text:   make(chan string, realtimeTextQueue),
		chunks: make(chan provider.AudioChunk, audioQueue),
		errc:   make(chan error, 1),
	}
	if cfg.Speed != 0 && cfg.Speed != 1 {
		s.speed = cfg.Speed
	}
	go s.run()
	return s, nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// rtStream owns one Realtime connection for the life of one Synthesize
// call. A single goroutine does all socket I/O: segments are strictly
// serialized (the API runs one response at a time anyway), so it writes a
// response.create and then reads until that response is done.
type rtStream struct {
	tts    *TTS
	voice  string
	speed  float64
	ctx    context.Context
	cancel context.CancelFunc

	text   chan string // segments waiting for the sequencer
	chunks chan provider.AudioChunk
	errc   chan error // terminal outcome, nil on a clean finish

	// endMu makes WriteText and EndInput safe against each other: EndInput
	// closes text, which a concurrent WriteText must never send on.
	endMu sync.RWMutex
	ended bool

	mu    sync.Mutex
	done  bool
	final error
}

// WriteText queues one segment; it never touches the network.
func (s *rtStream) WriteText(text string) error {
	s.endMu.RLock()
	defer s.endMu.RUnlock()
	if s.ended {
		return io.ErrClosedPipe
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	select {
	case s.text <- text:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// EndInput marks the end of text; the connection closes once every queued
// segment has been spoken.
func (s *rtStream) EndInput() error {
	s.endMu.Lock()
	defer s.endMu.Unlock()
	if s.ended {
		return nil
	}
	s.ended = true
	close(s.text)
	return nil
}

// ReadAudio implements provider.TTSStream.
func (s *rtStream) ReadAudio() (provider.AudioChunk, error) {
	s.mu.Lock()
	done, final := s.done, s.final
	s.mu.Unlock()
	if done {
		if final != nil {
			return provider.AudioChunk{}, final
		}
		return provider.AudioChunk{}, io.EOF
	}
	select {
	case c, ok := <-s.chunks:
		if ok {
			return c, nil
		}
		err := <-s.errc
		s.mu.Lock()
		s.done, s.final = true, err
		s.mu.Unlock()
		if err != nil {
			return provider.AudioChunk{}, err
		}
		return provider.AudioChunk{}, io.EOF
	case <-s.ctx.Done():
		return provider.AudioChunk{}, s.ctx.Err()
	}
}

// Close cancels the stream. Cancelling closes the socket; response.cancel is
// not used because audio is generated far faster than real time, so most of
// a segment is already on the wire by the time a cancel could land.
func (s *rtStream) Close() error {
	s.cancel()
	return nil
}

// run is the stream's only goroutine: dial, configure, then speak segments
// until the text channel closes or ctx ends.
func (s *rtStream) run() {
	started := time.Now()
	reason := "completed"
	var conn *websocket.Conn
	defer func() {
		if conn != nil {
			conn.CloseNow() //nolint:errcheck
		}
		slog.Debug("openai speech stream closed", "provider", Name, "transport", "realtime", "reason", reason, "lifetime_ms", time.Since(started).Milliseconds())
		close(s.chunks)
	}()
	fail := func(err error) {
		reason = "error"
		if s.ctx.Err() != nil {
			reason, err = "cancelled", s.ctx.Err()
		}
		s.errc <- err
	}

	var err error
	conn, err = s.connect()
	if err != nil {
		fail(err)
		return
	}
	var carry []byte // odd trailing byte between deltas
	for {
		var text string
		select {
		case t, ok := <-s.text:
			if !ok {
				s.errc <- nil
				return
			}
			text = t
		case <-s.ctx.Done():
			fail(s.ctx.Err())
			return
		}
		if strings.TrimSpace(text) == "" {
			continue
		}
		if err := s.speak(conn, text, &carry); err != nil {
			fail(err)
			return
		}
	}
}

// connect dials the socket and configures the session as a speech engine.
func (s *rtStream) connect() (*websocket.Conn, error) {
	opts := s.tts.opts
	dialCtx, cancel := context.WithTimeout(s.ctx, opts.RequestTimeout.Std())
	defer cancel()
	conn, resp, err := websocket.Dial(dialCtx, realtimeURL(opts.BaseURL, opts.Model), &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + s.tts.apiKey}},
	})
	if err != nil {
		if resp != nil {
			return nil, provider.ErrorFromStatus(Name, resp.StatusCode, err.Error())
		}
		if s.ctx.Err() != nil {
			return nil, s.ctx.Err()
		}
		return nil, &provider.Error{Provider: Name, Kind: provider.ErrTransient, Err: err}
	}
	conn.SetReadLimit(realtimeReadLimit)
	update := rtSessionUpdate{Type: "session.update", Session: rtSession{
		Type:             "realtime",
		OutputModalities: []string{"audio"},
		Instructions:     realtimeReadAloud,
		Reasoning:        rtReasoning{Effort: "none"},
		Audio: rtAudio{Output: rtAudioOutput{
			Format: rtFormat{Type: "audio/pcm", Rate: audio.SampleRate},
			Voice:  s.voice,
			Speed:  s.speed,
		}},
	}}
	if err := writeEvent(dialCtx, conn, update); err != nil {
		conn.CloseNow() //nolint:errcheck
		return nil, wsError(s.ctx, err)
	}
	// No response may be created before the session carries the read-aloud
	// instructions, or the first segment would be answered, not read.
	for {
		var ev rtEvent
		if err := readEvent(dialCtx, conn, &ev); err != nil {
			conn.CloseNow() //nolint:errcheck
			return nil, wsError(s.ctx, err)
		}
		switch ev.Type {
		case "session.updated":
			return conn, nil
		case "error":
			conn.CloseNow() //nolint:errcheck
			return nil, rtProviderError(ev.Error)
		}
	}
}

// speak sends one segment and forwards its audio until response.done.
func (s *rtStream) speak(conn *websocket.Conn, text string, carry *[]byte) error {
	req := rtResponseCreate{Type: "response.create", Response: rtResponse{
		Conversation:     "none",
		Input:            []any{},
		OutputModalities: []string{"audio"},
		Instructions:     realtimeSayPrefix + text,
	}}
	if err := writeEvent(s.ctx, conn, req); err != nil {
		return wsError(s.ctx, err)
	}
	idle := s.tts.opts.StreamIdleTimeout.Std()
	var gate silenceGate
	emit := func(pcm []byte) error {
		if len(pcm) == 0 {
			return nil
		}
		select {
		case s.chunks <- provider.AudioChunk{PCM: pcm}:
			return nil
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
	for {
		rctx, cancel := context.WithTimeout(s.ctx, idle)
		var ev rtEvent
		err := readEvent(rctx, conn, &ev)
		cancel()
		if err != nil {
			if s.ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
				return &provider.Error{Provider: Name, Kind: provider.ErrTransient, Err: errors.New("stream idle timeout")}
			}
			return wsError(s.ctx, err)
		}
		switch ev.Type {
		case "response.output_audio.delta":
			raw, err := base64.StdEncoding.DecodeString(ev.Delta)
			if err != nil {
				return fatalf("malformed audio delta: %v", err)
			}
			pcm := append(*carry, raw...)
			cut := audio.AlignDown(len(pcm))
			*carry = append([]byte(nil), pcm[cut:]...)
			if err := emit(gate.push(pcm[:cut])); err != nil {
				return err
			}
		case "error":
			return rtProviderError(ev.Error)
		case "response.done":
			if err := rtDoneError(&ev); err != nil {
				return err
			}
			return emit(gate.finish())
		}
	}
}

// rtDoneError maps a terminal response status; nil means completed.
func rtDoneError(ev *rtEvent) error {
	switch ev.Response.Status {
	case "", "completed":
		return nil
	case "failed":
		return rtProviderError(ev.Response.StatusDetails.Error)
	case "incomplete":
		return fatalf("realtime response incomplete: %s", ev.Response.StatusDetails.Reason)
	}
	return &provider.Error{Provider: Name, Kind: provider.ErrTransient, Err: fmt.Errorf("realtime response %s", ev.Response.Status)}
}

func writeEvent(ctx context.Context, conn *websocket.Conn, v any) error {
	buf, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, buf)
}

func readEvent(ctx context.Context, conn *websocket.Conn, ev *rtEvent) error {
	_, data, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, ev); err != nil {
		return fatalf("malformed realtime event: %v", err)
	}
	return nil
}

// wsError classifies a socket failure: a cancelled ctx is the caller's, a
// provider.Error passes through, anything else is a transient transport fault.
func wsError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var pe *provider.Error
	if errors.As(err, &pe) {
		return err
	}
	return &provider.Error{Provider: Name, Kind: provider.ErrTransient, Err: err}
}

var _ provider.TTSStream = (*rtStream)(nil)
