package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// TTSOptions is the "options" block of providers.tts for type "openai".
type TTSOptions struct {
	httpOptions
	Model string `json:"model"`
}

const (
	defaultTTSModel = "tts-1"
	// audioChunkMs is the read granularity of the streamed PCM body.
	audioChunkMs = 100
	audioQueue   = 32
)

func init() {
	provider.Register(provider.KindTTS, Name, func(apiKey string, raw json.RawMessage) (provider.Provider, error) {
		var o TTSOptions
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &o); err != nil {
				return nil, fmt.Errorf("openai tts options: %w", err)
			}
		}
		return NewTTS(apiKey, o), nil
	})
}

// TTS is the speech endpoint client. It has neither incremental text input
// nor alignment: the pipeline issues one Synthesize per sentence.
type TTS struct {
	apiKey string
	opts   TTSOptions
	client *http.Client
}

// NewTTS builds the provider with defaults applied.
func NewTTS(apiKey string, o TTSOptions) *TTS {
	o.applyDefaults()
	if o.Model == "" {
		o.Model = defaultTTSModel
	}
	return &TTS{apiKey: apiKey, opts: o, client: newClient(o.httpOptions)}
}

// Kind implements provider.Provider.
func (t *TTS) Kind() provider.Kind { return provider.KindTTS }

// Capabilities implements provider.TTS.
func (t *TTS) Capabilities() provider.TTSCaps { return provider.TTSCaps{} }

type speechRequest struct {
	Model          string  `json:"model"`
	Input          string  `json:"input"`
	Voice          string  `json:"voice"`
	ResponseFormat string  `json:"response_format"`
	Speed          float64 `json:"speed,omitempty"`
}

// Synthesize implements provider.TTS.
func (t *TTS) Synthesize(ctx context.Context, cfg provider.TTSConfig) (provider.TTSStream, error) {
	ctx, cancel := context.WithCancel(ctx)
	return &ttsStream{tts: t, cfg: cfg, ctx: ctx, cancel: cancel, chunks: make(chan provider.AudioChunk, audioQueue), errc: make(chan error, 1)}, nil
}

type ttsStream struct {
	tts    *TTS
	cfg    provider.TTSConfig
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	text   strings.Builder
	ended  bool
	chunks chan provider.AudioChunk
	errc   chan error // terminal error, or nil on EOF
	final  error      // terminal outcome once chunks is drained
	done   bool
}

// WriteText buffers text; the request is issued at EndInput.
func (s *ttsStream) WriteText(text string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return io.ErrClosedPipe
	}
	s.text.WriteString(text)
	return nil
}

// EndInput issues the speech request and streams the PCM body.
func (s *ttsStream) EndInput() error {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return nil
	}
	s.ended = true
	text := s.text.String()
	s.mu.Unlock()
	go s.fetch(text)
	return nil
}

func (s *ttsStream) fetch(text string) {
	defer close(s.chunks)
	started := time.Now()
	reason := "completed"
	defer func() {
		slog.Debug("openai speech stream closed", "provider", Name, "reason", reason, "lifetime_ms", time.Since(started).Milliseconds())
	}()
	body := speechRequest{Model: s.tts.opts.Model, Input: text, Voice: s.cfg.Voice, ResponseFormat: "pcm"}
	if s.cfg.Speed != 0 && s.cfg.Speed != 1 {
		body.Speed = s.cfg.Speed
	}
	resp, err := postJSON(s.ctx, s.tts.client, s.tts.apiKey, s.tts.opts.BaseURL+"/audio/speech", body)
	if err != nil {
		reason = "error"
		if s.ctx.Err() != nil {
			reason = "cancelled"
		}
		s.errc <- err
		return
	}
	defer resp.Body.Close()
	wd := newIdleWatchdog(s.tts.opts.StreamIdleTimeout.Std(), s.cancel)
	defer wd.stop()
	chunk := audio.MsToBytes(audioChunkMs)
	var carry []byte // odd trailing byte between reads
	for {
		buf := make([]byte, chunk)
		n, err := io.ReadFull(resp.Body, buf)
		if n > 0 {
			wd.progress()
			pcm := append(carry, buf[:n]...)
			cut := audio.AlignDown(len(pcm))
			carry = append([]byte(nil), pcm[cut:]...)
			if cut > 0 {
				select {
				case s.chunks <- provider.AudioChunk{PCM: pcm[:cut]}:
				case <-s.ctx.Done():
					reason = "cancelled"
					s.errc <- s.ctx.Err()
					return
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				s.errc <- nil
				return
			}
			if s.ctx.Err() != nil {
				reason = "cancelled"
				s.errc <- s.ctx.Err()
				return
			}
			reason = "error"
			s.errc <- &provider.Error{Provider: Name, Kind: provider.ErrTransient, Err: err}
			return
		}
	}
}

// ReadAudio implements provider.TTSStream.
func (s *ttsStream) ReadAudio() (provider.AudioChunk, error) {
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

// Close aborts any in-flight request.
func (s *ttsStream) Close() error {
	s.cancel()
	return nil
}
