package qwen

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// TTSOptions is the "options" block of providers.tts for type "qwen".
// Voice is the fallback used when the session carries no voice of its own.
type TTSOptions struct {
	wsOptions
	Model string `json:"model"`
	Voice string `json:"voice"`
	// NoFlushNudge disables the whitespace flush described on flushNudge.
	// It exists so the workaround can be switched off if the service ever
	// stops needing it; leaving it on then costs one tiny frame per
	// sentence and nothing else.
	NoFlushNudge bool `json:"no_flush_nudge"`
}

// flushNudge is a whitespace-only continue-task sent after a sentence when no
// further text is ready. The service holds a completed sentence back until
// more input arrives, so without it the first audio frame waits for the LLM's
// next sentence: measured at 1.05 s with an 800 ms gap versus 0.25 s with the
// nudge, for byte-identical spoken content (hack/qwenbench, docs/decisions.md).
const flushNudge = " "

const (
	defaultTTSModel = "qwen-audio-3.0-tts-flash"
	// defaultVoice is a voice the flash model ships with; the session's
	// audio.output.voice normally overrides it.
	defaultVoice  = "longanlingxi"
	ttsAudioQueue = 64
	ttsTextQueue  = 32
)

func (o *TTSOptions) applyDefaults() {
	o.wsOptions.applyDefaults()
	if o.Model == "" {
		o.Model = defaultTTSModel
	}
	if o.Voice == "" {
		o.Voice = defaultVoice
	}
}

func init() {
	provider.Register(provider.KindTTS, Name, func(apiKey string, raw json.RawMessage) (provider.Provider, error) {
		var o TTSOptions
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &o); err != nil {
				return nil, fmt.Errorf("qwen tts options: %w", err)
			}
		}
		return NewTTS(apiKey, o), nil
	})
}

// TTS is the streaming synthesis provider. It accepts incremental text, so
// the pipeline runs one stream per response and sentences are appended as
// the LLM produces them.
type TTS struct {
	apiKey string
	opts   TTSOptions
}

// NewTTS builds the provider with defaults applied.
func NewTTS(apiKey string, o TTSOptions) *TTS {
	o.applyDefaults()
	return &TTS{apiKey: apiKey, opts: o}
}

// Kind implements provider.Provider.
func (t *TTS) Kind() provider.Kind { return provider.KindTTS }

// Capabilities implements provider.TTS. The service reports sentence
// boundaries but leaves the per-word timing array empty, so there is no
// character alignment to expose.
func (t *TTS) Capabilities() provider.TTSCaps {
	return provider.TTSCaps{IncrementalText: true}
}

// ttsParameters is the run-task parameter block for synthesis.
type ttsParameters struct {
	TextType   string  `json:"text_type"`
	Format     string  `json:"format"`
	SampleRate int     `json:"sample_rate"`
	Voice      string  `json:"voice"`
	Rate       float64 `json:"rate,omitempty"`
}

// Synthesize implements provider.TTS. The socket is dialed here, which the
// pipeline does while the LLM is still generating, so the connection cost
// stays off the text-to-audio path.
func (t *TTS) Synthesize(ctx context.Context, cfg provider.TTSConfig) (provider.TTSStream, error) {
	conn, err := dial(ctx, t.opts.wsOptions, t.apiKey)
	if err != nil {
		return nil, err
	}
	rate := cfg.SampleRate
	if rate == 0 {
		rate = audio.SampleRate
	}
	params := ttsParameters{
		TextType:   "PlainText",
		Format:     "pcm",
		SampleRate: rate,
		Voice:      firstNonEmpty(cfg.Voice, t.opts.Voice),
	}
	if cfg.Speed != 0 && cfg.Speed != 1 {
		params.Rate = cfg.Speed
	}
	sctx, scancel := context.WithCancel(ctx)
	s := &ttsStream{
		opts:       t.opts,
		model:      t.opts.Model,
		params:     params,
		taskID:     taskID(),
		conn:       conn,
		ctx:        sctx,
		cancel:     scancel,
		text:       make(chan string, ttsTextQueue),
		started:    make(chan struct{}),
		chunks:     make(chan provider.AudioChunk, ttsAudioQueue),
		errc:       make(chan error, 1),
		readerDone: make(chan struct{}),
		writerDone: make(chan struct{}),
	}
	slog.Debug("qwen tts stream opened", "provider", Name, "model", s.model, "voice", params.Voice, "sample_rate", rate)
	go s.reader()
	go s.writer()
	return s, nil
}

// ---- stream ----------------------------------------------------------------

// ttsStream runs exactly one synthesis task: one run-task, one
// continue-task per sentence and one finish-task, all under the same
// task_id, with audio streamed out as it is produced.
type ttsStream struct {
	opts   TTSOptions
	model  string
	params ttsParameters
	taskID string
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc

	text    chan string   // sentences waiting for the socket writer
	started chan struct{} // closed by the reader on task-started

	chunks chan provider.AudioChunk
	errc   chan error // terminal outcome, nil on a clean task-finished

	readerDone chan struct{} // closed once the terminal outcome is settled
	writerDone chan struct{}

	// runAtNanos is when run-task went on the wire, so the reader can
	// report how long the service took to acknowledge the task.
	runAtNanos atomic.Int64

	// endMu makes WriteText and EndInput safe against each other: EndInput
	// closes the text channel, which a concurrent WriteText must never see
	// as a send on a closed channel.
	endMu sync.RWMutex
	ended bool

	mu    sync.Mutex
	done  bool
	final error
}

// WriteText implements provider.TTSStream. Text is queued and sent as a
// continue-task once the task has started.
func (s *ttsStream) WriteText(text string) error {
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
	case <-s.writerDone:
		return io.ErrClosedPipe
	}
}

// EndInput implements provider.TTSStream: no more text follows, so the task
// is finished once everything queued has been sent.
func (s *ttsStream) EndInput() error {
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
		return s.terminal()
	case <-s.ctx.Done():
		// Cancellation and the task's real outcome race: whichever side
		// cancelled, the reader is bounded by the same ctx, so waiting for
		// it is prompt and yields the outcome the caller should see.
		<-s.readerDone
		select {
		case c, ok := <-s.chunks:
			if ok {
				return c, nil
			}
			return s.terminal()
		default:
			return provider.AudioChunk{}, s.ctx.Err()
		}
	}
}

// terminal records and returns the stream's final outcome once the audio
// channel has been drained.
func (s *ttsStream) terminal() (provider.AudioChunk, error) {
	err := <-s.errc
	s.mu.Lock()
	s.done, s.final = true, err
	s.mu.Unlock()
	if err != nil {
		return provider.AudioChunk{}, err
	}
	return provider.AudioChunk{}, io.EOF
}

// Close aborts the task and reclaims both goroutines.
func (s *ttsStream) Close() error {
	s.cancel()
	return nil
}

// ---- writer ----------------------------------------------------------------

// writer owns conn.Write: run-task, then one continue-task per queued
// sentence once task-started has arrived, then finish-task.
func (s *ttsStream) writer() {
	defer close(s.writerDone)
	frame := runFrame[ttsParameters]{
		Header: newOutHeader(actionRunTask, s.taskID),
		Payload: runPayload[ttsParameters]{
			TaskGroup:  "audio",
			Task:       "tts",
			Function:   "SpeechSynthesizer",
			Model:      s.model,
			Parameters: s.params,
		},
	}
	s.runAtNanos.Store(time.Now().UnixNano())
	if writeJSON(s.ctx, s.conn, frame) != nil {
		s.cancel()
		return
	}
	// Nothing may be sent before the service acknowledges the task.
	select {
	case <-s.started:
	case <-s.ctx.Done():
		return
	}
	firstText := true
	write := func(text string) bool {
		msg := continueFrame{
			Header:  newOutHeader(actionContinueTask, s.taskID),
			Payload: continuePayload{Input: textInput{Text: text}},
		}
		return writeJSON(s.ctx, s.conn, msg) == nil
	}
	for text := range s.text {
		if text == "" {
			continue
		}
		if firstText {
			firstText = false
			slog.Debug("qwen tts first text", "provider", Name, "chars", len(text))
		}
		if !write(text) {
			s.cancel()
			return
		}
		// Nothing else is ready to send, so this sentence would otherwise
		// sit unsynthesized until the LLM produces the next one.
		if !s.opts.NoFlushNudge && len(s.text) == 0 {
			if !write(flushNudge) {
				s.cancel()
				return
			}
		}
	}
	if writeJSON(s.ctx, s.conn, finishFrame{Header: newOutHeader(actionFinishTask, s.taskID)}) != nil {
		s.cancel()
	}
}

// ---- reader ----------------------------------------------------------------

// reader owns conn.Read: binary frames are audio, text frames are the task
// lifecycle. It closes chunks and reports the terminal outcome on errc.
func (s *ttsStream) reader() {
	started := time.Now()
	reason := "cancelled"
	var audioBytes int
	defer func() {
		s.conn.CloseNow() //nolint:errcheck
		close(s.chunks)
		slog.Debug("qwen tts stream closed", "provider", Name, "reason", reason,
			"audio_ms", audio.BytesToMs(audioBytes), "lifetime_ms", time.Since(started).Milliseconds())
		s.cancel()
		close(s.readerDone)
	}()
	fail := func(err error) { s.errc <- err }

	idle := time.AfterFunc(s.opts.IdleTimeout.Std(), s.cancel)
	defer idle.Stop()
	startedSeen := false
	for {
		typ, data, err := s.conn.Read(s.ctx)
		if err != nil {
			if s.ctx.Err() != nil {
				fail(s.ctx.Err())
				return
			}
			reason = "error"
			fail(transientf("connection closed before task-finished: %v", err))
			return
		}
		idle.Reset(s.opts.IdleTimeout.Std())
		if typ == websocket.MessageBinary {
			if audioBytes == 0 {
				slog.Debug("qwen tts first audio", "provider", Name, "bytes", len(data))
			}
			audioBytes += len(data)
			select {
			case s.chunks <- provider.AudioChunk{PCM: data}:
			case <-s.ctx.Done():
				fail(s.ctx.Err())
				return
			}
			continue
		}
		var frame struct {
			Header inHeader `json:"header"`
		}
		if err := json.Unmarshal(data, &frame); err != nil || frame.Header.Event == "" {
			slog.Debug("qwen tts ignoring unparseable frame", "provider", Name, "bytes", len(data))
			continue
		}
		switch frame.Header.Event {
		case eventTaskStarted:
			if !startedSeen {
				startedSeen = true
				if at := s.runAtNanos.Load(); at != 0 {
					slog.Debug("qwen tts task started", "provider", Name,
						"started_ms", time.Since(time.Unix(0, at)).Seconds()*1000)
				}
				close(s.started)
			}
		case eventResultGenerated:
			// Sentence boundary progress; the audio itself arrives as
			// binary frames and the word timing array is always empty.
		case eventTaskFinished:
			reason = "completed"
			fail(nil)
			return
		case eventTaskFailed:
			reason = "task_failed"
			fail(taskFailed(frame.Header))
			return
		}
	}
}

var _ provider.TTSStream = (*ttsStream)(nil)
