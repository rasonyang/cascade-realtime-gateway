package qwen

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// ASROptions is the "options" block of providers.asr for type "qwen".
type ASROptions struct {
	wsOptions
	Model    string `json:"model"`
	Language string `json:"language"`
	// FinalizeTimeout bounds the wait for task-finished after a Finalize.
	// On expiry the stream synthesizes EndOfTurn and restarts the task so
	// the ASRStream contract still holds.
	FinalizeTimeout config.Duration `json:"finalize_timeout"`
	// AudioQueueFrames bounds audio buffered ahead of the socket writer; a
	// full queue makes PushAudio fail, which the session treats as fatal.
	// The queue also absorbs the audio that arrives while a task is
	// restarting, so nothing is dropped between turns.
	AudioQueueFrames int `json:"audio_queue_frames"`
	// KeepAliveInterval is how long a running task may go without audio
	// before the writer sends a short frame of silence. DashScope fails a
	// task 23 s after its last audio frame, so it must stay below that.
	KeepAliveInterval config.Duration `json:"keepalive_interval"`
}

const (
	defaultASRModel         = "qwen-audio-3.0-asr-flash-streaming"
	defaultFinalizeTimeout  = 3 * time.Second
	defaultAudioQueueFrames = 500 // 10 s of 20 ms frames
	asrEventQueue           = 32
	asrCtlQueue             = 8

	// dashScopeNoAudioLimit is how long the service lets a recognition task
	// run without an audio frame. When it expires the service ends the task
	// itself, which with no audio fails as "SERVER_ERROR: DecodePost resample
	// audio from 24000 to 16000 failed" and closes the socket with 1011.
	// Measured on the live endpoint; not documented.
	dashScopeNoAudioLimit    = 23 * time.Second
	defaultKeepAliveInterval = 10 * time.Second
	// keepAliveMs is the length of one keep-alive silence frame. Recognition
	// is billed by audio duration, so it is kept as short as possible.
	keepAliveMs = 20
)

func (o *ASROptions) applyDefaults() {
	o.wsOptions.applyDefaults()
	if o.Model == "" {
		o.Model = defaultASRModel
	}
	if o.FinalizeTimeout == 0 {
		o.FinalizeTimeout = config.Duration(defaultFinalizeTimeout)
	}
	if o.AudioQueueFrames == 0 {
		o.AudioQueueFrames = defaultAudioQueueFrames
	}
	if o.KeepAliveInterval == 0 {
		o.KeepAliveInterval = config.Duration(defaultKeepAliveInterval)
	}
}

func (o *ASROptions) validate() error {
	if ka := o.KeepAliveInterval.Std(); ka < 0 || ka >= dashScopeNoAudioLimit {
		return fmt.Errorf("qwen asr options: keepalive_interval must be positive and below %s", dashScopeNoAudioLimit)
	}
	return nil
}

func init() {
	provider.Register(provider.KindASR, Name, func(apiKey string, raw json.RawMessage) (provider.Provider, error) {
		var o ASROptions
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &o); err != nil {
				return nil, fmt.Errorf("qwen asr options: %w", err)
			}
		}
		if err := o.validate(); err != nil {
			return nil, err
		}
		return NewASR(apiKey, o), nil
	})
}

// ASR is the streaming recognition provider.
type ASR struct {
	apiKey string
	opts   ASROptions
}

// NewASR builds the provider with defaults applied.
func NewASR(apiKey string, o ASROptions) *ASR {
	o.applyDefaults()
	return &ASR{apiKey: apiKey, opts: o}
}

// Kind implements provider.Provider.
func (a *ASR) Kind() provider.Kind { return provider.KindASR }

// asrParameters is the run-task parameter block for recognition.
type asrParameters struct {
	Format        string   `json:"format"`
	SampleRate    int      `json:"sample_rate"`
	LanguageHints []string `json:"language_hints,omitempty"`
}

// asrResult is the result-generated payload. Only the outer output.sentence
// is read; the service repeats the same object nested one level deeper.
type asrResult struct {
	Output struct {
		Sentence *asrSentence `json:"sentence"`
	} `json:"output"`
}

type asrSentence struct {
	SentenceID  int    `json:"sentence_id"`
	BeginTime   *int   `json:"begin_time"`
	EndTime     *int   `json:"end_time"`
	Text        string `json:"text"`
	SentenceEnd bool   `json:"sentence_end"`
}

// OpenStream implements provider.ASR. It dials DashScope (bounded by
// connect_timeout) and starts the reader and writer goroutines; the first
// run-task is sent by the writer.
//
// One socket carries the whole session, but recognition runs as one task per
// turn: Finalize ends the current task with finish-task, which flushes the
// final transcript in a few hundred milliseconds instead of waiting for the
// model's own end-of-utterance judgement, and the next task is started on
// the same connection for ~25 ms. Audio that arrives while a task is
// restarting waits in the queue, so no audio is ever sent before
// task-started and none is dropped.
//
// Silence handling: DashScope fails a task that receives no audio for 23 s
// and also fails a finish-task on a task that received none, both with a
// misleading resample error that closes the socket. The writer therefore
// sends a keep-alive silence frame after keepalive_interval without audio,
// and a Finalize on a task that has received nothing is answered with a
// synthesized EndOfTurn instead of a finish-task.
func (a *ASR) OpenStream(ctx context.Context, cfg provider.ASRConfig) (provider.ASRStream, error) {
	conn, err := dial(ctx, a.opts.wsOptions, a.apiKey)
	if err != nil {
		return nil, err
	}
	rate := cfg.SampleRate
	if rate == 0 {
		rate = audio.SampleRate
	}
	params := asrParameters{Format: "pcm", SampleRate: rate}
	if lang := firstNonEmpty(cfg.Language, a.opts.Language); lang != "" {
		params.LanguageHints = []string{lang}
	}
	sctx, scancel := context.WithCancel(ctx)
	s := &asrStream{
		opts:       a.opts,
		model:      firstNonEmpty(cfg.Model, a.opts.Model),
		params:     params,
		conn:       conn,
		ctx:        sctx,
		cancel:     scancel,
		in:         make(chan []byte, a.opts.AudioQueueFrames),
		finalize:   make(chan struct{}, 1),
		closeReq:   make(chan struct{}),
		ctl:        make(chan inHeader, asrCtlQueue),
		events:     make(chan provider.ASREvent, asrEventQueue),
		done:       make(chan struct{}),
		writerDone: make(chan struct{}),
		closeSent:  make(chan struct{}),
		openedAt:   time.Now(),
	}
	// Armed for the first run-task, which the writer sends immediately.
	s.ack = time.AfterFunc(a.opts.IdleTimeout.Std(), scancel)
	slog.Debug("qwen asr stream opened", "provider", Name, "model", s.model, "sample_rate", rate)
	go s.reader()
	go s.writer()
	return s, nil
}

// ---- stream ----------------------------------------------------------------

type asrStream struct {
	opts   ASROptions
	model  string
	params asrParameters
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc

	in       chan []byte   // audio waiting for the socket writer
	finalize chan struct{} // Finalize requests
	closeReq chan struct{} // Close requests, so the writer can end the task
	ctl      chan inHeader // task lifecycle frames, reader → writer
	events   chan provider.ASREvent

	done       chan struct{} // closed when the reader exits
	writerDone chan struct{} // closed when the writer exits
	closeSent  chan struct{} // closed once the final finish-task is on the wire
	openedAt   time.Time

	// baseMs is the audio timeline offset contributed by tasks that have
	// already finished; it is bumped by the writer between tasks, which the
	// reader observes only after task-finished, so no result can straddle it.
	baseMs atomic.Int64
	// closing suppresses the EndOfTurn that the Close handshake's
	// task-finished would otherwise produce.
	closing atomic.Bool
	// runAtNanos is when the current run-task went on the wire, so the
	// reader can report how long the service took to acknowledge it.
	runAtNanos atomic.Int64

	// ack bounds the wait for task-started after each run-task (idle_timeout).
	// It is not armed while a task runs: a silent caller legitimately
	// produces no inbound frames for as long as the silence lasts.
	ack *time.Timer

	// currentTask is written by the writer before run-task and read by the
	// reader for logging; guarded because both goroutines touch it.
	mu          sync.Mutex
	currentTask string
	finalizeAt  time.Time
	closed      bool
	// gaps are the keep-alive silence frames inserted into the current task,
	// on the service's timeline, so result offsets can be mapped back onto
	// the pushed-audio timeline.
	gaps []silenceGap
}

// silenceGap is one keep-alive frame: at is its offset in the task's audio
// as the service sees it, ms its length.
type silenceGap struct{ at, ms int }

// PushAudio implements provider.ASRStream; it never blocks on the network.
func (s *asrStream) PushAudio(pcm []byte) error {
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	select {
	case s.in <- pcm:
		return nil
	default:
		return transientf("audio queue full: socket writer stalled")
	}
}

// Finalize implements provider.ASRStream: it asks the writer to end the
// current recognition task, which makes the service emit the final
// transcript immediately.
func (s *asrStream) Finalize() error {
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	select {
	case s.finalize <- struct{}{}:
	default: // one already pending; the current task ends either way
	}
	return nil
}

// Events implements provider.ASRStream.
func (s *asrStream) Events() <-chan provider.ASREvent { return s.events }

// Close implements provider.ASRStream: finish-task is sent best-effort so the
// service releases the task, then the socket is torn down and every
// goroutine reclaimed.
func (s *asrStream) Close() error {
	s.mu.Lock()
	already := s.closed
	s.closed = true
	s.mu.Unlock()
	if !already {
		s.closing.Store(true)
		close(s.closeReq)
		select {
		case <-s.closeSent:
		case <-s.writerDone:
		case <-time.After(s.opts.ConnectTimeout.Std()):
		}
		s.cancel()
	}
	<-s.done
	<-s.writerDone
	return nil
}

func (s *asrStream) emit(ev provider.ASREvent) bool {
	select {
	case s.events <- ev:
		return true
	case <-s.ctx.Done():
		return false
	}
}

// ---- writer ----------------------------------------------------------------

// writer owns conn.Write. It runs one recognition task at a time: run-task,
// then audio once task-started has arrived, then finish-task on Finalize,
// then the next run-task once task-finished has arrived.
func (s *asrStream) writer() {
	defer close(s.writerDone)
	defer s.cancel()

	var (
		started    bool
		finalizing bool
		pending    bool // Finalize arrived before task-started
		sentMs     int  // audio milliseconds written in the current task
		padMs      int  // keep-alive silence milliseconds written in the current task
		deadline   <-chan time.Time
		timer      *time.Timer
	)
	keepAliveEvery := s.opts.KeepAliveInterval.Std()
	keepAlive := time.NewTimer(keepAliveEvery)
	defer keepAlive.Stop()
	silence := make([]byte, s.params.SampleRate*2*keepAliveMs/1000)
	writeAudio := func(pcm []byte) bool {
		if s.conn.Write(s.ctx, websocket.MessageBinary, pcm) != nil {
			return false
		}
		sentMs += audio.BytesToMs(len(pcm))
		keepAlive.Reset(keepAliveEvery)
		return true
	}
	stopTimer := func() {
		if timer != nil {
			timer.Stop()
			timer, deadline = nil, nil
		}
	}
	defer stopTimer()

	beginFinalize := func() bool {
		// Everything already queued belongs to this turn: flush it before
		// ending the task so no speech is lost.
		for {
			select {
			case pcm := <-s.in:
				if !writeAudio(pcm) {
					return false
				}
				continue
			default:
			}
			break
		}
		if sentMs+padMs == 0 {
			// Nothing reached this task, so there is nothing to flush, and
			// DashScope fails a finish-task on an empty task. Honor the
			// contract directly and keep the task running.
			pending = false
			return s.emit(provider.ASREvent{Kind: provider.ASREndOfTurn})
		}
		if !s.sendFinish() {
			return false
		}
		finalizing = true
		s.mu.Lock()
		s.finalizeAt = time.Now()
		s.mu.Unlock()
		timer = time.NewTimer(s.opts.FinalizeTimeout.Std())
		deadline = timer.C
		return true
	}
	restart := func() bool {
		stopTimer()
		s.mu.Lock()
		s.baseMs.Add(int64(sentMs))
		s.gaps = nil
		s.mu.Unlock()
		sentMs, padMs = 0, 0
		started, finalizing, pending = false, false, false
		return s.sendRun()
	}

	if !s.sendRun() {
		return
	}
	for {
		// A nil channel blocks forever, which is exactly the gate: audio is
		// written only while a task is running and not being finalized.
		var (
			audioCh     chan []byte
			keepAliveCh <-chan time.Time
		)
		if started && !finalizing {
			audioCh = s.in
			keepAliveCh = keepAlive.C
		}
		select {
		case <-s.ctx.Done():
			return

		case <-s.closeReq:
			// End the task politely, then let Close tear the socket down.
			if started {
				s.sendFinish()
			}
			close(s.closeSent)
			return

		case h := <-s.ctl:
			switch h.Event {
			case eventTaskStarted:
				started = true
				keepAlive.Reset(keepAliveEvery)
				if pending && !beginFinalize() {
					return
				}
			case eventTaskFinished:
				if !restart() {
					return
				}
			case eventTaskFailed:
				return // the reader has already emitted the error
			}

		case <-s.finalize:
			if finalizing {
				continue // already ending this task
			}
			if !started {
				pending = true
				continue
			}
			if !beginFinalize() {
				return
			}

		case <-deadline:
			// No task-finished within finalize_timeout. Honor the contract
			// with a synthesized EndOfTurn and start a fresh task.
			s.mu.Lock()
			waited := time.Since(s.finalizeAt)
			s.mu.Unlock()
			slog.Debug("qwen asr finalize timed out", "provider", Name, "waited_ms", waited.Milliseconds())
			if !s.emit(provider.ASREvent{Kind: provider.ASREndOfTurn}) {
				return
			}
			if !restart() {
				return
			}

		case pcm := <-audioCh:
			if !writeAudio(pcm) {
				return
			}

		case <-keepAliveCh:
			// No audio for keepalive_interval: the caller is silent or on
			// hold. One short silence frame keeps the task alive.
			if s.conn.Write(s.ctx, websocket.MessageBinary, silence) != nil {
				return
			}
			s.mu.Lock()
			s.gaps = append(s.gaps, silenceGap{at: sentMs + padMs, ms: keepAliveMs})
			s.mu.Unlock()
			padMs += keepAliveMs
			keepAlive.Reset(keepAliveEvery)
		}
	}
}

func (s *asrStream) sendRun() bool {
	id := taskID()
	s.mu.Lock()
	s.currentTask = id
	s.mu.Unlock()
	s.runAtNanos.Store(time.Now().UnixNano())
	s.ack.Reset(s.opts.IdleTimeout.Std())
	frame := runFrame[asrParameters]{
		Header: newOutHeader(actionRunTask, id),
		Payload: runPayload[asrParameters]{
			TaskGroup:  "audio",
			Task:       "asr",
			Function:   "recognition",
			Model:      s.model,
			Parameters: s.params,
		},
	}
	return writeJSON(s.ctx, s.conn, frame) == nil
}

func (s *asrStream) sendFinish() bool {
	s.mu.Lock()
	id := s.currentTask
	s.mu.Unlock()
	return writeJSON(s.ctx, s.conn, finishFrame{Header: newOutHeader(actionFinishTask, id)}) == nil
}

// ---- reader ----------------------------------------------------------------

// reader owns conn.Read. It maps result-generated payloads to ASR events and
// forwards task lifecycle frames to the writer. On exit it closes the socket
// and the events channel, so cancellation tears everything down at once.
func (s *asrStream) reader() {
	defer close(s.done)
	defer close(s.events)
	defer s.conn.CloseNow() //nolint:errcheck
	defer s.cancel()
	reason := "cancelled"
	defer func() {
		slog.Debug("qwen asr stream closed", "provider", Name, "reason", reason, "lifetime_ms", time.Since(s.openedAt).Milliseconds())
	}()

	defer s.ack.Stop()
	for {
		typ, data, err := s.conn.Read(s.ctx)
		if err != nil {
			if s.ctx.Err() != nil {
				return // cancelled or closed by us
			}
			if s.closing.Load() || websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				reason = "closed"
				return
			}
			reason = "error"
			s.emit(provider.ASREvent{Kind: provider.ASRError, Err: transientf("connection closed: %v", err)})
			return
		}
		if typ != websocket.MessageText {
			continue // recognition never sends binary frames
		}
		var frame struct {
			Header  inHeader        `json:"header"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(data, &frame); err != nil || frame.Header.Event == "" {
			// Malformed or unrecognized frames are not fatal on their own;
			// a genuinely broken task still ends in task-failed or a close.
			slog.Debug("qwen asr ignoring unparseable frame", "provider", Name, "bytes", len(data))
			continue
		}
		if !s.handle(frame.Header, frame.Payload) {
			reason = reasonFor(frame.Header.Event)
			return
		}
	}
}

func reasonFor(event string) string {
	if event == eventTaskFailed {
		return "task_failed"
	}
	return "cancelled"
}

// handle processes one frame and reports whether the reader should continue.
func (s *asrStream) handle(h inHeader, payload json.RawMessage) bool {
	switch h.Event {
	case eventResultGenerated:
		var res asrResult
		if err := json.Unmarshal(payload, &res); err != nil || res.Output.Sentence == nil {
			return true // nothing usable in this frame
		}
		return s.emitSentence(res.Output.Sentence)

	case eventTaskStarted:
		s.ack.Stop()
		if at := s.runAtNanos.Load(); at != 0 {
			slog.Debug("qwen asr task started", "provider", Name,
				"started_ms", time.Since(time.Unix(0, at)).Seconds()*1000)
		}
		return s.toWriter(h)

	case eventTaskFinished:
		// The turn is over: the final transcript has already been emitted.
		if !s.closing.Load() && !s.emit(provider.ASREvent{Kind: provider.ASREndOfTurn}) {
			return false
		}
		return s.toWriter(h)

	case eventTaskFailed:
		s.emit(provider.ASREvent{Kind: provider.ASRError, Err: taskFailed(h)})
		s.toWriter(h)
		return false
	}
	return true
}

// emitSentence maps one recognition sentence onto the stream's audio
// timeline. A sentence_end sentence is the turn's final transcript;
// everything else is the interim text for the utterance in progress.
func (s *asrStream) emitSentence(sn *asrSentence) bool {
	if sn.Text == "" && !sn.SentenceEnd {
		return true
	}
	base := int(s.baseMs.Load())
	ev := provider.ASREvent{Text: sn.Text, StartMs: base, EndMs: base}
	if sn.BeginTime != nil {
		ev.StartMs = base + s.unpad(*sn.BeginTime)
	}
	if sn.EndTime != nil {
		ev.EndMs = base + s.unpad(*sn.EndTime)
	}
	if sn.SentenceEnd {
		ev.Kind = provider.ASRFinal
	} else {
		ev.Kind = provider.ASRPartial
		ev.EndMs = ev.StartMs
	}
	return s.emit(ev)
}

// unpad maps an offset on the service's task timeline, which includes
// keep-alive silence, onto the pushed-audio timeline. An offset inside a
// keep-alive frame maps to where that frame was inserted.
func (s *asrStream) unpad(ms int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	shift := 0
	for _, g := range s.gaps {
		if ms <= g.at {
			break
		}
		if ms < g.at+g.ms {
			return g.at - shift
		}
		shift += g.ms
	}
	return ms - shift
}

// toWriter hands a lifecycle frame to the writer's state machine.
func (s *asrStream) toWriter(h inHeader) bool {
	select {
	case s.ctl <- h:
		return true
	case <-s.ctx.Done():
		return false
	case <-s.writerDone:
		return false
	}
}

var _ provider.ASRStream = (*asrStream)(nil)
