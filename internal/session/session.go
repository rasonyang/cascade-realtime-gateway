package session

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/observability"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
	"github.com/rasonyang/cascade-realtime-gateway/internal/recorder"
	"github.com/rasonyang/cascade-realtime-gateway/internal/vad"
)

// Options configures one Session. Session is deep-copied into an immutable
// snapshot; Limits supplies every queue size and timeout.
type Options struct {
	ID      string
	Session config.SessionDefaults
	Limits  config.Limits
	ASR     provider.ASR
	LLM     provider.LLM
	TTS     provider.TTS
	// ASRLanguage and ASRModel configure the ASR stream. They come from the
	// profile, not from the protocol-visible transcription object.
	ASRLanguage string
	ASRModel    string
	// Temperature is nil when the LLM provider's own default applies.
	Temperature *float64
	// Profile and the three instance names are labels only: they identify
	// which runtime configuration served this session in spans and metrics.
	Profile  string
	ASRName  string
	LLMName  string
	TTSName  string
	Logger   *slog.Logger
	Recorder recorder.Recorder
	// Telemetry is optional; nil records nothing.
	Telemetry *observability.Telemetry
}

// Errors returned by Post / PostAudio.
var (
	ErrClosed         = errors.New("session: closed")
	ErrInputQueueFull = errors.New("session: input audio queue full")
)

// Eagerness → extra silence the semantic_vad approximation waits after a
// Final that does not end in sentence punctuation (docs/decisions.md).
const (
	eagernessHighMs   = 400
	eagernessMediumMs = 800
	eagernessLowMs    = 1500
)

// Session is the single-goroutine actor owning all conversation state.
type Session struct {
	id     string
	cfg    config.SessionDefaults
	limits config.Limits
	asr    provider.ASR
	llm    provider.LLM
	tts    provider.TTS
	asrCfg provider.ASRConfig
	temp   *float64
	attrs  []attribute.KeyValue // profile / instance names, shared by span and metrics
	log    *slog.Logger
	rec    recorder.Recorder
	tel    *observability.Telemetry
	span   trace.Span // session span; response spans are its children

	ctx    context.Context
	cancel context.CancelCauseFunc

	// Inbound audio and commands travel on separate bounded queues so that
	// control never waits behind audio. Every posted item carries a sequence
	// number, and the actor restores arrival order across the two queues
	// (see run), so "append, append, commit" and "clear, append" behave as
	// the client sent them.
	seq        atomic.Uint64
	cmds       chan cmdEnvelope
	audioIn    chan audioFrame
	stashCmd   *cmdEnvelope // pulled ahead of turn while restoring order
	stashAudio *audioFrame
	respEvents chan Event
	events     chan Event
	done       chan struct{}

	// Actor-owned state below; touched only by run().
	asrStream provider.ASRStream
	asrEvents <-chan provider.ASREvent
	buffer    *inputAudioBuffer
	detector  vad.Detector
	turns     *TurnManager
	conv      *conversation

	active    *response
	activeGen Generation
	nextResp  ResponseRef

	audioEmitted bool // first output audio delta sent: voice is locked (GA)

	speechItem    ItemRef   // pre-allocated at speech start, reused by commit
	pendingItem   *item     // committed audio item awaiting its transcript
	committedAt   time.Time // when pendingItem was committed; commit→final latency
	speechStartAt time.Time // VAD speech start of the current turn (metrics)
	speechEndAt   time.Time // VAD speech stop of the current turn (metrics)
	interruptAt   time.Time // interrupt trigger (speech start or cancel command)
	firstPartial  bool      // a partial has been recorded for the current turn
	transcriptAcc string    // Finals accumulated since the last commit
	partial       string    // latest ASR partial

	asrWaitTimer *time.Timer
	asrWait      <-chan time.Time
	eotTimer     *time.Timer
	eot          <-chan time.Time

	// semantic_vad deferred trigger: the committed item whose transcript
	// decides whether a response is created, and the bounded wait for it.
	deferItem      ItemRef
	deferAnchor    time.Time
	deferWaitTimer *time.Timer
	deferWait      <-chan time.Time

	pipelines sync.WaitGroup
	startedAt time.Time
	responses int
}

// New builds a session; Start opens the ASR stream and runs the actor.
func New(opts Options) *Session {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	tel := opts.Telemetry
	if tel == nil {
		tel = observability.Noop()
	}
	cfg := opts.Session.Clone()
	if td := cfg.Audio.Input.TurnDetection; td != nil {
		td.ApplyDefaults() // DefaultConfig leaves mode-specific fields nil
	}
	s := &Session{
		id:         opts.ID,
		cfg:        cfg,
		limits:     opts.Limits,
		asr:        opts.ASR,
		llm:        opts.LLM,
		tts:        opts.TTS,
		temp:       opts.Temperature,
		log:        log.With("session_id", opts.ID),
		rec:        opts.Recorder,
		tel:        tel,
		cmds:       make(chan cmdEnvelope, opts.Limits.OutputEventQueue),
		audioIn:    make(chan audioFrame, opts.Limits.InputAudioQueueFrames),
		respEvents: make(chan Event, opts.Limits.OutputEventQueue),
		events:     make(chan Event, opts.Limits.OutputEventQueue),
		done:       make(chan struct{}),
		buffer:     newInputAudioBuffer(opts.Limits.InputAudioBufferMaxMs),
		conv:       newConversation(),
	}
	s.asrCfg = provider.ASRConfig{
		SampleRate: audio.SampleRate,
		Language:   opts.ASRLanguage,
		Model:      opts.ASRModel,
	}
	if tr := cfg.Audio.Input.Transcription; tr != nil {
		s.asrCfg.Prompt = tr.Prompt
	}
	s.attrs = []attribute.KeyValue{
		observability.KeyProfile.String(opts.Profile),
		observability.KeyASR.String(opts.ASRName),
		observability.KeyLLM.String(opts.LLMName),
		observability.KeyTTS.String(opts.TTSName),
	}
	s.applyTurnDetection()
	return s
}

// Start opens the ASR stream and launches the actor goroutine.
func (s *Session) Start(ctx context.Context) error {
	ctx, s.span = s.tel.Tracer.Start(ctx, "session",
		trace.WithAttributes(append([]attribute.KeyValue{observability.KeySessionID.String(s.id)}, s.attrs...)...))
	s.ctx, s.cancel = context.WithCancelCause(ctx)
	stream, err := s.asr.OpenStream(s.ctx, s.asrCfg)
	if err != nil {
		s.cancel(closeError{CloseProviderError})
		s.tel.Metrics.ProviderErrors.Add(ctx, 1, metric.WithAttributes(observability.KeyProvider.String("asr")))
		s.span.RecordError(err)
		s.span.SetStatus(codes.Error, "asr open failed")
		s.span.End()
		close(s.events)
		close(s.done)
		return fmt.Errorf("open asr stream: %w", err)
	}
	s.tel.Metrics.SessionsActive.Add(ctx, 1, metric.WithAttributes(s.attrs...))
	s.asrStream = stream
	s.asrEvents = stream.Events()
	s.startedAt = time.Now()
	go s.run()
	return nil
}

type cmdEnvelope struct {
	seq uint64
	cmd Command
}

type audioFrame struct {
	seq uint64
	pcm []byte
}

// Post delivers a control command to the actor. It blocks only while the
// command queue is full and the session is alive.
func (s *Session) Post(cmd Command) error {
	select {
	case <-s.ctx.Done():
		return ErrClosed
	default:
	}
	env := cmdEnvelope{seq: s.seq.Add(1), cmd: cmd}
	select {
	case s.cmds <- env:
		return nil
	case <-s.ctx.Done():
		return ErrClosed
	}
}

// PostAudio enqueues inbound audio without blocking. A full queue is a
// fatal condition: the session closes with CloseInputOverflow and the caller
// must disconnect. Nothing is ever silently dropped.
func (s *Session) PostAudio(pcm []byte) error {
	select {
	case <-s.ctx.Done():
		return ErrClosed
	default:
	}
	select {
	case s.audioIn <- audioFrame{seq: s.seq.Add(1), pcm: pcm}:
		return nil
	default:
		s.cancel(closeError{CloseInputOverflow})
		return ErrInputQueueFull
	}
}

// Events is the outbound fact stream; closed after the actor exits.
func (s *Session) Events() <-chan Event { return s.events }

// Done is closed once the actor and every child goroutine have exited.
func (s *Session) Done() <-chan struct{} { return s.done }

// Close asks the session to end with the given reason.
func (s *Session) Close(reason CloseReason) { s.cancel(closeError{reason}) }

// ID returns the session identifier.
func (s *Session) ID() string { return s.id }

// fieldOf extracts the dotted field path from a config validation error.
func fieldOf(err error) string {
	var fe *config.FieldError
	if errors.As(err, &fe) {
		return fe.Field
	}
	return ""
}

// closeError carries the CloseReason through context cancellation causes.
type closeError struct{ reason CloseReason }

func (e closeError) Error() string { return "session closed: " + string(e.reason) }

func reasonOf(ctx context.Context) CloseReason {
	cause := context.Cause(ctx)
	var ce closeError
	if errors.As(cause, &ce) {
		return ce.reason
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		return CloseSessionTimeout // the server bounds the session ctx by session_timeout
	}
	return CloseContextCancelled
}

// ---- actor loop ------------------------------------------------------------

func (s *Session) run() {
	defer s.cleanup()
	for {
		if s.ctx.Err() != nil {
			return
		}
		// Items pulled ahead of turn while restoring order go first.
		switch {
		case s.stashCmd != nil && (s.stashAudio == nil || s.stashCmd.seq < s.stashAudio.seq):
			env := *s.stashCmd
			s.stashCmd = nil
			s.runCommand(env)
			continue
		case s.stashAudio != nil:
			fr := *s.stashAudio
			s.stashAudio = nil
			s.runAudio(fr)
			continue
		}
		select {
		case <-s.ctx.Done():
			return
		case env := <-s.cmds:
			s.runCommand(env)
		case fr := <-s.audioIn:
			s.runAudio(fr)
		case ev, ok := <-s.asrEvents:
			if !ok {
				s.asrEvents = nil
				continue
			}
			s.handleASR(ev)
		case ev := <-s.respEvents:
			s.handleResponseEvent(ev)
		case <-s.asrWait:
			s.asrWait = nil
			s.handleTranscriptTimeout()
		case <-s.eot:
			s.eot = nil
			s.apply(s.turns.Step(asrEndOfTurn{}), "")
		case <-s.deferWait:
			s.deferWait = nil
			s.handleDeferredTimeout()
		}
	}
}

// runCommand handles env after every audio frame that was posted before it.
func (s *Session) runCommand(env cmdEnvelope) {
	s.drainAudioBefore(env.seq)
	s.handleCommand(env.cmd)
}

// runAudio handles fr after every command that was posted before it.
func (s *Session) runAudio(fr audioFrame) {
	s.drainCommandsBefore(fr.seq)
	s.handleAudio(fr.pcm)
}

func (s *Session) drainAudioBefore(seq uint64) {
	for s.ctx.Err() == nil {
		if s.stashAudio == nil {
			select {
			case fr := <-s.audioIn:
				s.stashAudio = &fr
			default:
				return
			}
		}
		if s.stashAudio.seq > seq {
			return
		}
		fr := *s.stashAudio
		s.stashAudio = nil
		s.handleAudio(fr.pcm)
	}
}

func (s *Session) drainCommandsBefore(seq uint64) {
	for s.ctx.Err() == nil {
		if s.stashCmd == nil {
			select {
			case env := <-s.cmds:
				s.stashCmd = &env
			default:
				return
			}
		}
		if s.stashCmd.seq > seq {
			return
		}
		env := *s.stashCmd
		s.stashCmd = nil
		s.handleCommand(env.cmd)
	}
}

// emit delivers an event to the consumer. Slow consumers stall the actor
// (bounded by the outbound queue); the server layer disconnects such a
// client after client_write_timeout, which cancels ctx and unblocks us.
func (s *Session) emit(ev Event) {
	select {
	case s.events <- ev:
	case <-s.ctx.Done():
	}
}

func (s *Session) emitError(tag, code, msg string) {
	s.emit(EvError{Tag: tag, Code: code, Message: msg})
}

func (s *Session) emitParamError(tag, code, param, msg string) {
	s.emit(EvError{Tag: tag, Code: code, Message: msg, Param: param})
}

// fatal emits a fatal error and closes the session.
func (s *Session) fatal(code, msg string, reason CloseReason) {
	s.emit(EvError{Code: code, Message: msg, Fatal: true})
	s.cancel(closeError{reason})
}

func (s *Session) cleanup() {
	reason := reasonOf(s.ctx)
	if s.active != nil {
		s.finish(s.active, ResponseCancelled, ReasonNone, nil)
	}
	s.stopTimers()
	if s.asrStream != nil {
		_ = s.asrStream.Close()
	}
	s.pipelines.Wait()
	// Bounded wait: the consumer may already be gone, and shutdown must not
	// hang on it.
	select {
	case s.events <- EvSessionClosed{Reason: reason}:
	case <-time.After(s.limits.ClientWriteTimeout.Std()):
	}
	close(s.events)
	bg := context.Background()
	s.tel.Metrics.SessionsActive.Add(bg, -1, metric.WithAttributes(s.attrs...))
	s.tel.Metrics.SessionsEnded.Add(bg, 1,
		metric.WithAttributes(append([]attribute.KeyValue{observability.KeyReason.String(string(reason))}, s.attrs...)...))
	s.span.SetAttributes(observability.KeyReason.String(string(reason)), attribute.Int("cascade.items", s.conv.count()), attribute.Int("cascade.responses", s.responses))
	s.span.End()
	if s.rec != nil {
		summary := recorder.Summary{
			SessionID: s.id, StartedAt: s.startedAt, EndedAt: time.Now(),
			EndReason: string(reason), Items: s.conv.count(), Responses: s.responses,
		}
		go s.rec.SessionEnded(context.Background(), summary)
	}
	close(s.done)
}

func (s *Session) stopTimers() {
	if s.asrWaitTimer != nil {
		s.asrWaitTimer.Stop()
		s.asrWaitTimer = nil
		s.asrWait = nil
	}
	s.disarmEndOfTurn()
	s.dropDeferredTrigger()
}

// ---- configuration ---------------------------------------------------------

func (s *Session) ttsConfig(voice string) provider.TTSConfig {
	return provider.TTSConfig{
		Voice:      voice,
		Speed:      s.cfg.Audio.Output.Speed,
		SampleRate: audio.SampleRate,
	}
}

// applyTurnDetection (re)builds the VAD and TurnManager from the snapshot.
func (s *Session) applyTurnDetection() {
	td := s.cfg.Audio.Input.TurnDetection
	prevActive := s.turns != nil && s.turns.responseActive
	s.turns = newTurnManager(td)
	s.turns.responseActive = prevActive
	s.detector = nil
	if s.turns.vadEnabled() {
		s.detector = vad.NewEnergy(vad.Config{
			Threshold:         s.vadThreshold(),
			SilenceDurationMs: s.vadSilenceMs(),
		})
	}
	s.disarmEndOfTurn()
	s.speechItem = 0
}

// The acoustic VAD under semantic_vad uses the server_vad defaults because
// GA exposes no tuning fields for it (docs/decisions.md).
func (s *Session) vadThreshold() float64 {
	if td := s.cfg.Audio.Input.TurnDetection; td != nil && td.Threshold != nil {
		return *td.Threshold
	}
	return config.DefaultVADThreshold
}

func (s *Session) vadPrefixMs() int {
	if td := s.cfg.Audio.Input.TurnDetection; td != nil && td.PrefixPaddingMs != nil {
		return *td.PrefixPaddingMs
	}
	return config.DefaultVADPrefixPaddingMs
}

func (s *Session) vadSilenceMs() int {
	if td := s.cfg.Audio.Input.TurnDetection; td != nil && td.SilenceDurationMs != nil {
		return *td.SilenceDurationMs
	}
	return config.DefaultVADSilenceDurationMs
}

func (s *Session) eagernessMs() int {
	td := s.cfg.Audio.Input.TurnDetection
	if td == nil || td.Eagerness == nil {
		return eagernessMediumMs
	}
	switch *td.Eagerness {
	case "high":
		return eagernessHighMs
	case "low":
		return eagernessLowMs
	}
	return eagernessMediumMs
}

func (s *Session) transcriptionEnabled() bool { return s.cfg.Audio.Input.Transcription != nil }

func (s *Session) textOnly(mods []string) bool {
	if mods == nil {
		mods = s.cfg.OutputModalities
	}
	return len(mods) == 1 && mods[0] == config.ModalityText
}

// ---- commands --------------------------------------------------------------

func (s *Session) handleCommand(cmd Command) {
	tag := cmd.CommandTag()
	switch c := cmd.(type) {
	case CmdUpdateSession:
		s.updateSession(c.Patch, tag)
	case CmdCommitAudio:
		s.apply(s.turns.Step(clientCommit{}), tag)
	case CmdClearAudio:
		s.buffer.clear()
		if s.detector != nil {
			s.detector.Reset()
		}
		s.turns.resetTurn()
		s.disarmEndOfTurn()
		s.speechItem = 0
		s.emit(EvAudioBufferCleared{})
	case CmdCreateItem:
		s.createItem(c, tag)
	case CmdDeleteItem:
		s.deleteItem(c.ID, tag)
	case CmdTruncateItem:
		s.truncateItem(c, tag)
	case CmdCreateResponse:
		if s.active != nil {
			s.emitError(tag, ErrCodeResponseInProgress, "a response is already in progress")
			return
		}
		if s.turns.Step(clientCreateResponse{}).Trigger {
			s.createResponse(c.Overrides, tag)
		}
	case CmdCancelResponse:
		if s.active == nil {
			s.emitError(tag, ErrCodeResponseCancelNotActive, "no response is in progress")
			return
		}
		if c.Resp != 0 && c.Resp != s.active.ref {
			s.emitParamError(tag, ErrCodeResponseCancelNotActive, "response_id", "response is not in progress")
			return
		}
		s.interruptAt = time.Now()
		s.interrupt(ReasonClientCancelled)
	case CmdClose:
		s.cancel(closeError{c.Reason})
	}
}

func (s *Session) updateSession(p SessionPatch, tag string) {
	next := s.cfg.Clone()
	if p.Instructions != nil {
		next.Instructions = *p.Instructions
	}
	if p.OutputModalities != nil {
		next.OutputModalities = append([]string(nil), p.OutputModalities...)
	}
	if p.Voice != nil {
		if *p.Voice != s.cfg.Audio.Output.Voice && s.audioEmitted {
			s.emitParamError(tag, ErrCodeVoiceLocked, "session.audio.output.voice", "voice cannot be changed after the session has produced audio")
			return
		}
		next.Audio.Output.Voice = *p.Voice
	}
	if p.Speed != nil {
		next.Audio.Output.Speed = *p.Speed
	}
	if p.MaxOutputTokens != nil {
		next.MaxOutputTokens = *p.MaxOutputTokens
	}
	if p.Transcription.Set {
		next.Audio.Input.Transcription = p.Transcription.Value
	}
	if p.TurnDetection.Set {
		next.Audio.Input.TurnDetection = p.TurnDetection.Value.Clone()
		if next.Audio.Input.TurnDetection != nil {
			next.Audio.Input.TurnDetection.ApplyDefaults()
		}
	}
	if p.Tools != nil {
		next.Tools = config.CloneTools(p.Tools)
	}
	if p.ToolChoice != nil {
		next.ToolChoice = *p.ToolChoice
	}
	if err := next.Validate("session"); err != nil {
		s.emitParamError(tag, ErrCodeInvalidSession, fieldOf(err), err.Error())
		return
	}
	s.cfg = next
	if p.TurnDetection.Set {
		s.applyTurnDetection()
	}
	s.emit(EvSessionUpdated{Config: s.cfg.Clone()})
}

func (s *Session) createItem(c CmdCreateItem, tag string) {
	it, ok := s.newClientItem(c.Item, tag)
	if !ok {
		return
	}
	prev, ok := s.conv.insert(it, c.PreviousItem, c.AtRoot)
	if !ok {
		s.emitParamError(tag, ErrCodeItemNotFound, "previous_item_id", "previous item not found")
		return
	}
	s.emit(EvItemAdded{Item: it.snapshot(), PreviousItem: prev})
	s.emit(EvItemDone{Item: it.snapshot()})
}

// newClientItem validates a client-created item spec and builds the item, or
// emits the rejection and returns false.
func (s *Session) newClientItem(spec ItemSpec, tag string) (*item, bool) {
	if spec.Content == ContentToolOutput {
		return s.newToolOutputItem(spec, tag)
	}
	switch spec.Role {
	case RoleUser, RoleAssistant, RoleSystem:
	default:
		s.emitParamError(tag, ErrCodeInvalidItem, "item.role", fmt.Sprintf("unsupported role %q", spec.Role))
		return nil, false
	}
	if spec.Text == "" {
		s.emitParamError(tag, ErrCodeInvalidItem, "item.content", "item text must not be empty")
		return nil, false
	}
	return &item{Item: Item{ClientID: spec.ClientID, Role: spec.Role, Content: ContentText,
		Status: ItemCompleted, Text: spec.Text, TranscriptDone: true}}, true
}

// newToolOutputItem builds a function_call_output. The tool result itself is
// opaque and may be empty; what must hold is that it answers a call the model
// actually made, exactly once.
func (s *Session) newToolOutputItem(spec ItemSpec, tag string) (*item, bool) {
	call, ok := s.conv.get(spec.CallItem)
	if !ok || call.Content != ContentToolCall {
		s.emitParamError(tag, ErrCodeInvalidItem, "item.call_id", "no function_call with this call_id exists")
		return nil, false
	}
	if s.conv.answered(call.CallID) {
		s.emitParamError(tag, ErrCodeInvalidItem, "item.call_id", "a function_call_output already exists for this call")
		return nil, false
	}
	return &item{Item: Item{ClientID: spec.ClientID, Content: ContentToolOutput, Status: ItemCompleted,
		Text: spec.Text, CallID: call.CallID, Name: call.Name, TranscriptDone: true}}, true
}

func (s *Session) deleteItem(ref ItemRef, tag string) {
	if s.active != nil && s.active.item == ref {
		s.emitParamError(tag, ErrCodeInvalidItem, "item_id", "cannot delete the output item of an in-progress response")
		return
	}
	if !s.conv.remove(ref) {
		s.emitParamError(tag, ErrCodeItemNotFound, "item_id", "item not found")
		return
	}
	if s.pendingItem != nil && s.pendingItem.Ref == ref {
		s.pendingItem = nil
	}
	s.emit(EvItemDeleted{ID: ref})
}

func (s *Session) truncateItem(c CmdTruncateItem, tag string) {
	it, ok := s.conv.get(c.ID)
	if !ok {
		s.emitParamError(tag, ErrCodeItemNotFound, "item_id", "item not found")
		return
	}
	if it.Role != RoleAssistant || it.Content != ContentAudio {
		s.emitParamError(tag, ErrCodeItemNotTruncatable, "item_id", "only assistant audio items can be truncated")
		return
	}
	if c.ContentIndex != 0 {
		s.emitParamError(tag, ErrCodeInvalidItem, "content_index", "content_index out of range")
		return
	}
	cut := audio.MsToBytes(c.AudioEndMs)
	if c.AudioEndMs < 0 || cut > it.audioB {
		s.emitParamError(tag, ErrCodeTruncateOutOfRange, "audio_end_ms",
			fmt.Sprintf("audio_end_ms must be within [0, %d]", it.AudioMs))
		return
	}
	before := len(it.Text)
	generatedMs := it.AudioMs
	it.Text = trimText(it.Text, it.segments, it.alignment, cut, it.audioB)
	if cut < it.audioB {
		it.audioB = cut
		it.AudioMs = c.AudioEndMs
	}
	// How much the client reports was actually heard, and what trimming it
	// cost the text the next turn will see.
	s.log.Debug("item truncated", "item", c.ID, "audio_end_ms", c.AudioEndMs,
		"generated_ms", generatedMs, "chars_before", before, "chars_after", len(it.Text))
	s.emit(EvItemTruncated{ID: c.ID, ContentIndex: c.ContentIndex, AudioEndMs: c.AudioEndMs})
}

// ---- audio and turns -------------------------------------------------------

func (s *Session) handleAudio(pcm []byte) {
	if err := s.buffer.append(pcm); err != nil {
		s.fatal(ErrCodeBufferOverflow, "input audio buffer exceeded input_audio_buffer_max_ms", CloseBufferOverflow)
		return
	}
	if err := s.asrStream.PushAudio(pcm); err != nil {
		s.tel.Metrics.ProviderErrors.Add(s.ctx, 1, metric.WithAttributes(observability.KeyProvider.String("asr")))
		s.fatal(ErrCodeProviderError, "asr: "+err.Error(), CloseProviderError)
		return
	}
	if s.detector == nil {
		return
	}
	for _, ev := range s.detector.Process(pcm) {
		switch ev.Kind {
		case vad.SpeechStart:
			if s.speechItem == 0 {
				s.speechItem = s.conv.newRef()
			}
			startMs := s.buffer.markSpeechStart(ev.OffsetMs, s.vadPrefixMs())
			s.disarmEndOfTurn()
			d := s.turns.Step(vadSpeechStart{})
			if d.EmitSpeechStarted {
				s.speechStartAt = time.Now()
				s.firstPartial = false
				// The instant a client's barge-in guard is measured from;
				// responding reports whether a response was interrupted.
				s.log.Debug("speech started", "item", s.speechItem, "audio_start_ms", startMs,
					"responding", s.active != nil, "interrupt", d.Interrupt)
				s.emit(EvSpeechStarted{AudioStartMs: startMs, Item: s.speechItem})
			}
			if d.Interrupt {
				s.interruptAt = time.Now()
				s.interrupt(ReasonTurnDetected)
			}
			if d.EmitSpeechStarted && s.turns.interruptResponse && s.deferItem != 0 {
				// The equivalent of interrupting an awaiting response: the
				// user resumed before the previous turn's transcript was in.
				s.log.Debug("deferred response dropped: speech resumed", "item", s.deferItem)
				s.dropDeferredTrigger()
			}
			s.apply(d, "")
		case vad.SpeechEnd:
			endMs := s.buffer.markSpeechEnd(ev.OffsetMs, s.vadSilenceMs())
			d := s.turns.Step(vadSpeechEnd{})
			if d.EmitSpeechStopped {
				s.speechEndAt = time.Now()
				s.emit(EvSpeechStopped{AudioEndMs: endMs, Item: s.speechItem})
			}
			s.apply(d, "")
		}
	}
	// Idle audio before the prefix-padding window can never be committed;
	// drop it so a long silence does not reach the memory cap. The extra
	// frame covers the detector's pending partial frame, whose speech start
	// may be reported at an offset up to one frame before the current end.
	s.buffer.trimIdle(s.vadPrefixMs() + vad.FrameMs)
}

// apply executes a TurnDecision's commit / trigger / timer parts. Speech
// events and interrupts are handled at the call site because they need the
// millisecond values.
func (s *Session) apply(d TurnDecision, tag string) {
	if d.ArmEndOfTurnTimer {
		s.armEndOfTurn(time.Duration(s.eagernessMs()) * time.Millisecond)
	}
	if d.ArmFinalWaitTimer {
		s.armEndOfTurn(s.finalWait())
	}
	if d.Commit {
		if s.commit(tag) && d.DeferTrigger {
			s.deferTrigger()
		}
	}
	if d.Trigger {
		if s.active != nil {
			// Auto-trigger while a response is active (interrupt_response=false):
			// the new turn waits for the client, matching GA's one-response rule.
			s.log.Debug("auto trigger skipped: response in progress")
			return
		}
		s.createResponse(ResponseOverrides{}, tag)
	}
}

// commit reports whether an item was committed.
func (s *Session) commit(tag string) bool {
	pcm, startMs, endMs, ok := s.buffer.commit()
	if !ok {
		// PROTOCOL-VERIFY: GA may enforce a minimum buffer duration; Phase 2.
		s.emitError(tag, ErrCodeBufferEmpty, "input audio buffer is empty")
		return false
	}
	_ = pcm // raw audio is not retained; the ASR stream already received it
	if s.detector != nil {
		s.detector.Reset()
	}
	ref := s.speechItem
	if ref == 0 {
		ref = s.conv.newRef()
	}
	s.speechItem = 0
	it := &item{Item: Item{
		Ref: ref, Role: RoleUser, Content: ContentAudio, Status: ItemInProgress,
		Text: s.transcriptAcc, AudioMs: endMs - startMs,
	}}
	prev := s.conv.append(it)
	s.pendingItem = it
	s.committedAt = time.Now()
	if s.detector == nil {
		// Manual mode: the commit is the only turn boundary the metrics can use.
		s.speechStartAt, s.speechEndAt = s.committedAt, s.committedAt
		s.firstPartial = false
	}
	s.emit(EvAudioBufferCommitted{Item: ref, PreviousItem: prev})
	s.emit(EvItemAdded{Item: it.snapshot(), PreviousItem: prev})
	if err := s.asrStream.Finalize(); err != nil {
		s.fatal(ErrCodeProviderError, "asr finalize: "+err.Error(), CloseProviderError)
		return false
	}
	return true
}

// deferTrigger records that the item just committed decides the response:
// semantic_vad creates it once that transcript is final and non-empty
// (resolveDeferredTrigger), bounded by asr_final_timeout.
func (s *Session) deferTrigger() {
	it := s.pendingItem
	if it == nil {
		return
	}
	s.dropDeferredTrigger()
	s.deferItem = it.Ref
	s.deferAnchor = s.committedAt
	s.deferWaitTimer = time.NewTimer(s.limits.ASRFinalTimeout.Std())
	s.deferWait = s.deferWaitTimer.C
}

func (s *Session) dropDeferredTrigger() {
	if s.deferWaitTimer != nil {
		s.deferWaitTimer.Stop()
		s.deferWaitTimer = nil
	}
	s.deferWait = nil
	s.deferItem = 0
	s.deferAnchor = time.Time{}
}

// resolveDeferredTrigger runs when ref's transcript is final. An empty
// transcript is non-speech noise: the turn ends with no response.
func (s *Session) resolveDeferredTrigger(ref ItemRef, text string) {
	if s.deferItem == 0 || s.deferItem != ref {
		return
	}
	anchor := s.deferAnchor
	s.dropDeferredTrigger()
	if strings.TrimSpace(text) == "" {
		s.log.Info("no response: turn transcript is empty", "item", ref)
		return
	}
	if s.active != nil {
		s.log.Debug("auto trigger skipped: response in progress")
		return
	}
	s.createResponseAt(ResponseOverrides{}, "", anchor)
}

// handleDeferredTimeout is asr_final_timeout for a deferred trigger. A
// Partial still starts the response; with nothing heard there is no
// response and no error, since nothing was said.
func (s *Session) handleDeferredTimeout() {
	ref, anchor := s.deferItem, s.deferAnchor
	s.dropDeferredTrigger()
	it := s.pendingItem
	if ref == 0 || it == nil || it.Ref != ref {
		return
	}
	best := joinTranscript(s.transcriptAcc, s.partial)
	if strings.TrimSpace(best) == "" {
		s.log.Warn("asr final timeout with no transcript; no response for this turn", "item", ref)
		return
	}
	if s.active != nil {
		s.log.Debug("auto trigger skipped: response in progress")
		return
	}
	s.log.Warn("asr final timeout; starting with partial transcript", "item", ref)
	it.Text = best
	s.createResponseAt(ResponseOverrides{}, "", anchor)
	if r := s.active; r != nil && r.awaiting {
		s.stopASRWait()
		s.startPipeline(r)
	}
}

func (s *Session) armEndOfTurn(d time.Duration) {
	s.disarmEndOfTurn()
	s.eotTimer = time.NewTimer(d)
	s.eot = s.eotTimer.C
}

// finalWait bounds how long semantic_vad waits after speech_stopped for a
// Final to judge: asr_final_timeout, but never shorter than the eagerness
// delay a Final without punctuation would get.
func (s *Session) finalWait() time.Duration {
	return max(s.limits.ASRFinalTimeout.Std(), time.Duration(s.eagernessMs())*time.Millisecond)
}

func (s *Session) disarmEndOfTurn() {
	if s.eotTimer != nil {
		s.eotTimer.Stop()
		s.eotTimer = nil
	}
	s.eot = nil
}

// ---- ASR -------------------------------------------------------------------

func joinTranscript(acc, text string) string {
	switch {
	case text == "":
		return acc
	case acc == "":
		return text
	}
	return acc + " " + text
}

func (s *Session) handleASR(ev provider.ASREvent) {
	switch ev.Kind {
	case provider.ASRPartial:
		s.partial = ev.Text
		if !s.firstPartial && !s.speechStartAt.IsZero() && ev.Text != "" {
			s.firstPartial = true
			s.tel.Metrics.ASRFirstTranscriptMs.Record(s.ctx, millis(time.Since(s.speechStartAt)))
		}
	case provider.ASRFinal:
		s.partial = ""
		if s.pendingItem != nil {
			delta := ev.Text
			s.completePendingItem(joinTranscript(s.transcriptAcc, ev.Text), delta)
		} else {
			s.transcriptAcc = joinTranscript(s.transcriptAcc, ev.Text)
		}
		s.apply(s.turns.Step(asrFinal{Text: ev.Text}), "")
	case provider.ASREndOfTurn:
		if s.pendingItem != nil {
			s.completePendingItem(s.transcriptAcc, "")
		}
		s.apply(s.turns.Step(asrEndOfTurn{}), "")
	case provider.ASRError:
		s.tel.Metrics.ProviderErrors.Add(s.ctx, 1, metric.WithAttributes(observability.KeyProvider.String("asr")))
		s.fatal(ErrCodeProviderError, "asr: "+ev.Err.Error(), CloseProviderError)
	}
}

// completePendingItem finalizes the committed audio item's transcript and
// releases any response awaiting it.
func (s *Session) completePendingItem(text, delta string) {
	it := s.pendingItem
	s.pendingItem = nil
	s.transcriptAcc = ""
	commitToFinal := time.Since(s.committedAt)
	s.tel.Metrics.CommitToFinalMs.Record(s.ctx, millis(commitToFinal))
	if !s.speechEndAt.IsZero() {
		s.tel.Metrics.ASRFinalTranscriptMs.Record(s.ctx, millis(time.Since(s.speechEndAt)))
	}
	s.log.Info("transcript final", "item", it.Ref, "commit_to_final_ms", commitToFinal.Milliseconds(), "chars", len(text))
	it.Text = text
	it.TranscriptDone = true
	it.Status = ItemCompleted
	if s.transcriptionEnabled() {
		if delta != "" {
			s.emit(EvInputTranscriptDelta{Item: it.Ref, Delta: delta})
		}
		s.emit(EvInputTranscriptDone{Item: it.Ref, Text: text})
	}
	s.emit(EvItemDone{Item: it.snapshot()})
	if r := s.active; r != nil && r.awaiting && r.dependsOn == it.Ref {
		s.stopASRWait()
		s.startPipeline(r)
	}
	s.resolveDeferredTrigger(it.Ref, text)
}

func (s *Session) handleTranscriptTimeout() {
	r := s.active
	if r == nil || !r.awaiting {
		return
	}
	best := joinTranscript(s.transcriptAcc, s.partial)
	if best == "" {
		s.log.Warn("asr final timeout with no transcript; failing response", "response", r.ref)
		s.failResponse(r, ErrCodeTranscriptTimeout, errors.New("no transcript before asr_final_timeout"))
		return
	}
	s.log.Warn("asr final timeout; starting with partial transcript", "response", r.ref)
	if s.pendingItem != nil {
		s.pendingItem.Text = best
	}
	s.startPipeline(r)
}

func (s *Session) stopASRWait() {
	if s.asrWaitTimer != nil {
		s.asrWaitTimer.Stop()
		s.asrWaitTimer = nil
	}
	s.asrWait = nil
}

// ---- responses -------------------------------------------------------------

func (s *Session) createResponse(ov ResponseOverrides, tag string) {
	s.createResponseAt(ov, tag, time.Time{})
}

// createResponseAt is createResponse with an explicit E2E latency anchor; a
// zero anchor uses the pending commit, or now.
func (s *Session) createResponseAt(ov ResponseOverrides, tag string, anchor time.Time) {
	probe := s.cfg.Clone()
	if ov.OutputModalities != nil {
		probe.OutputModalities = ov.OutputModalities
	}
	if ov.MaxOutputTokens != nil {
		probe.MaxOutputTokens = *ov.MaxOutputTokens
	}
	if ov.Voice != nil {
		if *ov.Voice != s.cfg.Audio.Output.Voice && s.audioEmitted {
			s.emitParamError(tag, ErrCodeVoiceLocked, "response.audio.output.voice", "voice cannot be changed after the session has produced audio")
			return
		}
		probe.Audio.Output.Voice = *ov.Voice
	}
	if err := probe.Validate("response"); err != nil {
		s.emitParamError(tag, ErrCodeInvalidOverrides, fieldOf(err), err.Error())
		return
	}
	s.nextResp++
	r := &response{
		ref:          s.nextResp,
		status:       ResponseInProgress,
		awaiting:     true,
		textOnly:     s.textOnly(probe.OutputModalities),
		instructions: s.cfg.Instructions,
		voice:        probe.Audio.Output.Voice,
		tag:          tag,
		anchorAt:     time.Now(),
		tools:        toolDefs(s.cfg.Tools),
		toolChoice:   toolChoice(s.cfg.ToolChoice),
	}
	if !anchor.IsZero() {
		r.anchorAt = anchor
	} else if p := s.pendingItem; p != nil && !s.committedAt.IsZero() {
		r.anchorAt = s.committedAt // E2E latency starts at the user's turn end
	}
	r.ctx, r.span = s.tel.Tracer.Start(s.ctx, "response", trace.WithAttributes(
		observability.KeyResponseID.Int64(int64(r.ref)),
		observability.KeyModalities.StringSlice(probe.OutputModalities),
	))
	if ov.Instructions != nil {
		r.instructions = *ov.Instructions
	}
	if !probe.MaxOutputTokens.Inf {
		r.maxTokens = probe.MaxOutputTokens.N
	}
	s.active = r
	s.responses++
	s.emit(EvResponseCreated{
		Resp:             r.ref,
		OutputModalities: append([]string(nil), probe.OutputModalities...),
		Voice:            r.voice,
		MaxOutputTokens:  probe.MaxOutputTokens,
		Metadata:         ov.Metadata,
	})
	s.turns.Step(responseStarted{})

	if p := s.pendingItem; p != nil && !p.TranscriptDone {
		r.dependsOn = p.Ref
		s.asrWaitTimer = time.NewTimer(s.limits.ASRFinalTimeout.Std())
		s.asrWait = s.asrWaitTimer.C
		return
	}
	s.startPipeline(r)
}

func (s *Session) startPipeline(r *response) {
	r.awaiting = false
	s.activeGen++
	r.gen = s.activeGen

	// The message item is created lazily, on the first piece of output (see
	// ensureOutputItem): a turn that only calls a tool must not leave an
	// empty message item in response.done.output.
	ctx, cancel := context.WithCancel(r.ctx)
	r.cancel = cancel
	r.startedAt = time.Now()
	ctx, r.llmSpan = s.tel.Tracer.Start(ctx, "llm")
	p := &pipeline{
		ctx: ctx, gen: r.gen, resp: r.ref, textOnly: r.textOnly,
		req: provider.ChatRequest{
			Instructions:    r.instructions,
			Messages:        s.conv.messages(),
			MaxOutputTokens: r.maxTokens,
			Temperature:     s.temp,
			Tools:           r.tools,
			ToolChoice:      r.toolChoice,
		},
		ttsCfg: s.ttsConfig(r.voice),
		llm:    s.llm, tts: s.tts, out: s.respEvents,
	}
	chunks := make(chan string, textChunkQueue)
	sentences := make(chan sentence, sentenceQueue)
	s.pipelines.Add(3)
	go func() { defer s.pipelines.Done(); p.llmStage(chunks) }()
	go func() { defer s.pipelines.Done(); p.sentencerStage(chunks, sentences) }()
	go func() {
		defer s.pipelines.Done()
		if r.textOnly {
			for range sentences { // drain; nothing to synthesize
			}
			return
		}
		p.ttsStage(sentences)
	}()
}

func (s *Session) handleResponseEvent(ev Event) {
	r := s.active
	st, ok := ev.(stamped)
	if !ok || r == nil || r.awaiting || st.generation() != s.activeGen {
		return // stale generation or no active response
	}
	switch e := ev.(type) {
	case EvOutputTextDelta:
		out := s.ensureOutputItem(r)
		out.Text += e.Delta
		s.noteFirstText(r)
		e.Item = r.item
		s.emit(e)
	case EvOutputAudioTranscriptDelta:
		out := s.ensureOutputItem(r)
		out.Text += e.Delta
		s.noteFirstText(r)
		e.Item = r.item
		s.emit(e)
	case pipeToolCall:
		s.deliverToolCall(r, e.Call)
	case EvOutputAudioDelta:
		out := s.ensureOutputItem(r)
		out.audioB += len(e.PCM)
		out.AudioMs = audio.BytesToMs(out.audioB)
		s.audioEmitted = true
		e.Item = r.item
		if r.firstAudioAt.IsZero() {
			r.firstAudioAt = time.Now()
			if !r.ttsStartedAt.IsZero() {
				s.tel.Metrics.TTSFirstAudioMs.Record(s.ctx, millis(r.firstAudioAt.Sub(r.ttsStartedAt)))
			}
			s.tel.Metrics.E2EMs.Record(s.ctx, millis(r.firstAudioAt.Sub(r.anchorAt)))
		}
		s.emit(e)
	case pipeTTSStart:
		r.ttsStartedAt = time.Now()
		_, r.ttsSpan = s.tel.Tracer.Start(r.ctx, "tts")
	case pipeAlignment:
		if out, ok := s.conv.get(r.item); ok {
			out.alignment = append(out.alignment, e.Timings...)
		}
	case pipeSegment:
		if out, ok := s.conv.get(r.item); ok {
			out.segments = append(out.segments, e.Seg)
		}
	case pipeLLMDone:
		r.llmDone = true
		r.finish = e.Finish
		r.usage = e.Usage
		if r.llmSpan != nil {
			r.llmSpan.SetAttributes(attribute.String("cascade.finish_reason", string(e.Finish)),
				attribute.Int("cascade.input_tokens", e.Usage.InputTokens), attribute.Int("cascade.output_tokens", e.Usage.OutputTokens))
			r.llmSpan.End()
			r.llmSpan = nil
		}
		s.maybeComplete(r)
	case pipeTTSDone:
		r.ttsDone = true
		r.audioFlushed = true // deltas precede this event on the same channel
		if out, spoke := s.conv.get(r.item); r.ttsSpan != nil {
			if spoke {
				r.ttsSpan.SetAttributes(attribute.Int("cascade.audio_ms", out.AudioMs))
			}
			r.ttsSpan.End()
			r.ttsSpan = nil
		}
		s.maybeComplete(r)
	case pipeError:
		s.log.Error("pipeline error", "stage", e.Stage, "err", e.Err)
		s.tel.Metrics.ProviderErrors.Add(s.ctx, 1, metric.WithAttributes(observability.KeyProvider.String(e.Stage)))
		if e.Stage == "llm" && r.llmSpan != nil {
			r.llmSpan.RecordError(e.Err)
		}
		if e.Stage == "tts" && r.ttsSpan != nil {
			r.ttsSpan.RecordError(e.Err)
		}
		s.failResponse(r, ErrCodeProviderError, e.Err)
	}
}

// ensureOutputItem returns the response's message item, creating it on the
// first piece of output. Deferring creation to the first delta is what keeps
// a call-only turn from emitting an empty message item, which GA does not do.
func (s *Session) ensureOutputItem(r *response) *item {
	if out, ok := s.conv.get(r.item); ok {
		return out
	}
	content := ContentAudio
	if r.textOnly {
		content = ContentText
	}
	out := &item{Item: Item{Role: RoleAssistant, Content: content, Status: ItemInProgress, TranscriptDone: true}}
	prev := s.conv.append(out)
	r.item = out.Ref
	s.emit(EvOutputItemAdded{Resp: r.ref, Item: out.snapshot(), PreviousItem: prev})
	return out
}

// deliverToolCall commits the function_call item and emits its whole
// lifecycle the moment the adapter yields a complete call. This is the single
// delivery path, and delivery is final: a later cancel does not retract the
// item, it only stops whatever would have followed.
func (s *Session) deliverToolCall(r *response, call provider.ToolCall) {
	if r.toolItem != 0 {
		return // at most one call per response
	}
	it := &item{Item: Item{Content: ContentToolCall, Status: ItemInProgress,
		Name: call.Name, CallID: call.ID, Text: call.Arguments, TranscriptDone: true}}
	prev := s.conv.append(it)
	r.toolItem = it.Ref
	s.emit(EvOutputItemAdded{Resp: r.ref, Item: it.snapshot(), PreviousItem: prev})
	s.emit(EvToolCallArguments{Resp: r.ref, Item: it.Ref, Arguments: call.Arguments})
	it.Status = ItemCompleted
	s.emit(EvOutputItemDone{Resp: r.ref, Item: it.snapshot()})
}

func (s *Session) maybeComplete(r *response) {
	if !r.done() {
		return
	}
	status, reason := r.outcome()
	s.finish(r, status, reason, nil)
}

func (s *Session) failResponse(r *response, code string, err error) {
	r.failCode = code
	s.emitError(r.tag, code, err.Error())
	s.finish(r, ResponseFailed, ReasonNone, err)
}

// interrupt cancels the active response with the fixed order: cancel ctx →
// FSM cancelled → terminal events → generation bump (inside finish).
func (s *Session) interrupt(reason StatusReason) {
	if s.active == nil {
		return
	}
	s.finish(s.active, ResponseCancelled, reason, nil)
}

// finish moves r to a terminal status and emits the terminal events
// synchronously; they carry no generation and are never filtered.
func (s *Session) finish(r *response, status ResponseStatus, reason StatusReason, err error) {
	if r.terminal() {
		return
	}
	if r.cancel != nil {
		r.cancel()
	}
	r.status = status
	s.stopASRWait()

	var output []Item
	if out, ok := s.conv.get(r.item); ok {
		if status == ResponseCompleted {
			out.Status = ItemCompleted
		} else {
			out.Status = ItemIncomplete
		}
		if r.textOnly {
			s.emit(EvOutputTextDone{Resp: r.ref, Item: r.item, Text: out.Text})
		} else {
			// Profile §7: audio done precedes transcript done.
			s.emit(EvOutputAudioDone{Resp: r.ref, Item: r.item})
			s.emit(EvOutputAudioTranscriptDone{Resp: r.ref, Item: r.item, Text: out.Text})
		}
		s.emit(EvOutputItemDone{Resp: r.ref, Item: out.snapshot()})
		output = append(output, out.snapshot())
	}
	// A delivered function_call item closed its own lifecycle at delivery, so
	// it is only listed here — never re-emitted, and never reopened by a
	// cancel that arrived afterwards.
	if call, ok := s.conv.get(r.toolItem); ok {
		output = append(output, call.snapshot())
	}
	s.emit(EvResponseDone{Resp: r.ref, Status: status, Reason: reason, ErrCode: r.failCode, Err: err, Usage: r.usage, Output: output})
	s.active = nil
	s.turns.Step(responseEnded{})
	s.activeGen++ // any late pipeline event is now stale

	if status == ResponseCancelled && !s.interruptAt.IsZero() {
		s.tel.Metrics.InterruptMs.Record(s.ctx, millis(time.Since(s.interruptAt)))
		s.interruptAt = time.Time{}
	}
	for _, sp := range []trace.Span{r.llmSpan, r.ttsSpan} {
		if sp != nil {
			sp.SetAttributes(observability.KeyStatus.String(string(status)))
			sp.End()
		}
	}
	r.llmSpan, r.ttsSpan = nil, nil
	r.span.SetAttributes(observability.KeyStatus.String(string(status)), observability.KeyReason.String(string(reason)))
	if err != nil {
		r.span.RecordError(err)
		r.span.SetStatus(codes.Error, string(status))
	}
	r.span.End()
}

// noteFirstText records that the turn spoke, plus LLM TTFT (and E2E for
// text-only responses).
func (s *Session) noteFirstText(r *response) {
	r.spoke = true
	if !r.firstTextAt.IsZero() {
		return
	}
	r.firstTextAt = time.Now()
	s.tel.Metrics.LLMTTFTMs.Record(s.ctx, millis(r.firstTextAt.Sub(r.startedAt)))
	if r.textOnly {
		s.tel.Metrics.E2EMs.Record(s.ctx, millis(r.firstTextAt.Sub(r.anchorAt)))
	}
}

func millis(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// toolDefs and toolChoice project the session's tool configuration onto the
// provider types. tool_choice is always resolved to an explicit value here,
// so no adapter ever falls back on a provider default.
func toolDefs(tools []config.Tool) []provider.ToolDef {
	if len(tools) == 0 {
		return nil
	}
	out := make([]provider.ToolDef, len(tools))
	for i, t := range tools {
		out[i] = provider.ToolDef{Name: t.Name, Description: t.Description, Parameters: t.Parameters}
	}
	return out
}

func toolChoice(tc config.ToolChoice) provider.ToolChoice {
	if tc.Mode == "" {
		return provider.ToolChoice{Mode: provider.ToolChoiceAuto}
	}
	return provider.ToolChoice{Mode: tc.Mode, Name: tc.Name}
}
