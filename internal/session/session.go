package session

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
	"github.com/rasonyang/cascade-realtime-gateway/internal/recorder"
	"github.com/rasonyang/cascade-realtime-gateway/internal/vad"
)

// Options configures one Session. Session is deep-copied into an immutable
// snapshot; Limits supplies every queue size and timeout.
type Options struct {
	ID       string
	Session  config.SessionDefaults
	Limits   config.Limits
	ASR      provider.ASR
	LLM      provider.LLM
	TTS      provider.TTS
	Logger   *slog.Logger
	Recorder recorder.Recorder
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
	log    *slog.Logger
	rec    recorder.Recorder

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

	speechItem    ItemRef // pre-allocated at speech start, reused by commit
	pendingItem   *item   // committed audio item awaiting its transcript
	transcriptAcc string  // Finals accumulated since the last commit
	partial       string  // latest ASR partial

	asrWaitTimer *time.Timer
	asrWait      <-chan time.Time
	eotTimer     *time.Timer
	eot          <-chan time.Time

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
		log:        log.With("session_id", opts.ID),
		rec:        opts.Recorder,
		cmds:       make(chan cmdEnvelope, opts.Limits.OutputEventQueue),
		audioIn:    make(chan audioFrame, opts.Limits.InputAudioQueueFrames),
		respEvents: make(chan Event, opts.Limits.OutputEventQueue),
		events:     make(chan Event, opts.Limits.OutputEventQueue),
		done:       make(chan struct{}),
		buffer:     newInputAudioBuffer(opts.Limits.InputAudioBufferMaxMs),
		conv:       newConversation(),
	}
	s.applyTurnDetection()
	return s
}

// Start opens the ASR stream and launches the actor goroutine.
func (s *Session) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancelCause(ctx)
	stream, err := s.asr.OpenStream(s.ctx, s.asrConfig())
	if err != nil {
		s.cancel(closeError{CloseProviderError})
		close(s.events)
		close(s.done)
		return fmt.Errorf("open asr stream: %w", err)
	}
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
	var ce closeError
	if errors.As(context.Cause(ctx), &ce) {
		return ce.reason
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
}

// ---- configuration ---------------------------------------------------------

func (s *Session) asrConfig() provider.ASRConfig {
	cfg := provider.ASRConfig{SampleRate: audio.SampleRate}
	if tr := s.cfg.Audio.Input.Transcription; tr != nil {
		cfg.Language, cfg.Prompt, cfg.Model = tr.Language, tr.Prompt, tr.Model
	}
	return cfg
}

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
	switch c.Item.Role {
	case RoleUser, RoleAssistant, RoleSystem:
	default:
		s.emitParamError(tag, ErrCodeInvalidItem, "item.role", fmt.Sprintf("unsupported role %q", c.Item.Role))
		return
	}
	if c.Item.Text == "" {
		s.emitParamError(tag, ErrCodeInvalidItem, "item.content", "item text must not be empty")
		return
	}
	it := &item{Item: Item{ClientID: c.Item.ClientID, Role: c.Item.Role, Content: ContentText, Status: ItemCompleted, Text: c.Item.Text, TranscriptDone: true}}
	prev, ok := s.conv.insert(it, c.PreviousItem, c.AtRoot)
	if !ok {
		s.emitParamError(tag, ErrCodeItemNotFound, "previous_item_id", "previous item not found")
		return
	}
	s.emit(EvItemAdded{Item: it.snapshot(), PreviousItem: prev})
	s.emit(EvItemDone{Item: it.snapshot()})
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
	it.Text = trimText(it.Text, it.segments, it.alignment, cut, it.audioB)
	if cut < it.audioB {
		it.audioB = cut
		it.AudioMs = c.AudioEndMs
	}
	s.emit(EvItemTruncated{ID: c.ID, ContentIndex: c.ContentIndex, AudioEndMs: c.AudioEndMs})
}

// ---- audio and turns -------------------------------------------------------

func (s *Session) handleAudio(pcm []byte) {
	if err := s.buffer.append(pcm); err != nil {
		s.fatal(ErrCodeBufferOverflow, "input audio buffer exceeded input_audio_buffer_max_ms", CloseBufferOverflow)
		return
	}
	if err := s.asrStream.PushAudio(pcm); err != nil {
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
				s.emit(EvSpeechStarted{AudioStartMs: startMs, Item: s.speechItem})
			}
			if d.Interrupt {
				s.interrupt(ReasonTurnDetected)
			}
			s.apply(d, "")
		case vad.SpeechEnd:
			endMs := s.buffer.markSpeechEnd(ev.OffsetMs, s.vadSilenceMs())
			d := s.turns.Step(vadSpeechEnd{})
			if d.EmitSpeechStopped {
				s.emit(EvSpeechStopped{AudioEndMs: endMs, Item: s.speechItem})
			}
			s.apply(d, "")
		}
	}
}

// apply executes a TurnDecision's commit / trigger / timer parts. Speech
// events and interrupts are handled at the call site because they need the
// millisecond values.
func (s *Session) apply(d TurnDecision, tag string) {
	if d.ArmEndOfTurnTimer {
		s.armEndOfTurn()
	}
	if d.Commit {
		s.commit(tag)
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

func (s *Session) commit(tag string) {
	pcm, startMs, endMs, ok := s.buffer.commit()
	if !ok {
		// PROTOCOL-VERIFY: GA may enforce a minimum buffer duration; Phase 2.
		s.emitError(tag, ErrCodeBufferEmpty, "input audio buffer is empty")
		return
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
	s.emit(EvAudioBufferCommitted{Item: ref, PreviousItem: prev})
	s.emit(EvItemAdded{Item: it.snapshot(), PreviousItem: prev})
	if err := s.asrStream.Finalize(); err != nil {
		s.fatal(ErrCodeProviderError, "asr finalize: "+err.Error(), CloseProviderError)
	}
}

func (s *Session) armEndOfTurn() {
	s.disarmEndOfTurn()
	s.eotTimer = time.NewTimer(time.Duration(s.eagernessMs()) * time.Millisecond)
	s.eot = s.eotTimer.C
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
		s.fatal(ErrCodeProviderError, "asr: "+ev.Err.Error(), CloseProviderError)
	}
}

// completePendingItem finalizes the committed audio item's transcript and
// releases any response awaiting it.
func (s *Session) completePendingItem(text, delta string) {
	it := s.pendingItem
	s.pendingItem = nil
	s.transcriptAcc = ""
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
	}
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

	content := ContentAudio
	if r.textOnly {
		content = ContentText
	}
	out := &item{Item: Item{Role: RoleAssistant, Content: content, Status: ItemInProgress, TranscriptDone: true}}
	prev := s.conv.append(out)
	r.item = out.Ref
	s.emit(EvOutputItemAdded{Resp: r.ref, Item: out.snapshot(), PreviousItem: prev})

	ctx, cancel := context.WithCancel(s.ctx)
	r.cancel = cancel
	p := &pipeline{
		ctx: ctx, gen: r.gen, resp: r.ref, item: r.item, textOnly: r.textOnly,
		req: provider.ChatRequest{
			Instructions:    r.instructions,
			Messages:        s.conv.messages(),
			MaxOutputTokens: r.maxTokens,
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
	out, ok := s.conv.get(r.item)
	if !ok {
		return // defensive: the output item is protected from deletion above
	}
	switch e := ev.(type) {
	case EvOutputTextDelta:
		out.Text += e.Delta
		s.emit(e)
	case EvOutputAudioTranscriptDelta:
		out.Text += e.Delta
		s.emit(e)
	case EvOutputAudioDelta:
		out.audioB += len(e.PCM)
		out.AudioMs = audio.BytesToMs(out.audioB)
		s.audioEmitted = true
		s.emit(e)
	case pipeAlignment:
		out.alignment = append(out.alignment, e.Timings...)
	case pipeSegment:
		out.segments = append(out.segments, e.Seg)
	case pipeLLMDone:
		r.llmDone = true
		r.finish = e.Finish
		r.usage = e.Usage
		s.maybeComplete(r)
	case pipeTTSDone:
		r.ttsDone = true
		r.audioFlushed = true // deltas precede this event on the same channel
		if len(out.segments) == 0 && len(out.alignment) == 0 {
			// Incremental provider without alignment: tier-3 single segment.
			out.segments = []audioSegment{{textStart: 0, textEnd: len(out.Text), audioStart: 0, audioEnd: out.audioB}}
		}
		s.maybeComplete(r)
	case pipeError:
		s.log.Error("pipeline error", "stage", e.Stage, "err", e.Err)
		s.failResponse(r, ErrCodeProviderError, e.Err)
	}
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
	if r.item != 0 {
		out, _ := s.conv.get(r.item)
		if out != nil {
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
			output = []Item{out.snapshot()}
		}
	}
	s.emit(EvResponseDone{Resp: r.ref, Status: status, Reason: reason, ErrCode: r.failCode, Err: err, Usage: r.usage, Output: output})
	s.active = nil
	s.turns.Step(responseEnded{})
	s.activeGen++ // any late pipeline event is now stale
}
