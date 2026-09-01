package session

import (
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider/mock"
)

func TestNormalTurnServerVAD(t *testing.T) {
	h := newHarness(t, defaultScripts(), serverVAD(true, true))
	h.silence(100)
	h.speak(300)
	h.silence(600)

	started, _ := h.waitFor("speech_started", func(e Event) bool { _, ok := e.(EvSpeechStarted); return ok })
	if st := started.(EvSpeechStarted); st.AudioStartMs != 0 || st.Item == 0 {
		// speech at 100 ms rolled back by 100 ms prefix padding → 0
		t.Fatalf("speech_started = %+v", st)
	}
	stopped, _ := h.waitFor("speech_stopped", func(e Event) bool { _, ok := e.(EvSpeechStopped); return ok })
	if sp := stopped.(EvSpeechStopped); sp.AudioEndMs != 600 || sp.Item != started.(EvSpeechStarted).Item {
		// last voiced frame ends at 400 ms + 200 ms silence window
		t.Fatalf("speech_stopped = %+v", sp)
	}
	done := h.waitResponseDone(0)
	if done.Status != ResponseCompleted {
		t.Fatalf("status = %s (%v)", done.Status, done.Err)
	}
	events := h.all()

	// Commit happened, item added with the speech item ref, transcript arrived.
	committed, ci := first[EvAudioBufferCommitted](events)
	if ci < 0 || committed.Item != started.(EvSpeechStarted).Item {
		t.Fatalf("committed = %+v", committed)
	}
	if h.asr.Streams()[0].Finalizes() != 1 {
		t.Fatal("commit must call Finalize on the ASR stream")
	}
	tr, _ := first[EvInputTranscriptDone](events)
	if tr.Item != committed.Item || tr.Text != "hello there" {
		t.Fatalf("transcript = %+v", tr)
	}

	// response.created precedes the LLM start; the request carries the Final.
	_, createdIdx := first[EvResponseCreated](events)
	_, outIdx := first[EvOutputItemAdded](events)
	if createdIdx < 0 || outIdx < createdIdx {
		t.Fatalf("response.created (%d) must precede output item (%d)", createdIdx, outIdx)
	}
	reqs := h.llm.Requests()
	if len(reqs) != 1 || len(reqs[0].Messages) != 1 || reqs[0].Messages[0].Content != "hello there" || reqs[0].Messages[0].Role != provider.RoleUser {
		t.Fatalf("llm request = %+v", reqs)
	}
	if reqs[0].Instructions != "You are a helpful assistant." {
		t.Fatalf("instructions = %q", reqs[0].Instructions)
	}

	// Output: transcript deltas, audio deltas, then done events in order.
	if count[EvOutputAudioTranscriptDelta](events) != 5 || count[EvOutputAudioDelta](events) == 0 || count[EvOutputTextDelta](events) != 0 {
		t.Fatalf("delta mix wrong: %s", describe(events))
	}
	if len(done.Output) != 1 || done.Output[0].Text != "Hello world. Second sentence here." || done.Output[0].Status != ItemCompleted {
		t.Fatalf("output = %+v", done.Output)
	}
	if done.Output[0].AudioMs != 33*10 {
		t.Fatalf("audio ms = %d", done.Output[0].AudioMs)
	}
	_, adIdx := first[EvOutputAudioDone](events)
	_, oiIdx := first[EvOutputItemDone](events)
	_, rdIdx := first[EvResponseDone](events)
	if !(adIdx < oiIdx && oiIdx < rdIdx) {
		t.Fatalf("terminal order wrong: %s", describe(events))
	}
	// Per-sentence TTS: two streams, one per sentence.
	if streams := h.tts.Streams(); len(streams) != 2 || streams[0].Text() != "Hello world." {
		t.Fatalf("tts streams = %d", len(streams))
	}
}

func TestResponseCreatedPrecedesLLMStart(t *testing.T) {
	// The Final is delayed, so the response must sit in the awaiting phase:
	// response.created has been emitted before llm.Chat is ever called, and
	// the request still carries the Final text.
	sc := defaultScripts()
	sc.asr.FinalDelay = mock.Duration(150 * time.Millisecond)
	h := newHarness(t, sc, serverVAD(true, true))
	var createdBeforeChat atomic.Bool
	var chatAt time.Time
	h.llm.OnChat = func(provider.ChatRequest) {
		chatAt = time.Now()
		createdBeforeChat.Store(count[EvResponseCreated](h.all()) == 1)
	}
	start := time.Now()
	h.speak(300)
	h.silence(600)
	d := h.waitResponseDone(0)
	if d.Status != ResponseCompleted {
		t.Fatalf("done = %+v", d)
	}
	if !createdBeforeChat.Load() {
		t.Fatal("llm.Chat was called before response.created was emitted")
	}
	if chatAt.Sub(start) < 150*time.Millisecond {
		t.Fatal("llm.Chat was called before the ASR Final arrived")
	}
	if got := h.llm.Requests()[0].Messages[0].Content; got != "hello there" {
		t.Fatalf("request text = %q", got)
	}
}

func TestCannotDeleteActiveOutputItem(t *testing.T) {
	sc := defaultScripts()
	sc.llm.Block, sc.llm.BlockAfter = true, 1
	h := newHarness(t, sc, manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "hi"}})
	h.post(CmdCreateResponse{})
	added, _ := h.waitFor("output item", func(e Event) bool { _, ok := e.(EvOutputItemAdded); return ok })
	h.post(CmdDeleteItem{Meta: Meta{Tag: "del"}, ID: added.(EvOutputItemAdded).Item.Ref})
	ev, _ := h.waitFor("refused", func(e Event) bool { er, ok := e.(EvError); return ok && er.Tag == "del" })
	if ev.(EvError).Code != ErrCodeInvalidItem {
		t.Fatalf("code = %s", ev.(EvError).Code)
	}
	h.post(CmdCancelResponse{})
	if d := h.waitResponseDone(0); d.Status != ResponseCancelled {
		t.Fatalf("done = %+v", d)
	}
	// After the response ended the item can go.
	h.post(CmdDeleteItem{ID: added.(EvOutputItemAdded).Item.Ref})
	h.waitFor("deleted", func(e Event) bool { _, ok := e.(EvItemDeleted); return ok })
}

func TestCreateResponseFalseStillCommits(t *testing.T) {
	h := newHarness(t, defaultScripts(), serverVAD(false, true))
	h.speak(300)
	h.silence(600)
	h.waitFor("committed", func(e Event) bool { _, ok := e.(EvAudioBufferCommitted); return ok })
	h.waitFor("item done", func(e Event) bool { _, ok := e.(EvItemDone); return ok })
	time.Sleep(50 * time.Millisecond)
	if count[EvResponseCreated](h.all()) != 0 {
		t.Fatalf("create_response=false must not trigger: %s", describe(h.all()))
	}
	// The client can still ask for a response, which uses the committed turn.
	h.post(CmdCreateResponse{})
	if d := h.waitResponseDone(0); d.Status != ResponseCompleted {
		t.Fatalf("status = %s", d.Status)
	}
	if reqs := h.llm.Requests(); len(reqs) != 1 || reqs[0].Messages[0].Content != "hello there" {
		t.Fatalf("request = %+v", reqs)
	}
}

func TestManualModeIsClientDriven(t *testing.T) {
	h := newHarness(t, defaultScripts(), manual)
	h.post(CmdCommitAudio{Meta: Meta{Tag: "c0"}})
	ev, _ := h.waitFor("empty commit error", func(e Event) bool { _, ok := e.(EvError); return ok })
	if e := ev.(EvError); e.Code != ErrCodeBufferEmpty || e.Tag != "c0" || e.Fatal {
		t.Fatalf("error = %+v", e)
	}
	h.speak(300)
	h.silence(300)
	time.Sleep(30 * time.Millisecond)
	if count[EvSpeechStarted](h.all()) != 0 || count[EvSpeechStopped](h.all()) != 0 || count[EvAudioBufferCommitted](h.all()) != 0 {
		t.Fatalf("manual mode emitted VAD events: %s", describe(h.all()))
	}
	h.post(CmdCommitAudio{})
	ev, _ = h.waitFor("committed", func(e Event) bool { _, ok := e.(EvAudioBufferCommitted); return ok })
	c := ev.(EvAudioBufferCommitted)
	if c.PreviousItem != 0 {
		t.Fatalf("committed = %+v", c)
	}
	added, _ := h.waitFor("item added", func(e Event) bool { _, ok := e.(EvItemAdded); return ok })
	if it := added.(EvItemAdded).Item; it.Ref != c.Item || it.Role != RoleUser || it.Content != ContentAudio || it.AudioMs != 600 || it.Status != ItemInProgress {
		t.Fatalf("item = %+v", it)
	}
	h.post(CmdCreateResponse{Meta: Meta{Tag: "r1"}})
	h.post(CmdCreateResponse{Meta: Meta{Tag: "r2"}})
	ev, _ = h.waitFor("in-progress error", func(e Event) bool { er, ok := e.(EvError); return ok && er.Code == ErrCodeResponseInProgress })
	if ev.(EvError).Tag != "r2" {
		t.Fatalf("tag = %q", ev.(EvError).Tag)
	}
	if d := h.waitResponseDone(0); d.Status != ResponseCompleted {
		t.Fatalf("status = %s", d.Status)
	}
	if h.asr.Streams()[0].Finalizes() != 1 {
		t.Fatal("manual commit must Finalize")
	}
}

func TestInterruptWhileLLMBlocked(t *testing.T) {
	sc := defaultScripts()
	sc.llm.Block, sc.llm.BlockAfter = true, 1
	h := newHarness(t, sc, serverVAD(true, true))
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "hi"}})
	h.post(CmdCreateResponse{})
	h.waitFor("first delta", func(e Event) bool { _, ok := e.(EvOutputAudioTranscriptDelta); return ok })
	created, _ := first[EvResponseCreated](h.all())

	h.speak(200) // user barges in
	done := h.waitResponseDone(created.Resp)
	if done.Status != ResponseCancelled || done.Reason != ReasonTurnDetected {
		t.Fatalf("done = %+v", done)
	}
	eventually(t, "llm ctx cancelled", func() bool { return !h.llm.CancelledAt().IsZero() })
	eventually(t, "llm goroutine exit", func() bool { return h.llm.Active() == 0 })

	// Inject a stale-generation delta straight into the pipeline channel: it
	// must be filtered, while response.done above was not.
	h.s.respEvents <- EvOutputAudioTranscriptDelta{Resp: created.Resp, Item: done.Output[0].Ref, Gen: 1, Delta: "STALE"}
	h.silence(600) // let the actor turn a few times
	h.waitFor("speech stopped", func(e Event) bool { _, ok := e.(EvSpeechStopped); return ok })
	events := h.all()
	_, doneIdx := first[EvResponseDone](events)
	for _, ev := range events[doneIdx+1:] {
		if d, ok := ev.(EvOutputAudioTranscriptDelta); ok && d.Resp == created.Resp {
			t.Fatalf("delta after cancelled response.done: %+v", d)
		}
	}
	if len(done.Output) != 1 || done.Output[0].Text != "Hello" || done.Output[0].Status != ItemIncomplete {
		t.Fatalf("cancelled output = %+v", done.Output)
	}
}

func TestInterruptDuringAwaiting(t *testing.T) {
	sc := defaultScripts()
	sc.asr.FinalDelay = mock.Duration(5 * time.Second) // Final never arrives in time
	h := newHarness(t, sc, chain(serverVAD(true, true), func(o *Options) {
		o.Limits.ASRFinalTimeout = config.Duration(10 * time.Second)
	}))
	h.speak(300)
	h.silence(600)
	created, _ := h.waitFor("response created", func(e Event) bool { _, ok := e.(EvResponseCreated); return ok })
	h.speak(200)
	done := h.waitResponseDone(created.(EvResponseCreated).Resp)
	if done.Status != ResponseCancelled || len(done.Output) != 0 {
		t.Fatalf("done = %+v", done)
	}
	if count[EvOutputItemAdded](h.all()) != 0 || len(h.llm.Requests()) != 0 {
		t.Fatalf("pipeline must not have started: %s", describe(h.all()))
	}
}

func TestTruncateTrimsContextForNextTurn(t *testing.T) {
	h := newHarness(t, defaultScripts(), manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "hi"}})
	h.post(CmdCreateResponse{})
	done := h.waitResponseDone(0)
	out := done.Output[0]
	if out.Text != "Hello world. Second sentence here." || out.AudioMs != 330 {
		t.Fatalf("output = %+v", out)
	}
	// Cut exactly at the end of the first sentence (12 runes × 10 ms).
	h.post(CmdTruncateItem{Meta: Meta{Tag: "t1"}, ID: out.Ref, AudioEndMs: 120})
	h.waitFor("truncated", func(e Event) bool { tr, ok := e.(EvItemTruncated); return ok && tr.AudioEndMs == 120 })

	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "go on"}})
	h.post(CmdCreateResponse{})
	h.waitResponseDone(2)
	reqs := h.llm.Requests()
	if len(reqs) != 2 {
		t.Fatalf("requests = %d", len(reqs))
	}
	msgs := reqs[1].Messages
	if len(msgs) != 3 || msgs[1].Role != provider.RoleAssistant || msgs[1].Content != "Hello world." || msgs[2].Content != "go on" {
		t.Fatalf("messages after truncate = %+v", msgs)
	}
	if strings.Contains(msgs[1].Content, "Second") {
		t.Fatal("truncated text leaked into context")
	}

	// Errors: unknown item, non-assistant item.
	h.post(CmdTruncateItem{Meta: Meta{Tag: "bad"}, ID: 999, AudioEndMs: 1})
	ev, _ := h.waitFor("not found", func(e Event) bool { er, ok := e.(EvError); return ok && er.Tag == "bad" })
	if ev.(EvError).Code != ErrCodeItemNotFound {
		t.Fatalf("code = %s", ev.(EvError).Code)
	}
	userItem, _ := first[EvItemAdded](h.all())
	h.post(CmdTruncateItem{Meta: Meta{Tag: "bad2"}, ID: userItem.Item.Ref, AudioEndMs: 1})
	ev, _ = h.waitFor("not truncatable", func(e Event) bool { er, ok := e.(EvError); return ok && er.Tag == "bad2" })
	if ev.(EvError).Code != ErrCodeItemNotTruncatable {
		t.Fatalf("code = %s", ev.(EvError).Code)
	}
}

func TestTruncateTierAlignmentThroughSession(t *testing.T) {
	sc := defaultScripts()
	sc.tts = mock.TTSScript{IncrementalText: true, Alignment: true, MsPerRune: 10, ChunkMs: 30}
	h := newHarness(t, sc, manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "hi"}})
	h.post(CmdCreateResponse{})
	done := h.waitResponseDone(0)
	if len(h.tts.Streams()) != 1 {
		t.Fatalf("incremental provider must get one stream, got %d", len(h.tts.Streams()))
	}
	// The stream received trimmed sentences: "Hello world." + "Second sentence here."
	// = 33 runes → 330 ms; char 12 ("S") starts at 120 ms.
	h.post(CmdTruncateItem{ID: done.Output[0].Ref, AudioEndMs: 125})
	h.waitFor("truncated", func(e Event) bool { _, ok := e.(EvItemTruncated); return ok })
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "x"}})
	h.post(CmdCreateResponse{})
	h.waitResponseDone(2)
	msgs := h.llm.Requests()[1].Messages
	if msgs[1].Content != "Hello world. " {
		t.Fatalf("aligned trim = %q", msgs[1].Content)
	}
}

func TestTruncateTierWholeResponseThroughSession(t *testing.T) {
	sc := defaultScripts()
	sc.tts = mock.TTSScript{IncrementalText: true, MsPerRune: 10}
	h := newHarness(t, sc, manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "hi"}})
	h.post(CmdCreateResponse{})
	done := h.waitResponseDone(0)
	total := done.Output[0].AudioMs
	h.post(CmdTruncateItem{ID: done.Output[0].Ref, AudioEndMs: total / 2})
	h.waitFor("truncated", func(e Event) bool { _, ok := e.(EvItemTruncated); return ok })
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "x"}})
	h.post(CmdCreateResponse{})
	h.waitResponseDone(2)
	got := h.llm.Requests()[1].Messages[1].Content
	full := done.Output[0].Text
	if !strings.HasPrefix(full, got) || len([]rune(got)) != len([]rune(full))/2 {
		t.Fatalf("proportional trim = %q of %q", got, full)
	}
}

func TestBufferOverflowClosesSession(t *testing.T) {
	h := newHarness(t, defaultScripts(), chain(manual, func(o *Options) { o.Limits.InputAudioBufferMaxMs = 100 }))
	h.silence(120)
	ev, _ := h.waitFor("fatal error", func(e Event) bool { er, ok := e.(EvError); return ok && er.Fatal })
	if ev.(EvError).Code != ErrCodeBufferOverflow {
		t.Fatalf("code = %s", ev.(EvError).Code)
	}
	closed, _ := h.waitFor("closed", func(e Event) bool { _, ok := e.(EvSessionClosed); return ok })
	if closed.(EvSessionClosed).Reason != CloseBufferOverflow {
		t.Fatalf("reason = %s", closed.(EvSessionClosed).Reason)
	}
	<-h.s.Done()
	if err := h.s.PostAudio(make([]byte, 48)); !errors.Is(err, ErrClosed) {
		t.Fatalf("post after close = %v", err)
	}
}

func TestInputQueueOverflowClosesSession(t *testing.T) {
	// Fill the outbound queue so the actor blocks in emit, then overflow the
	// bounded audio queue from the "read loop".
	def := config.DefaultConfig()
	opts := Options{ID: "q", Session: def.SessionDefaults, Limits: def.Limits}
	opts.Session.Audio.Input.TurnDetection = nil
	opts.Limits.OutputEventQueue = 1
	opts.Limits.InputAudioQueueFrames = 2
	opts.Limits.ClientWriteTimeout = config.Duration(100 * time.Millisecond)
	sc := defaultScripts()
	opts.ASR, opts.LLM, opts.TTS = mock.NewASR(sc.asr), mock.NewLLM(sc.llm), mock.NewTTS(sc.tts)
	s := New(opts)
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Two commands: the first blocks the actor in emit (two events, one
	// slot, no consumer); the second sits in the command queue.
	for range 2 {
		s.Post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "x"}})
	}
	frame := make([]byte, 48)
	var err error
	eventually(t, "queue overflow", func() bool {
		err = s.PostAudio(frame)
		return err != nil
	})
	if !errors.Is(err, ErrInputQueueFull) {
		t.Fatalf("err = %v", err)
	}
	var last Event
	for ev := range s.Events() {
		last = ev
	}
	<-s.Done()
	if c, ok := last.(EvSessionClosed); !ok || c.Reason != CloseInputOverflow {
		t.Fatalf("last event = %#v", last)
	}
}

func TestIncompleteAndFailedOutcomes(t *testing.T) {
	sc := defaultScripts()
	sc.llm.FinishReason = provider.FinishLength
	h := newHarness(t, sc, manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "hi"}})
	h.post(CmdCreateResponse{})
	if d := h.waitResponseDone(0); d.Status != ResponseIncomplete || d.Reason != ReasonMaxOutputTokens || d.Output[0].Status != ItemIncomplete {
		t.Fatalf("done = %+v", d)
	}

	sc = defaultScripts()
	sc.llm.Err = mock.ErrScripted
	h2 := newHarness(t, sc, manual)
	h2.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "hi"}})
	h2.post(CmdCreateResponse{Meta: Meta{Tag: "r"}})
	d := h2.waitResponseDone(0)
	if d.Status != ResponseFailed || !errors.Is(d.Err, mock.ErrScripted) {
		t.Fatalf("done = %+v", d)
	}
	er, ei := first[EvError](h2.all())
	_, di := first[EvResponseDone](h2.all())
	if ei < 0 || ei > di || er.Code != ErrCodeResponseFailed || er.Tag != "r" || er.Fatal {
		t.Fatalf("error = %+v (idx %d, done %d)", er, ei, di)
	}

	sc = defaultScripts()
	sc.tts.SynthErr = mock.ErrScripted
	h3 := newHarness(t, sc, manual)
	h3.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "hi"}})
	h3.post(CmdCreateResponse{})
	if d := h3.waitResponseDone(0); d.Status != ResponseFailed {
		t.Fatalf("tts failure: %+v", d)
	}
	eventually(t, "llm cancelled after tts failure", func() bool { return h3.llm.Active() == 0 })
}

func TestTextOnlyModality(t *testing.T) {
	h := newHarness(t, defaultScripts(), chain(manual, textOnly))
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "hi"}})
	h.post(CmdCreateResponse{})
	d := h.waitResponseDone(0)
	if d.Status != ResponseCompleted || d.Output[0].Content != ContentText {
		t.Fatalf("done = %+v", d)
	}
	events := h.all()
	if count[EvOutputTextDelta](events) != 5 || count[EvOutputAudioDelta](events) != 0 || count[EvOutputAudioTranscriptDelta](events) != 0 {
		t.Fatalf("delta mix: %s", describe(events))
	}
	if count[EvOutputTextDone](events) != 1 || count[EvOutputAudioDone](events) != 0 || len(h.tts.Streams()) != 0 {
		t.Fatalf("text-only must not touch TTS: %s", describe(events))
	}
	// Per-response override to audio.
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "again"}})
	h.post(CmdCreateResponse{Overrides: ResponseOverrides{OutputModalities: []string{config.ModalityAudio}}})
	if d := h.waitResponseDone(2); d.Output[0].Content != ContentAudio {
		t.Fatalf("override ignored: %+v", d)
	}
	h.post(CmdCreateResponse{Meta: Meta{Tag: "bad"}, Overrides: ResponseOverrides{OutputModalities: []string{"video"}}})
	ev, _ := h.waitFor("bad override", func(e Event) bool { er, ok := e.(EvError); return ok && er.Tag == "bad" })
	if ev.(EvError).Code != ErrCodeInvalidOverrides {
		t.Fatalf("code = %s", ev.(EvError).Code)
	}
}

func TestSessionUpdate(t *testing.T) {
	h := newHarness(t, defaultScripts(), serverVAD(true, true))
	instr := "Be brief."
	h.post(CmdUpdateSession{Meta: Meta{Tag: "u1"}, Patch: SessionPatch{
		Instructions:  &instr,
		TurnDetection: Nullable[config.TurnDetection]{Set: true}, // null → manual
	}})
	ev, _ := h.waitFor("session updated", func(e Event) bool { _, ok := e.(EvSessionUpdated); return ok })
	cfg := ev.(EvSessionUpdated).Config
	if cfg.Instructions != instr || cfg.Audio.Input.TurnDetection != nil || cfg.Audio.Output.Voice != "alloy" {
		t.Fatalf("echo = %+v", cfg)
	}
	h.speak(300)
	h.silence(400)
	time.Sleep(30 * time.Millisecond)
	if count[EvSpeechStarted](h.all()) != 0 {
		t.Fatal("VAD still running after switching to manual")
	}
	bad := 3.0
	h.post(CmdUpdateSession{Meta: Meta{Tag: "u2"}, Patch: SessionPatch{Speed: &bad}})
	ev, _ = h.waitFor("invalid update", func(e Event) bool { er, ok := e.(EvError); return ok && er.Tag == "u2" })
	if e := ev.(EvError); e.Code != ErrCodeInvalidSession || !strings.Contains(e.Message, "session.audio.output.speed") {
		t.Fatalf("error = %+v", e)
	}
	// Instructions apply to the next response.
	h.post(CmdCommitAudio{})
	h.post(CmdCreateResponse{})
	h.waitResponseDone(0)
	if reqs := h.llm.Requests(); reqs[0].Instructions != instr {
		t.Fatalf("instructions = %q", reqs[0].Instructions)
	}
	// Switching to semantic_vad rebuilds the detector and echoes eagerness.
	high := "high"
	h.post(CmdUpdateSession{Patch: SessionPatch{TurnDetection: Nullable[config.TurnDetection]{Set: true, Value: &config.TurnDetection{Type: config.TurnDetectionSemanticVAD, Eagerness: &high, CreateResponse: true, InterruptResponse: true}}}})
	ev, _ = h.waitFor("semantic echo", func(e Event) bool {
		u, ok := e.(EvSessionUpdated)
		return ok && u.Config.Audio.Input.TurnDetection != nil
	})
	td := ev.(EvSessionUpdated).Config.Audio.Input.TurnDetection
	if td.Type != config.TurnDetectionSemanticVAD || *td.Eagerness != "high" || td.Threshold != nil {
		t.Fatalf("echo = %+v", td)
	}
}

func TestSemanticVADEndOfTurnThroughSession(t *testing.T) {
	sc := defaultScripts()
	sc.asr.Utterances = []string{"what time is it?"}
	h := newHarness(t, sc, func(o *Options) {
		e := "high"
		o.Session.Audio.Input.TurnDetection = &config.TurnDetection{Type: config.TurnDetectionSemanticVAD, Eagerness: &e, CreateResponse: true, InterruptResponse: true}
	})
	h.speak(300)
	h.silence(700)
	h.waitFor("speech stopped", func(e Event) bool { _, ok := e.(EvSpeechStopped); return ok })
	time.Sleep(30 * time.Millisecond)
	if count[EvAudioBufferCommitted](h.all()) != 0 {
		t.Fatal("semantic_vad must not commit on VAD end alone")
	}
	// No Final arrives without Finalize, so the eagerness timer must not be
	// armed yet either; a client commit resolves the turn.
	h.post(CmdCommitAudio{})
	h.post(CmdCreateResponse{})
	if d := h.waitResponseDone(0); d.Status != ResponseCompleted {
		t.Fatalf("done = %+v", d)
	}
	if h.llm.Requests()[0].Messages[0].Content != "what time is it?" {
		t.Fatalf("request = %+v", h.llm.Requests()[0].Messages)
	}
}

func TestASRFinalTimeoutFallsBackToPartialOrFails(t *testing.T) {
	sc := defaultScripts()
	sc.asr.PartialAfterMs = 100
	sc.asr.FinalDelay = mock.Duration(2 * time.Second)
	h := newHarness(t, sc, chain(manual, func(o *Options) { o.Limits.ASRFinalTimeout = config.Duration(100 * time.Millisecond) }))
	h.speak(300)
	h.post(CmdCommitAudio{})
	h.post(CmdCreateResponse{})
	d := h.waitResponseDone(0)
	if d.Status != ResponseCompleted {
		t.Fatalf("done = %+v", d)
	}
	if got := h.llm.Requests()[0].Messages[0].Content; got != "hello" { // first half of "hello there"
		t.Fatalf("partial not used: %q", got)
	}

	sc = defaultScripts()
	sc.asr.FinalDelay = mock.Duration(2 * time.Second)
	h2 := newHarness(t, sc, chain(manual, func(o *Options) { o.Limits.ASRFinalTimeout = config.Duration(100 * time.Millisecond) }))
	h2.speak(300)
	h2.post(CmdCommitAudio{})
	h2.post(CmdCreateResponse{})
	if d := h2.waitResponseDone(0); d.Status != ResponseFailed || len(h2.llm.Requests()) != 0 {
		t.Fatalf("no partial must fail: %+v", d)
	}
}

func TestItemCommandsAndClear(t *testing.T) {
	h := newHarness(t, defaultScripts(), manual)
	h.post(CmdCreateItem{Meta: Meta{Tag: "bad"}, Item: ItemSpec{Role: "robot", Text: "x"}})
	ev, _ := h.waitFor("bad role", func(e Event) bool { er, ok := e.(EvError); return ok && er.Tag == "bad" })
	if ev.(EvError).Code != ErrCodeInvalidItem {
		t.Fatal("bad role code")
	}
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleSystem, Text: "sys"}})
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "u"}})
	sysAdded, _ := h.waitFor("sys added", func(e Event) bool { a, ok := e.(EvItemAdded); return ok && a.Item.Role == RoleSystem })
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleAssistant, Text: "a"}, AtRoot: true})
	h.waitFor("root added", func(e Event) bool {
		a, ok := e.(EvItemAdded)
		return ok && a.Item.Role == RoleAssistant && a.PreviousItem == 0
	})
	h.post(CmdDeleteItem{Meta: Meta{Tag: "d"}, ID: sysAdded.(EvItemAdded).Item.Ref})
	h.waitFor("deleted", func(e Event) bool { _, ok := e.(EvItemDeleted); return ok })
	h.post(CmdDeleteItem{Meta: Meta{Tag: "d2"}, ID: 4242})
	h.waitFor("delete unknown", func(e Event) bool {
		er, ok := e.(EvError)
		return ok && er.Tag == "d2" && er.Code == ErrCodeItemNotFound
	})

	h.silence(100)
	h.post(CmdClearAudio{})
	h.waitFor("cleared", func(e Event) bool { _, ok := e.(EvAudioBufferCleared); return ok })
	h.post(CmdCommitAudio{Meta: Meta{Tag: "empty"}})
	h.waitFor("empty after clear", func(e Event) bool { er, ok := e.(EvError); return ok && er.Tag == "empty" })

	h.post(CmdCreateResponse{})
	h.waitResponseDone(0)
	msgs := h.llm.Requests()[0].Messages
	if len(msgs) != 2 || msgs[0].Content != "a" || msgs[1].Content != "u" {
		t.Fatalf("context = %+v", msgs)
	}
}

func TestCancelResponseCommand(t *testing.T) {
	sc := defaultScripts()
	sc.llm.Block, sc.llm.BlockAfter = true, 2
	h := newHarness(t, sc, manual)
	h.post(CmdCancelResponse{}) // nothing active: no-op
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "hi"}})
	h.post(CmdCreateResponse{})
	h.waitFor("delta", func(e Event) bool { _, ok := e.(EvOutputAudioTranscriptDelta); return ok })
	h.post(CmdCancelResponse{Resp: 99}) // wrong ref: no-op
	time.Sleep(20 * time.Millisecond)
	if count[EvResponseDone](h.all()) != 0 {
		t.Fatal("cancel with a foreign ref must be ignored")
	}
	h.post(CmdCancelResponse{})
	if d := h.waitResponseDone(0); d.Status != ResponseCancelled || d.Reason != ReasonClientCancelled {
		t.Fatalf("done = %+v", d)
	}
}

func TestCleanupReclaimsGoroutines(t *testing.T) {
	baseline := runtime.NumGoroutine()
	for i := 0; i < 3; i++ {
		sc := defaultScripts()
		if i == 1 {
			sc.llm.Block, sc.llm.BlockAfter = true, 1 // close mid-generation
		}
		def := config.DefaultConfig()
		opts := Options{ID: "g", Session: def.SessionDefaults, Limits: def.Limits}
		opts.Session.Audio.Input.TurnDetection = nil
		opts.Limits.ClientWriteTimeout = config.Duration(100 * time.Millisecond)
		opts.ASR, opts.LLM, opts.TTS = mock.NewASR(sc.asr), mock.NewLLM(sc.llm), mock.NewTTS(sc.tts)
		s := New(opts)
		if err := s.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		drained := make(chan struct{})
		var sawDone bool
		go func() {
			defer close(drained)
			for ev := range s.Events() {
				if _, ok := ev.(EvResponseDone); ok {
					sawDone = true
					if i != 1 {
						s.Close(CloseClient)
					}
				}
				if _, ok := ev.(EvOutputAudioTranscriptDelta); ok && i == 1 {
					s.Close(CloseClient)
				}
			}
		}()
		s.Post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "hi"}})
		s.Post(CmdCreateResponse{})
		select {
		case <-s.Done():
		case <-time.After(waitTimeout):
			t.Fatal("session did not stop")
		}
		<-drained
		if i != 1 && !sawDone {
			t.Fatal("expected a completed response before close")
		}
		if err := s.Post(CmdCommitAudio{}); !errors.Is(err, ErrClosed) {
			t.Fatalf("post after close = %v", err)
		}
	}
	eventually(t, "goroutines back to baseline", func() bool { return runtime.NumGoroutine() <= baseline })
}

func TestArrivalOrderAcrossQueues(t *testing.T) {
	// clear → append must not lose the append, and append → commit must
	// include the append, regardless of which queue the actor polls first.
	h := newHarness(t, defaultScripts(), manual)
	for i := 0; i < 50; i++ {
		h.post(CmdClearAudio{})
		h.silence(40)
		h.post(CmdCommitAudio{})
	}
	h.waitFor("50th commit", func(e Event) bool {
		return count[EvAudioBufferCommitted](h.all()) == 50
	})
	for _, ev := range h.all() {
		if a, ok := ev.(EvItemAdded); ok && a.Item.AudioMs != 40 {
			t.Fatalf("item audio = %d ms, want 40 (order violated)", a.Item.AudioMs)
		}
		if er, ok := ev.(EvError); ok {
			t.Fatalf("unexpected error %+v", er)
		}
	}
}

func TestAudioMsAccounting(t *testing.T) {
	h := newHarness(t, defaultScripts(), manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "hi"}})
	h.post(CmdCreateResponse{})
	d := h.waitResponseDone(0)
	total := 0
	for _, ev := range h.all() {
		if a, ok := ev.(EvOutputAudioDelta); ok {
			total += len(a.PCM)
		}
	}
	if audio.BytesToMs(total) != d.Output[0].AudioMs {
		t.Fatalf("audio deltas %d ms vs item %d ms", audio.BytesToMs(total), d.Output[0].AudioMs)
	}
}
