package session

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider/mock"
)

const waitTimeout = 3 * time.Second

// harness wires a Session to the three mocks and records every event.
type harness struct {
	t   *testing.T
	s   *Session
	asr *mock.ASR
	llm *mock.LLM
	tts *mock.TTS

	mu     sync.Mutex
	events []Event
	closed chan struct{}
}

type scripts struct {
	asr mock.ASRScript
	llm mock.LLMScript
	tts mock.TTSScript
}

func defaultScripts() scripts {
	return scripts{
		asr: mock.ASRScript{Utterances: []string{"hello there", "second utterance"}},
		llm: mock.LLMScript{Tokens: []string{"Hello", " world.", " Second", " sentence", " here."}},
		tts: mock.TTSScript{MsPerRune: 10},
	}
}

func newHarness(t *testing.T, sc scripts, tweak func(*Options)) *harness {
	t.Helper()
	def := config.DefaultConfig()
	opts := Options{
		ID:      "test",
		Session: def.SessionDefaults,
		Limits:  def.Limits,
	}
	opts.Limits.ASRFinalTimeout = config.Duration(time.Second)
	opts.Limits.ClientWriteTimeout = config.Duration(200 * time.Millisecond)
	if tweak != nil {
		tweak(&opts)
	}
	h := &harness{t: t, asr: mock.NewASR(sc.asr), llm: mock.NewLLM(sc.llm), tts: mock.NewTTS(sc.tts), closed: make(chan struct{})}
	opts.ASR, opts.LLM, opts.TTS = h.asr, h.llm, h.tts
	h.s = New(opts)
	if err := h.s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	go func() {
		defer close(h.closed)
		for ev := range h.s.Events() {
			h.mu.Lock()
			h.events = append(h.events, ev)
			h.mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		h.s.Close(CloseClient)
		select {
		case <-h.s.Done():
		case <-time.After(waitTimeout):
			t.Error("session did not finish on cleanup")
		}
	})
	return h
}

func (h *harness) all() []Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]Event(nil), h.events...)
}

// waitFor blocks until an event satisfying pred has been recorded and
// returns it together with its index.
func (h *harness) waitFor(desc string, pred func(Event) bool) (Event, int) {
	h.t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		for i, ev := range h.all() {
			if pred(ev) {
				return ev, i
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	h.t.Fatalf("timeout waiting for %s; events: %s", desc, describe(h.all()))
	return nil, -1
}

func (h *harness) waitResponseDone(resp ResponseRef) EvResponseDone {
	h.t.Helper()
	ev, _ := h.waitFor("response.done", func(e Event) bool {
		d, ok := e.(EvResponseDone)
		return ok && (resp == 0 || d.Resp == resp)
	})
	return ev.(EvResponseDone)
}

func (h *harness) post(cmd Command) {
	h.t.Helper()
	if err := h.s.Post(cmd); err != nil {
		h.t.Fatalf("post %T: %v", cmd, err)
	}
}

func (h *harness) feed(pcm []byte) {
	h.t.Helper()
	frame := audio.MsToBytes(20)
	for off := 0; off < len(pcm); off += frame {
		end := min(off+frame, len(pcm))
		if err := h.s.PostAudio(pcm[off:end]); err != nil {
			h.t.Fatalf("PostAudio: %v", err)
		}
	}
}

func (h *harness) speak(ms int)   { h.feed(tone(ms)) }
func (h *harness) silence(ms int) { h.feed(make([]byte, audio.MsToBytes(ms))) }

// tone is a 440 Hz sine at half scale: well above the VAD threshold.
func tone(ms int) []byte {
	n := ms * audio.SampleRate / 1000
	buf := make([]byte, n*audio.BytesPerSample)
	for i := 0; i < n; i++ {
		audio.PutSample(buf, i, int16(0.5*math.MaxInt16*math.Sin(2*math.Pi*440*float64(i)/audio.SampleRate)))
	}
	return buf
}

func describe(events []Event) string {
	s := ""
	for _, ev := range events {
		switch e := ev.(type) {
		case EvError:
			s += " Error(" + e.Code + ")"
		case EvResponseDone:
			s += " ResponseDone(" + string(e.Status) + ")"
		default:
			s += " " + typeName(ev)
		}
	}
	return s
}

func typeName(ev Event) string {
	switch ev.(type) {
	case EvSessionUpdated:
		return "SessionUpdated"
	case EvAudioBufferCommitted:
		return "Committed"
	case EvAudioBufferCleared:
		return "Cleared"
	case EvSpeechStarted:
		return "SpeechStarted"
	case EvSpeechStopped:
		return "SpeechStopped"
	case EvItemAdded:
		return "ItemAdded"
	case EvItemDone:
		return "ItemDone"
	case EvItemDeleted:
		return "ItemDeleted"
	case EvItemTruncated:
		return "ItemTruncated"
	case EvInputTranscriptDelta:
		return "InputTranscriptDelta"
	case EvInputTranscriptDone:
		return "InputTranscriptDone"
	case EvResponseCreated:
		return "ResponseCreated"
	case EvOutputItemAdded:
		return "OutputItemAdded"
	case EvOutputTextDelta:
		return "TextDelta"
	case EvOutputAudioTranscriptDelta:
		return "TranscriptDelta"
	case EvOutputAudioDelta:
		return "AudioDelta"
	case EvOutputTextDone:
		return "TextDone"
	case EvOutputAudioTranscriptDone:
		return "TranscriptDone"
	case EvOutputAudioDone:
		return "AudioDone"
	case EvOutputItemDone:
		return "OutputItemDone"
	case EvSessionClosed:
		return "SessionClosed"
	}
	return "?"
}

func serverVAD(create, interrupt bool) func(*Options) {
	return func(o *Options) {
		td := &config.TurnDetection{Type: config.TurnDetectionServerVAD, CreateResponse: create, InterruptResponse: interrupt}
		th, pre, sil := 0.5, 100, 200
		td.Threshold, td.PrefixPaddingMs, td.SilenceDurationMs = &th, &pre, &sil
		o.Session.Audio.Input.TurnDetection = td
	}
}

func manual(o *Options) { o.Session.Audio.Input.TurnDetection = nil }

func textOnly(o *Options) { o.Session.OutputModalities = []string{config.ModalityText} }

func chain(fs ...func(*Options)) func(*Options) {
	return func(o *Options) {
		for _, f := range fs {
			f(o)
		}
	}
}

func count[T Event](events []Event) int {
	n := 0
	for _, ev := range events {
		if _, ok := ev.(T); ok {
			n++
		}
	}
	return n
}

func first[T Event](events []Event) (T, int) {
	for i, ev := range events {
		if e, ok := ev.(T); ok {
			return e, i
		}
	}
	var zero T
	return zero, -1
}

func eventually(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", desc)
}
