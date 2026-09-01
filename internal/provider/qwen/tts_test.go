package qwen

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

func openTTS(t *testing.T, f *fakeWS, url string, cfg provider.TTSConfig) provider.TTSStream {
	t.Helper()
	o := TTSOptions{wsOptions: wsOptions{URL: url, ConnectTimeout: config.Duration(2 * time.Second), IdleTimeout: config.Duration(5 * time.Second)}}
	s, err := NewTTS("test-key", o).Synthesize(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Synthesize: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// ttsScript replies to the standard synthesis lifecycle, returning audioMs of
// PCM per continue-task.
func ttsScript(f *fakeWS, audioMs int) func(recordedFrame) []reply {
	return func(fr recordedFrame) []reply {
		switch fr.Action {
		case actionRunTask:
			return []reply{f.ackStarted(fr.TaskID)}
		case actionContinueTask:
			if isNudge(fr.Text) {
				return nil // a flush nudge carries no text to synthesize
			}
			return []reply{bin(audio.MsToBytes(audioMs))}
		case actionFinishTask:
			return []reply{taskFinishedFrame(fr.TaskID)}
		}
		return nil
	}
}

// isNudge reports whether a continue-task carried only the flush whitespace.
func isNudge(text string) bool { return strings.TrimSpace(text) == "" }

// sentenceTexts returns the continue-task texts that carried real content.
func sentenceTexts(f *fakeWS) []string {
	frames, _, _ := f.snapshot()
	var out []string
	for _, fr := range frames {
		if fr.Action == actionContinueTask && !isNudge(fr.Text) {
			out = append(out, fr.Text)
		}
	}
	return out
}

func TestTTSCapabilities(t *testing.T) {
	caps := NewTTS("k", TTSOptions{}).Capabilities()
	if !caps.IncrementalText {
		t.Error("IncrementalText = false; the service accepts continue-task while audio streams")
	}
	if caps.Alignment {
		t.Error("Alignment = true, but the service leaves the word timing array empty")
	}
}

// TestTTSLifecycleAndTaskID covers the whole task: one run-task, one
// continue-task per sentence, one finish-task, all under the same task_id.
func TestTTSLifecycleAndTaskID(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = ttsScript(f, 100)
	s := openTTS(t, f, wsURL(srv), provider.TTSConfig{Voice: "longanlingxi", Speed: 1, SampleRate: audio.SampleRate})

	for _, sentence := range []string{"The ocean is vast. ", "It covers most of the planet."} {
		if err := s.WriteText(sentence); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.EndInput(); err != nil {
		t.Fatal(err)
	}
	total := 0
	for {
		c, err := s.ReadAudio()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadAudio: %v", err)
		}
		total += len(c.PCM)
		if len(c.Alignment) != 0 {
			t.Error("alignment reported, but the provider declares no alignment capability")
		}
	}
	if want := 2 * audio.MsToBytes(100); total != want {
		t.Errorf("audio = %d bytes, want %d", total, want)
	}

	frames, _, _ := f.snapshot()
	id := ""
	for _, fr := range frames {
		if id == "" {
			id = fr.TaskID
		}
		if fr.TaskID != id {
			t.Errorf("%s used task_id %s, want %s for the whole task", fr.Action, fr.TaskID, id)
		}
	}
	if id == "" {
		t.Error("frames carry no task_id")
	}
	if frames[0].Action != actionRunTask {
		t.Errorf("first action = %s, want run-task", frames[0].Action)
	}
	if last := frames[len(frames)-1].Action; last != actionFinishTask {
		t.Errorf("last action = %s, want finish-task", last)
	}
	texts := sentenceTexts(f)
	if len(texts) != 2 || texts[0] != "The ocean is vast. " || texts[1] != "It covers most of the planet." {
		t.Errorf("continue-task texts = %q, want the two sentences verbatim", texts)
	}
}

// TestTTSFlushNudge: a sentence with nothing queued behind it is followed by
// a whitespace continue-task, which is what makes the service synthesize it
// instead of waiting for the next sentence.
func TestTTSFlushNudge(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = ttsScript(f, 20)
	s := openTTS(t, f, wsURL(srv), provider.TTSConfig{Voice: "v"})
	if err := s.WriteText("One complete sentence."); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return countAction(f, actionContinueTask) >= 2 }, "the sentence and its flush nudge")
	frames, _, _ := f.snapshot()
	var texts []string
	for _, fr := range frames {
		if fr.Action == actionContinueTask {
			texts = append(texts, fr.Text)
		}
	}
	if len(texts) < 2 || !isNudge(texts[1]) {
		t.Fatalf("continue-task texts = %q, want the sentence followed by a whitespace nudge", texts)
	}
	s.EndInput()
	drainTTS(t, s)
}

// TestTTSNoFlushNudge: the workaround can be switched off.
func TestTTSNoFlushNudge(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = ttsScript(f, 20)
	o := TTSOptions{wsOptions: wsOptions{URL: wsURL(srv)}, NoFlushNudge: true}
	s, err := NewTTS("k", o).Synthesize(context.Background(), provider.TTSConfig{Voice: "v"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.WriteText("One complete sentence.")
	s.EndInput()
	drainTTS(t, s)
	frames, _, _ := f.snapshot()
	for _, fr := range frames {
		if fr.Action == actionContinueTask && isNudge(fr.Text) {
			t.Fatal("a flush nudge was sent even though no_flush_nudge is set")
		}
	}
}

func TestTTSRunTaskShape(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = ttsScript(f, 20)
	s := openTTS(t, f, wsURL(srv), provider.TTSConfig{Voice: "longanlingxi", Speed: 1.2})
	s.WriteText("hello")
	s.EndInput()
	drainTTS(t, s)

	frames, _, _ := f.snapshot()
	run := frames[0]
	if run.Model != defaultTTSModel {
		t.Errorf("model = %q, want %q", run.Model, defaultTTSModel)
	}
	if got := run.Params["format"]; got != "pcm" {
		t.Errorf("format = %v, want pcm", got)
	}
	// PCM 24 kHz output is the format Cascade streams to the client.
	if got := run.Params["sample_rate"]; got != float64(audio.SampleRate) {
		t.Errorf("sample_rate = %v, want %d", got, audio.SampleRate)
	}
	if got := run.Params["voice"]; got != "longanlingxi" {
		t.Errorf("voice = %v, want the session voice", got)
	}
	if got := run.Params["rate"]; got != 1.2 {
		t.Errorf("rate = %v, want the session speed 1.2", got)
	}
	if got := run.Params["text_type"]; got != "PlainText" {
		t.Errorf("text_type = %v, want PlainText", got)
	}
}

// TestTTSVoiceFallback: the option's voice is used only when the session
// carries none.
func TestTTSVoiceFallback(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = ttsScript(f, 20)
	o := TTSOptions{wsOptions: wsOptions{URL: wsURL(srv)}, Voice: "configured"}
	s, err := NewTTS("k", o).Synthesize(context.Background(), provider.TTSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.WriteText("x")
	s.EndInput()
	drainTTS(t, s)
	frames, _, _ := f.snapshot()
	if got := frames[0].Params["voice"]; got != "configured" {
		t.Errorf("voice = %v, want the configured fallback", got)
	}
	if got := frames[0].Params["sample_rate"]; got != float64(audio.SampleRate) {
		t.Errorf("sample_rate = %v, want the 24 kHz default", got)
	}
}

// TestTTSWaitsForTaskStarted: no text may be sent before the service
// acknowledges the task.
func TestTTSWaitsForTaskStarted(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	const ackDelay = 300 * time.Millisecond
	f.onFrame = func(fr recordedFrame) []reply {
		switch fr.Action {
		case actionRunTask:
			ack := f.ackStarted(fr.TaskID)
			ack.delay = ackDelay
			return []reply{ack}
		case actionContinueTask:
			if isNudge(fr.Text) {
				return nil
			}
			return []reply{bin(audio.MsToBytes(20))}
		case actionFinishTask:
			return []reply{taskFinishedFrame(fr.TaskID)}
		}
		return nil
	}
	s := openTTS(t, f, wsURL(srv), provider.TTSConfig{Voice: "v"})
	go func() {
		s.WriteText("first sentence")
		s.EndInput()
	}()
	time.Sleep(ackDelay / 2)
	if n := countAction(f, actionContinueTask); n != 0 {
		t.Fatalf("%d continue-task frames were sent before task-started", n)
	}
	drainTTS(t, s)
	if got := sentenceTexts(f); len(got) != 1 || got[0] != "first sentence" {
		t.Errorf("sentences = %q, want the one written after the acknowledgement", got)
	}
}

// TestTTSStreamsImmediately: audio is readable while the task is still
// running, so nothing waits for the whole utterance.
func TestTTSStreamsImmediately(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	finish := make(chan struct{})
	f.onFrame = func(fr recordedFrame) []reply {
		switch fr.Action {
		case actionRunTask:
			return []reply{f.ackStarted(fr.TaskID)}
		case actionContinueTask:
			if isNudge(fr.Text) {
				return nil
			}
			return []reply{bin(audio.MsToBytes(100))}
		case actionFinishTask:
			<-finish // hold the task open until the test has read audio
			return []reply{taskFinishedFrame(fr.TaskID)}
		}
		return nil
	}
	s := openTTS(t, f, wsURL(srv), provider.TTSConfig{Voice: "v"})
	s.WriteText("a sentence")
	s.EndInput()

	c, err := s.ReadAudio()
	if err != nil {
		t.Fatalf("ReadAudio before task-finished: %v", err)
	}
	if len(c.PCM) != audio.MsToBytes(100) {
		t.Errorf("first chunk = %d bytes, want %d", len(c.PCM), audio.MsToBytes(100))
	}
	close(finish)
	drainTTS(t, s)
}

func TestTTSTaskFailed(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = func(fr recordedFrame) []reply {
		if fr.Action == actionRunTask {
			return []reply{f.ackStarted(fr.TaskID), taskFailedFrame(fr.TaskID, "InvalidParameter", "Request voice is invalid!")}
		}
		return nil
	}
	s := openTTS(t, f, wsURL(srv), provider.TTSConfig{Voice: "nope"})
	s.WriteText("x")
	s.EndInput()
	_, err := s.ReadAudio()
	var perr *provider.Error
	if !errors.As(err, &perr) {
		t.Fatalf("ReadAudio error = %v, want a *provider.Error", err)
	}
	if perr.Kind != provider.ErrFatal {
		t.Errorf("error kind = %s, want fatal for InvalidParameter", perr.Kind)
	}
	// The terminal error is sticky.
	if _, err2 := s.ReadAudio(); !errors.Is(err2, err) {
		t.Errorf("second ReadAudio = %v, want the same terminal error", err2)
	}
}

func TestTTSMalformedMessages(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = func(fr recordedFrame) []reply {
		switch fr.Action {
		case actionRunTask:
			return []reply{
				text(`{{{ not json`),
				text(`{"header":{"task_id":"x"}}`),
				text(`{"header":{"task_id":"x","event":"result-generated"},"payload":{"output":{"sentence":{"type":"sentence-begin"}}}}`),
				f.ackStarted(fr.TaskID),
			}
		case actionContinueTask:
			if isNudge(fr.Text) {
				return nil
			}
			return []reply{bin(audio.MsToBytes(40))}
		case actionFinishTask:
			return []reply{taskFinishedFrame(fr.TaskID)}
		}
		return nil
	}
	s := openTTS(t, f, wsURL(srv), provider.TTSConfig{Voice: "v"})
	s.WriteText("x")
	s.EndInput()
	if got := drainTTS(t, s); got != audio.MsToBytes(40) {
		t.Errorf("audio = %d bytes, want %d; junk frames must not derail the task", got, audio.MsToBytes(40))
	}
}

func TestTTSUnexpectedClose(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = func(fr recordedFrame) []reply {
		switch fr.Action {
		case actionRunTask:
			return []reply{f.ackStarted(fr.TaskID)}
		case actionContinueTask:
			if isNudge(fr.Text) {
				return nil
			}
			return []reply{bin(audio.MsToBytes(20)), closeWith(websocket.StatusAbnormalClosure)}
		}
		return nil
	}
	s := openTTS(t, f, wsURL(srv), provider.TTSConfig{Voice: "v"})
	s.WriteText("x")
	s.EndInput()
	if _, err := s.ReadAudio(); err != nil {
		t.Fatalf("first ReadAudio: %v", err)
	}
	_, err := s.ReadAudio()
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("ReadAudio after an abnormal close = %v, want an error", err)
	}
	var perr *provider.Error
	if !errors.As(err, &perr) || perr.Kind != provider.ErrTransient {
		t.Errorf("error = %v, want a transient provider error", err)
	}
}

func TestTTSCancellation(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = func(fr recordedFrame) []reply {
		if fr.Action == actionRunTask {
			return []reply{f.ackStarted(fr.TaskID)}
		}
		return nil // never produce audio
	}
	ctx, cancel := context.WithCancel(context.Background())
	s, err := NewTTS("k", TTSOptions{wsOptions: wsOptions{URL: wsURL(srv)}}).Synthesize(ctx, provider.TTSConfig{Voice: "v"})
	if err != nil {
		t.Fatal(err)
	}
	s.WriteText("x")
	s.EndInput()
	read := make(chan error, 1)
	go func() { _, err := s.ReadAudio(); read <- err }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-read:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("ReadAudio = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadAudio did not return within 2s of cancellation")
	}
	waitFor(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.closeCode != 0 || f.audioBytes >= 0 }, "the socket to be torn down")
	s.Close()
}

// TestTTSCloseAbortsInFlight: Close cancels the task even mid-stream.
func TestTTSCloseAbortsInFlight(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = func(fr recordedFrame) []reply {
		if fr.Action == actionRunTask {
			return []reply{f.ackStarted(fr.TaskID)}
		}
		return nil
	}
	s := openTTS(t, f, wsURL(srv), provider.TTSConfig{Voice: "v"})
	s.WriteText("x")
	read := make(chan error, 1)
	go func() { _, err := s.ReadAudio(); read <- err }()
	time.Sleep(100 * time.Millisecond)
	s.Close()
	select {
	case err := <-read:
		if err == nil {
			t.Error("ReadAudio returned no error after Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadAudio did not return within 2s of Close")
	}
	if err := s.WriteText("more"); err == nil {
		t.Error("WriteText succeeded after Close")
	}
}

func TestTTSWriteAfterEndInput(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = ttsScript(f, 20)
	s := openTTS(t, f, wsURL(srv), provider.TTSConfig{Voice: "v"})
	s.WriteText("x")
	if err := s.EndInput(); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteText("late"); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("WriteText after EndInput = %v, want io.ErrClosedPipe", err)
	}
	if err := s.EndInput(); err != nil {
		t.Errorf("second EndInput = %v, want nil", err)
	}
	drainTTS(t, s)
}

func TestTTSRegisteredUnderQwen(t *testing.T) {
	if !provider.Known("tts", Name) {
		t.Fatal("tts provider not registered as qwen")
	}
	p, err := provider.New(provider.KindTTS, Name, "k", json.RawMessage(`{"voice":"v"}`))
	if err != nil {
		t.Fatal(err)
	}
	if p.Kind() != provider.KindTTS {
		t.Errorf("kind = %s", p.Kind())
	}
}

func TestTTSOptionsDefaults(t *testing.T) {
	var o TTSOptions
	o.applyDefaults()
	if o.URL != defaultWSURL || o.Model != defaultTTSModel || o.Voice != defaultVoice {
		t.Errorf("defaults = %+v", o)
	}
}

// drainTTS reads to EOF and returns the audio byte count.
func drainTTS(t *testing.T, s provider.TTSStream) int {
	t.Helper()
	total := 0
	for {
		c, err := s.ReadAudio()
		if errors.Is(err, io.EOF) {
			return total
		}
		if err != nil {
			t.Fatalf("ReadAudio: %v", err)
		}
		total += len(c.PCM)
	}
}
