package qwen

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// openASR wires a provider to the fake and opens one stream.
func openASR(t *testing.T, f *fakeWS, url string, tweak func(*ASROptions)) provider.ASRStream {
	t.Helper()
	o := ASROptions{wsOptions: wsOptions{URL: url, ConnectTimeout: config.Duration(2 * time.Second), IdleTimeout: config.Duration(5 * time.Second)}}
	if tweak != nil {
		tweak(&o)
	}
	a := NewASR("test-key", o)
	s, err := a.OpenStream(context.Background(), provider.ASRConfig{SampleRate: audio.SampleRate, Language: "en"})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// nextEvent reads one ASR event with a deadline.
func nextEvent(t *testing.T, s provider.ASRStream) (provider.ASREvent, bool) {
	t.Helper()
	select {
	case ev, ok := <-s.Events():
		return ev, ok
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for an ASR event")
		return provider.ASREvent{}, false
	}
}

func TestASRRunTaskShape(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	started := make(chan struct{})
	f.onFrame = func(fr recordedFrame) []reply {
		if fr.Action == actionRunTask {
			defer close(started)
			return []reply{f.ackStarted(fr.TaskID)}
		}
		return nil
	}
	openASR(t, f, wsURL(srv), nil)
	<-started

	frames, _, _ := f.snapshot()
	if len(frames) != 1 || frames[0].Action != actionRunTask {
		t.Fatalf("first frame = %+v, want one run-task", frames)
	}
	run := frames[0]
	if run.Model != defaultASRModel {
		t.Errorf("model = %q, want %q", run.Model, defaultASRModel)
	}
	if got := run.Params["format"]; got != "pcm" {
		t.Errorf("format = %v, want pcm", got)
	}
	// PCM 24 kHz is the only format Cascade speaks.
	if got := run.Params["sample_rate"]; got != float64(audio.SampleRate) {
		t.Errorf("sample_rate = %v, want %d", got, audio.SampleRate)
	}
	hints, _ := run.Params["language_hints"].([]any)
	if len(hints) != 1 || hints[0] != "en" {
		t.Errorf("language_hints = %v, want [en]", run.Params["language_hints"])
	}
	f.mu.Lock()
	auth, inspection := f.auth, f.inspection
	f.mu.Unlock()
	if auth != "bearer test-key" {
		t.Errorf("Authorization = %q, want lowercase bearer scheme", auth)
	}
	if inspection != "enable" {
		t.Errorf("X-DashScope-DataInspection = %q, want enable", inspection)
	}
}

// TestASRWaitsForTaskStarted is the protocol's hard rule: not one audio byte
// may reach the service before task-started.
func TestASRWaitsForTaskStarted(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	const ackDelay = 400 * time.Millisecond
	f.onFrame = func(fr recordedFrame) []reply {
		if fr.Action == actionRunTask {
			ack := f.ackStarted(fr.TaskID)
			ack.delay = ackDelay
			return []reply{ack}
		}
		return nil
	}
	s := openASR(t, f, wsURL(srv), nil)

	// Push a full turn of audio while the acknowledgement is still pending.
	frame := make([]byte, audio.MsToBytes(20))
	const frames = 25
	for i := 0; i < frames; i++ {
		if err := s.PushAudio(frame); err != nil {
			t.Fatalf("PushAudio: %v", err)
		}
	}
	time.Sleep(ackDelay / 2)
	if _, total, _ := f.snapshot(); total != 0 {
		t.Fatalf("%d audio bytes reached the service before task-started", total)
	}

	// Once acknowledged the queued audio flows, and none of it was early.
	waitFor(t, func() bool { _, total, _ := f.snapshot(); return total == frames*len(frame) }, "queued audio to be flushed after task-started")
	if _, _, beforeStart := f.snapshot(); beforeStart != 0 {
		t.Errorf("%d audio bytes were sent before task-started", beforeStart)
	}
}

func TestASRPartialAndFinal(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = func(fr recordedFrame) []reply {
		switch fr.Action {
		case actionRunTask:
			return []reply{f.ackStarted(fr.TaskID)}
		case actionFinishTask:
			return []reply{
				asrSentenceFrame(fr.TaskID, "The ocean is vast and deep.", 160, 2500, true),
				taskFinishedFrame(fr.TaskID),
			}
		}
		return nil
	}
	sent := 0
	f.onAudio = func(total int) []reply {
		sent++
		if sent == 1 {
			return []reply{asrSentenceFrame("", "The ocean", 160, 0, false)}
		}
		return nil
	}
	s := openASR(t, f, wsURL(srv), nil)
	frame := make([]byte, audio.MsToBytes(20))
	for i := 0; i < 3; i++ {
		if err := s.PushAudio(frame); err != nil {
			t.Fatal(err)
		}
	}

	ev, _ := nextEvent(t, s)
	if ev.Kind != provider.ASRPartial || ev.Text != "The ocean" {
		t.Fatalf("first event = %+v, want partial %q", ev, "The ocean")
	}
	if ev.StartMs != 160 {
		t.Errorf("partial StartMs = %d, want 160", ev.StartMs)
	}

	if err := s.Finalize(); err != nil {
		t.Fatal(err)
	}
	ev, _ = nextEvent(t, s)
	if ev.Kind != provider.ASRFinal || ev.Text != "The ocean is vast and deep." {
		t.Fatalf("second event = %+v, want the final transcript", ev)
	}
	if ev.StartMs != 160 || ev.EndMs != 2500 {
		t.Errorf("final offsets = %d..%d, want 160..2500", ev.StartMs, ev.EndMs)
	}
	ev, _ = nextEvent(t, s)
	if ev.Kind != provider.ASREndOfTurn {
		t.Fatalf("third event = %+v, want end_of_turn", ev)
	}
}

// TestASRFinalizeRestartsTask covers the full finish lifecycle: Finalize ends
// the task and the next turn runs as a new task on the same connection.
func TestASRFinalizeRestartsTask(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = func(fr recordedFrame) []reply {
		switch fr.Action {
		case actionRunTask:
			return []reply{f.ackStarted(fr.TaskID)}
		case actionFinishTask:
			return []reply{
				asrSentenceFrame(fr.TaskID, "turn text", 0, 500, true),
				taskFinishedFrame(fr.TaskID),
			}
		}
		return nil
	}
	s := openASR(t, f, wsURL(srv), nil)
	frame := make([]byte, audio.MsToBytes(100))

	for turn := 0; turn < 2; turn++ {
		if err := s.PushAudio(frame); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { _, total, _ := f.snapshot(); return total >= (turn+1)*len(frame) }, "audio to reach the service")
		if err := s.Finalize(); err != nil {
			t.Fatal(err)
		}
		ev, _ := nextEvent(t, s)
		if ev.Kind != provider.ASRFinal {
			t.Fatalf("turn %d: event = %+v, want final", turn, ev)
		}
		// The second turn's offsets are shifted by the first turn's audio.
		if want := turn * 100; ev.StartMs != want {
			t.Errorf("turn %d: StartMs = %d, want %d (audio timeline offset)", turn, ev.StartMs, want)
		}
		if ev, _ := nextEvent(t, s); ev.Kind != provider.ASREndOfTurn {
			t.Fatalf("turn %d: event = %+v, want end_of_turn", turn, ev)
		}
		waitFor(t, func() bool { return countAction(f, actionRunTask) == turn+2 }, "the next run-task")
	}

	frames, _, before := f.snapshot()
	if before != 0 {
		t.Errorf("%d audio bytes were sent before a task-started", before)
	}
	var runIDs []string
	for _, fr := range frames {
		if fr.Action == actionRunTask {
			runIDs = append(runIDs, fr.TaskID)
		}
		if fr.TaskID == "" {
			t.Errorf("frame %s carries no task_id", fr.Action)
		}
	}
	if len(runIDs) < 3 {
		t.Fatalf("run-task count = %d, want 3 (initial plus one per finalized turn)", len(runIDs))
	}
	for i := 1; i < len(runIDs); i++ {
		if runIDs[i] == runIDs[i-1] {
			t.Errorf("run-task %d reused task_id %s; each task needs a fresh id", i, runIDs[i])
		}
	}
	// finish-task must always carry the id of the task it ends.
	var current string
	for _, fr := range frames {
		switch fr.Action {
		case actionRunTask:
			current = fr.TaskID
		case actionFinishTask:
			if fr.TaskID != current {
				t.Errorf("finish-task task_id = %s, want the running task %s", fr.TaskID, current)
			}
		}
	}
}

// TestASRFinalizeTimeout: when the service never sends task-finished, the
// stream still honors the contract with a synthesized EndOfTurn.
func TestASRFinalizeTimeout(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = func(fr recordedFrame) []reply {
		if fr.Action == actionRunTask {
			return []reply{f.ackStarted(fr.TaskID)}
		}
		return nil // swallow finish-task
	}
	s := openASR(t, f, wsURL(srv), func(o *ASROptions) {
		o.FinalizeTimeout = config.Duration(150 * time.Millisecond)
	})
	waitFor(t, func() bool { return countAction(f, actionRunTask) == 1 }, "the first run-task")
	if err := s.Finalize(); err != nil {
		t.Fatal(err)
	}
	ev, _ := nextEvent(t, s)
	if ev.Kind != provider.ASREndOfTurn {
		t.Fatalf("event = %+v, want a synthesized end_of_turn", ev)
	}
	waitFor(t, func() bool { return countAction(f, actionRunTask) == 2 }, "the task to restart after the timeout")
}

func TestASRTaskFailed(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = func(fr recordedFrame) []reply {
		if fr.Action == actionRunTask {
			return []reply{taskFailedFrame(fr.TaskID, "Throttling.RateQuota", "Requests rate limit exceeded")}
		}
		return nil
	}
	s := openASR(t, f, wsURL(srv), nil)
	ev, _ := nextEvent(t, s)
	if ev.Kind != provider.ASRError {
		t.Fatalf("event = %+v, want an error", ev)
	}
	var perr *provider.Error
	if !errors.As(ev.Err, &perr) {
		t.Fatalf("error %v is not a *provider.Error", ev.Err)
	}
	if perr.Kind != provider.ErrRateLimit {
		t.Errorf("error kind = %s, want rate_limit", perr.Kind)
	}
	if perr.Provider != Name {
		t.Errorf("error provider = %s, want %s", perr.Provider, Name)
	}
	// The stream is finished: the events channel closes.
	if _, ok := nextEvent(t, s); ok {
		t.Error("events channel still open after task-failed")
	}
}

// TestASRMalformedMessages: junk frames are ignored, and a well-formed frame
// after them is still delivered.
func TestASRMalformedMessages(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = func(fr recordedFrame) []reply {
		if fr.Action == actionRunTask {
			return []reply{
				text(`not json at all`),
				text(`{"header":{}}`),
				text(`{"header":{"task_id":"x","event":"unknown-event"},"payload":{"weird":true}}`),
				text(`{"header":{"task_id":"x","event":"result-generated"},"payload":{"output":{}}}`),
				text(`{"header":{"task_id":"x","event":"result-generated"},"payload":"not-an-object"}`),
				f.ackStarted(fr.TaskID),
				asrSentenceFrame(fr.TaskID, "still here", 0, 0, false),
			}
		}
		return nil
	}
	s := openASR(t, f, wsURL(srv), nil)
	ev, ok := nextEvent(t, s)
	if !ok {
		t.Fatal("stream died on malformed frames")
	}
	if ev.Kind != provider.ASRPartial || ev.Text != "still here" {
		t.Fatalf("event = %+v, want the partial that followed the junk", ev)
	}
}

func TestASRUnexpectedClose(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = func(fr recordedFrame) []reply {
		if fr.Action == actionRunTask {
			return []reply{f.ackStarted(fr.TaskID), closeWith(websocket.StatusInternalError)}
		}
		return nil
	}
	s := openASR(t, f, wsURL(srv), nil)
	ev, ok := nextEvent(t, s)
	if !ok {
		t.Fatal("events channel closed without reporting the abnormal close")
	}
	if ev.Kind != provider.ASRError {
		t.Fatalf("event = %+v, want an error", ev)
	}
	var perr *provider.Error
	if !errors.As(ev.Err, &perr) || perr.Kind != provider.ErrTransient {
		t.Errorf("error = %v, want a transient provider error", ev.Err)
	}
}

// TestASRCancellation: cancelling ctx closes the socket and every goroutine,
// and the events channel is closed.
func TestASRCancellation(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = func(fr recordedFrame) []reply {
		if fr.Action == actionRunTask {
			return []reply{f.ackStarted(fr.TaskID)}
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := NewASR("k", ASROptions{wsOptions: wsOptions{URL: wsURL(srv)}})
	s, err := a.OpenStream(ctx, provider.ASRConfig{SampleRate: audio.SampleRate})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return countAction(f, actionRunTask) == 1 }, "the first run-task")
	cancel()
	select {
	case _, ok := <-s.Events():
		if ok {
			// drain until closed
			for range s.Events() {
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("events channel not closed within 2s of cancellation")
	}
	if err := s.PushAudio(make([]byte, 10)); err == nil {
		t.Error("PushAudio succeeded after cancellation")
	}
	if err := s.Finalize(); err == nil {
		t.Error("Finalize succeeded after cancellation")
	}
	s.Close() // must not hang or panic
}

// TestASRCloseEndsTask: Close ends the task politely and reclaims the socket
// without emitting a spurious EndOfTurn.
func TestASRCloseEndsTask(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = func(fr recordedFrame) []reply {
		switch fr.Action {
		case actionRunTask:
			return []reply{f.ackStarted(fr.TaskID)}
		case actionFinishTask:
			return []reply{taskFinishedFrame(fr.TaskID)}
		}
		return nil
	}
	s := openASR(t, f, wsURL(srv), nil)
	waitFor(t, func() bool { return countAction(f, actionRunTask) == 1 }, "the first run-task")

	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return within 3s")
	}
	waitFor(t, func() bool { return countAction(f, actionFinishTask) == 1 }, "a finish-task on close")
	for ev := range s.Events() {
		if ev.Kind == provider.ASREndOfTurn {
			t.Error("Close emitted a spurious end_of_turn")
		}
	}
}

func TestASRAudioQueueFull(t *testing.T) {
	f, srv := newFakeWS(t)
	defer srv.Close()
	f.onFrame = func(recordedFrame) []reply { return nil } // never acknowledge
	s := openASR(t, f, wsURL(srv), func(o *ASROptions) { o.AudioQueueFrames = 4 })
	frame := make([]byte, audio.MsToBytes(20))
	var err error
	for i := 0; i < 64 && err == nil; i++ {
		err = s.PushAudio(frame)
	}
	if err == nil {
		t.Fatal("PushAudio never reported a full queue")
	}
	var perr *provider.Error
	if !errors.As(err, &perr) || perr.Kind != provider.ErrTransient {
		t.Errorf("queue-full error = %v, want a transient provider error", err)
	}
}

func TestASROptionsDefaults(t *testing.T) {
	var o ASROptions
	o.applyDefaults()
	if o.URL != defaultWSURL {
		t.Errorf("url = %q, want %q", o.URL, defaultWSURL)
	}
	if o.Model != defaultASRModel {
		t.Errorf("model = %q, want %q", o.Model, defaultASRModel)
	}
	if o.FinalizeTimeout == 0 || o.AudioQueueFrames == 0 || o.ConnectTimeout == 0 || o.IdleTimeout == 0 {
		t.Errorf("defaults left a zero value: %+v", o)
	}
}

func TestASRRegisteredUnderQwen(t *testing.T) {
	if !provider.Known("asr", Name) {
		t.Fatal("asr provider not registered as qwen")
	}
	p, err := provider.New(provider.KindASR, Name, "k", json.RawMessage(`{"model":"m","language":"zh"}`))
	if err != nil {
		t.Fatal(err)
	}
	if p.Kind() != provider.KindASR {
		t.Errorf("kind = %s", p.Kind())
	}
}

// ---- helpers ---------------------------------------------------------------

func countAction(f *fakeWS, action string) int {
	n := 0
	for _, a := range f.actions() {
		if a == action {
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
