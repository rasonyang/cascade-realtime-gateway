package protocol

import (
	"strings"
	"testing"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/session"
)

// checkStreamInvariants asserts the causal rules of §8.3 for one response:
// created before any delta, every delta stream closed by its done, deltas
// only for content_index 0 / output_index 0, and no delta after response.done.
func checkStreamInvariants(t *testing.T, frames []frame, respID string) {
	t.Helper()
	created, done := -1, -1
	deltas := map[string]int{}
	dones := map[string]bool{}
	for i, f := range frames {
		if f.str("response_id") != respID && f.str("response.id") != respID {
			continue
		}
		switch typ := f.typ(); {
		case typ == "response.created":
			created = i
		case typ == "response.done":
			done = i
		case len(typ) > 6 && typ[len(typ)-6:] == ".delta":
			if created < 0 {
				t.Fatalf("%s before response.created", typ)
			}
			if done >= 0 {
				t.Fatalf("%s after response.done", typ)
			}
			if f.get("content_index") != float64(0) || f.get("output_index") != float64(0) {
				t.Fatalf("unexpected indices in %v", f)
			}
			deltas[typ]++
		case len(typ) > 5 && typ[len(typ)-5:] == ".done":
			dones[typ] = true
		}
	}
	if created < 0 || done < 0 {
		t.Fatalf("response %s lacks created/done", respID)
	}
	for d := range deltas {
		if !dones[d[:len(d)-6]+".done"] {
			t.Fatalf("stream %s has no matching done", d)
		}
	}
}

func TestInvariantsAudioResponse(t *testing.T) {
	r := newRig(t, defaultScripts(), "", manual)
	r.send(`{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"Hi"}]}}`)
	r.send(`{"type":"response.create","event_id":"c1"}`)
	done := r.waitType("response.done")
	frames := r.all()
	assertMonotonicEventIDs(t, frames)
	respID := done.str("response.id")
	checkStreamInvariants(t, frames, respID)
	if r.count("response.output_audio.delta") == 0 || r.count("response.output_audio_transcript.delta") != 5 {
		t.Fatalf("delta mix: %v", r.types())
	}
	// Terminal order per profile §7 (searching after the first delta so the
	// user item's own conversation.item.done is skipped).
	order := []string{"response.output_audio.done", "response.output_audio_transcript.done", "response.content_part.done",
		"response.output_item.done", "conversation.item.done", "response.done"}
	last := r.index(func(f frame) bool { return strings.HasSuffix(f.typ(), ".delta") })
	for _, typ := range order {
		i := -1
		for j := last + 1; j < len(frames); j++ {
			if frames[j].typ() == typ {
				i = j
				break
			}
		}
		if i <= last {
			t.Fatalf("%s out of order: %v", typ, r.types())
		}
		last = i
	}
	if done.str("response.status") != "completed" || done.get("response.usage.total_tokens") != float64(12) {
		t.Fatalf("done = %v", done)
	}
	out := done.get("response.output").([]any)[0].(map[string]any)
	part := out["content"].([]any)[0].(map[string]any)
	if part["type"] != "output_audio" || part["transcript"] != "Hello world. Second sentence here." {
		t.Fatalf("output item = %v", out)
	}
}

func TestInvariantsInterruptCancelsAndStopsDeltas(t *testing.T) {
	sc := defaultScripts()
	sc.llm.Block, sc.llm.BlockAfter = true, 1
	r := newRig(t, sc, "", serverVAD(true, true))
	r.send(`{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"Hi"}]}}`)
	r.send(`{"type":"response.create"}`)
	r.waitType("response.output_audio_transcript.delta")
	created := r.waitType("response.created")
	r.append(tone(200))
	done := r.waitFor("cancelled", func(f frame) bool {
		return f.typ() == "response.done" && f.str("response.id") == created.str("response.id")
	})
	if done.str("response.status") != "cancelled" || done.str("response.status_details.reason") != "turn_detected" {
		t.Fatalf("done = %v", done)
	}
	if r.index(isType("input_audio_buffer.speech_started")) > r.index(isType("response.done")) {
		t.Fatal("speech_started must precede the cancelled response.done")
	}
	r.append(silence(600))
	r.waitType("input_audio_buffer.speech_stopped")
	time.Sleep(30 * time.Millisecond)
	checkStreamInvariants(t, r.all(), created.str("response.id"))
}

func TestInvariantsCreateResponseFalseStillCommits(t *testing.T) {
	r := newRig(t, defaultScripts(), "", serverVAD(false, true))
	r.append(tone(300))
	r.append(silence(600))
	committed := r.waitType("input_audio_buffer.committed")
	started := r.waitType("input_audio_buffer.speech_started")
	stopped := r.waitType("input_audio_buffer.speech_stopped")
	if started.str("item_id") != committed.str("item_id") || stopped.str("item_id") != committed.str("item_id") {
		t.Fatalf("item ids differ: %v %v %v", started, stopped, committed)
	}
	r.waitType("conversation.item.done")
	time.Sleep(30 * time.Millisecond)
	if r.count("response.created") != 0 {
		t.Fatalf("create_response=false must not create a response: %v", r.types())
	}
	item := r.waitType("conversation.item.done")
	part := item.get("item.content").([]any)[0].(map[string]any)
	if part["type"] != "input_audio" || part["transcript"] != "hello there" {
		t.Fatalf("committed item = %v", item)
	}
	if got := r.waitType("conversation.item.input_audio_transcription.completed").get("usage.seconds"); got == nil {
		t.Fatal("transcription usage missing")
	}
}

func TestInvariantsFatalErrorHasNoClientEventID(t *testing.T) {
	r := newRig(t, defaultScripts(), "", func(o *session.Options) {
		manual(o)
		o.Limits.InputAudioBufferMaxMs = 40
	})
	r.append(silence(60))
	f := r.waitType("error")
	if f.str("error.type") != "server_error" || f.str("error.code") != "input_audio_buffer_overflow" || f.get("error.event_id") != nil {
		t.Fatalf("fatal error frame = %v", f)
	}
}

// TestInvariantsAudioToolCall covers the audio-mode text+call turn, whose
// transcript and audio streams interleave non-deterministically and are
// therefore checked against causal invariants only, not a golden trace.
func TestInvariantsAudioToolCall(t *testing.T) {
	r := newRig(t, toolScripts("Let me check", " that for you."), "", manual)
	r.send(`{"type":"session.update","event_id":"s1","session":{"type":"realtime","tools":[{"type":"function","name":"transfer_to_agent"}]}}`)
	r.waitType("session.updated")
	r.send(`{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"sales please"}]}}`)
	r.waitType("conversation.item.done")
	r.send(`{"type":"response.create","event_id":"c1"}`)
	r.waitType("response.done")
	frames := r.all()
	assertMonotonicEventIDs(t, frames)

	if r.count("response.output_audio.delta") == 0 {
		t.Fatalf("no audio was produced; the assertions below would be vacuous: %v", r.types())
	}

	// The arguments events sit inside the function_call item's lifecycle,
	// which is itself at output_index 1, after the message item.
	callAdded, callDone := -1, -1
	argsDelta, argsDone, respDone := -1, -1, -1
	for i, f := range frames {
		item, _ := f.get("item").(map[string]any)
		isCall := item != nil && item["type"] == "function_call"
		switch {
		case f.typ() == "response.output_item.added" && isCall:
			callAdded = i
			if f.get("output_index") != float64(1) {
				t.Errorf("function_call output_index = %v, want 1 after the message item", f.get("output_index"))
			}
		case f.typ() == "response.output_item.done" && isCall:
			callDone = i
		case f.typ() == "response.function_call_arguments.delta":
			argsDelta = i
		case f.typ() == "response.function_call_arguments.done":
			argsDone = i
		case f.typ() == "response.done":
			respDone = i
		}
	}
	if callAdded < 0 || argsDelta < 0 || argsDone < 0 || callDone < 0 || respDone < 0 {
		t.Fatalf("missing tool frames: %v", r.types())
	}
	if !(callAdded < argsDelta && argsDelta < argsDone && argsDone < callDone) {
		t.Fatalf("arguments events outside the item lifecycle: added=%d delta=%d done=%d itemDone=%d",
			callAdded, argsDelta, argsDone, callDone)
	}
	if argsDone > respDone {
		t.Fatal("arguments.done after response.done")
	}
	// No arguments event may follow the response's terminal frame.
	for i, f := range frames {
		if i > respDone && strings.HasPrefix(f.typ(), "response.function_call_arguments.") {
			t.Fatalf("%s after response.done", f.typ())
		}
	}

	// A function_call item has no content parts, and the arguments never
	// reach the spoken transcript or the audio stream.
	var transcript string
	for _, f := range frames {
		if f.typ() == "response.output_audio_transcript.delta" || f.typ() == "response.output_audio_transcript.done" {
			transcript += f.str("delta") + f.str("transcript")
		}
		if strings.HasPrefix(f.typ(), "response.content_part.") && f.get("output_index") == float64(1) {
			t.Fatalf("a function_call item must have no content parts: %v", f)
		}
	}
	for _, fragment := range []string{"department", "sales", "{", "}", "transfer_to_agent"} {
		if strings.Contains(transcript, fragment) {
			t.Fatalf("tool-call text %q reached the spoken transcript %q", fragment, transcript)
		}
	}
	if transcript == "" {
		t.Fatal("no transcript; the assertion above would be vacuous")
	}
}
