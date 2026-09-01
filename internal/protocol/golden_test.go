package protocol

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider/mock"
	"github.com/rasonyang/cascade-realtime-gateway/internal/session"
)

var update = flag.Bool("update", false, "rewrite golden traces")

// checkGolden normalizes the recorded frames and compares them, in full
// order, with testdata/<name>.golden.jsonl.
func checkGolden(t *testing.T, name string, frames []frame) {
	t.Helper()
	assertMonotonicEventIDs(t, frames)
	n := newNormalizer()
	var lines []string
	for _, f := range frames {
		lines = append(lines, canonical(n.frame(f)))
	}
	got := strings.Join(lines, "\n") + "\n"
	path := filepath.Join("testdata", name+".golden.jsonl")
	if *update {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("missing golden %s (run with -update): %v", path, err)
	}
	if string(want) != got {
		wantLines := strings.Split(strings.TrimSpace(string(want)), "\n")
		for i := range lines {
			if i >= len(wantLines) {
				t.Fatalf("%s: extra frame %d:\n%s", name, i, lines[i])
			}
			if lines[i] != wantLines[i] {
				t.Fatalf("%s: frame %d differs\n got: %s\nwant: %s", name, i, lines[i], wantLines[i])
			}
		}
		t.Fatalf("%s: golden has %d frames, got %d", name, len(wantLines), len(lines))
	}
}

func TestGoldenTextTurn(t *testing.T) {
	r := newRig(t, defaultScripts(), "gpt-realtime", nil)
	r.send(`{"type":"session.update","event_id":"evt_1","session":{"type":"realtime","output_modalities":["text"],"instructions":"Be terse."}}`)
	r.waitType("session.updated")
	r.send(`{"type":"conversation.item.create","event_id":"evt_2","item":{"id":"msg_001","type":"message","role":"user","content":[{"type":"input_text","text":"Hello"}]}}`)
	r.waitType("conversation.item.done")
	r.send(`{"type":"response.create","event_id":"evt_3","response":{"metadata":{"k":"v"}}}`)
	r.waitType("response.done")
	checkGolden(t, "text_turn", r.all())
}

func TestGoldenCancelText(t *testing.T) {
	sc := defaultScripts()
	sc.llm.Block, sc.llm.BlockAfter = true, 1
	r := newRig(t, sc, "", nil)
	r.send(`{"type":"session.update","event_id":"evt_1","session":{"type":"realtime","output_modalities":["text"]}}`)
	r.waitType("session.updated")
	r.send(`{"type":"conversation.item.create","event_id":"evt_2","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"Hello"}]}}`)
	r.waitType("conversation.item.done")
	r.send(`{"type":"response.create","event_id":"evt_3"}`)
	r.waitType("response.output_text.delta")
	r.send(`{"type":"response.cancel","event_id":"evt_4"}`)
	r.waitType("response.done")
	checkGolden(t, "cancel_text", r.all())
}

func TestGoldenManualCommit(t *testing.T) {
	r := newRig(t, defaultScripts(), "", nil)
	r.send(`{"type":"session.update","event_id":"evt_1","session":{"type":"realtime","audio":{"input":{"transcription":{"language":"en"},"turn_detection":null}}}}`)
	r.waitType("session.updated")
	r.append(silence(100))
	r.send(`{"type":"input_audio_buffer.commit","event_id":"evt_2"}`)
	r.waitType("conversation.item.done")
	r.send(`{"type":"response.create","event_id":"evt_3","response":{"output_modalities":["text"]}}`)
	r.waitType("response.done")
	checkGolden(t, "manual_commit", r.all())
}

func TestGoldenErrors(t *testing.T) {
	r := newRig(t, defaultScripts(), "", manual)
	bad := []string{
		`not json`,
		`{"event_id":"e1"}`,
		`{"type":"scooby.dooby.doo","event_id":"e2"}`,
		`{"type":"session.update","event_id":"e3","session":{"type":"realtime","modalities":["text"]}}`,
		`{"type":"session.update","event_id":"e4","session":{"type":"realtime","audio":{"output":{"speed":3}}}}`,
		`{"type":"session.update","event_id":"e5","session":{"type":"realtime","tools":[{"type":"function"}]}}`,
		`{"type":"session.update","event_id":"e6","session":{"instructions":"x"}}`,
		`{"type":"input_audio_buffer.commit","event_id":"e7"}`,
		`{"type":"response.cancel","event_id":"e8"}`,
		`{"type":"conversation.item.delete","event_id":"e9","item_id":"nope"}`,
		`{"type":"conversation.item.truncate","event_id":"e10","item_id":"nope","content_index":0,"audio_end_ms":1}`,
		`{"type":"conversation.item.create","event_id":"e11","item":{"type":"message","role":"user","content":[{"type":"input_audio","audio":"AAAA"}]}}`,
		`{"type":"conversation.item.create","event_id":"e12","item":{"type":"function_call","name":"f","arguments":"{}"}}`,
		`{"type":"conversation.item.retrieve","event_id":"e13","item_id":"x"}`,
		`{"type":"output_audio_buffer.clear","event_id":"e14"}`,
		`{"type":"response.create","event_id":"e15","response":{"tools":[]}}`,
		`{"type":"input_audio_buffer.append","event_id":"e16","audio":"!!!"}`,
		`{"type":"session.update","event_id":"e17","session":{"type":"realtime","audio":{"input":{"turn_detection":{"type":"semantic_vad","threshold":0.3}}}}}`,
		`{"type":"session.update","event_id":"e18","session":{"type":"realtime","audio":{"output":{"voice":{"id":"voice_1"}}}}}`,
		`{"type":"response.create","event_id":"e19","response":{"conversation":"none"}}`,
		`{"type":"session.update","event_id":"e20","session":{"type":"transcription"}}`,
		`{"type":"session.update","event_id":"e21","session":{"type":"realtime","tool_choice":"sometimes"}}`,
		`{"type":"session.update","event_id":"e22","session":{"type":"realtime","tools":[{"type":"function","name":"f"},{"type":"function","name":"f"}]}}`,
		`{"type":"conversation.item.create","event_id":"e23","item":{"type":"function_call_output","call_id":"call_nope","output":"ok"}}`,
		`{"type":"conversation.item.create","event_id":"e24","item":{"type":"function_call_output","call_id":"call_1"}}`,
	}
	for i, js := range bad {
		r.send(js)
		r.waitCount("error", i+1)
	}
	checkGolden(t, "errors", r.all())
	// Every error back-references its client event_id (none for the first).
	n := 0
	for _, f := range r.all() {
		if f.typ() != "error" {
			continue
		}
		n++
		if n == 1 {
			if f.get("error.event_id") != nil {
				t.Fatalf("unparsable frame must not carry a client event_id: %v", f)
			}
			continue
		}
		if got := f.str("error.event_id"); got == "" {
			t.Fatalf("error %d lacks event_id back-reference: %v", n, f)
		}
	}
	_ = session.CloseClient
}

// ---- function calling -------------------------------------------------------

// toolScripts scripts a generation that emits tokens and then one complete
// tool call, which is the shape a provider adapter produces once it has
// accumulated the argument fragments.
func toolScripts(tokens ...string) scripts {
	sc := defaultScripts()
	sc.llm.Tokens = tokens
	sc.llm.ToolCall = &provider.ToolCall{
		ID: "provider_call_1", Name: "transfer_to_agent", Arguments: `{"department":"sales"}`,
	}
	return sc
}

// textSession puts the session in text mode, where the whole frame order is
// deterministic and can be compared in full.
const textSession = `{"type":"session.update","event_id":"evt_1","session":{"type":"realtime","output_modalities":["text"],` +
	`"tools":[{"type":"function","name":"transfer_to_agent","description":"hand off to a human",` +
	`"parameters":{"type":"object","properties":{"department":{"type":"string"}}}}]}}`

// TestGoldenToolCallOnly: a turn that only calls emits no message item at
// all — the function_call item sits at output_index 0.
func TestGoldenToolCallOnly(t *testing.T) {
	r := newRig(t, toolScripts(), "", nil)
	r.send(textSession)
	r.waitType("session.updated")
	r.send(`{"type":"conversation.item.create","event_id":"evt_2","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"sales please"}]}}`)
	r.waitType("conversation.item.done")
	r.send(`{"type":"response.create","event_id":"evt_3"}`)
	r.waitType("response.done")
	checkGolden(t, "tool_call_only", r.all())
}

// TestGoldenToolCallWithText: the message item at index 0, the function_call
// at index 1.
func TestGoldenToolCallWithText(t *testing.T) {
	r := newRig(t, toolScripts("Let me check", " that for you."), "", nil)
	r.send(textSession)
	r.waitType("session.updated")
	r.send(`{"type":"conversation.item.create","event_id":"evt_2","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"sales please"}]}}`)
	r.waitType("conversation.item.done")
	r.send(`{"type":"response.create","event_id":"evt_3"}`)
	r.waitType("response.done")
	checkGolden(t, "tool_call_with_text", r.all())
}

// TestGoldenToolRoundTrip: call → function_call_output → a second response
// that speaks. This is the whole path aicc depends on.
func TestGoldenToolRoundTrip(t *testing.T) {
	sc := toolScripts()
	r := newRig(t, sc, "", nil)
	r.send(textSession)
	r.waitType("session.updated")
	r.send(`{"type":"conversation.item.create","event_id":"evt_2","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"sales please"}]}}`)
	r.waitType("conversation.item.done")
	r.send(`{"type":"response.create","event_id":"evt_3"}`)
	done := r.waitType("response.function_call_arguments.done")
	r.waitType("response.done")

	// The client executes the tool and returns its result, then asks for the
	// next generation with a bare response.create.
	r.sendf(`{"type":"conversation.item.create","event_id":"evt_4","item":{"type":"function_call_output","call_id":%q,"output":"no agent available"}}`,
		done.str("call_id"))
	r.waitCount("conversation.item.done", 3)
	// The second generation speaks rather than calling again.
	sc.llm.ToolCall = nil
	r.llm.SetScript(mock.LLMScript{Tokens: []string{"Nobody", " is free."}})
	r.send(`{"type":"response.create","event_id":"evt_5"}`)
	r.waitCount("response.done", 2)
	checkGolden(t, "tool_round_trip", r.all())
}

// TestGoldenCancelBeforeToolCall: a call the adapter never completed is never
// delivered, so the trace holds no function_call item and no arguments events.
func TestGoldenCancelBeforeToolCall(t *testing.T) {
	sc := toolScripts()
	sc.llm.Block, sc.llm.BlockAfter = true, 0
	r := newRig(t, sc, "", nil)
	r.send(textSession)
	r.waitType("session.updated")
	r.send(`{"type":"conversation.item.create","event_id":"evt_2","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"sales please"}]}}`)
	r.waitType("conversation.item.done")
	r.send(`{"type":"response.create","event_id":"evt_3"}`)
	r.waitType("response.created")
	r.send(`{"type":"response.cancel","event_id":"evt_4"}`)
	r.waitType("response.done")
	checkGolden(t, "tool_cancel_before_call", r.all())
}

// TestGoldenCancelAfterToolCall: once delivered the item stays, and it is
// still listed in the cancelled response's output.
func TestGoldenCancelAfterToolCall(t *testing.T) {
	sc := toolScripts()
	sc.llm.BlockAfterToolCall = true
	r := newRig(t, sc, "", nil)
	r.send(textSession)
	r.waitType("session.updated")
	r.send(`{"type":"conversation.item.create","event_id":"evt_2","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"sales please"}]}}`)
	r.waitType("conversation.item.done")
	r.send(`{"type":"response.create","event_id":"evt_3"}`)
	r.waitType("response.function_call_arguments.done")
	r.send(`{"type":"response.cancel","event_id":"evt_4"}`)
	r.waitType("response.done")
	checkGolden(t, "tool_cancel_after_call", r.all())
}
