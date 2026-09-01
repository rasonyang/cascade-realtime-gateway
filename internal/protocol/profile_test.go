package protocol

import (
	"strings"
	"testing"

	"github.com/rasonyang/cascade-realtime-gateway/internal/provider/mock"
)

// TestProfileRejectedFields sends one client event per Rejected row of the
// profile and asserts the error code and param.
func TestProfileRejectedFields(t *testing.T) {
	cases := []struct {
		name, js, code, param string
	}{
		{"beta modalities", `{"type":"session.update","session":{"type":"realtime","modalities":["audio"]}}`, "invalid_value", "session.modalities"},
		{"beta voice", `{"type":"session.update","session":{"type":"realtime","voice":"alloy"}}`, "invalid_value", "session.voice"},
		{"beta turn_detection", `{"type":"session.update","session":{"type":"realtime","turn_detection":null}}`, "invalid_value", "session.turn_detection"},
		{"missing session.type", `{"type":"session.update","session":{}}`, "invalid_value", "session.type"},
		{"transcription type", `{"type":"session.update","session":{"type":"transcription"}}`, "invalid_value", "session.type"},
		{"unknown field", `{"type":"session.update","session":{"type":"realtime","temperature2":1}}`, "invalid_value", "session.temperature2"},
		{"pcmu", `{"type":"session.update","session":{"type":"realtime","audio":{"input":{"format":{"type":"audio/pcmu"}}}}}`, "invalid_value", "session.audio.input.format.type"},
		{"rate", `{"type":"session.update","session":{"type":"realtime","audio":{"output":{"format":{"type":"audio/pcm","rate":16000}}}}}`, "invalid_value", "session.audio.output.format.rate"},
		{"transcription delay", `{"type":"session.update","session":{"type":"realtime","audio":{"input":{"transcription":{"delay":"low"}}}}}`, "invalid_value", "session.audio.input.transcription.delay"},
		{"noise reduction", `{"type":"session.update","session":{"type":"realtime","audio":{"input":{"noise_reduction":{"type":"near_field"}}}}}`, "invalid_value", "session.audio.input.noise_reduction"},
		{"idle timeout", `{"type":"session.update","session":{"type":"realtime","audio":{"input":{"turn_detection":{"type":"server_vad","idle_timeout_ms":5000}}}}}`, "invalid_value", "session.audio.input.turn_detection.idle_timeout_ms"},
		{"eagerness on server_vad", `{"type":"session.update","session":{"type":"realtime","audio":{"input":{"turn_detection":{"type":"server_vad","eagerness":"high"}}}}}`, "invalid_value", "session.audio.input.turn_detection.eagerness"},
		{"vad type", `{"type":"session.update","session":{"type":"realtime","audio":{"input":{"turn_detection":{"type":"client_vad"}}}}}`, "invalid_value", "session.audio.input.turn_detection.type"},
		{"threshold range", `{"type":"session.update","session":{"type":"realtime","audio":{"input":{"turn_detection":{"type":"server_vad","threshold":2}}}}}`, "invalid_value", "session.audio.input.turn_detection.threshold"},
		{"custom voice", `{"type":"session.update","session":{"type":"realtime","audio":{"output":{"voice":{"id":"v"}}}}}`, "invalid_value", "session.audio.output.voice"},
		{"speed", `{"type":"session.update","session":{"type":"realtime","audio":{"output":{"speed":0.1}}}}`, "invalid_value", "session.audio.output.speed"},
		{"modalities both", `{"type":"session.update","session":{"type":"realtime","output_modalities":["audio","text"]}}`, "invalid_value", "session.output_modalities"},
		{"max tokens", `{"type":"session.update","session":{"type":"realtime","max_output_tokens":9999}}`, "invalid_value", "session.max_output_tokens"},
		{"max tokens type", `{"type":"session.update","session":{"type":"realtime","max_output_tokens":"lots"}}`, "invalid_value", "session.max_output_tokens"},
		{"tools", `{"type":"session.update","session":{"type":"realtime","tools":[{"type":"function","name":"f"}]}}`, "invalid_value", "session.tools"},
		{"tool_choice", `{"type":"session.update","session":{"type":"realtime","tool_choice":"required"}}`, "invalid_value", "session.tool_choice"},
		{"prompt", `{"type":"session.update","session":{"type":"realtime","prompt":{"id":"p"}}}`, "invalid_value", "session.prompt"},
		{"tracing", `{"type":"session.update","session":{"type":"realtime","tracing":"auto"}}`, "invalid_value", "session.tracing"},
		{"truncation", `{"type":"session.update","session":{"type":"realtime","truncation":"disabled"}}`, "invalid_value", "session.truncation"},
		{"include", `{"type":"session.update","session":{"type":"realtime","include":["item.input_audio_transcription.logprobs"]}}`, "invalid_value", "session.include"},
		{"reasoning", `{"type":"session.update","session":{"type":"realtime","reasoning":{"effort":"low"}}}`, "invalid_value", "session.reasoning"},
		{"parallel tool calls", `{"type":"session.update","session":{"type":"realtime","parallel_tool_calls":true}}`, "invalid_value", "session.parallel_tool_calls"},
		{"instructions type", `{"type":"session.update","session":{"type":"realtime","instructions":5}}`, "invalid_value", "session.instructions"},
		{"missing session", `{"type":"session.update"}`, "invalid_value", "session"},
		{"append missing audio", `{"type":"input_audio_buffer.append"}`, "invalid_value", "audio"},
		{"append odd bytes", `{"type":"input_audio_buffer.append","audio":"AAAA"}`, "invalid_value", "audio"},
		{"commit extra field", `{"type":"input_audio_buffer.commit","foo":1}`, "invalid_value", "foo"},
		{"item missing", `{"type":"conversation.item.create"}`, "invalid_value", "item"},
		{"item type missing", `{"type":"conversation.item.create","item":{"role":"user","content":[{"type":"input_text","text":"x"}]}}`, "invalid_value", "item.type"},
		{"item role", `{"type":"conversation.item.create","item":{"type":"message","role":"robot","content":[{"type":"input_text","text":"x"}]}}`, "invalid_value", "item.role"},
		{"item content empty", `{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[]}}`, "invalid_value", "item.content"},
		{"item two parts", `{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"a"},{"type":"input_text","text":"b"}]}}`, "invalid_value", "item.content"},
		{"item image", `{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:"}]}}`, "invalid_value", "item.content[0].type"},
		{"assistant audio", `{"type":"conversation.item.create","item":{"type":"message","role":"assistant","content":[{"type":"output_audio","audio":"AA=="}]}}`, "invalid_value", "item.content[0].type"},
		{"system output_text", `{"type":"conversation.item.create","item":{"type":"message","role":"system","content":[{"type":"output_text","text":"x"}]}}`, "invalid_value", "item.content[0].type"},
		{"item text missing", `{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text"}]}}`, "invalid_value", "item.content[0].text"},
		{"item empty text", `{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":""}]}}`, "invalid_value", "item.content"},
		{"item reference", `{"type":"conversation.item.create","item":{"type":"item_reference","id":"x"}}`, "invalid_value", "item.type"},
		{"previous unknown", `{"type":"conversation.item.create","previous_item_id":"nope","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"x"}]}}`, "invalid_value", "previous_item_id"},
		{"delete missing id", `{"type":"conversation.item.delete"}`, "invalid_value", "item_id"},
		{"truncate missing ms", `{"type":"conversation.item.truncate","item_id":"x","content_index":0}`, "invalid_value", "audio_end_ms"},
		{"truncate float ms", `{"type":"conversation.item.truncate","item_id":"x","content_index":0,"audio_end_ms":1.5}`, "invalid_value", "audio_end_ms"},
		{"retrieve", `{"type":"conversation.item.retrieve","item_id":"x"}`, "invalid_event", "type"},
		{"response input", `{"type":"response.create","response":{"input":[]}}`, "invalid_value", "response.input"},
		{"response tool_choice", `{"type":"response.create","response":{"tool_choice":"auto"}}`, "invalid_value", "response.tool_choice"},
		{"response conversation none", `{"type":"response.create","response":{"conversation":"none"}}`, "invalid_value", "response.conversation"},
		{"response modalities", `{"type":"response.create","response":{"output_modalities":["video"]}}`, "invalid_value", "response.output_modalities"},
		{"response voice object", `{"type":"response.create","response":{"audio":{"output":{"voice":{"id":"v"}}}}}`, "invalid_value", "response.audio.output.voice"},
		{"response metadata size", `{"type":"response.create","response":{"metadata":{"a":1}}}`, "invalid_value", "response.metadata.a"},
		{"response unknown", `{"type":"response.create","response":{"temperature":0.8}}`, "invalid_value", "response.temperature"},
		{"cancel unknown response", `{"type":"response.cancel","response_id":"resp_nope"}`, "invalid_value", "response_id"},
		{"cancel nothing", `{"type":"response.cancel"}`, "invalid_event", ""},
		{"commit empty", `{"type":"input_audio_buffer.commit"}`, "invalid_event", ""},
		{"output buffer clear", `{"type":"output_audio_buffer.clear"}`, "invalid_event", "type"},
		{"transcription session", `{"type":"transcription_session.update","session":{}}`, "invalid_event", "type"},
		{"unknown type", `{"type":"nope"}`, "invalid_value", "type"},
		{"missing type", `{}`, "invalid_event", ""},
		{"event_id too long", `{"type":"input_audio_buffer.clear","event_id":"` + strings.Repeat("a", 513) + `"}`, "invalid_value", "event_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, defaultScripts(), "", manual)
			before := r.count("error")
			r.send(tc.js)
			r.waitCount("error", before+1)
			f := r.all()[len(r.all())-1]
			if f.typ() != "error" {
				f = r.waitType("error")
			}
			if f.str("error.type") != "invalid_request_error" || f.str("error.code") != tc.code {
				t.Fatalf("code = %s/%s, want invalid_request_error/%s (%v)", f.str("error.type"), f.str("error.code"), tc.code, f)
			}
			if got := f.str("error.param"); got != tc.param {
				t.Fatalf("param = %q, want %q (%v)", got, tc.param, f)
			}
			if f.str("error.message") == "" {
				t.Fatal("empty message")
			}
			// Non-fatal: the session still answers.
			r.send(`{"type":"input_audio_buffer.clear","event_id":"alive"}`)
			r.waitType("input_audio_buffer.cleared")
		})
	}
}

// TestProfileIgnoredFieldsEcho sends every Ignored value from §3 and checks
// the session.updated echo.
func TestProfileIgnoredFieldsEcho(t *testing.T) {
	r := newRig(t, defaultScripts(), "", nil)
	hello := r.waitType("session.created")
	if hello.str("session.model") != "cascade" || hello.str("session.id") != "sess_test" || hello.str("session.object") != "realtime.session" {
		t.Fatalf("session.created = %v", hello)
	}
	if hello.str("session.audio.input.turn_detection.type") != "server_vad" || hello.get("session.audio.input.turn_detection.threshold") != float64(0.5) {
		t.Fatalf("defaults not echoed: %v", hello)
	}
	if hello.get("session.audio.input.transcription") == nil {
		t.Fatal("transcription default ({}) must echo as an object")
	}
	r.send(`{"type":"session.update","session":{"type":"realtime","model":"m2","tools":[],"tool_choice":"none","truncation":"auto","tracing":null,"prompt":null,"include":[],` +
		`"audio":{"input":{"noise_reduction":null,"transcription":null,"turn_detection":{"type":"semantic_vad","eagerness":"high","create_response":false}},"output":{"voice":"coral","speed":1.2}},"max_output_tokens":200}}`)
	u := r.waitType("session.updated")
	checks := map[string]any{
		"session.model":                                         "m2",
		"session.tool_choice":                                   "auto",
		"session.truncation":                                    "auto",
		"session.tracing":                                       nil,
		"session.prompt":                                        nil,
		"session.audio.input.noise_reduction":                   nil,
		"session.audio.input.transcription":                     nil,
		"session.audio.input.turn_detection.type":               "semantic_vad",
		"session.audio.input.turn_detection.eagerness":          "high",
		"session.audio.input.turn_detection.create_response":    false,
		"session.audio.input.turn_detection.interrupt_response": true,
		"session.audio.output.voice":                            "coral",
		"session.audio.output.speed":                            1.2,
		"session.max_output_tokens":                             float64(200),
		"session.audio.input.format.type":                       "audio/pcm",
		"session.audio.output.format.rate":                      float64(24000),
		"session.type":                                          "realtime",
	}
	for path, want := range checks {
		if got := u.get(path); got != want {
			t.Errorf("%s = %v, want %v", path, got, want)
		}
	}
	if tools, ok := u.get("session.tools").([]any); !ok || len(tools) != 0 {
		t.Errorf("tools = %v", u.get("session.tools"))
	}
	if _, has := u.get("session.audio.input.turn_detection").(map[string]any)["threshold"]; has {
		t.Error("semantic_vad echo must not carry server_vad fields")
	}
	// Server VAD echo fills the defaults for omitted tuning fields.
	r.send(`{"type":"session.update","session":{"type":"realtime","audio":{"input":{"turn_detection":{"type":"server_vad","threshold":0.9}}}}}`)
	r.waitCount("session.updated", 2)
	u = r.all()[len(r.all())-1]
	td := u.get("session.audio.input.turn_detection").(map[string]any)
	if td["threshold"] != 0.9 || td["prefix_padding_ms"] != float64(300) || td["silence_duration_ms"] != float64(500) || td["create_response"] != true || td["idle_timeout_ms"] != nil {
		t.Fatalf("server_vad echo = %v", td)
	}
	if u.str("session.model") != "m2" {
		t.Fatal("model must persist across updates")
	}
}

func TestProfileAcceptedItemsAndOverrides(t *testing.T) {
	r := newRig(t, defaultScripts(), "", manual)
	r.send(`{"type":"conversation.item.create","item":{"id":"sys1","type":"message","role":"system","content":[{"type":"input_text","text":"Be kind."}],"status":"completed","object":"realtime.item"}}`)
	r.send(`{"type":"conversation.item.create","item":{"id":"a1","type":"message","role":"assistant","content":[{"type":"text","text":"Earlier answer."}]}}`)
	r.send(`{"type":"conversation.item.create","item":{"id":"u1","type":"message","role":"user","content":[{"type":"input_text","text":"Question?"}]},"previous_item_id":"sys1"}`)
	r.send(`{"type":"conversation.item.create","item":{"id":"first","type":"message","role":"user","content":[{"type":"input_text","text":"Very first."}]},"previous_item_id":"root"}`)
	r.waitCount("conversation.item.done", 4)
	added := []frame{}
	for _, f := range r.all() {
		if f.typ() == "conversation.item.added" {
			added = append(added, f)
		}
	}
	if added[2].str("previous_item_id") != "sys1" || added[3].get("previous_item_id") != nil || added[1].str("item.content.0.type") != "" {
		t.Fatalf("insert positions wrong: %v", added)
	}
	if got := added[1].get("item.content").([]any)[0].(map[string]any)["type"]; got != "output_text" {
		t.Fatalf("assistant text part echoed as %v", got)
	}
	r.send(`{"type":"conversation.item.create","event_id":"dup","item":{"id":"u1","type":"message","role":"user","content":[{"type":"input_text","text":"again"}]}}`)
	f := r.waitFor("dup", func(f frame) bool { return f.str("error.event_id") == "dup" })
	if f.str("error.param") != "item.id" {
		t.Fatalf("duplicate id error = %v", f)
	}
	// Context order: first, sys1, u1, a1 — assistant text is included.
	r.send(`{"type":"response.create","response":{"instructions":"Short.","output_modalities":["text"],"max_output_tokens":50,"metadata":{"trace":"t1"}}}`)
	done := r.waitType("response.done")
	if done.str("response.metadata.trace") != "t1" || done.get("response.max_output_tokens") != float64(50) || done.get("response.output_modalities").([]any)[0] != "text" {
		t.Fatalf("response echo = %v", done)
	}
	req := r.llm.Requests()[0]
	if req.Instructions != "Short." || req.MaxOutputTokens != 50 || len(req.Messages) != 4 || req.Messages[0].Content != "Very first." || req.Messages[3].Content != "Earlier answer." {
		t.Fatalf("llm request = %+v", req)
	}
	// Delete then the id is unknown.
	r.send(`{"type":"conversation.item.delete","item_id":"a1"}`)
	r.waitType("conversation.item.deleted")
	r.send(`{"type":"conversation.item.delete","event_id":"d2","item_id":"a1"}`)
	f = r.waitFor("d2", func(f frame) bool { return f.str("error.event_id") == "d2" })
	if f.str("error.param") != "item_id" {
		t.Fatalf("second delete = %v", f)
	}
}

func TestProfileTruncateAndVoiceLock(t *testing.T) {
	r := newRig(t, defaultScripts(), "", manual)
	r.send(`{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"Hi"}]}}`)
	r.send(`{"type":"response.create"}`)
	done := r.waitType("response.done")
	itemID := done.get("response.output").([]any)[0].(map[string]any)["id"].(string)
	r.sendf(`{"type":"conversation.item.truncate","event_id":"t1","item_id":%q,"content_index":0,"audio_end_ms":120}`, itemID)
	tr := r.waitType("conversation.item.truncated")
	if tr.str("item_id") != itemID || tr.get("audio_end_ms") != float64(120) || tr.get("content_index") != float64(0) {
		t.Fatalf("truncated = %v", tr)
	}
	r.sendf(`{"type":"conversation.item.truncate","event_id":"t2","item_id":%q,"content_index":0,"audio_end_ms":99999}`, itemID)
	f := r.waitFor("t2", func(f frame) bool { return f.str("error.event_id") == "t2" })
	if f.str("error.code") != "invalid_value" || f.str("error.param") != "audio_end_ms" {
		t.Fatalf("out of range = %v", f)
	}
	r.sendf(`{"type":"conversation.item.truncate","event_id":"t3","item_id":%q,"content_index":1,"audio_end_ms":10}`, itemID)
	f = r.waitFor("t3", func(f frame) bool { return f.str("error.event_id") == "t3" })
	if f.str("error.param") != "content_index" {
		t.Fatalf("content index = %v", f)
	}
	// Voice is locked after audio was produced.
	r.send(`{"type":"session.update","event_id":"v1","session":{"type":"realtime","audio":{"output":{"voice":"verse"}}}}`)
	f = r.waitFor("v1", func(f frame) bool { return f.str("error.event_id") == "v1" })
	if f.str("error.code") != "invalid_value" || f.str("error.param") != "session.audio.output.voice" {
		t.Fatalf("voice lock = %v", f)
	}
	r.send(`{"type":"response.create","event_id":"v2","response":{"audio":{"output":{"voice":"verse"}}}}`)
	f = r.waitFor("v2", func(f frame) bool { return f.str("error.event_id") == "v2" })
	if f.str("error.param") != "response.audio.output.voice" {
		t.Fatalf("response voice lock = %v", f)
	}
	// Failed response carries status_details.error.
	sc := defaultScripts()
	sc.llm.Err = mock.ErrScripted
	r2 := newRig(t, sc, "", manual)
	r2.send(`{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"Hi"}]}}`)
	r2.send(`{"type":"response.create","event_id":"rc"}`)
	d := r2.waitType("response.done")
	if d.str("response.status") != "failed" || d.str("response.status_details.error.code") != "provider_error" || d.str("response.status_details.error.type") != "server_error" {
		t.Fatalf("failed done = %v", d)
	}
	e := r2.waitType("error")
	if e.str("error.type") != "server_error" || e.str("error.code") != "provider_error" || e.str("error.event_id") != "rc" {
		t.Fatalf("provider error frame = %v", e)
	}
}
