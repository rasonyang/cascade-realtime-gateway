package session

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// TestResponseDoneCallOnlyTurn: a turn that produced no text has nothing to
// synthesize, so the TTS flags must not gate completion.
func TestResponseDoneCallOnlyTurn(t *testing.T) {
	for _, tc := range []struct {
		name string
		r    response
		want bool
	}{
		{"llm still running", response{}, false},
		{"spoken audio turn awaiting tts", response{llmDone: true, spoke: true}, false},
		{"spoken audio turn flushed", response{llmDone: true, spoke: true, ttsDone: true, audioFlushed: true}, true},
		{"call-only turn", response{llmDone: true}, true},
		{"text-only turn", response{llmDone: true, textOnly: true, spoke: true}, true},
	} {
		if got := tc.r.done(); got != tc.want {
			t.Errorf("%s: done() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestToolChoiceAlwaysExplicit: Cascade's session model owns the default, so
// every generation carries an explicit tool_choice — no provider default is
// ever inherited.
func TestToolChoiceAlwaysExplicit(t *testing.T) {
	h := newHarness(t, defaultScripts(), manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "hello"}})
	h.post(CmdCreateResponse{})
	h.waitResponseDone(0)

	reqs := h.llm.Requests()
	if len(reqs) != 1 {
		t.Fatalf("requests = %d", len(reqs))
	}
	if reqs[0].ToolChoice != (provider.ToolChoice{Mode: provider.ToolChoiceAuto}) {
		t.Errorf("tool_choice = %+v, want an explicit auto", reqs[0].ToolChoice)
	}
	if reqs[0].Tools != nil {
		t.Errorf("tools = %+v, want none for a session that declared none", reqs[0].Tools)
	}
}

// TestToolsReachTheProvider: session.update tools are projected faithfully
// onto the provider types, schema included.
func TestToolsReachTheProvider(t *testing.T) {
	h := newHarness(t, defaultScripts(), manual)
	schema := json.RawMessage(`{"type":"object","properties":{"department":{"type":"string"}}}`)
	h.post(CmdUpdateSession{Patch: SessionPatch{
		Tools: []config.Tool{{Type: config.ToolTypeFunction, Name: "transfer_to_agent",
			Description: "hand off to a human", Parameters: schema}},
		ToolChoice: &config.ToolChoice{Mode: config.ToolChoiceRequired},
	}})
	h.waitFor("session updated", func(e Event) bool { _, ok := e.(EvSessionUpdated); return ok })
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "sales please"}})
	h.post(CmdCreateResponse{})
	h.waitResponseDone(0)

	req := h.llm.Requests()[0]
	if len(req.Tools) != 1 {
		t.Fatalf("tools = %+v", req.Tools)
	}
	got := req.Tools[0]
	if got.Name != "transfer_to_agent" || got.Description != "hand off to a human" {
		t.Errorf("tool = %+v", got)
	}
	if string(got.Parameters) != string(schema) {
		t.Errorf("parameters = %s, want the schema verbatim", got.Parameters)
	}
	if req.ToolChoice != (provider.ToolChoice{Mode: provider.ToolChoiceRequired}) {
		t.Errorf("tool_choice = %+v", req.ToolChoice)
	}
}

// TestToolsApplyFromTheNextResponse: a session.update landing mid-response
// must not change the generation already under way.
func TestToolsApplyFromTheNextResponse(t *testing.T) {
	sc := defaultScripts()
	sc.llm.Block, sc.llm.BlockAfter = true, 1
	h := newHarness(t, sc, manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "hello"}})
	h.post(CmdCreateResponse{})
	h.waitFor("first delta", func(e Event) bool { _, ok := e.(EvOutputAudioTranscriptDelta); return ok })

	h.post(CmdUpdateSession{Patch: SessionPatch{
		Tools: []config.Tool{{Type: config.ToolTypeFunction, Name: "hangup"}},
	}})
	h.waitFor("session updated", func(e Event) bool { _, ok := e.(EvSessionUpdated); return ok })
	h.post(CmdCancelResponse{})
	h.waitResponseDone(0)

	if got := h.llm.Requests()[0].Tools; got != nil {
		t.Errorf("the in-flight generation saw tools = %+v; they must apply from the next response", got)
	}
}

// TestInvalidToolsRejected: the field paths clients are held to.
func TestInvalidToolsRejected(t *testing.T) {
	for _, tc := range []struct {
		name  string
		patch SessionPatch
		param string
	}{
		{"non-function type", SessionPatch{Tools: []config.Tool{{Type: "mcp", Name: "x"}}}, "session.tools[0].type"},
		{"missing name", SessionPatch{Tools: []config.Tool{{Type: config.ToolTypeFunction}}}, "session.tools[0].name"},
		{"duplicate name", SessionPatch{Tools: []config.Tool{
			{Type: config.ToolTypeFunction, Name: "a"}, {Type: config.ToolTypeFunction, Name: "a"}}},
			"session.tools[1].name"},
		{"non-object parameters", SessionPatch{Tools: []config.Tool{
			{Type: config.ToolTypeFunction, Name: "a", Parameters: json.RawMessage(`[1,2]`)}}},
			"session.tools[0].parameters"},
		{"unknown mode", SessionPatch{ToolChoice: &config.ToolChoice{Mode: "sometimes"}}, "session.tool_choice"},
		{"forced function not declared", SessionPatch{
			ToolChoice: &config.ToolChoice{Mode: config.ToolChoiceFunction, Name: "nope"}},
			"session.tool_choice.name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, defaultScripts(), manual)
			h.post(CmdUpdateSession{Meta: Meta{Tag: "evt_1"}, Patch: tc.patch})
			ev, _ := h.waitFor("error", func(e Event) bool { _, ok := e.(EvError); return ok })
			err := ev.(EvError)
			if err.Code != ErrCodeInvalidSession || err.Param != tc.param {
				t.Fatalf("error = %+v, want param %q", err, tc.param)
			}
			if err.Tag != "evt_1" {
				t.Errorf("error does not echo the command tag: %+v", err)
			}
		})
	}
}

// TestToolOutputRejections: an output must answer a real function_call. The
// duplicate-output rule and the full round trip need a delivered call, so
// they are exercised where the pipeline produces one.
func TestToolOutputRejections(t *testing.T) {
	t.Run("unknown call", func(t *testing.T) {
		h := newHarness(t, defaultScripts(), manual)
		h.post(CmdCreateItem{Meta: Meta{Tag: "e1"}, Item: ItemSpec{Content: ContentToolOutput, CallItem: 999}})
		ev, _ := h.waitFor("error", func(e Event) bool { _, ok := e.(EvError); return ok })
		err := ev.(EvError)
		if err.Param != "item.call_id" || err.Code != ErrCodeInvalidItem || err.Tag != "e1" {
			t.Fatalf("error = %+v", err)
		}
	})
	t.Run("not a call item", func(t *testing.T) {
		h := newHarness(t, defaultScripts(), manual)
		h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "hello"}})
		ev, _ := h.waitFor("item added", func(e Event) bool { _, ok := e.(EvItemAdded); return ok })
		h.post(CmdCreateItem{Item: ItemSpec{Content: ContentToolOutput, CallItem: ev.(EvItemAdded).Item.Ref}})
		e2, _ := h.waitFor("error", func(e Event) bool { _, ok := e.(EvError); return ok })
		if err := e2.(EvError); err.Param != "item.call_id" {
			t.Fatalf("error = %+v", err)
		}
	})
}

// ---- delivery through the pipeline -----------------------------------------

func toolScript(tokens []string, call *provider.ToolCall) scripts {
	sc := defaultScripts()
	sc.llm.Tokens = tokens
	sc.llm.ToolCall = call
	return sc
}

var transferCall = provider.ToolCall{ID: "call_p1", Name: "transfer_to_agent", Arguments: `{"department":"sales"}`}

// waitToolCall blocks until the function_call item has been delivered.
func waitToolCall(h *harness) Item {
	h.t.Helper()
	ev, _ := h.waitFor("tool call delivered", func(e Event) bool {
		d, ok := e.(EvOutputItemDone)
		return ok && d.Item.Content == ContentToolCall
	})
	return ev.(EvOutputItemDone).Item
}

// TestCallOnlyTurn: no message item at all, and the response completes as
// soon as the LLM does — there is nothing to synthesize.
func TestCallOnlyTurn(t *testing.T) {
	h := newHarness(t, toolScript(nil, &transferCall), manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "sales please"}})
	h.post(CmdCreateResponse{})
	done := h.waitResponseDone(0)

	if done.Status != ResponseCompleted {
		t.Fatalf("status = %s (%v)", done.Status, done.Err)
	}
	if len(done.Output) != 1 || done.Output[0].Content != ContentToolCall {
		t.Fatalf("output = %+v, want the function_call item alone", done.Output)
	}
	call := done.Output[0]
	if call.Name != "transfer_to_agent" || call.CallID != "call_p1" || call.Text != `{"department":"sales"}` {
		t.Errorf("call item = %+v", call)
	}
	if call.Status != ItemCompleted {
		t.Errorf("call item status = %s, want completed", call.Status)
	}
	for _, e := range h.all() {
		if a, ok := e.(EvOutputItemAdded); ok && a.Item.Content != ContentToolCall {
			t.Fatalf("a call-only turn created a message item: %+v", a.Item)
		}
	}
}

// TestToolCallOrdering: the item is delivered the moment the call arrives,
// with its whole lifecycle, and always before the response ends.
func TestToolCallOrdering(t *testing.T) {
	h := newHarness(t, toolScript(nil, &transferCall), manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "sales please"}})
	h.post(CmdCreateResponse{})
	h.waitResponseDone(0)

	var order []string
	for _, e := range h.all() {
		switch ev := e.(type) {
		case EvOutputItemAdded:
			order = append(order, "added")
		case EvToolCallArguments:
			if ev.Arguments != `{"department":"sales"}` {
				t.Errorf("arguments = %q", ev.Arguments)
			}
			order = append(order, "arguments")
		case EvOutputItemDone:
			order = append(order, "item_done")
		case EvResponseDone:
			order = append(order, "response_done")
		}
	}
	want := []string{"added", "arguments", "item_done", "response_done"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

// TestTextAndCallTurn: a spoken preamble and a call in one response; the
// message item comes first and the call after it.
func TestTextAndCallTurn(t *testing.T) {
	h := newHarness(t, toolScript([]string{"Let me check.", " One moment."}, &transferCall), manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "sales please"}})
	h.post(CmdCreateResponse{})
	done := h.waitResponseDone(0)

	if done.Status != ResponseCompleted {
		t.Fatalf("status = %s (%v)", done.Status, done.Err)
	}
	if len(done.Output) != 2 {
		t.Fatalf("output = %+v, want the message then the call", done.Output)
	}
	if done.Output[0].Content != ContentAudio || done.Output[0].Text != "Let me check. One moment." {
		t.Errorf("output[0] = %+v", done.Output[0])
	}
	if done.Output[1].Content != ContentToolCall {
		t.Errorf("output[1] = %+v", done.Output[1])
	}
}

// TestTTSNeverReceivesToolArguments: the assertion the whole routing exists
// for — arguments must never reach the synthesizer.
func TestTTSNeverReceivesToolArguments(t *testing.T) {
	h := newHarness(t, toolScript([]string{"Let me check that."}, &transferCall), manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "sales please"}})
	h.post(CmdCreateResponse{})
	h.waitResponseDone(0)

	var spoken string
	for _, st := range h.tts.Streams() {
		spoken += st.Text()
	}
	if spoken == "" {
		t.Fatal("nothing was synthesized; the assertion would be vacuous")
	}
	for _, fragment := range []string{"department", "sales", "{", "}", "transfer_to_agent", "call_p1"} {
		if strings.Contains(spoken, fragment) {
			t.Fatalf("TTS received tool-call text %q in %q", fragment, spoken)
		}
	}
	if spoken != "Let me check that." {
		t.Errorf("synthesized %q, want the spoken text only", spoken)
	}
}

// TestCancelBeforeCompleteCall: a call the adapter never completed is never
// delivered, so the client is not asked to answer a call it did not see.
func TestCancelBeforeCompleteCall(t *testing.T) {
	sc := toolScript(nil, &transferCall)
	sc.llm.Block, sc.llm.BlockAfter = true, 0
	h := newHarness(t, sc, manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "sales please"}})
	h.post(CmdCreateResponse{})
	h.waitFor("response created", func(e Event) bool { _, ok := e.(EvResponseCreated); return ok })
	h.post(CmdCancelResponse{})
	done := h.waitResponseDone(0)

	if done.Status != ResponseCancelled {
		t.Fatalf("status = %s", done.Status)
	}
	if len(done.Output) != 0 {
		t.Fatalf("output = %+v, want nothing", done.Output)
	}
	for _, e := range h.all() {
		if _, ok := e.(EvToolCallArguments); ok {
			t.Fatal("arguments were emitted for a call that never completed")
		}
	}
}

// TestCancelAfterDeliveredCall: once delivered, the item stays — cancelling
// does not retract a call the client has already been asked to answer.
func TestCancelAfterDeliveredCall(t *testing.T) {
	sc := toolScript(nil, &transferCall)
	sc.llm.BlockAfterToolCall = true
	h := newHarness(t, sc, manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "sales please"}})
	h.post(CmdCreateResponse{})
	call := waitToolCall(h)
	h.post(CmdCancelResponse{})
	done := h.waitResponseDone(0)

	if done.Status != ResponseCancelled {
		t.Fatalf("status = %s", done.Status)
	}
	if len(done.Output) != 1 || done.Output[0].Ref != call.Ref {
		t.Fatalf("output = %+v, want the delivered call", done.Output)
	}
	if done.Output[0].Status != ItemCompleted {
		t.Errorf("a delivered call was reopened by the cancel: %+v", done.Output[0])
	}
	// The item lifecycle closed at delivery and is not replayed.
	n := 0
	for _, e := range h.all() {
		if d, ok := e.(EvOutputItemDone); ok && d.Item.Content == ContentToolCall {
			n++
		}
	}
	if n != 1 {
		t.Errorf("output_item.done for the call emitted %d times, want once", n)
	}
}

// TestToolRoundTrip: call → client-supplied result → a second response that
// sees both. This is the path the phase exists for.
func TestToolRoundTrip(t *testing.T) {
	h := newHarness(t, toolScript(nil, &transferCall), manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "sales please"}})
	h.post(CmdCreateResponse{})
	call := waitToolCall(h)
	h.waitResponseDone(0)

	h.post(CmdCreateItem{Item: ItemSpec{Content: ContentToolOutput, CallItem: call.Ref, Text: "no agent available"}})
	h.waitFor("tool output", func(e Event) bool {
		d, ok := e.(EvItemDone)
		return ok && d.Item.Content == ContentToolOutput
	})
	h.post(CmdCreateResponse{})
	h.waitFor("second response done", func(e Event) bool {
		d, ok := e.(EvResponseDone)
		return ok && d.Resp == 2
	})

	reqs := h.llm.Requests()
	if len(reqs) != 2 {
		t.Fatalf("requests = %d, want two generations", len(reqs))
	}
	msgs := reqs[1].Messages
	if len(msgs) != 3 {
		t.Fatalf("second generation saw %+v", msgs)
	}
	if len(msgs[1].ToolCalls) != 1 || msgs[1].ToolCalls[0] != transferCall {
		t.Errorf("messages[1] = %+v, want the call the model made", msgs[1])
	}
	if msgs[2].Role != provider.RoleTool || msgs[2].ToolCallID != "call_p1" || msgs[2].Content != "no agent available" {
		t.Errorf("messages[2] = %+v, want the tool result", msgs[2])
	}
}

// TestDuplicateToolOutputRejected: two results for one call would put
// contradictory tool output into the context with no rule for which wins.
func TestDuplicateToolOutputRejected(t *testing.T) {
	h := newHarness(t, toolScript(nil, &transferCall), manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "sales please"}})
	h.post(CmdCreateResponse{})
	call := waitToolCall(h)
	h.waitResponseDone(0)

	h.post(CmdCreateItem{Item: ItemSpec{Content: ContentToolOutput, CallItem: call.Ref, Text: "first"}})
	h.waitFor("tool output", func(e Event) bool {
		d, ok := e.(EvItemDone)
		return ok && d.Item.Content == ContentToolOutput
	})
	h.post(CmdCreateItem{Meta: Meta{Tag: "e2"}, Item: ItemSpec{Content: ContentToolOutput, CallItem: call.Ref, Text: "second"}})
	ev, _ := h.waitFor("error", func(e Event) bool { _, ok := e.(EvError); return ok })
	err := ev.(EvError)
	if err.Code != ErrCodeInvalidItem || err.Param != "item.call_id" || err.Tag != "e2" {
		t.Fatalf("error = %+v", err)
	}
	if !strings.Contains(err.Message, "already exists") {
		t.Errorf("message = %q", err.Message)
	}
}

// TestToolCallInTextMode: a call in text mode is handled identically; only
// the message item's kind differs.
func TestToolCallInTextMode(t *testing.T) {
	h := newHarness(t, toolScript([]string{"Checking."}, &transferCall), func(o *Options) {
		manual(o)
		o.Session.OutputModalities = []string{config.ModalityText}
	})
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "sales please"}})
	h.post(CmdCreateResponse{})
	done := h.waitResponseDone(0)

	if done.Status != ResponseCompleted {
		t.Fatalf("status = %s (%v)", done.Status, done.Err)
	}
	if len(done.Output) != 2 || done.Output[0].Content != ContentText || done.Output[1].Content != ContentToolCall {
		t.Fatalf("output = %+v", done.Output)
	}
}

// TestMaxOutputTokensMidArguments: a truncated generation delivers no call —
// the adapter never completes one — and the response is incomplete.
func TestMaxOutputTokensMidArguments(t *testing.T) {
	sc := toolScript([]string{"Let me"}, nil)
	sc.llm.FinishReason = provider.FinishLength
	h := newHarness(t, sc, manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "sales please"}})
	h.post(CmdCreateResponse{})
	done := h.waitResponseDone(0)

	if done.Status != ResponseIncomplete || done.Reason != ReasonMaxOutputTokens {
		t.Fatalf("status = %s / %s", done.Status, done.Reason)
	}
	for _, it := range done.Output {
		if it.Content == ContentToolCall {
			t.Fatal("a truncated generation delivered a call")
		}
	}
}
