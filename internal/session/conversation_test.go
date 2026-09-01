package session

import (
	"testing"

	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

func refs(c *conversation) []ItemRef {
	out := make([]ItemRef, len(c.items))
	for i, it := range c.items {
		out[i] = it.Ref
	}
	return out
}

func TestConversationInsertDeleteOrder(t *testing.T) {
	c := newConversation()
	a := &item{Item: Item{Role: RoleUser, Content: ContentText, Text: "a"}}
	b := &item{Item: Item{Role: RoleAssistant, Content: ContentText, Text: "b"}}
	if prev := c.append(a); prev != 0 || a.Ref != 1 {
		t.Fatalf("append a: prev=%d ref=%d", prev, a.Ref)
	}
	if prev := c.append(b); prev != a.Ref {
		t.Fatalf("append b: prev=%d", prev)
	}
	sys := &item{Item: Item{Role: RoleSystem, Content: ContentText, Text: "s"}}
	if prev, ok := c.insert(sys, 0, true); !ok || prev != 0 {
		t.Fatalf("insert at root: prev=%d ok=%v", prev, ok)
	}
	mid := &item{Item: Item{Role: RoleUser, Content: ContentText, Text: "m"}}
	if prev, ok := c.insert(mid, a.Ref, false); !ok || prev != a.Ref {
		t.Fatalf("insert after a: prev=%d ok=%v", prev, ok)
	}
	if got := refs(c); len(got) != 4 || got[0] != sys.Ref || got[1] != a.Ref || got[2] != mid.Ref || got[3] != b.Ref {
		t.Fatalf("order = %v", got)
	}
	if _, ok := c.insert(&item{}, 999, false); ok {
		t.Fatal("insert after unknown ref must fail")
	}
	if !c.remove(mid.Ref) || c.remove(mid.Ref) {
		t.Fatal("remove semantics")
	}
	if _, ok := c.get(mid.Ref); ok {
		t.Fatal("removed item still indexed")
	}
	if c.last(RoleUser) != a || c.last(RoleAssistant) != b || c.last("nope") != nil {
		t.Fatal("last() wrong")
	}
	msgs := c.messages()
	if len(msgs) != 3 || msgs[0].Role != "system" || msgs[2].Content != "b" {
		t.Fatalf("messages = %+v", msgs)
	}
	c.append(&item{Item: Item{Role: RoleUser, Content: ContentAudio}})
	if len(c.messages()) != 3 {
		t.Fatal("audio item without transcript must be skipped")
	}
	if c.count() != 4 {
		t.Fatalf("count = %d", c.count())
	}
}

// TestMessagesToolReplay: an assistant turn that spoke and called is one
// message, not two; the tool result is a tool-role message keyed by the
// provider's call id.
func TestMessagesToolReplay(t *testing.T) {
	c := newConversation()
	c.append(&item{Item: Item{Role: RoleUser, Content: ContentAudio, Text: "put me through to sales", TranscriptDone: true}})
	c.append(&item{Item: Item{Role: RoleAssistant, Content: ContentAudio, Text: "One moment."}})
	c.append(&item{Item: Item{Content: ContentToolCall, Name: "transfer_to_agent",
		CallID: "call_p1", Text: `{"department":"sales"}`}})
	c.append(&item{Item: Item{Content: ContentToolOutput, CallID: "call_p1", Text: "no agent available"}})
	c.append(&item{Item: Item{Role: RoleAssistant, Content: ContentAudio, Text: "Nobody is free."}})

	msgs := c.messages()
	if len(msgs) != 4 {
		t.Fatalf("messages = %+v, want user, assistant+call, tool, assistant", msgs)
	}
	if msgs[0].Role != provider.RoleUser || msgs[0].ToolCalls != nil {
		t.Errorf("messages[0] = %+v", msgs[0])
	}
	spoke := msgs[1]
	if spoke.Role != provider.RoleAssistant || spoke.Content != "One moment." || len(spoke.ToolCalls) != 1 {
		t.Fatalf("messages[1] = %+v, want the spoken text and the call merged", spoke)
	}
	want := provider.ToolCall{ID: "call_p1", Name: "transfer_to_agent", Arguments: `{"department":"sales"}`}
	if spoke.ToolCalls[0] != want {
		t.Errorf("merged call = %+v, want %+v", spoke.ToolCalls[0], want)
	}
	result := msgs[2]
	if result.Role != provider.RoleTool || result.ToolCallID != "call_p1" || result.Content != "no agent available" {
		t.Errorf("messages[2] = %+v", result)
	}
	if msgs[3].Content != "Nobody is free." || msgs[3].ToolCalls != nil {
		t.Errorf("messages[3] = %+v", msgs[3])
	}
}

// TestMessagesCallOnlyTurn: a turn that only called produces its own
// assistant message; it must not merge into the previous turn's text.
func TestMessagesCallOnlyTurn(t *testing.T) {
	c := newConversation()
	c.append(&item{Item: Item{Role: RoleAssistant, Content: ContentAudio, Text: "Earlier answer."}})
	c.append(&item{Item: Item{Content: ContentToolCall, Name: "hangup", CallID: "call_a", Text: `{"a":1}`}})
	c.append(&item{Item: Item{Content: ContentToolOutput, CallID: "call_a", Text: "ok"}})
	c.append(&item{Item: Item{Role: RoleUser, Content: ContentAudio, Text: "still there?", TranscriptDone: true}})
	c.append(&item{Item: Item{Content: ContentToolCall, Name: "hangup", CallID: "call_b"}})

	// Five items, four messages: the first call merges into the assistant
	// message of its own turn.
	msgs := c.messages()
	if len(msgs) != 4 {
		t.Fatalf("messages = %+v", msgs)
	}
	if len(msgs[0].ToolCalls) != 1 || msgs[0].Content != "Earlier answer." {
		t.Errorf("messages[0] = %+v", msgs[0])
	}
	// The last call follows a user message: it gets its own assistant turn.
	last := msgs[3]
	if last.Role != provider.RoleAssistant || last.Content != "" || len(last.ToolCalls) != 1 {
		t.Fatalf("messages[4] = %+v, want a call-only assistant turn", last)
	}
	if last.ToolCalls[0].Arguments != "" {
		t.Errorf("no-argument call = %+v, want empty arguments preserved", last.ToolCalls[0])
	}
}

// TestMessagesKeepsEmptyToolItems: the empty-text skip must not swallow a
// no-argument call or an empty tool result.
func TestMessagesKeepsEmptyToolItems(t *testing.T) {
	c := newConversation()
	c.append(&item{Item: Item{Content: ContentToolCall, Name: "hangup", CallID: "call_a"}})
	c.append(&item{Item: Item{Content: ContentToolOutput, CallID: "call_a"}})
	msgs := c.messages()
	if len(msgs) != 2 {
		t.Fatalf("messages = %+v, want both tool items kept", msgs)
	}
	if msgs[1].Role != provider.RoleTool || msgs[1].ToolCallID != "call_a" {
		t.Errorf("messages[1] = %+v", msgs[1])
	}
}

// TestMessagesTwoCallsDoNotMerge: a second call never folds into an assistant
// message that already carries one.
func TestMessagesTwoCallsDoNotMerge(t *testing.T) {
	c := newConversation()
	c.append(&item{Item: Item{Role: RoleAssistant, Content: ContentAudio, Text: "hi"}})
	c.append(&item{Item: Item{Content: ContentToolCall, Name: "a", CallID: "call_a"}})
	c.append(&item{Item: Item{Content: ContentToolCall, Name: "b", CallID: "call_b"}})
	msgs := c.messages()
	if len(msgs) != 2 {
		t.Fatalf("messages = %+v", msgs)
	}
	if len(msgs[0].ToolCalls) != 1 || len(msgs[1].ToolCalls) != 1 {
		t.Errorf("calls = %+v / %+v, want one each", msgs[0].ToolCalls, msgs[1].ToolCalls)
	}
}

func TestConversationAnswered(t *testing.T) {
	c := newConversation()
	c.append(&item{Item: Item{Content: ContentToolCall, Name: "hangup", CallID: "call_a"}})
	if c.answered("call_a") {
		t.Fatal("an unanswered call reports as answered")
	}
	c.append(&item{Item: Item{Content: ContentToolOutput, CallID: "call_a"}})
	if !c.answered("call_a") || c.answered("call_b") {
		t.Fatal("answered() wrong")
	}
}
