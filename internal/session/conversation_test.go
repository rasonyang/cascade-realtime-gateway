package session

import (
	"testing"
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
