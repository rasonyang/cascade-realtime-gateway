package session

import (
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// item is the actor-private conversation entry: the public Item plus the
// audio bookkeeping used by truncate.
type item struct {
	Item
	segments  []audioSegment
	alignment []provider.CharTiming
	audioB    int // generated audio bytes (assistant items)
}

// conversation is an ordered list of items with ref lookup. Only the actor
// touches it.
type conversation struct {
	items []*item
	index map[ItemRef]*item
	next  ItemRef
}

func newConversation() *conversation {
	return &conversation{index: map[ItemRef]*item{}}
}

func (c *conversation) newRef() ItemRef {
	c.next++
	return c.next
}

// insert places it according to the CmdCreateItem rules and returns the ref
// of the item it now follows (0 when first).
func (c *conversation) insert(it *item, previous ItemRef, atRoot bool) (ItemRef, bool) {
	if it.Ref == 0 {
		it.Ref = c.newRef()
	}
	pos := len(c.items)
	switch {
	case atRoot:
		pos = 0
	case previous != 0:
		p, ok := c.index[previous]
		if !ok {
			return 0, false
		}
		pos = c.position(p) + 1
	}
	c.items = append(c.items, nil)
	copy(c.items[pos+1:], c.items[pos:])
	c.items[pos] = it
	c.index[it.Ref] = it
	if pos == 0 {
		return 0, true
	}
	return c.items[pos-1].Ref, true
}

func (c *conversation) append(it *item) ItemRef {
	prev, _ := c.insert(it, 0, false)
	return prev
}

func (c *conversation) position(it *item) int {
	for i, x := range c.items {
		if x == it {
			return i
		}
	}
	return -1
}

func (c *conversation) get(ref ItemRef) (*item, bool) {
	it, ok := c.index[ref]
	return it, ok
}

func (c *conversation) remove(ref ItemRef) bool {
	it, ok := c.index[ref]
	if !ok {
		return false
	}
	pos := c.position(it)
	c.items = append(c.items[:pos], c.items[pos+1:]...)
	delete(c.index, ref)
	return true
}

// last returns the most recent item with the given role, or nil.
func (c *conversation) last(role Role) *item {
	for i := len(c.items) - 1; i >= 0; i-- {
		if c.items[i].Role == role {
			return c.items[i]
		}
	}
	return nil
}

func (c *conversation) count() int { return len(c.items) }

// messages projects the conversation into LLM chat messages.
//
// Two shapes need care. An assistant turn that spoke and then called a tool
// is stored as two items but must be replayed as one assistant message
// carrying both the text and the call, so the tool result that follows
// answers a call the model can see. And a function_call_output becomes a
// tool-role message keyed by the provider's call id.
//
// Items without text (an audio item whose transcript never arrived) are
// skipped, but that must never swallow a tool item: a no-argument call and an
// empty tool result are both legitimate and both carry no text.
func (c *conversation) messages() []provider.Message {
	out := make([]provider.Message, 0, len(c.items))
	for _, it := range c.items {
		switch it.Content {
		case ContentToolCall:
			call := provider.ToolCall{ID: it.CallID, Name: it.Name, Arguments: it.Text}
			// Merge into the assistant message immediately before, when there
			// is one: one turn, one message.
			if n := len(out); n > 0 && out[n-1].Role == provider.RoleAssistant && out[n-1].ToolCalls == nil {
				out[n-1].ToolCalls = []provider.ToolCall{call}
				continue
			}
			out = append(out, provider.Message{Role: provider.RoleAssistant, ToolCalls: []provider.ToolCall{call}})
		case ContentToolOutput:
			out = append(out, provider.Message{Role: provider.RoleTool, ToolCallID: it.CallID, Content: it.Text})
		default:
			if it.Text == "" {
				continue
			}
			out = append(out, provider.Message{Role: provider.Role(it.Role), Content: it.Text})
		}
	}
	return out
}

// answered reports whether a function_call_output for callID already exists.
func (c *conversation) answered(callID string) bool {
	for _, it := range c.items {
		if it.Content == ContentToolOutput && it.CallID == callID {
			return true
		}
	}
	return false
}

// snapshot returns the public view of an item.
func (it *item) snapshot() Item { return it.Item }
