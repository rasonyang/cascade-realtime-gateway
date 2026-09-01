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

// messages projects the conversation into LLM chat messages. Items without
// text (an audio item whose transcript never arrived) are skipped.
func (c *conversation) messages() []provider.Message {
	out := make([]provider.Message, 0, len(c.items))
	for _, it := range c.items {
		if it.Text == "" {
			continue
		}
		out = append(out, provider.Message{Role: provider.Role(it.Role), Content: it.Text})
	}
	return out
}

// snapshot returns the public view of an item.
func (it *item) snapshot() Item { return it.Item }
