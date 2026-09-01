package protocol

import (
	"crypto/rand"
	"fmt"
)

const idAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

const idLen = 16

// newID returns prefix + 16 random lowercase alphanumerics, e.g. item_k3….
func newID(prefix string) string {
	var buf [idLen]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic("protocol: crypto/rand unavailable: " + err.Error())
	}
	for i := range buf {
		buf[i] = idAlphabet[int(buf[i])%len(idAlphabet)]
	}
	return prefix + string(buf[:])
}

// eventID returns a per-session monotonic server event id.
func (a *Adapter) eventID() string {
	a.eventSeq++
	return fmt.Sprintf("event_%s%06d", a.eventPrefix, a.eventSeq)
}
