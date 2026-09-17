package session

import (
	"errors"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
)

var errBufferOverflow = errors.New("input audio buffer overflow")

// inputAudioBuffer is the logical (domain-level) uncommitted audio buffer.
// It owns every millisecond value the protocol reports: the session timeline
// is the total audio ever appended, and never resets on clear or commit.
//
// VAD offsets are relative to baseMs because the detector is reset whenever
// the buffer is cleared or committed. In VAD modes the buffer may drop idle
// audio from its head (trimIdle), so pcm[0] sits at headMs >= baseMs; the
// timeline itself never shifts.
type inputAudioBuffer struct {
	pcm      []byte
	baseMs   int // timeline offset the VAD's relative offsets are measured from
	headMs   int // timeline offset of pcm[0]
	maxBytes int

	// Speech segment marked by VAD, absolute timeline ms; -1 when unset.
	segStartMs int
	segEndMs   int
}

func newInputAudioBuffer(maxMs int) *inputAudioBuffer {
	return &inputAudioBuffer{maxBytes: audio.MsToBytes(maxMs), segStartMs: -1, segEndMs: -1}
}

// append adds audio; exceeding the cap is an error and nothing is dropped.
func (b *inputAudioBuffer) append(pcm []byte) error {
	if len(b.pcm)+len(pcm) > b.maxBytes {
		return errBufferOverflow
	}
	b.pcm = append(b.pcm, pcm...)
	return nil
}

// totalMs is the current position on the session timeline.
func (b *inputAudioBuffer) totalMs() int { return b.headMs + audio.BytesToMs(len(b.pcm)) }

func (b *inputAudioBuffer) len() int { return len(b.pcm) }

// markSpeechStart records a VAD speech start at offsetMs (relative to the
// VAD origin), rolled back by prefixPaddingMs but never before the audio
// still held. It returns the resulting audio_start_ms.
func (b *inputAudioBuffer) markSpeechStart(offsetMs, prefixPaddingMs int) int {
	start := b.baseMs + offsetMs - prefixPaddingMs
	if start < b.headMs {
		start = b.headMs
	}
	b.segStartMs = start
	b.segEndMs = -1
	return start
}

// markSpeechEnd records a VAD speech end at offsetMs, advanced by silenceMs
// but never past the audio actually present. It returns audio_end_ms.
func (b *inputAudioBuffer) markSpeechEnd(offsetMs, silenceMs int) int {
	end := b.baseMs + offsetMs + silenceMs
	if total := b.totalMs(); end > total {
		end = total
	}
	b.segEndMs = end
	return end
}

// commit slices the committed audio out of the buffer and clears it. With a
// VAD segment marked, the slice is [segStart, segEnd); otherwise the whole
// buffer. ok is false when there is nothing to commit.
func (b *inputAudioBuffer) commit() (pcm []byte, startMs, endMs int, ok bool) {
	if len(b.pcm) == 0 {
		return nil, 0, 0, false
	}
	startMs, endMs = b.headMs, b.totalMs()
	if b.segStartMs >= 0 {
		startMs = b.segStartMs
	}
	if b.segEndMs >= 0 && b.segEndMs > startMs {
		endMs = b.segEndMs
	}
	from := audio.MsToBytes(startMs - b.headMs)
	to := audio.MsToBytes(endMs - b.headMs)
	if to > len(b.pcm) {
		to = len(b.pcm)
	}
	pcm = append([]byte(nil), b.pcm[from:to]...)
	b.clear()
	return pcm, startMs, endMs, true
}

// clear drops uncommitted audio; the timeline continues from the current
// position.
func (b *inputAudioBuffer) clear() {
	b.baseMs = b.totalMs()
	b.headMs = b.baseMs
	b.pcm = nil
	b.segStartMs = -1
	b.segEndMs = -1
}

// trimIdle drops audio older than keepMs from the head of the buffer while no
// speech segment is marked. Only VAD modes call it: there, idle audio before
// the prefix-padding window can never be part of a commit, so a caller that
// stays silent (e.g. on hold) must not run into the memory cap. The timeline
// and the VAD origin are unchanged. A marked segment is never trimmed, so an
// over-long speech still overflows.
func (b *inputAudioBuffer) trimIdle(keepMs int) {
	if b.segStartMs >= 0 {
		return
	}
	drop := audio.BytesToMs(len(b.pcm)) - keepMs
	if drop <= 0 {
		return
	}
	// Reslicing is O(1); the next append that outgrows the capacity copies
	// only the live tail, which releases the dropped head.
	b.pcm = b.pcm[audio.MsToBytes(drop):]
	b.headMs += drop
}
