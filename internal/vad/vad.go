// Package vad provides the acoustic voice-activity detector used for
// speech-start (interrupt) and speech-end detection ahead of ASR.
//
// Layering: vad may import internal/audio only. It reports offsets relative
// to the audio fed since the last Reset; the session's InputAudioBuffer maps
// them onto the session timeline and applies prefix padding.
package vad

import (
	"math"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
)

// EventKind distinguishes the two detector outputs.
type EventKind int

const (
	SpeechStart EventKind = iota + 1
	SpeechEnd
)

func (k EventKind) String() string {
	switch k {
	case SpeechStart:
		return "speech_start"
	case SpeechEnd:
		return "speech_end"
	}
	return "unknown"
}

// Event is a detector output. OffsetMs is measured from the start of the
// audio fed since the last Reset: for SpeechStart it is the start of the
// first voiced frame; for SpeechEnd it is the end of the last voiced frame
// (the moment silence began), reported once SilenceDurationMs of silence has
// elapsed after it.
type Event struct {
	Kind     EventKind
	OffsetMs int
}

// Detector is the interface the session drives frame by frame.
type Detector interface {
	Process(pcm []byte) []Event
	Reset()
}

// Config tunes the energy detector. Threshold is in [0, 1] on a normalized
// RMS scale; SilenceDurationMs is how long the signal must stay below the
// exit threshold before SpeechEnd is reported.
type Config struct {
	Threshold         float64
	SilenceDurationMs int
}

// Algorithm constants. They are not protocol- or user-facing and are
// therefore named constants rather than configuration (see docs/decisions.md).
const (
	// FrameMs is the analysis frame length.
	FrameMs = 20
	// hysteresisRatio scales the entry threshold to obtain the exit
	// threshold, so a signal hovering near the threshold does not flap.
	hysteresisRatio = 0.6
	// thresholdScale maps the [0, 1] protocol threshold onto normalized RMS
	// (full-scale int16 = 1.0). Speech RMS in typical telephony/PC audio sits
	// around 0.02–0.2, so threshold 0.5 → 0.05 RMS.
	thresholdScale = 0.1
)

// Energy is an RMS detector with hysteresis and a silence window.
type Energy struct {
	enter, exit float64
	silenceMs   int

	pending    []byte // partial frame carried between Process calls
	elapsedMs  int    // audio consumed since Reset, whole frames only
	speaking   bool
	lastVoiced int // end offset (ms) of the last voiced frame
}

// NewEnergy builds a detector from cfg.
func NewEnergy(cfg Config) *Energy {
	e := &Energy{
		enter:     cfg.Threshold * thresholdScale,
		silenceMs: cfg.SilenceDurationMs,
	}
	e.exit = e.enter * hysteresisRatio
	return e
}

// Reset clears all state; subsequent offsets restart at 0.
func (e *Energy) Reset() {
	e.pending = e.pending[:0]
	e.elapsedMs = 0
	e.speaking = false
	e.lastVoiced = 0
}

// Process consumes pcm and returns zero or more events in order.
func (e *Energy) Process(pcm []byte) []Event {
	frameBytes := audio.MsToBytes(FrameMs)
	e.pending = append(e.pending, pcm...)
	var events []Event
	for len(e.pending) >= frameBytes {
		frame := e.pending[:frameBytes]
		e.pending = e.pending[frameBytes:]
		frameStart := e.elapsedMs
		e.elapsedMs += FrameMs
		level := rms(frame)
		switch {
		case !e.speaking && level >= e.enter:
			e.speaking = true
			e.lastVoiced = e.elapsedMs
			events = append(events, Event{Kind: SpeechStart, OffsetMs: frameStart})
		case e.speaking && level >= e.exit:
			e.lastVoiced = e.elapsedMs
		case e.speaking && e.elapsedMs-e.lastVoiced >= e.silenceMs:
			e.speaking = false
			events = append(events, Event{Kind: SpeechEnd, OffsetMs: e.lastVoiced})
		}
	}
	// Keep pending in a fresh slice so we never retain the caller's buffer.
	if len(e.pending) > 0 && cap(e.pending) > 2*frameBytes {
		e.pending = append(make([]byte, 0, frameBytes), e.pending...)
	}
	return events
}

// rms returns the root-mean-square of a 16-bit PCM frame normalized to [0, 1].
func rms(frame []byte) float64 {
	n := len(frame) / audio.BytesPerSample
	if n == 0 {
		return 0
	}
	var sum float64
	for i := 0; i < n; i++ {
		s := float64(audio.Sample(frame, i)) / math.MaxInt16
		sum += s * s
	}
	return math.Sqrt(sum / float64(n))
}
