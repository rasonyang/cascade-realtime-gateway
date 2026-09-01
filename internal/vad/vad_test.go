package vad

import (
	"math"
	"testing"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
)

// tone returns ms of a full-scale-scaled sine wave; amp in [0, 1].
func tone(ms int, amp float64) []byte {
	n := ms * audio.SampleRate / 1000
	buf := make([]byte, n*audio.BytesPerSample)
	for i := 0; i < n; i++ {
		v := amp * math.MaxInt16 * math.Sin(2*math.Pi*440*float64(i)/audio.SampleRate)
		audio.PutSample(buf, i, int16(v))
	}
	return buf
}

func silence(ms int) []byte { return make([]byte, audio.MsToBytes(ms)) }

func cfg() Config { return Config{Threshold: 0.5, SilenceDurationMs: 200} }

func TestSilenceProducesNothing(t *testing.T) {
	d := NewEnergy(cfg())
	if ev := d.Process(silence(1000)); len(ev) != 0 {
		t.Fatalf("events on silence: %v", ev)
	}
}

func TestBurstStartAndEnd(t *testing.T) {
	d := NewEnergy(cfg())
	var events []Event
	events = append(events, d.Process(silence(100))...)
	events = append(events, d.Process(tone(300, 0.5))...)
	events = append(events, d.Process(silence(500))...)
	if len(events) != 2 {
		t.Fatalf("want start+end, got %v", events)
	}
	if events[0].Kind != SpeechStart || events[0].OffsetMs != 100 {
		t.Fatalf("start = %v", events[0])
	}
	if events[1].Kind != SpeechEnd || events[1].OffsetMs != 400 {
		t.Fatalf("end = %v (want offset 400 = end of last voiced frame)", events[1])
	}
}

func TestShortGapDoesNotEnd(t *testing.T) {
	d := NewEnergy(cfg())
	var events []Event
	events = append(events, d.Process(tone(200, 0.5))...)
	events = append(events, d.Process(silence(100))...) // shorter than silence window
	events = append(events, d.Process(tone(200, 0.5))...)
	events = append(events, d.Process(silence(300))...)
	if len(events) != 2 || events[1].OffsetMs != 500 {
		t.Fatalf("gap shorter than window must not split speech: %v", events)
	}
}

func TestHysteresisKeepsSpeechBetweenThresholds(t *testing.T) {
	d := NewEnergy(cfg())
	// enter = 0.05 RMS; a 0.5-amplitude sine has RMS ≈ 0.35, a 0.06-amplitude
	// sine has RMS ≈ 0.042: below enter (0.05) but above exit (0.03).
	var events []Event
	events = append(events, d.Process(tone(100, 0.5))...)
	events = append(events, d.Process(tone(400, 0.06))...)
	events = append(events, d.Process(silence(300))...)
	if len(events) != 2 || events[1].OffsetMs != 500 {
		t.Fatalf("level between exit and enter must keep speech alive: %v", events)
	}
	// The same low level alone must not start speech.
	d.Reset()
	if ev := d.Process(tone(400, 0.06)); len(ev) != 0 {
		t.Fatalf("sub-threshold level started speech: %v", ev)
	}
}

func TestPartialFramesAndReset(t *testing.T) {
	d := NewEnergy(cfg())
	sig := tone(100, 0.5)
	var events []Event
	for i := 0; i < len(sig); i += 7 { // odd chunk sizes
		end := i + 7
		if end > len(sig) {
			end = len(sig)
		}
		events = append(events, d.Process(sig[i:end])...)
	}
	if len(events) != 1 || events[0].Kind != SpeechStart || events[0].OffsetMs != 0 {
		t.Fatalf("chunked input: %v", events)
	}
	d.Reset()
	events = d.Process(silence(100))
	events = append(events, d.Process(tone(40, 0.5))...)
	if len(events) != 1 || events[0].OffsetMs != 100 {
		t.Fatalf("offsets must restart after Reset: %v", events)
	}
}
