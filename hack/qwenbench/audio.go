package main

import (
	"math"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
)

// silenceRMS is the amplitude below which a 20 ms frame counts as silence,
// as a fraction of full scale. Synthesized speech has a clean noise floor,
// so a low threshold is safe.
const silenceRMS = 0.01

// trimSilence removes the leading and trailing silence a synthesizer puts
// around an utterance, leaving a small margin. Without it "end of user
// speech" would be the end of the trailing silence, which is neither what a
// caller experiences nor what the VAD reacts to.
func trimSilence(pcm []byte) []byte {
	frame := audio.MsToBytes(20)
	loud := func(b []byte) bool {
		if len(b) < audio.BytesPerSample {
			return false
		}
		var sum float64
		n := len(b) / audio.BytesPerSample
		for i := 0; i < n; i++ {
			v := float64(audio.Sample(b, i)) / math.MaxInt16
			sum += v * v
		}
		return math.Sqrt(sum/float64(n)) > silenceRMS
	}
	first, last := -1, -1
	for off := 0; off+frame <= len(pcm); off += frame {
		if loud(pcm[off : off+frame]) {
			if first < 0 {
				first = off
			}
			last = off + frame
		}
	}
	if first < 0 {
		return pcm // all silence: leave it alone rather than return nothing
	}
	// Keep 100 ms of lead-in so the first phoneme is not clipped, and 60 ms
	// of tail so the last one is not either.
	first = max(0, first-audio.MsToBytes(100))
	last = min(len(pcm), last+audio.MsToBytes(60))
	return pcm[first:last]
}
