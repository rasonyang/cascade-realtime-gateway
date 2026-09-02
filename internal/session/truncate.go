package session

import (
	"unicode/utf8"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// audioSegment maps a span of response text to the span of generated audio
// it produced. Text offsets are bytes into the full text; audio offsets are
// PCM bytes into the response's audio.
type audioSegment struct {
	textStart, textEnd   int
	audioStart, audioEnd int
}

// trimText returns the prefix of text the user is assumed to have heard when
// playback stopped at cutBytes of audio. Three tiers (docs/decisions.md):
//
//  1. alignment present: cut at the first character whose audio starts at or
//     after the cut;
//  2. segments present: keep whole segments that ended before the cut, and
//     trim the straddling segment proportionally by rune count;
//  3. no information: tier 2 with a single whole-response segment covering
//     the whole response, synthesized here.
//
// Tier 3 is synthesized rather than supplied by the caller because the case
// that needs it most is a response cancelled mid-stream — a barge-in, which
// is the only time truncate is interesting. An incremental TTS provider
// reports no per-sentence segments at all, and a cancelled response never
// reaches the end of its TTS stage, so relying on the pipeline to record a
// fallback segment left exactly that case with no segments and trimmed the
// whole turn away.
//
// Invariants: the result is a prefix of text, monotonic in cutBytes, and
// equals text when cutBytes >= totalAudioBytes.
func trimText(text string, segs []audioSegment, align []provider.CharTiming, cutBytes, totalAudioBytes int) string {
	if cutBytes >= totalAudioBytes || len(text) == 0 {
		return text
	}
	if cutBytes <= 0 {
		return ""
	}
	if len(align) > 0 {
		return trimByAlignment(text, align, cutBytes)
	}
	if len(segs) == 0 {
		segs = []audioSegment{{textStart: 0, textEnd: len(text), audioStart: 0, audioEnd: totalAudioBytes}}
	}
	return trimBySegments(text, segs, cutBytes)
}

func trimByAlignment(text string, align []provider.CharTiming, cutBytes int) string {
	cutMs := audio.BytesToMs(cutBytes)
	runes := []rune(text)
	keep := len(runes)
	for _, ct := range align {
		if ct.StartMs >= cutMs {
			if ct.CharIndex < keep {
				keep = ct.CharIndex
			}
		}
	}
	return string(runes[:keep])
}

func trimBySegments(text string, segs []audioSegment, cutBytes int) string {
	keepBytes := 0
	for _, seg := range segs {
		if seg.audioEnd <= cutBytes {
			keepBytes = seg.textEnd
			continue
		}
		if seg.audioStart >= cutBytes {
			break
		}
		// Straddling segment: proportional by runes.
		span := text[seg.textStart:seg.textEnd]
		n := utf8.RuneCountInString(span)
		frac := float64(cutBytes-seg.audioStart) / float64(seg.audioEnd-seg.audioStart)
		keepRunes := int(float64(n) * frac)
		runes := []rune(span)
		keepBytes = seg.textStart + len(string(runes[:keepRunes]))
		break
	}
	return text[:keepBytes]
}
