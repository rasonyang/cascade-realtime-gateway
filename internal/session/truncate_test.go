package session

import (
	"strings"
	"testing"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

const trText = "Hello world. 你好世界。Goodbye now."

// perSentence builds tier-2 segments: 10 ms of audio per rune.
func perSentence(text string) ([]audioSegment, int) {
	var segs []audioSegment
	audioPos := 0
	for _, s := range collect(text) {
		n := len([]rune(s.Text)) * audio.MsToBytes(10)
		segs = append(segs, audioSegment{textStart: s.Start, textEnd: s.End, audioStart: audioPos, audioEnd: audioPos + n})
		audioPos += n
	}
	return segs, audioPos
}

func linearAlignment(text string) ([]provider.CharTiming, int) {
	runes := []rune(text)
	al := make([]provider.CharTiming, len(runes))
	for i := range runes {
		al[i] = provider.CharTiming{CharIndex: i, StartMs: i * 10}
	}
	return al, audio.MsToBytes(len(runes) * 10)
}

func checkInvariants(t *testing.T, name string, trim func(cut int) string, total int) {
	t.Helper()
	prev := ""
	for cut := 0; cut <= total+audio.MsToBytes(50); cut += audio.MsToBytes(7) {
		got := trim(cut)
		if !strings.HasPrefix(trText, got) {
			t.Fatalf("%s: cut %d: %q is not a prefix", name, cut, got)
		}
		if len(got) < len(prev) {
			t.Fatalf("%s: cut %d: length decreased %d → %d", name, cut, len(prev), len(got))
		}
		if cut >= total && got != trText {
			t.Fatalf("%s: cut %d ≥ total %d must not trim, got %q", name, cut, total, got)
		}
		prev = got
	}
	if trim(0) != "" {
		t.Fatalf("%s: cut 0 must trim everything", name)
	}
}

func TestTrimTierAlignment(t *testing.T) {
	al, total := linearAlignment(trText)
	trim := func(cut int) string { return trimText(trText, nil, al, cut, total) }
	checkInvariants(t, "alignment", trim, total)
	// 55 ms → chars 0..5 started (5 starts at 50 ms), char 6 at 60 ms not yet.
	if got := trim(audio.MsToBytes(55)); got != "Hello " {
		t.Fatalf("got %q", got)
	}
}

func TestTrimTierPerSentence(t *testing.T) {
	segs, total := perSentence(trText)
	if len(segs) != 3 {
		t.Fatalf("segments = %d", len(segs))
	}
	trim := func(cut int) string { return trimText(trText, segs, nil, cut, total) }
	checkInvariants(t, "per-sentence", trim, total)
	// Exactly at the end of sentence 1 keeps sentence 1 whole.
	if got := trim(segs[0].audioEnd); got != "Hello world." {
		t.Fatalf("got %q", got)
	}
	// Halfway into sentence 2 (" 你好世界。" = 6 runes) keeps 3 runes.
	mid := segs[1].audioStart + (segs[1].audioEnd-segs[1].audioStart)/2
	if got := trim(mid); got != "Hello world. 你好" {
		t.Fatalf("got %q", got)
	}
}

func TestTrimTierWholeResponse(t *testing.T) {
	total := audio.MsToBytes(1000)
	segs := []audioSegment{{textStart: 0, textEnd: len(trText), audioStart: 0, audioEnd: total}}
	trim := func(cut int) string { return trimText(trText, segs, nil, cut, total) }
	checkInvariants(t, "whole", trim, total)
	n := len([]rune(trText))
	if got := trim(total / 2); len([]rune(got)) != n/2 {
		t.Fatalf("half cut kept %d of %d runes", len([]rune(got)), n)
	}
}

func TestTrimNoInformation(t *testing.T) {
	if got := trimText(trText, nil, nil, 10, 100); got != "" {
		t.Fatalf("no segments and no alignment must trim to empty, got %q", got)
	}
	if got := trimText("", nil, nil, 0, 0); got != "" {
		t.Fatal("empty text")
	}
}
