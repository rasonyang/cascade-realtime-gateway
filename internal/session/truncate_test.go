package session

import (
	"strings"
	"testing"
	"time"

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

// TestTrimNoInformation: with neither segments nor alignment, trimText used
// to return "" and tier 3 was the caller's job. That left the one case that
// matters — a barge-in on a cancelled response, where the pipeline never gets
// to record its fallback segment — trimming the whole turn away. trimText now
// synthesizes the whole-response segment itself, since cutBytes and
// totalAudioBytes are always available to it, so "no information" was never
// quite true.
func TestTrimNoInformation(t *testing.T) {
	if got := trimText(trText, nil, nil, 10, 100); got != "Hel" {
		t.Fatalf("no segments and no alignment must fall back to proportional, got %q", got)
	}
	if got := trimText("", nil, nil, 0, 0); got != "" {
		t.Fatal("empty text")
	}
	// Zero and negative cuts still mean "nothing was heard".
	if got := trimText(trText, nil, nil, 0, 100); got != "" {
		t.Fatalf("a zero cut must trim everything, got %q", got)
	}
}

// TestTrimTextNoSegments: the tier-3 case, which is reached whenever a
// response is cancelled mid-stream on an incremental TTS provider — no
// per-sentence segments are ever reported, and the response never finishes
// its TTS stage. Before tier 3 was synthesized here, this trimmed the whole
// turn away, so a caller who barged in near the end of a long answer left the
// model with no record of what it had just said.
func TestTrimTextNoSegments(t *testing.T) {
	const text = "Hello world. Second sentence here."
	total := audio.MsToBytes(300)
	for _, tc := range []struct {
		name string
		cut  int
		want string
	}{
		{"nothing heard", 0, ""},
		{"a third heard", audio.MsToBytes(100), "Hello world"},
		{"two thirds heard", audio.MsToBytes(200), "Hello world. Second se"},
		{"all heard", total, text},
		{"past the end", total + 1, text},
	} {
		got := trimText(text, nil, nil, tc.cut, total)
		if got != tc.want {
			t.Errorf("%s: trimText(cut=%d) = %q, want %q", tc.name, tc.cut, got, tc.want)
		}
	}
	// Monotonic and always a prefix.
	prev := 0
	for ms := 0; ms <= 300; ms += 10 {
		got := trimText(text, nil, nil, audio.MsToBytes(ms), total)
		if !strings.HasPrefix(text, got) {
			t.Fatalf("cut=%dms produced %q, which is not a prefix", ms, got)
		}
		if len(got) < prev {
			t.Fatalf("cut=%dms shortened the result from %d to %d bytes", ms, prev, len(got))
		}
		prev = len(got)
	}
}

// TestTruncateAfterCancelKeepsWhatWasHeard reproduces the live barge-in that
// exposed the tier-3 gap: an incremental TTS provider (qwen's shape), a
// response cancelled while audio was still streaming, then a truncate from
// the client reporting how much the caller actually heard.
func TestTruncateAfterCancelKeepsWhatWasHeard(t *testing.T) {
	sc := defaultScripts()
	sc.tts.IncrementalText = true // qwen declares this; it reports no segments
	sc.llm.Block, sc.llm.BlockAfter = true, 4
	h := newHarness(t, sc, manual)
	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "hi"}})
	h.post(CmdCreateResponse{})
	h.waitFor("audio", func(e Event) bool { _, ok := e.(EvOutputAudioDelta); return ok })
	h.post(CmdCancelResponse{})
	done := h.waitResponseDone(0)
	out := done.Output[0]
	if out.AudioMs == 0 || out.Text == "" {
		t.Fatalf("cancelled item carries nothing to trim: %+v", out)
	}

	// The caller heard most of it before speaking over the rest.
	cut := out.AudioMs * 2 / 3
	h.post(CmdTruncateItem{ID: out.Ref, AudioEndMs: cut})
	h.waitFor("truncated", func(e Event) bool { _, ok := e.(EvItemTruncated); return ok })

	h.post(CmdCreateItem{Item: ItemSpec{Role: RoleUser, Text: "go on"}})
	h.post(CmdCreateResponse{})
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) && len(h.llm.Requests()) < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	if len(h.llm.Requests()) < 2 {
		t.Fatal("the second generation was never issued")
	}
	var kept string
	for _, m := range h.llm.Requests()[1].Messages {
		if m.Role == provider.RoleAssistant {
			kept = m.Content
		}
	}
	if kept == "" {
		t.Fatalf("the whole turn was trimmed away; the caller heard %d of %d ms of %q", cut, out.AudioMs, out.Text)
	}
	if !strings.HasPrefix(out.Text, kept) {
		t.Fatalf("kept %q is not a prefix of %q", kept, out.Text)
	}
	if kept == out.Text {
		t.Fatalf("nothing was trimmed although only %d of %d ms was heard", cut, out.AudioMs)
	}
}
