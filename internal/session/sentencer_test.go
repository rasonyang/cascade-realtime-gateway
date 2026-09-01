package session

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func collect(chunks ...string) []sentence {
	var s sentencer
	var out []sentence
	for _, c := range chunks {
		out = append(out, s.push(c)...)
	}
	if last, ok := s.flush(); ok {
		out = append(out, last)
	}
	return out
}

func texts(ss []sentence) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = s.Text
	}
	return out
}

func assertTiling(t *testing.T, full string, ss []sentence) {
	t.Helper()
	pos := 0
	for _, s := range ss {
		if s.Start != pos || s.End <= s.Start || full[s.Start:s.End] != s.Text {
			t.Fatalf("sentence %+v does not tile full text at %d", s, pos)
		}
		pos = s.End
	}
	if pos != len(full) {
		t.Fatalf("sentences cover %d of %d bytes", pos, len(full))
	}
}

func TestSentencerPunctuationAndOffsets(t *testing.T) {
	full := "Hello there! How are you today? I am fine."
	ss := collect("Hello there! How ", "are you today? I am", " fine.")
	want := []string{"Hello there!", " How are you today?", " I am fine."}
	if got := texts(ss); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %q", got)
	}
	assertTiling(t, full, ss)
}

func TestSentencerAbbreviationsAndNumbers(t *testing.T) {
	full := "Dr. Smith paid 3.14 dollars, e.g. yesterday in the U.S. market. Then he left!"
	ss := collect(full)
	want := []string{"Dr. Smith paid 3.14 dollars, e.g. yesterday in the U.S. market.", " Then he left!"}
	if got := texts(ss); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %q", got)
	}
	assertTiling(t, full, ss)
}

func TestSentencerWaitsForWhitespaceAcrossChunks(t *testing.T) {
	var s sentencer
	if got := s.push("First sentence."); len(got) != 0 {
		t.Fatalf("must not split on a trailing '.' before seeing what follows: %q", texts(got))
	}
	got := s.push("5 is a number. Next")
	if len(got) != 1 || got[0].Text != "First sentence.5 is a number." {
		t.Fatalf("'.5' is not a boundary but '. N' is: %q", texts(got))
	}
	got = s.push(" one. ")
	if len(got) != 1 || got[0].Text != " Next one." {
		t.Fatalf("got %q", texts(got))
	}
}

func TestSentencerCJKAndNewlines(t *testing.T) {
	full := "今天天气很好。我们出去走走吧！你觉得怎么样？\n下一行"
	ss := collect("今天天气", "很好。我们出去走走吧！你觉得怎么样？\n下一行")
	// "\n" alone is shorter than minSentenceRunes and merges forward.
	want := []string{"今天天气很好。", "我们出去走走吧！", "你觉得怎么样？", "\n下一行"}
	if got := texts(ss); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %q", got)
	}
	assertTiling(t, full, ss)
}

func TestSentencerSplitRuneAcrossChunks(t *testing.T) {
	full := "很好。再见。"
	raw := []byte(full)
	var s sentencer
	var out []sentence
	for i := range raw {
		out = append(out, s.push(string(raw[i:i+1]))...)
	}
	if last, ok := s.flush(); ok {
		out = append(out, last)
	}
	// "很好。" is 3 runes < minSentenceRunes, so it merges with the next.
	if len(out) != 1 || out[0].Text != full {
		t.Fatalf("got %q", texts(out))
	}
	for _, sn := range out {
		if !utf8.ValidString(sn.Text) {
			t.Fatal("split rune leaked")
		}
	}
	assertTiling(t, full, out)
}

func TestSentencerLongStreamWithoutPunctuation(t *testing.T) {
	var s sentencer
	var chunk strings.Builder
	for i := 0; i < 200; i++ {
		chunk.WriteString("word ")
	}
	if got := s.push(chunk.String()); len(got) != 0 {
		t.Fatal("no punctuation must yield no sentences until flush")
	}
	last, ok := s.flush()
	if !ok || last.Text != chunk.String() || last.Start != 0 || last.End != chunk.Len() {
		t.Fatalf("flush = %+v", last)
	}
	if _, ok := s.flush(); ok {
		t.Fatal("second flush must be empty")
	}
}

func TestSentencerClosingQuotes(t *testing.T) {
	ss := collect(`She said "stop!" Then left.`)
	want := []string{`She said "stop!"`, " Then left."}
	if got := texts(ss); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %q", got)
	}
}
