package session

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// minSentenceRunes is the shortest text the sentencer will hand to TTS as a
// sentence. It keeps abbreviations such as "e.g." or "Dr." from being
// synthesized on their own. Algorithm constant, not user configuration (see
// docs/decisions.md).
const minSentenceRunes = 6

// sentence is a contiguous span of the full response text. Consecutive
// sentences tile the text: sentence k+1 starts where sentence k ends. Text is
// the span itself; TTS callers trim it.
type sentence struct {
	Text  string
	Start int // byte offset in the full text
	End   int
}

// sentencer splits a streaming text into TTS-sized sentences using
// punctuation only; no NLP.
type sentencer struct {
	buf  []byte // unconsumed text
	base int    // byte offset of buf[0] in the full text
	scan int    // bytes of buf already scanned for boundaries
}

// push appends a chunk and returns any complete sentences.
func (s *sentencer) push(chunk string) []sentence {
	s.buf = append(s.buf, chunk...)
	var out []sentence
	for {
		end, ok := s.findBoundary()
		if !ok {
			return out
		}
		if utf8.RuneCount(s.buf[:end]) < minSentenceRunes {
			s.scan = end // too short: keep accumulating past this boundary
			continue
		}
		out = append(out, s.take(end))
	}
}

// flush returns whatever remains as a final sentence.
func (s *sentencer) flush() (sentence, bool) {
	if len(s.buf) == 0 {
		return sentence{}, false
	}
	return s.take(len(s.buf)), true
}

func (s *sentencer) take(end int) sentence {
	out := sentence{Text: string(s.buf[:end]), Start: s.base, End: s.base + end}
	s.buf = append([]byte(nil), s.buf[end:]...)
	s.base += end
	s.scan = 0
	return out
}

// findBoundary scans forward from s.scan for a sentence end. It returns the
// byte offset just past the terminator run (and any closing quotes). Latin
// terminators need a following whitespace rune to count, so "3.14" and
// "e.g.x" do not split and a trailing "." waits for the next chunk.
func (s *sentencer) findBoundary() (int, bool) {
	i := s.scan
	for i < len(s.buf) {
		r, size := utf8.DecodeRune(s.buf[i:])
		if r == utf8.RuneError && size == 1 && !utf8.FullRune(s.buf[i:]) {
			return 0, false // incomplete rune at chunk end
		}
		if r == '\n' {
			s.scan = i + size
			return i + size, true
		}
		if !isTerminator(r) {
			i += size
			continue
		}
		// Consume the run of terminators and closing quotes.
		j := i + size
		for j < len(s.buf) {
			r2, size2 := utf8.DecodeRune(s.buf[j:])
			if r2 == utf8.RuneError && size2 == 1 && !utf8.FullRune(s.buf[j:]) {
				return 0, false
			}
			if !isTerminator(r2) && !isClosingQuote(r2) {
				break
			}
			j += size2
		}
		if isCJKTerminator(r) || cjkInRun(s.buf[i:j]) {
			s.scan = j
			return j, true
		}
		if j >= len(s.buf) {
			return 0, false // need to see what follows
		}
		next, _ := utf8.DecodeRune(s.buf[j:])
		if unicode.IsSpace(next) && !isAbbreviation(s.buf[:i]) {
			s.scan = j
			return j, true
		}
		i = j
	}
	s.scan = i
	return 0, false
}

// abbreviations that end with a period without ending a sentence. Single
// letters before a period ("e.g.", "U.S.") are handled generically.
var abbreviations = map[string]bool{"dr": true, "mr": true, "mrs": true, "ms": true, "prof": true, "st": true, "vs": true}

// isAbbreviation reports whether the word immediately before a '.' at the
// end of b is an abbreviation.
func isAbbreviation(b []byte) bool {
	end := len(b)
	start := end
	for start > 0 {
		r, size := utf8.DecodeLastRune(b[:start])
		if !unicode.IsLetter(r) {
			break
		}
		start -= size
	}
	word := b[start:end]
	if len(word) == 0 {
		return false
	}
	if utf8.RuneCount(word) == 1 {
		return true
	}
	return abbreviations[strings.ToLower(string(word))]
}

func cjkInRun(b []byte) bool {
	return strings.ContainsAny(string(b), "。！？")
}

func isTerminator(r rune) bool {
	return r == '.' || r == '!' || r == '?' || isCJKTerminator(r)
}

func isCJKTerminator(r rune) bool {
	return r == '。' || r == '！' || r == '？'
}

func isClosingQuote(r rune) bool {
	return r == '"' || r == '\'' || r == ')' || r == '”' || r == '’' || r == '」' || r == '』'
}
