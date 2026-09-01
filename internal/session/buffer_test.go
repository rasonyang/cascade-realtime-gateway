package session

import (
	"errors"
	"testing"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
)

func ms(n int) []byte { return make([]byte, audio.MsToBytes(n)) }

func TestBufferTimelineAndCommitSlicing(t *testing.T) {
	b := newInputAudioBuffer(1000)
	b.append(ms(100))
	if b.totalMs() != 100 {
		t.Fatalf("totalMs = %d", b.totalMs())
	}
	pcm, start, end, ok := b.commit()
	if !ok || start != 0 || end != 100 || len(pcm) != audio.MsToBytes(100) {
		t.Fatalf("manual commit = %d %d %d %v", start, end, len(pcm), ok)
	}
	if b.len() != 0 || b.totalMs() != 100 || b.baseMs != 100 {
		t.Fatal("commit must clear and keep the timeline")
	}
	if _, _, _, ok := b.commit(); ok {
		t.Fatal("empty commit must report ok=false")
	}

	// Second turn: VAD marks a segment inside the buffer.
	b.append(ms(500))
	if got := b.markSpeechStart(200, 300); got != 100 {
		t.Fatalf("prefix padding must not roll back before buffer start: %d", got)
	}
	if got := b.markSpeechStart(350, 300); got != 150 {
		t.Fatalf("rollback: %d", got)
	}
	if got := b.markSpeechEnd(400, 500); got != 600 {
		t.Fatalf("speech end must clamp to audio present: %d", got)
	}
	pcm, start, end, ok = b.commit()
	if !ok || start != 150 || end != 600 || len(pcm) != audio.MsToBytes(450) {
		t.Fatalf("segment commit = %d %d %d", start, end, len(pcm))
	}
	if b.segStartMs != -1 || b.segEndMs != -1 {
		t.Fatal("segment marks must reset on commit")
	}
}

func TestBufferSpeechEndAdvancesBySilence(t *testing.T) {
	b := newInputAudioBuffer(5000)
	b.append(ms(2000))
	b.markSpeechStart(500, 100)
	if got := b.markSpeechEnd(1000, 300); got != 1300 {
		t.Fatalf("end = %d", got)
	}
	_, start, end, _ := b.commit()
	if start != 400 || end != 1300 {
		t.Fatalf("commit = %d %d", start, end)
	}
}

func TestBufferClearKeepsTimeline(t *testing.T) {
	b := newInputAudioBuffer(1000)
	b.append(ms(300))
	b.markSpeechStart(0, 0)
	b.clear()
	if b.len() != 0 || b.baseMs != 300 || b.segStartMs != -1 {
		t.Fatalf("after clear: len=%d base=%d seg=%d", b.len(), b.baseMs, b.segStartMs)
	}
	b.append(ms(100))
	if b.markSpeechStart(50, 0) != 350 {
		t.Fatal("offsets after clear must be relative to the new base")
	}
}

func TestBufferOverflowIsAnError(t *testing.T) {
	b := newInputAudioBuffer(100)
	if err := b.append(ms(100)); err != nil {
		t.Fatal(err)
	}
	if err := b.append(ms(1)); !errors.Is(err, errBufferOverflow) {
		t.Fatalf("want overflow, got %v", err)
	}
	if b.len() != audio.MsToBytes(100) {
		t.Fatal("overflowing append must not partially apply")
	}
}
