package mock

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// TTSScript drives the mock synthesizer. Every rune of text becomes
// MsPerRune milliseconds of silence-valued PCM. With IncrementalText the
// audio for each WriteText is available immediately; without it, audio is
// produced only after EndInput, as one segment for the whole text. With
// Alignment each chunk carries linear character timings.
type TTSScript struct {
	IncrementalText bool     `json:"incremental_text"`
	Alignment       bool     `json:"alignment"`
	MsPerRune       int      `json:"ms_per_rune"`
	ChunkMs         int      `json:"chunk_ms"`
	FirstChunkDelay Duration `json:"first_chunk_delay"`
	SynthErr        error    `json:"-"`
}

const defaultMsPerRune = 10

// TTS is the mock provider.
type TTS struct {
	script TTSScript

	mu      sync.Mutex
	streams []*TTSStream
}

// NewTTS builds the provider.
func NewTTS(s TTSScript) *TTS {
	if s.MsPerRune <= 0 {
		s.MsPerRune = defaultMsPerRune
	}
	return &TTS{script: s}
}

// Kind implements provider.Provider.
func (t *TTS) Kind() provider.Kind { return provider.KindTTS }

// Capabilities implements provider.TTS.
func (t *TTS) Capabilities() provider.TTSCaps {
	return provider.TTSCaps{IncrementalText: t.script.IncrementalText, Alignment: t.script.Alignment}
}

// Streams returns every stream opened so far.
func (t *TTS) Streams() []*TTSStream {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]*TTSStream(nil), t.streams...)
}

// Synthesize implements provider.TTS.
func (t *TTS) Synthesize(ctx context.Context, cfg provider.TTSConfig) (provider.TTSStream, error) {
	if t.script.SynthErr != nil {
		return nil, t.script.SynthErr
	}
	s := &TTSStream{
		script: t.script,
		cfg:    cfg,
		ctx:    ctx,
		queue:  make(chan provider.AudioChunk, ttsSegmentQueue),
	}
	t.mu.Lock()
	t.streams = append(t.streams, s)
	t.mu.Unlock()
	return s, nil
}

// TTSStream is one mock synthesis stream.
type TTSStream struct {
	script TTSScript
	cfg    provider.TTSConfig
	ctx    context.Context
	queue  chan provider.AudioChunk

	mu        sync.Mutex
	text      string // all text written
	buffered  string // text not yet synthesized (non-incremental mode)
	runeCount int    // runes synthesized so far, for alignment indices
	audioMs   int    // audio produced so far, for alignment timings
	ended     bool
	closed    bool
	firstRead bool
}

// Text returns everything written to the stream.
func (s *TTSStream) Text() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.text
}

// WriteText implements provider.TTSStream.
func (s *TTSStream) WriteText(text string) error {
	s.mu.Lock()
	if s.ended || s.closed {
		s.mu.Unlock()
		return io.ErrClosedPipe
	}
	s.text += text
	if !s.script.IncrementalText {
		s.buffered += text
		s.mu.Unlock()
		return nil
	}
	chunks := s.chunks(text)
	s.mu.Unlock()
	for _, c := range chunks {
		s.queue <- c
	}
	return nil
}

// EndInput implements provider.TTSStream.
func (s *TTSStream) EndInput() error {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return nil
	}
	s.ended = true
	var chunks []provider.AudioChunk
	if s.buffered != "" {
		chunks = s.chunks(s.buffered)
		s.buffered = ""
	}
	s.mu.Unlock()
	for _, c := range chunks {
		s.queue <- c
	}
	close(s.queue)
	return nil
}

// chunks builds the audio chunks for text and advances the alignment
// counters; caller holds mu. Sending happens outside the lock.
func (s *TTSStream) chunks(text string) []provider.AudioChunk {
	runes := []rune(text)
	total := len(runes) * s.script.MsPerRune
	chunkMs := s.script.ChunkMs
	if chunkMs <= 0 || chunkMs > total {
		chunkMs = total
	}
	var out []provider.AudioChunk
	for off := 0; off < total; off += chunkMs {
		n := min(chunkMs, total-off)
		chunk := provider.AudioChunk{PCM: make([]byte, audio.MsToBytes(n))}
		if s.script.Alignment {
			for i := range runes {
				startMs := i * s.script.MsPerRune
				if startMs >= off && startMs < off+n {
					chunk.Alignment = append(chunk.Alignment, provider.CharTiming{
						CharIndex: s.runeCount + i,
						StartMs:   s.audioMs + startMs,
					})
				}
			}
		}
		out = append(out, chunk)
	}
	s.runeCount += len(runes)
	s.audioMs += total
	return out
}

// ReadAudio implements provider.TTSStream.
func (s *TTSStream) ReadAudio() (provider.AudioChunk, error) {
	s.mu.Lock()
	first := !s.firstRead
	s.firstRead = true
	s.mu.Unlock()
	if first {
		if d := time.Duration(s.script.FirstChunkDelay); d > 0 {
			select {
			case <-time.After(d):
			case <-s.ctx.Done():
				return provider.AudioChunk{}, s.ctx.Err()
			}
		}
	}
	select {
	case chunk, ok := <-s.queue:
		if !ok {
			return provider.AudioChunk{}, io.EOF
		}
		return chunk, nil
	case <-s.ctx.Done():
		return provider.AudioChunk{}, s.ctx.Err()
	}
}

// Close implements provider.TTSStream.
func (s *TTSStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if !s.ended {
		s.ended = true
		close(s.queue)
	}
	return nil
}
