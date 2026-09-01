package mock

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// ASRScript drives the mock transcriber. Each utterance is consumed by one
// Final. A Partial (the first half of the next utterance) is emitted once
// PartialAfterMs of audio has been pushed since the last Final; a Final is
// emitted automatically after FinalAfterMs, or immediately on Finalize.
// EndOfTurnAfterFinal additionally emits EndOfTurn after every Final.
type ASRScript struct {
	Utterances          []string `json:"utterances"`
	PartialAfterMs      int      `json:"partial_after_ms"`
	FinalAfterMs        int      `json:"final_after_ms"`
	EndOfTurnAfterFinal bool     `json:"end_of_turn_after_final"`
	// FinalDelay postpones every Final by wall-clock time, to exercise the
	// session's asr_final_timeout.
	FinalDelay Duration `json:"final_delay"`
	// OpenErr makes OpenStream fail; StreamErr is emitted as an ASRError
	// event on the first PushAudio.
	OpenErr   error `json:"-"`
	StreamErr error `json:"-"`
}

// ASR is the mock provider. Streams records every stream opened.
type ASR struct {
	script ASRScript

	mu      sync.Mutex
	streams []*ASRStream
}

// NewASR builds the provider.
func NewASR(s ASRScript) *ASR { return &ASR{script: s} }

// Kind implements provider.Provider.
func (a *ASR) Kind() provider.Kind { return provider.KindASR }

// Streams returns the streams opened so far.
func (a *ASR) Streams() []*ASRStream {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]*ASRStream(nil), a.streams...)
}

// OpenStream implements provider.ASR.
func (a *ASR) OpenStream(ctx context.Context, cfg provider.ASRConfig) (provider.ASRStream, error) {
	if a.script.OpenErr != nil {
		return nil, a.script.OpenErr
	}
	ctx, cancel := context.WithCancel(ctx)
	s := &ASRStream{
		script:  a.script,
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
		in:      make(chan asrInput, asrInputQueue),
		events:  make(chan provider.ASREvent, asrEventQueue),
		done:    make(chan struct{}),
		pending: append([]string(nil), a.script.Utterances...),
	}
	a.mu.Lock()
	a.streams = append(a.streams, s)
	a.mu.Unlock()
	go s.run()
	return s, nil
}

type asrInput struct {
	pcm      []byte
	finalize bool
}

// ASRStream is one mock transcription stream.
type ASRStream struct {
	script ASRScript
	cfg    provider.ASRConfig
	ctx    context.Context
	cancel context.CancelFunc
	in     chan asrInput
	events chan provider.ASREvent
	done   chan struct{}

	// run-goroutine state
	pending        []string
	msSincePartial int
	msSinceFinal   int
	totalMs        int
	partialSent    bool
	errSent        bool

	mu          sync.Mutex
	pushedBytes int
	finalizes   int
	cancelledAt time.Time
	closed      bool
}

// PushedMs returns how much audio has been pushed, in milliseconds.
func (s *ASRStream) PushedMs() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return audio.BytesToMs(s.pushedBytes)
}

// Finalizes returns how many times Finalize was called.
func (s *ASRStream) Finalizes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finalizes
}

// CancelledAt returns when the stream observed ctx cancellation (zero if
// it has not).
func (s *ASRStream) CancelledAt() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelledAt
}

// Done is closed when the run goroutine has exited.
func (s *ASRStream) Done() <-chan struct{} { return s.done }

// PushAudio implements provider.ASRStream.
func (s *ASRStream) PushAudio(pcm []byte) error {
	s.mu.Lock()
	s.pushedBytes += len(pcm)
	s.mu.Unlock()
	select {
	case s.in <- asrInput{pcm: pcm}:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// Finalize implements provider.ASRStream.
func (s *ASRStream) Finalize() error {
	s.mu.Lock()
	s.finalizes++
	s.mu.Unlock()
	select {
	case s.in <- asrInput{finalize: true}:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// Events implements provider.ASRStream.
func (s *ASRStream) Events() <-chan provider.ASREvent { return s.events }

// Close implements provider.ASRStream.
func (s *ASRStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.cancel()
	<-s.done
	return nil
}

func (s *ASRStream) run() {
	defer close(s.done)
	defer close(s.events)
	for {
		select {
		case <-s.ctx.Done():
			s.mu.Lock()
			s.cancelledAt = time.Now()
			s.mu.Unlock()
			return
		case in := <-s.in:
			if !s.handle(in) {
				return
			}
		}
	}
}

func (s *ASRStream) emit(ev provider.ASREvent) bool {
	select {
	case s.events <- ev:
		return true
	case <-s.ctx.Done():
		s.mu.Lock()
		s.cancelledAt = time.Now()
		s.mu.Unlock()
		return false
	}
}

func (s *ASRStream) handle(in asrInput) bool {
	if in.finalize {
		return s.final()
	}
	if s.script.StreamErr != nil && !s.errSent {
		s.errSent = true
		return s.emit(provider.ASREvent{Kind: provider.ASRError, Err: s.script.StreamErr})
	}
	ms := audio.BytesToMs(len(in.pcm))
	s.totalMs += ms
	s.msSincePartial += ms
	s.msSinceFinal += ms
	if s.script.PartialAfterMs > 0 && !s.partialSent && s.msSincePartial >= s.script.PartialAfterMs {
		s.partialSent = true
		text := s.nextText()
		half := text[:len(text)/2]
		if !s.emit(provider.ASREvent{Kind: provider.ASRPartial, Text: half}) {
			return false
		}
	}
	if s.script.FinalAfterMs > 0 && s.msSinceFinal >= s.script.FinalAfterMs {
		return s.final()
	}
	return true
}

func (s *ASRStream) nextText() string {
	if len(s.pending) == 0 {
		return ""
	}
	return s.pending[0]
}

func (s *ASRStream) final() bool {
	if d := time.Duration(s.script.FinalDelay); d > 0 {
		select {
		case <-time.After(d):
		case <-s.ctx.Done():
			return false
		}
	}
	text := s.nextText()
	if len(s.pending) > 0 {
		s.pending = s.pending[1:]
	}
	start := s.totalMs - s.msSinceFinal
	s.msSinceFinal = 0
	s.msSincePartial = 0
	s.partialSent = false
	if !s.emit(provider.ASREvent{Kind: provider.ASRFinal, Text: text, StartMs: start, EndMs: s.totalMs}) {
		return false
	}
	if s.script.EndOfTurnAfterFinal {
		return s.emit(provider.ASREvent{Kind: provider.ASREndOfTurn})
	}
	return true
}

// ErrScripted is a convenient injectable failure.
var ErrScripted = errors.New("mock: scripted failure")
