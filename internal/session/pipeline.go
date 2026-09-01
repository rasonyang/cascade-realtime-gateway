package session

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// pipeline is the per-response goroutine group: LLM → sentencer → TTS. It
// owns no session state; every fact flows back through respEvents stamped
// with the generation, and ctx is the only way to stop it.
type pipeline struct {
	ctx  context.Context
	gen  Generation
	resp ResponseRef
	item ItemRef

	textOnly bool
	req      provider.ChatRequest
	ttsCfg   provider.TTSConfig

	llm provider.LLM
	tts provider.TTS
	out chan<- Event

	firstAudioMs int64 // per-sentence TTFA, filled by drain (TTS goroutine only)
}

// textChunkQueue and sentenceQueue bound the data-plane channels between
// stages. They are small because back-pressure is meant to propagate: a
// slow TTS must slow the LLM read, and ctx cancels everything.
const (
	textChunkQueue = 16
	sentenceQueue  = 4
)

// send delivers ev to the actor unless ctx is done.
func (p *pipeline) send(ev Event) bool {
	select {
	case p.out <- ev:
		return true
	case <-p.ctx.Done():
		return false
	}
}

func (p *pipeline) fail(stage string, err error) {
	if errors.Is(err, context.Canceled) {
		return // cancellation is not a failure
	}
	p.send(pipeError{Gen: p.gen, Stage: stage, Err: err})
}

// llmStage streams the generation, forwarding every text delta to the actor
// and to the sentencer.
func (p *pipeline) llmStage(chunks chan<- string) {
	defer close(chunks)
	stream, err := p.llm.Chat(p.ctx, p.req)
	if err != nil {
		p.fail("llm", err)
		return
	}
	for c := range stream {
		switch c.Kind {
		case provider.LLMTextDelta:
			var ev Event
			if p.textOnly {
				ev = EvOutputTextDelta{Resp: p.resp, Item: p.item, Gen: p.gen, Delta: c.Text}
			} else {
				ev = EvOutputAudioTranscriptDelta{Resp: p.resp, Item: p.item, Gen: p.gen, Delta: c.Text}
			}
			if !p.send(ev) {
				return
			}
			select {
			case chunks <- c.Text:
			case <-p.ctx.Done():
				return
			}
		case provider.LLMDone:
			p.send(pipeLLMDone{Gen: p.gen, Finish: c.FinishReason, Usage: c.Usage})
			return
		case provider.LLMError:
			p.fail("llm", c.Err)
			return
		}
	}
	// Stream closed without Done: treat as a provider failure.
	p.fail("llm", errors.New("llm stream ended without a Done chunk"))
}

// sentencerStage splits chunks into sentences.
func (p *pipeline) sentencerStage(chunks <-chan string, sentences chan<- sentence) {
	defer close(sentences)
	var s sentencer
	emit := func(sn sentence) bool {
		select {
		case sentences <- sn:
			return true
		case <-p.ctx.Done():
			return false
		}
	}
	for chunk := range chunks {
		for _, sn := range s.push(chunk) {
			if !emit(sn) {
				return
			}
		}
	}
	if last, ok := s.flush(); ok {
		emit(last)
	}
}

// ttsStage synthesizes sentences. Providers without incremental text get one
// stream per sentence, which yields exact per-sentence audio segments (tier 2
// truncation); incremental providers get one stream for the whole response.
func (p *pipeline) ttsStage(sentences <-chan sentence) {
	if p.tts.Capabilities().IncrementalText {
		p.ttsIncremental(sentences)
	} else {
		p.ttsPerSentence(sentences)
	}
}

func (p *pipeline) ttsPerSentence(sentences <-chan sentence) {
	audioPos := 0
	started := false
	for sn := range sentences {
		seg := audioSegment{textStart: sn.Start, textEnd: sn.End, audioStart: audioPos}
		text := strings.TrimSpace(sn.Text)
		if text != "" {
			if !started {
				started = true
				if !p.send(pipeTTSStart{Gen: p.gen}) {
					return
				}
			}
			n, ok := p.synthesizeOne(text)
			if !ok {
				return
			}
			audioPos += n
		}
		seg.audioEnd = audioPos
		if !p.send(pipeSegment{Gen: p.gen, Seg: seg}) {
			return
		}
	}
	p.send(pipeTTSDone{Gen: p.gen})
}

// synthesizeOne runs a full request for one sentence and returns the number
// of audio bytes forwarded. One Debug log per sentence records the TTS
// time-to-first-audio and total time; nothing is logged per frame.
func (p *pipeline) synthesizeOne(text string) (int, bool) {
	started := time.Now()
	stream, err := p.tts.Synthesize(p.ctx, p.ttsCfg)
	if err != nil {
		p.fail("tts", err)
		return 0, false
	}
	defer stream.Close()
	if err := stream.WriteText(text); err != nil {
		p.fail("tts", err)
		return 0, false
	}
	if err := stream.EndInput(); err != nil {
		p.fail("tts", err)
		return 0, false
	}
	n, ok := p.drain(stream)
	slog.Debug("tts sentence", "chars", utf8.RuneCountInString(text), "audio_ms", audio.BytesToMs(n),
		"first_audio_ms", p.firstAudioMs, "total_ms", time.Since(started).Milliseconds(), "ok", ok)
	return n, ok
}

func (p *pipeline) ttsIncremental(sentences <-chan sentence) {
	if !p.send(pipeTTSStart{Gen: p.gen}) {
		return
	}
	stream, err := p.tts.Synthesize(p.ctx, p.ttsCfg)
	if err != nil {
		p.fail("tts", err)
		return
	}
	defer stream.Close()
	writeErr := make(chan error, 1)
	go func() {
		for sn := range sentences {
			text := strings.TrimSpace(sn.Text)
			if text == "" {
				continue
			}
			if err := stream.WriteText(text); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- stream.EndInput()
	}()
	if _, ok := p.drain(stream); !ok {
		return
	}
	if err := <-writeErr; err != nil {
		p.fail("tts", err)
		return
	}
	p.send(pipeTTSDone{Gen: p.gen})
}

// drain forwards audio chunks until EOF and returns the byte count. It
// records the time to the first audio chunk in firstAudioMs.
func (p *pipeline) drain(stream provider.TTSStream) (int, bool) {
	total := 0
	started := time.Now()
	p.firstAudioMs = -1
	for {
		chunk, err := stream.ReadAudio()
		if errors.Is(err, io.EOF) {
			return total, true
		}
		if err != nil {
			p.fail("tts", err)
			return total, false
		}
		if len(chunk.Alignment) > 0 {
			if !p.send(pipeAlignment{Gen: p.gen, Timings: chunk.Alignment}) {
				return total, false
			}
		}
		if len(chunk.PCM) == 0 {
			continue
		}
		pcm := chunk.PCM[:audio.AlignDown(len(chunk.PCM))]
		if p.firstAudioMs < 0 {
			p.firstAudioMs = time.Since(started).Milliseconds()
		}
		if !p.send(EvOutputAudioDelta{Resp: p.resp, Item: p.item, Gen: p.gen, PCM: pcm}) {
			return total, false
		}
		total += len(pcm)
	}
}
