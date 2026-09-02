package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider/qwen"
)

// synthesize renders text to 24 kHz PCM with the real TTS so the ASR and
// E2E benchmarks push genuine speech.
// benchHostEnv names the optional host override. The qwen package defaults to
// the public Model Studio endpoint; a dedicated deployment has an
// account-specific host and a key issued for one is rejected by the other.
const benchHostEnv = "QWEN_HOST"

func benchLLMOptions() qwen.LLMOptions {
	var o qwen.LLMOptions
	if h := os.Getenv(benchHostEnv); h != "" {
		o.BaseURL = "https://" + h + "/compatible-mode/v1"
	}
	return o
}

func benchWSURL() string {
	if h := os.Getenv(benchHostEnv); h != "" {
		return "wss://" + h + "/api-ws/v1/inference"
	}
	return ""
}

func benchASROptions() qwen.ASROptions {
	var o qwen.ASROptions
	o.URL = benchWSURL()
	return o
}

func benchTTSOptions() qwen.TTSOptions {
	var o qwen.TTSOptions
	o.URL = benchWSURL()
	return o
}

func synthesize(ctx context.Context, key, text string) ([]byte, error) {
	s, err := qwen.NewTTS(key, benchTTSOptions()).Synthesize(ctx, provider.TTSConfig{SampleRate: audio.SampleRate})
	if err != nil {
		return nil, err
	}
	defer s.Close()
	if err := s.WriteText(text); err != nil {
		return nil, err
	}
	if err := s.EndInput(); err != nil {
		return nil, err
	}
	var pcm []byte
	for {
		c, err := s.ReadAudio()
		if errors.Is(err, io.EOF) {
			return pcm, nil
		}
		if err != nil {
			return nil, err
		}
		pcm = append(pcm, c.PCM...)
	}
}

// ---- ASR -------------------------------------------------------------------

// benchASR measures one recognition turn per sample on a single long-lived
// connection, which is the steady state a call runs in. The connection dial
// is measured separately so cold connection cost never hides inside a turn.
func benchASR(ctx context.Context, key string, pcm []byte, samples int, cap *logCapture) (section, error) {
	asr := qwen.NewASR(key, benchASROptions())

	coldDial := &series{Name: "connect (cold)"}
	warmDial := &series{Name: "connect (warm)"}
	started := &series{Name: "run-task→task-started"}
	firstResult := &series{Name: "first audio→first result"}
	final := &series{Name: "end-of-speech→final"}
	endOfTurn := &series{Name: "end-of-speech→end_of_turn"}

	// Cold dial: the first socket of the process pays DNS and a full TLS
	// handshake. Warm dials reuse the resolver cache and TLS session tickets.
	t := time.Now()
	cold, err := asr.OpenStream(ctx, provider.ASRConfig{SampleRate: audio.SampleRate})
	if err != nil {
		return section{}, err
	}
	coldDial.add(time.Since(t))
	cold.Close()
	for i := 0; i < 5; i++ {
		t := time.Now()
		s, err := asr.OpenStream(ctx, provider.ASRConfig{SampleRate: audio.SampleRate})
		if err != nil {
			return section{}, err
		}
		warmDial.add(time.Since(t))
		s.Close()
	}

	stream, err := asr.OpenStream(ctx, provider.ASRConfig{SampleRate: audio.SampleRate})
	if err != nil {
		return section{}, err
	}
	defer stream.Close()

	for i := 0; i < samples; i++ {
		mark := cap.mark()
		var firstPartialAt, finalAt, endTurnAt time.Time
		audioStart := time.Now()
		pushDone := make(chan time.Time, 1)
		go func() {
			frame := audio.MsToBytes(20)
			for off := 0; off < len(pcm); off += frame {
				end := min(off+frame, len(pcm))
				if err := stream.PushAudio(pcm[off:end]); err != nil {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			pushDone <- time.Now()
		}()

		// Partials must be timestamped as they arrive, not after the
		// utterance has finished playing, or every one of them would look
		// as slow as the utterance is long.
		var endOfSpeech time.Time
		for endOfSpeech.IsZero() {
			select {
			case ev := <-stream.Events():
				switch ev.Kind {
				case provider.ASRPartial:
					if firstPartialAt.IsZero() {
						firstPartialAt = time.Now()
					}
				case provider.ASRFinal:
					finalAt = time.Now()
				case provider.ASRError:
					return section{}, fmt.Errorf("asr sample %d: %w", i, ev.Err)
				}
			case t := <-pushDone:
				endOfSpeech = t
			}
		}
		if err := stream.Finalize(); err != nil {
			return section{}, err
		}
		deadline := time.After(15 * time.Second)
		for endTurnAt.IsZero() {
			select {
			case ev := <-stream.Events():
				switch ev.Kind {
				case provider.ASRPartial:
					if firstPartialAt.IsZero() {
						firstPartialAt = time.Now()
					}
				case provider.ASRFinal:
					finalAt = time.Now()
				case provider.ASREndOfTurn:
					endTurnAt = time.Now()
				case provider.ASRError:
					return section{}, fmt.Errorf("asr sample %d: %w", i, ev.Err)
				}
			case <-deadline:
				return section{}, fmt.Errorf("asr sample %d: no end_of_turn within 15s", i)
			}
		}
		if !firstPartialAt.IsZero() {
			firstResult.add(firstPartialAt.Sub(audioStart))
		}
		if !finalAt.IsZero() {
			final.add(finalAt.Sub(endOfSpeech))
		}
		endOfTurn.add(endTurnAt.Sub(endOfSpeech))
		// The task for the next turn starts right after task-finished; its
		// acknowledgement is what the next turn waits on.
		if r, ok := cap.waitAfter(mark, "qwen asr task started", 5*time.Second); ok {
			if ms, ok := r.float("started_ms"); ok {
				started.add(time.Duration(ms * float64(time.Millisecond)))
			}
		}
		progress("asr", i+1, samples)
	}
	return summarizeAll("ASR — qwen-audio-3.0-asr-flash-streaming",
		"One turn per sample on one warm connection. \"first result\" is the first partial transcript;\n"+
			"\"final\" is measured from the last audio frame pushed, with Finalize issued immediately after.",
		coldDial, warmDial, started, firstResult, final, endOfTurn), nil
}

// ---- LLM -------------------------------------------------------------------

// benchLLM measures generation latency. The cold sample uses a fresh client
// so its TLS handshake is reported apart from the warm steady state.
func benchLLM(ctx context.Context, key string, samples int) (section, error) {
	cold := &series{Name: "request→first token (cold)"}
	ttft := &series{Name: "request→first token"}
	usable := &series{Name: "request→first TTS chunk"}
	total := &series{Name: "request→generation done"}

	req := provider.ChatRequest{
		Instructions: "You are a concise voice assistant. Answer in two short sentences.",
		Messages:     []provider.Message{{Role: provider.RoleUser, Content: "What is the largest ocean on Earth?"}},
	}
	measure := func(l provider.LLM) (first, chunk, done time.Duration, err error) {
		started := time.Now()
		ch, err := l.Chat(ctx, req)
		if err != nil {
			return 0, 0, 0, err
		}
		var acc strings.Builder
		for c := range ch {
			switch c.Kind {
			case provider.LLMTextDelta:
				if first == 0 {
					first = time.Since(started)
				}
				acc.WriteString(c.Text)
				if chunk == 0 && hasUsableSentence(acc.String()) {
					chunk = time.Since(started)
				}
			case provider.LLMError:
				return 0, 0, 0, c.Err
			}
		}
		done = time.Since(started)
		if chunk == 0 {
			chunk = done
		}
		return first, chunk, done, nil
	}

	// Cold: a provider built for this one request, so nothing is pooled.
	first, _, _, err := measure(qwen.NewLLM(key, benchLLMOptions()))
	if err != nil {
		return section{}, err
	}
	cold.add(first)

	l := qwen.NewLLM(key, benchLLMOptions())
	for i := 0; i < samples; i++ {
		first, chunk, done, err := measure(l)
		if err != nil {
			return section{}, fmt.Errorf("llm sample %d: %w", i, err)
		}
		ttft.add(first)
		usable.add(chunk)
		total.add(done)
		progress("llm", i+1, samples)
	}
	return summarizeAll("LLM — qwen3.6-flash (enable_thinking=false)",
		"\"first TTS chunk\" is the first sentence long enough to synthesize, matching the rule in\n"+
			"internal/session/sentencer.go: a terminator followed by at least six runes of text.",
		cold, ttft, usable, total), nil
}

// hasUsableSentence mirrors internal/session/sentencer.go closely enough to
// time the first sentence the pipeline would hand to TTS: a sentence-ending
// terminator with at least minSentenceRunes of text before it.
func hasUsableSentence(s string) bool {
	const minRunes = 6
	runes := []rune(s)
	for i, r := range runes {
		if strings.ContainsRune(".!?。！？", r) && i+1 >= minRunes {
			return true
		}
	}
	return false
}

// ---- TTS -------------------------------------------------------------------

// benchTTS measures one synthesis stream per sample. A stream is one socket,
// so the dial is reported on its own: the pipeline opens it while the LLM is
// still generating, which keeps it off the text-to-audio path.
// benchTTS measures one synthesis stream per sample. A stream is one socket,
// so the dial is reported on its own: the pipeline opens it while the LLM is
// still generating, which keeps it off the text-to-audio path.
//
// Two input patterns are measured, because they are not equivalent. One-shot
// writes the whole line and ends the task at once, which is what a
// non-streaming caller does. Incremental writes the first sentence and holds
// the task open, which is exactly what Cascade's pipeline does while the LLM
// is still generating.
func benchTTS(ctx context.Context, key string, opts qwen.TTSOptions, samples int, cap *logCapture) (section, error) {
	tts := qwen.NewTTS(key, opts)
	coldDial := &series{Name: "connect (cold)"}
	warmDial := &series{Name: "connect (warm)"}
	started := &series{Name: "run-task→task-started"}
	oneShot := &series{Name: "first text→audio (one-shot)"}
	incremental := &series{Name: "first text→audio (incremental)"}
	playable := &series{Name: "first text→100ms audio (one-shot)"}
	totalGen := &series{Name: "first text→last audio (one-shot)"}

	const first = "The Pacific Ocean is the largest ocean on Earth."
	const second = " It covers about a third of the surface of the planet."
	const playableMs = 100

	// run synthesizes one line. When hold is true the task is left open
	// after the first sentence, mimicking a generation still in flight.
	run := func(i int, hold bool) (dial, toAudio, toPlayable, toLast time.Duration, err error) {
		t := time.Now()
		s, err := tts.Synthesize(ctx, provider.TTSConfig{SampleRate: audio.SampleRate, Speed: 1})
		if err != nil {
			return 0, 0, 0, 0, err
		}
		defer s.Close()
		dial = time.Since(t)

		textAt := time.Now()
		if err := s.WriteText(first); err != nil {
			return 0, 0, 0, 0, err
		}
		if hold {
			// The pipeline cannot end the task until the LLM is done, so
			// the service sees a pause before the rest of the text.
			go func() {
				time.Sleep(300 * time.Millisecond)
				s.WriteText(second)
				s.EndInput()
			}()
		} else {
			if err := s.WriteText(second); err != nil {
				return 0, 0, 0, 0, err
			}
			if err := s.EndInput(); err != nil {
				return 0, 0, 0, 0, err
			}
		}
		var firstAt, playableAt, lastAt time.Time
		acc := 0
		for {
			c, err := s.ReadAudio()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return 0, 0, 0, 0, fmt.Errorf("tts sample %d: %w", i, err)
			}
			if firstAt.IsZero() && len(c.PCM) > 0 {
				firstAt = time.Now()
			}
			acc += len(c.PCM)
			if playableAt.IsZero() && audio.BytesToMs(acc) >= playableMs {
				playableAt = time.Now()
			}
			lastAt = time.Now()
		}
		return dial, firstAt.Sub(textAt), playableAt.Sub(textAt), lastAt.Sub(textAt), nil
	}

	// A cold sample first: its dial pays DNS and a full TLS handshake.
	dial, _, _, _, err := run(0, false)
	if err != nil {
		return section{}, err
	}
	coldDial.add(dial)

	for i := 1; i <= samples; i++ {
		mark := cap.mark()
		dial, toAudio, toPlayable, toLast, err := run(i, false)
		if err != nil {
			return section{}, err
		}
		warmDial.add(dial)
		oneShot.add(toAudio)
		playable.add(toPlayable)
		totalGen.add(toLast)
		if r, ok := cap.waitAfter(mark, "qwen tts task started", 2*time.Second); ok {
			if ms, ok := r.float("started_ms"); ok {
				started.add(time.Duration(ms * float64(time.Millisecond)))
			}
		}
		_, toAudioInc, _, _, err := run(i, true)
		if err != nil {
			return section{}, err
		}
		incremental.add(toAudioInc)
		progress("tts", i, samples)
	}
	return summarizeAll("TTS — qwen-audio-3.0-tts-flash",
		fmt.Sprintf("One stream per sample. \"100ms audio\" is the point at which %d ms of PCM has arrived,\n"+
			"the smallest buffer a client can start playing without an immediate underrun.\n"+
			"\"incremental\" holds the task open for 300 ms after the first sentence, as the pipeline does\n"+
			"while the LLM is still generating (flush nudge %v).", playableMs, !opts.NoFlushNudge),
		coldDial, warmDial, started, oneShot, incremental, playable, totalGen), nil
}

func progress(what string, i, n int) {
	fmt.Fprintf(stderr, "\r  %s %d/%d", what, i, n)
	if i == n {
		fmt.Fprintln(stderr)
	}
}
