package mock

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

func recv(t *testing.T, ch <-chan provider.ASREvent) provider.ASREvent {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("events closed")
		}
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ASR event")
	}
	return provider.ASREvent{}
}

func TestASRPartialFinalAndFinalize(t *testing.T) {
	asr := NewASR(ASRScript{Utterances: []string{"hello world", "second"}, PartialAfterMs: 100, FinalAfterMs: 300})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := asr.OpenStream(ctx, provider.ASRConfig{SampleRate: audio.SampleRate})
	if err != nil {
		t.Fatal(err)
	}
	s := st.(*ASRStream)
	frame := make([]byte, audio.MsToBytes(50))
	for range 2 {
		s.PushAudio(frame)
	}
	if ev := recv(t, s.Events()); ev.Kind != provider.ASRPartial || ev.Text != "hello" {
		t.Fatalf("partial = %+v", ev)
	}
	for range 4 {
		s.PushAudio(frame)
	}
	ev := recv(t, s.Events())
	if ev.Kind != provider.ASRFinal || ev.Text != "hello world" || ev.StartMs != 0 || ev.EndMs != 300 {
		t.Fatalf("final = %+v", ev)
	}
	s.PushAudio(frame)
	s.Finalize()
	if ev := recv(t, s.Events()); ev.Kind != provider.ASRFinal || ev.Text != "second" || ev.StartMs != 300 || ev.EndMs != 350 {
		t.Fatalf("finalized = %+v", ev)
	}
	if s.Finalizes() != 1 || s.PushedMs() != 350 {
		t.Fatalf("counters: finalizes=%d pushed=%d", s.Finalizes(), s.PushedMs())
	}
	cancel()
	select {
	case <-s.Done():
	case <-time.After(time.Second):
		t.Fatal("stream goroutine did not exit on cancel")
	}
	if s.CancelledAt().IsZero() {
		t.Fatal("cancellation moment not recorded")
	}
	if _, ok := <-s.Events(); ok {
		t.Fatal("events must be closed after exit")
	}
}

func TestASRErrorsAndEndOfTurn(t *testing.T) {
	if _, err := NewASR(ASRScript{OpenErr: ErrScripted}).OpenStream(context.Background(), provider.ASRConfig{}); !errors.Is(err, ErrScripted) {
		t.Fatalf("OpenErr not surfaced: %v", err)
	}
	asr := NewASR(ASRScript{Utterances: []string{"x"}, EndOfTurnAfterFinal: true, StreamErr: ErrScripted})
	st, _ := asr.OpenStream(context.Background(), provider.ASRConfig{})
	st.PushAudio(make([]byte, 48))
	if ev := recv(t, st.Events()); ev.Kind != provider.ASRError || !errors.Is(ev.Err, ErrScripted) {
		t.Fatalf("stream error = %+v", ev)
	}
	st.Finalize()
	if ev := recv(t, st.Events()); ev.Kind != provider.ASRFinal || ev.Text != "x" {
		t.Fatalf("final = %+v", ev)
	}
	if ev := recv(t, st.Events()); ev.Kind != provider.ASREndOfTurn {
		t.Fatalf("want end_of_turn, got %+v", ev)
	}
	st.Close()
}

func drain(t *testing.T, ch <-chan provider.LLMChunk) []provider.LLMChunk {
	t.Helper()
	var out []provider.LLMChunk
	timeout := time.After(2 * time.Second)
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, c)
		case <-timeout:
			t.Fatal("timeout draining LLM")
		}
	}
}

func TestLLMScriptAndBlock(t *testing.T) {
	llm := NewLLM(LLMScript{Tokens: []string{"a", "b"}, FinishReason: provider.FinishLength, Usage: provider.Usage{OutputTokens: 2}})
	ch, err := llm.Chat(context.Background(), provider.ChatRequest{Instructions: "i"})
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, ch)
	if len(got) != 3 || got[0].Text != "a" || got[2].Kind != provider.LLMDone || got[2].FinishReason != provider.FinishLength || got[2].Usage.OutputTokens != 2 {
		t.Fatalf("chunks = %+v", got)
	}
	if reqs := llm.Requests(); len(reqs) != 1 || reqs[0].Instructions != "i" {
		t.Fatalf("requests = %+v", reqs)
	}

	blocked := NewLLM(LLMScript{Tokens: []string{"a", "b", "c"}, Block: true, BlockAfter: 1})
	ctx, cancel := context.WithCancel(context.Background())
	ch, _ = blocked.Chat(ctx, provider.ChatRequest{})
	first := <-ch
	if first.Text != "a" {
		t.Fatalf("first = %+v", first)
	}
	select {
	case c := <-ch:
		t.Fatalf("should block after 1 token, got %+v", c)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	rest := drain(t, ch)
	if len(rest) != 1 || rest[0].Kind != provider.LLMError || blocked.CancelledAt().IsZero() || blocked.Active() != 0 {
		t.Fatalf("after cancel: %+v cancelledAt=%v active=%d", rest, blocked.CancelledAt(), blocked.Active())
	}

	if _, err := NewLLM(LLMScript{ChatErr: ErrScripted}).Chat(context.Background(), provider.ChatRequest{}); !errors.Is(err, ErrScripted) {
		t.Fatal("ChatErr not surfaced")
	}
	errCh, _ := NewLLM(LLMScript{Tokens: []string{"a"}, Err: ErrScripted}).Chat(context.Background(), provider.ChatRequest{})
	if got := drain(t, errCh); len(got) != 2 || got[1].Kind != provider.LLMError {
		t.Fatalf("Err not emitted: %+v", got)
	}
}

func readAll(t *testing.T, s provider.TTSStream) []provider.AudioChunk {
	t.Helper()
	var out []provider.AudioChunk
	for {
		c, err := s.ReadAudio()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("ReadAudio: %v", err)
		}
		out = append(out, c)
	}
}

func TestTTSNonIncrementalWaitsForEndInput(t *testing.T) {
	tts := NewTTS(TTSScript{MsPerRune: 10})
	if caps := tts.Capabilities(); caps.IncrementalText || caps.Alignment {
		t.Fatalf("caps = %+v", caps)
	}
	s, _ := tts.Synthesize(context.Background(), provider.TTSConfig{})
	s.WriteText("abc")
	done := make(chan struct{})
	go func() { s.ReadAudio(); close(done) }()
	select {
	case <-done:
		t.Fatal("non-incremental stream produced audio before EndInput")
	case <-time.After(30 * time.Millisecond):
	}
	s.WriteText("de")
	s.EndInput()
	<-done
	chunks := readAll(t, s)
	// First chunk was consumed by the goroutine; remaining must be empty as
	// non-incremental mode yields one segment.
	if len(chunks) != 0 {
		t.Fatalf("extra chunks: %d", len(chunks))
	}
}

func TestTTSIncrementalWithAlignmentAndChunks(t *testing.T) {
	tts := NewTTS(TTSScript{IncrementalText: true, Alignment: true, MsPerRune: 10, ChunkMs: 25})
	s, _ := tts.Synthesize(context.Background(), provider.TTSConfig{})
	s.WriteText("héllo") // 5 runes → 50 ms → chunks 25+25
	s.WriteText("!")     // 1 rune → 10 ms
	s.EndInput()
	chunks := readAll(t, s)
	if len(chunks) != 3 {
		t.Fatalf("chunks = %d", len(chunks))
	}
	total := 0
	var timings []provider.CharTiming
	for _, c := range chunks {
		total += len(c.PCM)
		timings = append(timings, c.Alignment...)
	}
	if audio.BytesToMs(total) != 60 {
		t.Fatalf("total ms = %d", audio.BytesToMs(total))
	}
	if len(timings) != 6 || timings[5].CharIndex != 5 || timings[5].StartMs != 50 || timings[2].StartMs != 20 {
		t.Fatalf("alignment = %+v", timings)
	}
}

func TestTTSCancelUnblocksRead(t *testing.T) {
	tts := NewTTS(TTSScript{IncrementalText: true, FirstChunkDelay: Duration(time.Second)})
	ctx, cancel := context.WithCancel(context.Background())
	s, _ := tts.Synthesize(ctx, provider.TTSConfig{})
	s.WriteText("x")
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	start := time.Now()
	if _, err := s.ReadAudio(); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("ReadAudio did not return promptly after cancel")
	}
	if err := s.WriteText("y"); err != nil {
		t.Fatalf("write after cancel before close: %v", err)
	}
	s.Close()
	if err := s.WriteText("z"); err == nil {
		t.Fatal("write after Close must fail")
	}
}

func TestRegistryFactories(t *testing.T) {
	for _, kind := range []provider.Kind{provider.KindASR, provider.KindLLM, provider.KindTTS} {
		if !provider.Known(string(kind), Name) {
			t.Fatalf("%s/mock not registered", kind)
		}
	}
	p, err := provider.New(provider.KindLLM, Name, "", []byte(`{"tokens":["hi"],"token_delay":"1ms"}`))
	if err != nil {
		t.Fatal(err)
	}
	ch, _ := p.(provider.LLM).Chat(context.Background(), provider.ChatRequest{})
	if got := drain(t, ch); len(got) != 2 || got[0].Text != "hi" {
		t.Fatalf("factory-built llm: %+v", got)
	}
	if _, err := provider.New(provider.KindTTS, Name, "", []byte(`{"ms_per_rune": "x"}`)); err == nil {
		t.Fatal("bad options must fail")
	}
}
