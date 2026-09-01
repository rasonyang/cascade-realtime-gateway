//go:build qwenreal

// Real endpoint validation. Opt in with:
//
//	ALIYUN_API_KEY=… go test -tags qwenreal -v ./internal/provider/qwen
//
// Every test here talks to the live Model Studio endpoint and is skipped
// when the key is absent. Nothing in this file logs the key.
package qwen

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

func realKey(t *testing.T) string {
	t.Helper()
	key := os.Getenv("ALIYUN_API_KEY")
	if key == "" {
		t.Skip("ALIYUN_API_KEY not set")
	}
	return key
}

const realSpeechText = "The ocean is vast and deep. It covers most of the planet."

var (
	speechOnce sync.Once
	speechPCM  []byte
	speechErr  error
)

// realSpeech synthesizes the fixture utterance once per run so the ASR tests
// have genuine 24 kHz speech to push.
func realSpeech(t *testing.T, key string) []byte {
	t.Helper()
	speechOnce.Do(func() {
		tts := NewTTS(key, TTSOptions{})
		s, err := tts.Synthesize(context.Background(), provider.TTSConfig{SampleRate: audio.SampleRate})
		if err != nil {
			speechErr = err
			return
		}
		defer s.Close()
		s.WriteText(realSpeechText)
		s.EndInput()
		for {
			c, err := s.ReadAudio()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				speechErr = err
				return
			}
			speechPCM = append(speechPCM, c.PCM...)
		}
	})
	if speechErr != nil {
		t.Fatalf("synthesizing the fixture utterance: %v", speechErr)
	}
	return speechPCM
}

func TestRealLLM(t *testing.T) {
	key := realKey(t)
	l := NewLLM(key, LLMOptions{})
	started := time.Now()
	ch, err := l.Chat(context.Background(), provider.ChatRequest{
		Instructions: "You are a concise voice assistant. Answer in one short sentence.",
		Messages:     []provider.Message{{Role: provider.RoleUser, Content: "What is the largest ocean on Earth?"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	var ttft time.Duration
	deltas := 0
	var last provider.LLMChunk
	for c := range ch {
		switch c.Kind {
		case provider.LLMTextDelta:
			if deltas == 0 {
				ttft = time.Since(started)
			}
			deltas++
			text.WriteString(c.Text)
		case provider.LLMError:
			t.Fatalf("stream error: %v", c.Err)
		}
		last = c
	}
	if last.Kind != provider.LLMDone {
		t.Fatalf("stream ended with %+v, want done", last)
	}
	answer := text.String()
	if answer == "" {
		t.Fatal("no text generated")
	}
	// enable_thinking=false must keep the reasoning trace out of the answer.
	for _, marker := range []string{"<think>", "Thinking Process", "reasoning_content"} {
		if strings.Contains(answer, marker) {
			t.Errorf("answer contains reasoning marker %q: %q", marker, answer)
		}
	}
	if deltas < 2 {
		t.Errorf("delta count = %d; the answer did not arrive incrementally", deltas)
	}
	if last.Usage.OutputTokens == 0 {
		t.Errorf("usage = %+v, want output tokens", last.Usage)
	}
	t.Logf("LLM  ttft=%v deltas=%d usage=%+v answer=%q", ttft.Round(time.Millisecond), deltas, last.Usage, answer)
}

func TestRealTTS(t *testing.T) {
	key := realKey(t)
	tts := NewTTS(key, TTSOptions{})
	if !tts.Capabilities().IncrementalText {
		t.Fatal("IncrementalText capability lost")
	}
	started := time.Now()
	s, err := tts.Synthesize(context.Background(), provider.TTSConfig{SampleRate: audio.SampleRate, Speed: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	dialed := time.Since(started)

	firstText := time.Now()
	if err := s.WriteText("The Pacific is the largest ocean. "); err != nil {
		t.Fatal(err)
	}
	// Audio for the first sentence must arrive before the second is written:
	// that is what "stream immediately, do not buffer the utterance" means.
	c, err := s.ReadAudio()
	if err != nil {
		t.Fatalf("first ReadAudio: %v", err)
	}
	firstAudio := time.Since(firstText)
	if len(c.PCM) == 0 {
		t.Fatal("first chunk carried no audio")
	}
	if len(c.PCM)%audio.BytesPerSample != 0 {
		t.Errorf("chunk of %d bytes is not whole 16-bit samples", len(c.PCM))
	}
	total := len(c.PCM)

	if err := s.WriteText("It covers a third of the surface of the Earth."); err != nil {
		t.Fatal(err)
	}
	if err := s.EndInput(); err != nil {
		t.Fatal(err)
	}
	for {
		c, err := s.ReadAudio()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadAudio: %v", err)
		}
		total += len(c.PCM)
	}
	// 24 kHz 16-bit mono: the byte count must map to a plausible duration.
	ms := audio.BytesToMs(total)
	if ms < 3000 || ms > 20000 {
		t.Errorf("synthesized %d ms for two sentences; expected a few seconds at 24 kHz", ms)
	}
	t.Logf("TTS  dial=%v first_text→first_audio=%v audio=%dms bytes=%d",
		dialed.Round(time.Millisecond), firstAudio.Round(time.Millisecond), ms, total)
}

func TestRealASRTwoTurnsOneConnection(t *testing.T) {
	key := realKey(t)
	pcm := realSpeech(t, key)
	t.Logf("fixture utterance: %d ms", audio.BytesToMs(len(pcm)))

	asr := NewASR(key, ASROptions{})
	stream, err := asr.OpenStream(context.Background(), provider.ASRConfig{SampleRate: audio.SampleRate})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	for turn := 1; turn <= 2; turn++ {
		partials, finals := 0, 0
		var finalText string
		var firstPartial, finalAt, endOfTurnAt time.Time
		started := time.Now()

		// Push in real time so the service sees a genuine utterance.
		go func() {
			frame := audio.MsToBytes(20)
			for off := 0; off < len(pcm); off += frame {
				end := min(off+frame, len(pcm))
				if err := stream.PushAudio(pcm[off:end]); err != nil {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
		}()

		// Consume partials for the length of the utterance, then finalize.
		speechDone := time.After(time.Duration(audio.BytesToMs(len(pcm))) * time.Millisecond)
		collecting := true
		for collecting {
			select {
			case ev := <-stream.Events():
				switch ev.Kind {
				case provider.ASRPartial:
					if partials == 0 {
						firstPartial = time.Now()
					}
					partials++
				case provider.ASRFinal:
					finals++
					finalText = ev.Text
					finalAt = time.Now()
				case provider.ASRError:
					t.Fatalf("turn %d: %v", turn, ev.Err)
				}
			case <-speechDone:
				collecting = false
			}
		}
		endOfSpeech := time.Now()
		if err := stream.Finalize(); err != nil {
			t.Fatalf("turn %d: Finalize: %v", turn, err)
		}
		deadline := time.After(10 * time.Second)
		for endOfTurnAt.IsZero() {
			select {
			case ev := <-stream.Events():
				switch ev.Kind {
				case provider.ASRPartial:
					partials++
				case provider.ASRFinal:
					finals++
					finalText = ev.Text
					finalAt = time.Now()
				case provider.ASREndOfTurn:
					endOfTurnAt = time.Now()
				case provider.ASRError:
					t.Fatalf("turn %d: %v", turn, ev.Err)
				}
			case <-deadline:
				t.Fatalf("turn %d: no end_of_turn within 10s of Finalize", turn)
			}
		}
		if partials == 0 {
			t.Errorf("turn %d: no partial transcripts", turn)
		}
		if finals == 0 || finalText == "" {
			t.Fatalf("turn %d: no final transcript", turn)
		}
		if !strings.Contains(strings.ToLower(finalText), "ocean") {
			t.Errorf("turn %d: transcript %q does not contain the spoken word \"ocean\"", turn, finalText)
		}
		t.Logf("ASR  turn %d: partials=%d first_partial=%v end_of_speech→final=%v →end_of_turn=%v text=%q",
			turn, partials, firstPartial.Sub(started).Round(time.Millisecond),
			finalAt.Sub(endOfSpeech).Round(time.Millisecond), endOfTurnAt.Sub(endOfSpeech).Round(time.Millisecond), finalText)
	}
}

// TestRealCascade runs the whole chain against the live endpoints: real
// speech in, transcript out, answer generated, answer spoken.
func TestRealCascade(t *testing.T) {
	key := realKey(t)
	pcm := realSpeech(t, key)

	// --- ASR --------------------------------------------------------------
	asr := NewASR(key, ASROptions{})
	stream, err := asr.OpenStream(context.Background(), provider.ASRConfig{SampleRate: audio.SampleRate})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	go func() {
		frame := audio.MsToBytes(20)
		for off := 0; off < len(pcm); off += frame {
			end := min(off+frame, len(pcm))
			if stream.PushAudio(pcm[off:end]) != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		stream.Finalize()
	}()
	var transcript string
	deadline := time.After(30 * time.Second)
	for transcript == "" {
		select {
		case ev := <-stream.Events():
			if ev.Kind == provider.ASRFinal {
				transcript = ev.Text
			}
			if ev.Kind == provider.ASRError {
				t.Fatal(ev.Err)
			}
		case <-deadline:
			t.Fatal("no transcript within 30s")
		}
	}
	t.Logf("CASCADE transcript=%q", transcript)

	// --- LLM --------------------------------------------------------------
	ch, err := NewLLM(key, LLMOptions{}).Chat(context.Background(), provider.ChatRequest{
		Instructions: "You are a concise voice assistant. Answer in one short sentence.",
		Messages:     []provider.Message{{Role: provider.RoleUser, Content: transcript}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var answer strings.Builder
	for c := range ch {
		if c.Kind == provider.LLMError {
			t.Fatal(c.Err)
		}
		if c.Kind == provider.LLMTextDelta {
			answer.WriteString(c.Text)
		}
	}
	if answer.Len() == 0 {
		t.Fatal("the LLM produced no answer")
	}
	t.Logf("CASCADE answer=%q", answer.String())

	// --- TTS --------------------------------------------------------------
	out, err := NewTTS(key, TTSOptions{}).Synthesize(context.Background(), provider.TTSConfig{SampleRate: audio.SampleRate})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	out.WriteText(answer.String())
	out.EndInput()
	spoken := 0
	for {
		c, err := out.ReadAudio()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		spoken += len(c.PCM)
	}
	if spoken == 0 {
		t.Fatal("the answer was not spoken")
	}
	t.Logf("CASCADE spoken=%dms", audio.BytesToMs(spoken))
}

// TestRealLLMToolCall verifies DashScope's compatible-mode tool_calls
// streaming shape against the live endpoint rather than assuming it matches
// OpenAI's: the service has already deviated from its documentation twice
// (reasoning_content in deltas, the TTS flush stall). It logs the raw
// fragments, then asserts what the adapter normalizes them into.
//
// Scope: only tool_choice "auto" is live-verified in Phase 7 — the only mode
// the gateway's consumer uses. The wire mapping of "none", "required" and a
// forced function is covered by TestLLMToolChoiceModes / TestLLMForcedToolChoice
// against the fake endpoint, and their behaviour on the live service is
// therefore unverified.
func TestRealLLMToolCall(t *testing.T) {
	key := realKey(t)
	tools := []provider.ToolDef{{
		Name:        "transfer_to_agent",
		Description: "Transfer the caller to a human agent in the named department.",
		Parameters: json.RawMessage(`{"type":"object","properties":` +
			`{"department":{"type":"string","description":"sales, support or billing"}},` +
			`"required":["department"]}`),
	}}
	req := provider.ChatRequest{
		Instructions: "You are a call-centre agent. Use the tools you are given; do not answer in prose.",
		Messages:     []provider.Message{{Role: provider.RoleUser, Content: "Please put me through to the sales department."}},
		Tools:        tools,
		ToolChoice:   provider.ToolChoice{Mode: provider.ToolChoiceAuto},
	}

	// --- raw shape ---------------------------------------------------------
	logRawToolStream(t, key, req)

	// --- through the adapter -----------------------------------------------
	ch, err := NewLLM(key, LLMOptions{}).Chat(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var calls []provider.ToolCall
	var text strings.Builder
	var last provider.LLMChunk
	for c := range ch {
		switch c.Kind {
		case provider.LLMTextDelta:
			text.WriteString(c.Text)
		case provider.LLMToolCall:
			calls = append(calls, c.ToolCall)
		case provider.LLMError:
			t.Fatalf("stream error: %v", c.Err)
		}
		last = c
	}
	if last.Kind != provider.LLMDone {
		t.Fatalf("stream ended with %+v, want done", last)
	}
	if last.FinishReason != provider.FinishStop {
		t.Errorf("finish reason = %q, want stop: a tool call is a completed turn", last.FinishReason)
	}
	if len(calls) != 1 {
		t.Fatalf("tool calls = %+v, want exactly one complete call", calls)
	}
	call := calls[0]
	if call.Name != "transfer_to_agent" {
		t.Errorf("call name = %q", call.Name)
	}
	if call.ID == "" {
		t.Error("call id is empty; the wire fragments carry no id")
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(call.Arguments), &args); err != nil {
		t.Fatalf("arguments %q are not valid JSON — fragment accumulation is wrong: %v", call.Arguments, err)
	}
	if args["department"] == nil {
		t.Errorf("arguments = %v, want the department the caller asked for", args)
	}
	t.Logf("TOOL id=%q name=%q arguments=%s preamble=%q usage=%+v",
		call.ID, call.Name, call.Arguments, text.String(), last.Usage)

	// --- history replay ----------------------------------------------------
	// The other direction: an assistant turn carrying tool_calls (with empty
	// content) followed by a role:"tool" result must be accepted, and the
	// model must answer in prose rather than calling again.
	follow := req
	follow.Messages = append(append([]provider.Message(nil), req.Messages...),
		provider.Message{Role: provider.RoleAssistant, Content: text.String(), ToolCalls: []provider.ToolCall{call}},
		provider.Message{Role: provider.RoleTool, ToolCallID: call.ID,
			Content: "No sales agent is available. Tell the caller and offer to take a message."})
	ch2, err := NewLLM(key, LLMOptions{}).Chat(context.Background(), follow)
	if err != nil {
		t.Fatal(err)
	}
	var reply strings.Builder
	var calls2 []provider.ToolCall
	for c := range ch2 {
		switch c.Kind {
		case provider.LLMTextDelta:
			reply.WriteString(c.Text)
		case provider.LLMToolCall:
			calls2 = append(calls2, c.ToolCall)
		case provider.LLMError:
			t.Fatalf("replay stream error: %v", c.Err)
		}
	}
	if reply.Len() == 0 && len(calls2) == 0 {
		t.Fatal("the tool result produced neither speech nor a further call")
	}
	t.Logf("REPLAY reply=%q calls=%+v", reply.String(), calls2)
}

// logRawToolStream issues the same request straight to the endpoint and logs
// every data: line, so the wire shape is on the record rather than inferred
// from the adapter's output.
func logRawToolStream(t *testing.T, key string, req provider.ChatRequest) {
	t.Helper()
	l := NewLLM(key, LLMOptions{})
	body := chatRequest{
		Model: l.opts.Model, Stream: true, StreamOptions: streamOptions{IncludeUsage: true},
		EnableThinking: false, Tools: chatTools(req.Tools),
		ToolChoice: chatToolChoice(req.ToolChoice), ParallelToolCalls: new(bool),
	}
	body.Messages = append(body.Messages, chatMessage{Role: "system", Content: req.Instructions})
	for _, m := range req.Messages {
		body.Messages = append(body.Messages, chatMessageOf(m))
	}
	resp, err := l.post(context.Background(), body)
	if err != nil {
		t.Fatalf("raw probe: %v", err)
	}
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)
	for n := 0; n < 200; n++ {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(line, "data:") {
			t.Logf("RAW %s", strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
}
