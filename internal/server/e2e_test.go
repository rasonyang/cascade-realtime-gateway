//go:build e2e

package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/rasonyang/cascade-realtime-gateway/internal/admin"
	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/observability"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider/openai"

	_ "github.com/rasonyang/cascade-realtime-gateway/internal/provider/deepgram"
)

// logCapture records slog records with timestamps so provider lifecycle
// facts can be checked against the interrupt moment.
type logCapture struct {
	mu   sync.Mutex
	recs []capturedRecord
}

type capturedRecord struct {
	at    time.Time
	msg   string
	attrs map[string]string
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *logCapture) WithGroup(string) slog.Handler            { return c }
func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	rec := capturedRecord{at: r.Time, msg: r.Message, attrs: map[string]string{}}
	r.Attrs(func(a slog.Attr) bool { rec.attrs[a.Key] = a.Value.String(); return true })
	c.mu.Lock()
	c.recs = append(c.recs, rec)
	c.mu.Unlock()
	return nil
}

func (c *logCapture) find(msg, reason string, after time.Time) (capturedRecord, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.recs {
		if r.msg == msg && r.attrs["reason"] == reason && !r.at.Before(after) {
			return r, true
		}
	}
	return capturedRecord{}, false
}

func (c *logCapture) waitFor(t *testing.T, msg, reason string, after time.Time) capturedRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r, ok := c.find(msg, reason, after); ok {
			return r
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no log %q reason=%q after %v", msg, reason, after)
	return capturedRecord{}
}

// last returns the most recent record with msg regardless of reason.
func (c *logCapture) last(msg string) (capturedRecord, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.recs) - 1; i >= 0; i-- {
		if c.recs[i].msg == msg {
			return c.recs[i], true
		}
	}
	return capturedRecord{}, false
}

// checkClosed asserts that a provider stream still open at cancelAt was
// closed with reason=cancelled within 200 ms; a stream that had already
// completed before the cancel has nothing to prove.
func checkClosed(t *testing.T, capture *logCapture, name, msg string, cancelAt time.Time, mustBeOpen bool) {
	t.Helper()
	time.Sleep(250 * time.Millisecond) // let late records land
	r, ok := capture.last(msg)
	if !ok {
		t.Fatalf("%s: no %q record at all", name, msg)
	}
	if r.at.Before(cancelAt) {
		if mustBeOpen {
			t.Fatalf("%s stream had already closed (%s) %v before the cancel", name, r.attrs["reason"], cancelAt.Sub(r.at))
		}
		t.Logf("INTERRUPT  %s stream had already %s %v before the cancel (nothing to cut)", name, r.attrs["reason"], cancelAt.Sub(r.at).Round(time.Millisecond))
		return
	}
	d := r.at.Sub(cancelAt)
	t.Logf("INTERRUPT  %s stream closed reason=%s %v after cancel (lifetime %s ms)", name, r.attrs["reason"], d.Round(time.Millisecond), r.attrs["lifetime_ms"])
	if r.attrs["reason"] != "cancelled" || d > 200*time.Millisecond {
		t.Errorf("%s stream: reason=%s closed %v after cancel; want cancelled within 200ms", name, r.attrs["reason"], d)
	}
}

// synthesize turns text into 24 kHz PCM with the real OpenAI TTS so the
// input side of the pipeline gets genuine speech.
func synthesize(t *testing.T, key, text string) []byte {
	t.Helper()
	tts := openai.NewTTS(key, openai.TTSOptions{})
	s, err := tts.Synthesize(context.Background(), provider.TTSConfig{Voice: "alloy", Speed: 1})
	if err != nil {
		t.Fatal(err)
	}
	s.WriteText(text)
	s.EndInput()
	var pcm []byte
	for {
		c, err := s.ReadAudio()
		if errors.Is(err, io.EOF) {
			return pcm
		}
		if err != nil {
			t.Fatalf("synthesize: %v", err)
		}
		pcm = append(pcm, c.PCM...)
	}
}

func TestE2EVoiceTurnAndInterrupt(t *testing.T) {
	dgKey, oaKey := os.Getenv("DEEPGRAM_API_KEY"), os.Getenv("OPENAI_API_KEY")
	if dgKey == "" || oaKey == "" {
		t.Skip("DEEPGRAM_API_KEY / OPENAI_API_KEY not set")
	}
	capture := &logCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(capture))
	defer slog.SetDefault(prev)

	cfg := config.DefaultConfig()
	cfg.Auth.APIKey = apiKey
	cfg.Limits.ASRFinalTimeout = config.Duration(5 * time.Second)
	if err := cfg.Validate(provider.Known); err != nil {
		t.Fatal(err)
	}
	llmModel := os.Getenv("CASCADE_E2E_LLM_MODEL")
	if llmModel == "" {
		llmModel = "gpt-4o-mini"
	}
	t.Logf("llm model: %s (override with CASCADE_E2E_LLM_MODEL)", llmModel)
	ttsModel := os.Getenv("CASCADE_E2E_TTS_MODEL")
	if ttsModel == "" {
		ttsModel = "tts-1"
	}
	prof := config.DefaultProfile()
	prof.Name, prof.ASR, prof.LLM, prof.TTS = "e2e", "deepgram-asr", "openai-llm", "openai-tts"
	prof.Instructions = "You are a concise voice assistant. Answer in one short sentence."
	prof.TurnDetection.ApplyDefaults() // DefaultProfile leaves tuning fields to the decoder
	// Synthesized speech pauses between sentences; a wider silence window
	// keeps one utterance in one turn (OpenAI's server VAD would split too).
	sil := 800
	prof.TurnDetection.SilenceDurationMs = &sil

	rt := admin.Runtime{
		Profile: prof.Name, ASRName: prof.ASR, LLMName: prof.LLM, TTSName: prof.TTS,
		Session: prof.SessionDefaults(), Temperature: prof.Temperature,
	}
	asr, err := provider.New(provider.KindASR, "deepgram", dgKey, json.RawMessage(`{"model":"nova-3","smart_format":true}`))
	if err != nil {
		t.Fatal(err)
	}
	llm, err := provider.New(provider.KindLLM, "openai", oaKey, json.RawMessage(fmt.Sprintf(`{"model":%q}`, llmModel)))
	if err != nil {
		t.Fatal(err)
	}
	tts, err := provider.New(provider.KindTTS, "openai", oaKey, json.RawMessage(fmt.Sprintf(`{"model":%q}`, ttsModel)))
	if err != nil {
		t.Fatal(err)
	}
	rt.ASR, rt.LLM, rt.TTS = asr.(provider.ASR), llm.(provider.LLM), tts.(provider.TTS)
	// In-memory telemetry so the run can be profiled from the Phase 5 metrics.
	spanExp := tracetest.NewInMemoryExporter()
	reader := sdkmetric.NewManualReader()
	tel := observability.New(sdktrace.NewTracerProvider(sdktrace.WithSyncer(spanExp)), sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	srv := New(Options{Config: cfg, Resolver: stubResolver{rt}, Logger: slog.New(capture), Telemetry: tel})
	f := &fixture{t: t, srv: srv, http: nil}
	f.http = newHTTPServer(srv)
	defer f.http.Close()
	defer srv.Shutdown(context.Background())

	c := f.connect()
	c.timeout = 30 * time.Second // real providers: reasoning models can stall for seconds
	c.readUntil("conversation.created")

	hello := synthesize(t, oaKey, "Hello, can you hear me, yes or no?")
	t.Logf("input speech: %d ms", audio.BytesToMs(len(hello)))

	type stamp struct {
		typ string
		at  time.Time
	}
	var stamps []stamp
	sendAudio := func(pcm []byte) {
		frame := audio.MsToBytes(20)
		for off := 0; off < len(pcm); off += frame {
			end := min(off+frame, len(pcm))
			c.send(fmt.Sprintf(`{"type":"input_audio_buffer.append","audio":%q}`, base64.StdEncoding.EncodeToString(pcm[off:end])))
		}
	}
	silence := func(ms int) []byte { return make([]byte, audio.MsToBytes(ms)) }

	// ---- Turn 1: a full voice round trip -----------------------------------
	sendAudio(silence(300))
	sendAudio(hello)
	sendAudio(silence(1500))
	var transcript, reply string
	audioBytes := 0
	var committedAt, transcriptAt, createdAt, firstAudioAt time.Time
	for {
		fr, err := c.read()
		if err != nil {
			t.Fatal(err)
		}
		typ := fr["type"].(string)
		stamps = append(stamps, stamp{typ, time.Now()})
		switch typ {
		case "error":
			t.Fatalf("error: %v", fr["error"])
		case "input_audio_buffer.committed":
			committedAt = time.Now()
		case "conversation.item.input_audio_transcription.completed":
			transcript = fr["transcript"].(string)
			transcriptAt = time.Now()
		case "response.created":
			createdAt = time.Now()
		case "response.output_audio.delta":
			if firstAudioAt.IsZero() {
				firstAudioAt = time.Now()
			}
			b, _ := base64.StdEncoding.DecodeString(fr["delta"].(string))
			audioBytes += len(b)
		case "response.output_audio_transcript.done":
			reply = fr["transcript"].(string)
		case "response.done":
			r := fr["response"].(map[string]any)
			if r["status"] == "cancelled" {
				// A pause inside the utterance split the turn; the next
				// speech segment interrupted the awaiting response.
				t.Logf("TURN 1  intermediate response cancelled (%v); continuing", r["status_details"])
				audioBytes, reply, firstAudioAt = 0, "", time.Time{}
				continue
			}
			if r["status"] != "completed" {
				t.Fatalf("turn 1 response.done = %v", r)
			}
			goto turn1done
		}
	}
turn1done:
	if transcript == "" || reply == "" || audioBytes == 0 {
		t.Fatalf("turn 1 incomplete: transcript=%q reply=%q audio=%d", transcript, reply, audioBytes)
	}
	t.Logf("TURN 1  transcript=%q", transcript)
	t.Logf("TURN 1  reply=%q  audio=%d ms", reply, audio.BytesToMs(audioBytes))
	t.Logf("TURN 1  commit→transcript %v | response.created→first audio %v | committed→first audio %v",
		transcriptAt.Sub(committedAt).Round(time.Millisecond), firstAudioAt.Sub(createdAt).Round(time.Millisecond), firstAudioAt.Sub(committedAt).Round(time.Millisecond))
	if r, ok := capture.find("transcript final", "", time.Time{}); ok {
		t.Logf("TURN 1  session commit_to_final_ms=%s", r.attrs["commit_to_final_ms"])
	}
	profileTurn(t, reader, spanExp, committedAt)
	capture.dump(t, "deepgram finalize satisfied")
	capture.dump(t, "deepgram finalize timed out")

	// ---- Profile turn: a three-sentence answer, streamed end to end ----------
	{
		ask := synthesize(t, oaKey, "Describe the ocean in exactly three short sentences.")
		sendAudio(silence(300))
		sendAudio(ask)
		sendAudio(silence(1500))
		var commit, firstText, firstAudio, lastAudio, doneAt time.Time
		textDeltas, audioDeltas := 0, 0
		var textAt []time.Duration
		for {
			fr, err := c.read()
			if err != nil {
				t.Fatal(err)
			}
			typ := fr["type"].(string)
			switch typ {
			case "error":
				t.Fatalf("profile turn error: %v", fr["error"])
			case "input_audio_buffer.committed":
				commit = time.Now()
			case "response.output_audio_transcript.delta":
				textDeltas++
				if firstText.IsZero() {
					firstText = time.Now()
				}
				textAt = append(textAt, time.Since(commit))
			case "response.output_audio.delta":
				audioDeltas++
				if firstAudio.IsZero() {
					firstAudio = time.Now()
				}
				lastAudio = time.Now()
			case "response.output_audio_transcript.done":
				t.Logf("PROFILE2 reply=%q", fr["transcript"])
			case "response.done":
				doneAt = time.Now()
				r := fr["response"].(map[string]any)
				if r["status"] == "cancelled" {
					t.Logf("PROFILE2 intermediate cancelled; continuing")
					continue
				}
				goto profiled
			}
		}
	profiled:
		off := func(at time.Time) time.Duration { return at.Sub(commit).Round(time.Millisecond) }
		t.Logf("PROFILE2 commit→first text %v | first text→first audio %v | first audio→last audio %v | last audio→done %v | text deltas %d (last at %v) | audio deltas %d",
			off(firstText), firstAudio.Sub(firstText).Round(time.Millisecond), lastAudio.Sub(firstAudio).Round(time.Millisecond), doneAt.Sub(lastAudio).Round(time.Millisecond),
			textDeltas, textAt[len(textAt)-1].Round(time.Millisecond), audioDeltas)
		capture.dump(t, "tts sentence")
		time.Sleep(300 * time.Millisecond)
	}

	// ---- Turns 2 and 3: interrupt mid-response --------------------------------
	// Turn 2 cancels at the first audio delta (TTS in flight, the LLM may
	// already be done); turn 3 cancels at the first transcript delta (the
	// LLM is certainly streaming, TTS may not have started).
	for turn, cancelOn := range map[int]string{2: "response.output_audio.delta", 3: "response.output_audio_transcript.delta"} {
		story := synthesize(t, oaKey, "Please tell me a long story about the ocean, with at least ten sentences.")
		sendAudio(silence(300))
		sendAudio(story)
		sendAudio(silence(1500))
		var cancelAt time.Time
		for {
			fr, err := c.read()
			if err != nil {
				t.Fatal(err)
			}
			typ := fr["type"].(string)
			if typ == "error" {
				t.Fatalf("turn %d error: %v", turn, fr["error"])
			}
			if typ == cancelOn && cancelAt.IsZero() {
				cancelAt = time.Now()
				c.send(`{"type":"response.cancel"}`)
			}
			if typ == "response.done" && !cancelAt.IsZero() {
				r := fr["response"].(map[string]any)
				if r["status"] != "cancelled" {
					t.Fatalf("turn %d response.done = %v", turn, r)
				}
				t.Logf("TURN %d  cancelled at first %s; response.done{cancelled} %v after response.cancel", turn, cancelOn, time.Since(cancelAt).Round(time.Millisecond))
				break
			}
		}
		checkClosed(t, capture, "llm", "openai chat stream closed", cancelAt, turn == 3)
		checkClosed(t, capture, "tts", "openai speech stream closed", cancelAt, turn == 2)
		// Drain any leftover turn events before the next utterance.
		time.Sleep(300 * time.Millisecond)
	}

	// ---- Disconnect: the ASR stream must go down promptly ------------------
	closeAt := time.Now()
	c.conn.Close(websocket.StatusNormalClosure, "")
	dg := capture.waitFor(t, "deepgram stream closed", "cancelled", closeAt)
	t.Logf("DISCONNECT deepgram stream closed %v after client close", dg.at.Sub(closeAt).Round(time.Millisecond))
	if d := dg.at.Sub(closeAt); d > 200*time.Millisecond {
		t.Errorf("deepgram stream closed %v after disconnect; limit 200ms", d)
	}
	var types []string
	for _, s := range stamps {
		types = append(types, s.typ)
	}
	t.Logf("TURN 1 events: %s", strings.Join(types, " → "))
}

// profileTurn prints a latency breakdown of the first response from the
// Phase 5 instruments: histogram values plus span start offsets.
func profileTurn(t *testing.T, reader *sdkmetric.ManualReader, spans *tracetest.InMemoryExporter, committedAt time.Time) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	hist := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if h, ok := m.Data.(metricdata.Histogram[float64]); ok {
				for _, dp := range h.DataPoints {
					if dp.Count > 0 {
						hist[m.Name] = dp.Sum / float64(dp.Count)
					}
				}
			}
		}
	}
	var resp, llm, tts *tracetest.SpanStub
	for i := range spans.GetSpans() {
		sp := spans.GetSpans()[i]
		switch sp.Name {
		case "response":
			if resp == nil {
				resp = &sp
			}
		case "llm":
			if llm == nil {
				llm = &sp
			}
		case "tts":
			if tts == nil {
				tts = &sp
			}
		}
	}
	if resp == nil || llm == nil || tts == nil {
		t.Logf("PROFILE  spans incomplete (response=%v llm=%v tts=%v)", resp != nil, llm != nil, tts != nil)
		return
	}
	off := func(at time.Time) string { return at.Sub(committedAt).Round(time.Millisecond).String() }
	t.Logf("PROFILE  t=0 commit | response.created %s | ASR final %s (asr.commit_to_final) | pipeline start %s | first text %s (llm.ttft %.0fms) | tts start %s | first audio %s (tts.first_audio %.0fms) | e2e %.0fms",
		off(resp.StartTime), off(committedAt.Add(time.Duration(hist["cascade.asr.commit_to_final_ms"]*float64(time.Millisecond)))),
		off(llm.StartTime), off(llm.StartTime.Add(time.Duration(hist["cascade.llm.ttft_ms"]*float64(time.Millisecond)))), hist["cascade.llm.ttft_ms"],
		off(tts.StartTime), off(tts.StartTime.Add(time.Duration(hist["cascade.tts.first_audio_ms"]*float64(time.Millisecond)))), hist["cascade.tts.first_audio_ms"], hist["cascade.e2e_ms"])
	t.Logf("PROFILE  breakdown: awaiting ASR final %v | LLM TTFT %v | first sentence complete after %v more | TTS first audio %v | llm span %v | tts span %v",
		llm.StartTime.Sub(resp.StartTime).Round(time.Millisecond),
		time.Duration(hist["cascade.llm.ttft_ms"]*float64(time.Millisecond)).Round(time.Millisecond),
		(tts.StartTime.Sub(llm.StartTime) - time.Duration(hist["cascade.llm.ttft_ms"]*float64(time.Millisecond))).Round(time.Millisecond),
		time.Duration(hist["cascade.tts.first_audio_ms"]*float64(time.Millisecond)).Round(time.Millisecond),
		llm.EndTime.Sub(llm.StartTime).Round(time.Millisecond), tts.EndTime.Sub(tts.StartTime).Round(time.Millisecond))
	t.Logf("PROFILE  asr: speech stop→final %.0fms, first partial after speech start %.0fms", hist["cascade.asr.final_transcript_ms"], hist["cascade.asr.first_transcript_ms"])
}

func (c *logCapture) dump(t *testing.T, msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.recs {
		if r.msg == msg {
			t.Logf("PROFILE  %s %v", msg, r.attrs)
		}
	}
}
