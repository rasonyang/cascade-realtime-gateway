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

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
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
	cfg.Providers.ASR = config.Provider{Type: "deepgram", APIKey: dgKey, Options: json.RawMessage(`{"model":"nova-3","smart_format":true}`)}
	llmModel := os.Getenv("CASCADE_E2E_LLM_MODEL")
	if llmModel == "" {
		llmModel = "gpt-4o-mini"
	}
	t.Logf("llm model: %s (override with CASCADE_E2E_LLM_MODEL)", llmModel)
	cfg.Providers.LLM = config.Provider{Type: "openai", APIKey: oaKey, Options: json.RawMessage(fmt.Sprintf(`{"model":%q}`, llmModel))}
	cfg.Providers.TTS = config.Provider{Type: "openai", APIKey: oaKey, Options: json.RawMessage(`{"model":"tts-1"}`)}
	cfg.SessionDefaults.Instructions = "You are a concise voice assistant. Answer in one short sentence."
	cfg.Limits.ASRFinalTimeout = config.Duration(5 * time.Second)
	cfg.SessionDefaults.Audio.Input.TurnDetection.ApplyDefaults() // DefaultConfig leaves tuning fields to Load
	// Synthesized speech pauses between sentences; a wider silence window
	// keeps one utterance in one turn (OpenAI's server VAD would split too).
	sil := 800
	cfg.SessionDefaults.Audio.Input.TurnDetection.SilenceDurationMs = &sil
	if err := cfg.Validate(provider.Known); err != nil {
		t.Fatal(err)
	}
	providers, err := Build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := New(Options{Config: cfg, Providers: providers, Logger: slog.New(capture)})
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
