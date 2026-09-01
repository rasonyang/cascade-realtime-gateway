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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/admin"
	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"

	_ "github.com/rasonyang/cascade-realtime-gateway/internal/provider/qwen"
)

// TestE2EQwenAdminProfile drives the whole Phase 6 path against the live
// Alibaba Cloud Model Studio endpoints: an empty Admin store refuses realtime
// connections, the runtime configuration is created entirely through
// /admin/v1 (with the API key left as an {env.NAME} placeholder), a real
// voice turn runs on the resolved profile, a client cancel tears the provider
// streams down, an Admin write is invisible to the live session but takes
// effect on the next one, and a restart from the state file serves again.
//
//	ALIYUN_API_KEY=… go test -tags e2e -run TestE2EQwen -v ./internal/server
func TestE2EQwenAdminProfile(t *testing.T) {
	if os.Getenv("ALIYUN_API_KEY") == "" {
		t.Skip("ALIYUN_API_KEY not set")
	}
	capture := &logCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(capture))
	defer slog.SetDefault(prev)

	statePath := filepath.Join(t.TempDir(), "state.json")
	store, err := admin.Open(admin.Options{StateFile: statePath, Known: provider.Known, Logger: slog.New(capture)})
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	cfg.Auth.APIKey, cfg.Auth.AdminAPIKey = apiKey, adminKey
	cfg.Limits.ASRFinalTimeout = config.Duration(5 * time.Second)
	if err := cfg.Validate(provider.Known); err != nil {
		t.Fatal(err)
	}
	srv := New(Options{Config: cfg, Resolver: store, Logger: slog.New(capture)})
	f := &fixture{t: t, srv: srv}
	f.http = newHTTPServer(srv)
	defer f.http.Close()
	defer srv.Shutdown(context.Background())

	adminAPI := store.Handler(adminKey)
	admWrite := func(method, path, body string) (int, []byte) {
		t.Helper()
		var r io.Reader
		if body != "" {
			r = strings.NewReader(body)
		}
		req := httptest.NewRequest(method, admin.BasePath+path, r)
		req.Header.Set("Authorization", "Bearer "+adminKey)
		w := httptest.NewRecorder()
		adminAPI.ServeHTTP(w, req)
		return w.Code, w.Body.Bytes()
	}

	// ---- 1. Nothing configured: realtime is refused -------------------------
	if _, resp, err := f.dial("", apiKey); err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("dial with no default profile: err=%v resp=%v", err, resp)
	}
	t.Log("STEP 1  no default profile → HTTP 503")

	// ---- 2. Configure the runtime through /admin/v1 -------------------------
	// The secret stays an {env.NAME} placeholder: it is resolved when the
	// provider is built, and the state file must keep it verbatim.
	for _, p := range []struct{ name, role string }{{"qwen-asr", "asr"}, {"qwen-llm", "llm"}, {"qwen-tts", "tts"}} {
		body := fmt.Sprintf(`{"name":%q,"type":"qwen","api_key":"{env.ALIYUN_API_KEY}"}`, p.name)
		if code, b := admWrite("PUT", "/providers/"+p.name, body); code != http.StatusOK {
			t.Fatalf("PUT provider %s = %d %s", p.name, code, b)
		}
	}
	// Synthesized speech pauses between sentences; a wider silence window
	// keeps one utterance in one turn.
	profileBody := `{"name":"voice","asr":"qwen-asr","llm":"qwen-llm","tts":"qwen-tts",
	  "instructions":"You are a concise voice assistant. Answer in one short sentence.",
	  "voice":"longanlingxi",
	  "asr_language":"en",
	  "turn_detection":{"type":"server_vad","silence_duration_ms":800}}`
	if code, b := admWrite("PUT", "/profiles/voice", profileBody); code != http.StatusOK {
		t.Fatalf("PUT profile = %d %s", code, b)
	}
	if code, b := admWrite("PUT", "/settings", `{"default_profile":"voice"}`); code != http.StatusOK {
		t.Fatalf("PUT settings = %d %s", code, b)
	}
	raw, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "{env.ALIYUN_API_KEY}") || strings.Contains(string(raw), os.Getenv("ALIYUN_API_KEY")) {
		t.Fatal("the state file must keep the placeholder, never the resolved secret")
	}
	if code, b := admWrite("GET", "/config", ""); code != http.StatusOK || strings.Contains(string(b), "{env.") {
		t.Fatalf("GET config = %d (secret must read as %q): %s", code, admin.SecretMask, b)
	}
	t.Log("STEP 2  providers + profile + default_profile created through /admin/v1; secret stored verbatim, read masked")

	// ---- 3. One real voice turn on the resolved profile ---------------------
	hello := synthesizeQwen(t, "What is the largest ocean on Earth?")
	t.Logf("input speech: %d ms", audio.BytesToMs(len(hello)))

	c := f.connect()
	c.timeout = 40 * time.Second
	sess := c.readUntil("session.created")
	if got := qwenVoice(t, sess); got != "longanlingxi" {
		t.Fatalf("session voice = %q, want the profile's", got)
	}
	c.readUntil("conversation.created")

	sendPCM(c, silencePCM(300))
	sendPCM(c, hello)
	sendPCM(c, silencePCM(1500))

	var transcript, reply string
	var committedAt, transcriptAt, createdAt, firstAudioAt time.Time
	audioBytes := 0
	for {
		fr, err := c.read()
		if err != nil {
			t.Fatal(err)
		}
		switch fr["type"].(string) {
		case "error":
			t.Fatalf("error: %v", fr["error"])
		case "input_audio_buffer.committed":
			committedAt = time.Now()
		case "conversation.item.input_audio_transcription.completed":
			transcript, transcriptAt = fr["transcript"].(string), time.Now()
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
				t.Logf("TURN 1  intermediate response cancelled (%v); continuing", r["status_details"])
				audioBytes, reply, firstAudioAt = 0, "", time.Time{}
				continue
			}
			if r["status"] != "completed" {
				t.Fatalf("turn 1 response.done = %v", r)
			}
			goto turnDone
		}
	}
turnDone:
	if transcript == "" || reply == "" || audioBytes == 0 {
		t.Fatalf("turn 1 incomplete: transcript=%q reply=%q audio=%d", transcript, reply, audioBytes)
	}
	// asr_language is the Phase 6 knob that reaches provider.ASRConfig; without
	// it this English utterance comes back transcribed into another language.
	if !strings.Contains(strings.ToLower(transcript), "ocean") {
		t.Fatalf("asr_language=en did not reach the ASR stream: transcript=%q", transcript)
	}
	t.Logf("TURN 1  transcript=%q", transcript)
	t.Logf("TURN 1  reply=%q  audio=%d ms", reply, audio.BytesToMs(audioBytes))
	t.Logf("TURN 1  commit→transcript %v | response.created→first audio %v | commit→first audio %v",
		transcriptAt.Sub(committedAt).Round(time.Millisecond),
		firstAudioAt.Sub(createdAt).Round(time.Millisecond),
		firstAudioAt.Sub(committedAt).Round(time.Millisecond))

	// ---- 4. Cancel mid-answer: provider streams must close promptly ---------
	c.send(`{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"Describe the ocean in three sentences."}]}}`)
	c.send(`{"type":"response.create"}`)
	var cancelAt time.Time
	for {
		fr, err := c.read()
		if err != nil {
			t.Fatal(err)
		}
		if fr["type"].(string) == "response.output_audio.delta" {
			cancelAt = time.Now()
			c.send(`{"type":"response.cancel"}`)
			break
		}
	}
	for {
		fr, err := c.read()
		if err != nil {
			t.Fatal(err)
		}
		if fr["type"].(string) == "response.done" {
			r := fr["response"].(map[string]any)
			if r["status"] != "cancelled" {
				t.Fatalf("cancel → response.done = %v", r)
			}
			t.Logf("INTERRUPT  first audio → response.done{cancelled} %v", time.Since(cancelAt).Round(time.Microsecond))
			break
		}
	}
	capture.dump(t, "qwen tts stream closed")
	capture.dump(t, "qwen chat stream closed")

	// ---- 5. An Admin write is invisible to the live session -----------------
	if code, b := admWrite("PUT", "/profiles/voice", strings.Replace(profileBody, `"voice":"longanlingxi"`, `"voice":"longwan"`, 1)); code != http.StatusOK {
		t.Fatalf("PUT profile = %d %s", code, b)
	}
	c.send(`{"type":"session.update","session":{"type":"realtime"}}`)
	if got := qwenVoice(t, c.readUntil("session.updated")); got != "longanlingxi" {
		t.Fatalf("live session voice changed to %q", got)
	}
	fresh := f.connect()
	fresh.timeout = 20 * time.Second
	if got := qwenVoice(t, fresh.readUntil("session.created")); got != "longwan" {
		t.Fatalf("new session voice = %q, want the updated profile", got)
	}
	t.Log("STEP 5  live session kept its snapshot; the next connection picked up the Admin write")

	// ---- 6. Restart from the state file --------------------------------------
	reopened, err := admin.Open(admin.Options{StateFile: statePath, Known: provider.Known, Logger: slog.New(capture)})
	if err != nil {
		t.Fatalf("reopen from the state file: %v", err)
	}
	rt, err := reopened.Resolve()
	if err != nil {
		t.Fatalf("resolve after restart: %v", err)
	}
	if rt.Profile != "voice" || rt.Session.Audio.Output.Voice != "longwan" || rt.ASRName != "qwen-asr" {
		t.Fatalf("resolved runtime after restart: %+v", rt)
	}
	if rt.Temperature != nil {
		t.Fatalf("temperature must stay unset unless a profile asks: %g", *rt.Temperature)
	}
	t.Log("STEP 6  restart from the state file resolves the same profile and rebuilds the providers")
}

// synthesizeQwen speaks text with the real Qwen TTS so the ASR has genuine
// 24 kHz audio to transcribe.
func synthesizeQwen(t *testing.T, text string) []byte {
	t.Helper()
	p, err := provider.New(provider.KindTTS, "qwen", os.Getenv("ALIYUN_API_KEY"), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	s, err := p.(provider.TTS).Synthesize(context.Background(), provider.TTSConfig{SampleRate: audio.SampleRate, Speed: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.WriteText(text); err != nil {
		t.Fatal(err)
	}
	if err := s.EndInput(); err != nil {
		t.Fatal(err)
	}
	var pcm []byte
	for {
		chunk, err := s.ReadAudio()
		if errors.Is(err, io.EOF) {
			return pcm
		}
		if err != nil {
			t.Fatalf("synthesize: %v", err)
		}
		pcm = append(pcm, chunk.PCM...)
	}
}

func sendPCM(c *client, pcm []byte) {
	frame := audio.MsToBytes(20)
	for off := 0; off < len(pcm); off += frame {
		end := min(off+frame, len(pcm))
		c.send(fmt.Sprintf(`{"type":"input_audio_buffer.append","audio":%q}`, base64.StdEncoding.EncodeToString(pcm[off:end])))
	}
}

func silencePCM(ms int) []byte { return make([]byte, audio.MsToBytes(ms)) }

func qwenVoice(t *testing.T, ev map[string]any) string {
	t.Helper()
	v, _ := ev["session"].(map[string]any)["audio"].(map[string]any)["output"].(map[string]any)["voice"].(string)
	return v
}
