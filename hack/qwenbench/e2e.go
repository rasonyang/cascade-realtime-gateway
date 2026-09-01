package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider/qwen"
	"github.com/rasonyang/cascade-realtime-gateway/internal/server"
)

const gatewayKey = "bench-key"

// benchE2E drives the real gateway over its own WebSocket protocol with the
// qwen providers behind it, and measures a complete voice turn from the
// client's point of view.
func benchE2E(ctx context.Context, key string, pcm []byte, turns int, ttsOpts qwen.TTSOptions, cap *logCapture) (section, error) {
	cfg := config.DefaultConfig()
	cfg.Auth.APIKey = gatewayKey
	opts := json.RawMessage(`{}`)
	cfg.Providers.ASR = config.Provider{Type: "qwen", APIKey: key, Options: opts}
	cfg.Providers.LLM = config.Provider{Type: "qwen", APIKey: key, Options: opts}
	ttsRaw, err := json.Marshal(ttsOpts)
	if err != nil {
		return section{}, err
	}
	cfg.Providers.TTS = config.Provider{Type: "qwen", APIKey: key, Options: ttsRaw}
	cfg.SessionDefaults.Instructions = "You are a concise voice assistant. Answer in two short sentences."
	cfg.SessionDefaults.Audio.Output.Voice = "longanlingxi"
	cfg.SessionDefaults.Audio.Input.TurnDetection.ApplyDefaults()
	cfg.Limits.ASRFinalTimeout = config.Duration(5 * time.Second)
	if err := cfg.Validate(provider.Known); err != nil {
		return section{}, err
	}
	providers, err := server.Build(cfg)
	if err != nil {
		return section{}, err
	}
	srv := server.New(server.Options{Config: cfg, Providers: providers, Logger: slog.Default()})
	http := httptest.NewServer(srv.Handler())
	defer http.Close()
	defer srv.Shutdown(context.Background())

	c, err := dialGateway(ctx, http.URL)
	if err != nil {
		return section{}, err
	}
	defer c.Close(websocket.StatusNormalClosure, "")

	toASR := &series{Name: "speech end→ASR final"}
	toToken := &series{Name: "speech end→LLM first token"}
	toTTSText := &series{Name: "speech end→first TTS text"}
	toAudio := &series{Name: "speech end→first audio"}
	ttsTextToAudio := &series{Name: "first TTS text→provider audio"}
	providerToClient := &series{Name: "provider audio→client audio"}
	toPlayable := &series{Name: "speech end→first playable audio"}
	commitToPlayable := &series{Name: "commit→first playable audio"}
	speechToCommit := &series{Name: "speech end→commit (VAD hold)"}
	toDone := &series{Name: "speech end→response.done"}

	silenceMs := *cfg.SessionDefaults.Audio.Input.TurnDetection.SilenceDurationMs
	const playableMs = 100

	events := make(chan stampedEvent, 256)
	readErr := make(chan error, 1)
	go func() { readErr <- readLoop(ctx, c, events) }()

	for turn := 0; turn < turns; turn++ {
		mark := cap.mark()
		stop := make(chan struct{})
		endOfSpeech := make(chan time.Time, 1)
		sendErr := make(chan error, 1)
		go func() { sendErr <- pushTurn(ctx, c, pcm, endOfSpeech, stop) }()
		speechEnd := <-endOfSpeech

		var commitAt, asrAt, tokenAt, audioAt, playableAt, doneAt time.Time
		acc := 0
		deadline := time.After(60 * time.Second)
		for {
			var ev stampedEvent
			select {
			case ev = <-events:
			case err := <-readErr:
				close(stop)
				<-sendErr
				return section{}, fmt.Errorf("turn %d: %w", turn, err)
			case <-deadline:
				close(stop)
				<-sendErr
				return section{}, fmt.Errorf("turn %d: no completed response within 60s", turn)
			}
			fr, at := ev.frame, ev.at
			typ, _ := fr["type"].(string)
			switch typ {
			case "error":
				close(stop)
				<-sendErr
				return section{}, fmt.Errorf("turn %d: gateway error: %v", turn, fr["error"])
			case "input_audio_buffer.committed":
				if commitAt.IsZero() {
					commitAt = at
				}
			case "conversation.item.input_audio_transcription.completed":
				if asrAt.IsZero() {
					asrAt = at
				}
			case "response.output_audio_transcript.delta":
				if tokenAt.IsZero() {
					tokenAt = at
				}
			case "response.output_audio.delta":
				b, _ := base64.StdEncoding.DecodeString(fr["delta"].(string))
				if audioAt.IsZero() {
					audioAt = at
				}
				acc += len(b)
				if playableAt.IsZero() && audio.BytesToMs(acc) >= playableMs {
					playableAt = at
				}
			case "response.done":
				resp, _ := fr["response"].(map[string]any)
				if status, _ := resp["status"].(string); status != "completed" {
					// A pause inside the utterance can split the turn; the
					// superseded response is cancelled and the next one is
					// the real answer.
					acc = 0
					audioAt, playableAt, tokenAt = time.Time{}, time.Time{}, time.Time{}
					continue
				}
				doneAt = at
				goto turnDone
			}
		}
	turnDone:
		close(stop)
		if err := <-sendErr; err != nil {
			return section{}, fmt.Errorf("turn %d: sending audio: %w", turn, err)
		}
		if playableAt.IsZero() {
			return section{}, fmt.Errorf("turn %d: the response carried less than %d ms of audio", turn, playableMs)
		}
		speechToCommit.add(commitAt.Sub(speechEnd))
		toASR.add(asrAt.Sub(speechEnd))
		toToken.add(tokenAt.Sub(speechEnd))
		toAudio.add(audioAt.Sub(speechEnd))
		toPlayable.add(playableAt.Sub(speechEnd))
		commitToPlayable.add(playableAt.Sub(commitAt))
		toDone.add(doneAt.Sub(speechEnd))
		// Split the text-to-audio leg into the provider's own work and
		// Cascade's, so the dominant cost is attributable.
		textRec, textOK := cap.after(mark, "qwen tts first text")
		audioRec, audioOK := cap.after(mark, "qwen tts first audio")
		if textOK {
			toTTSText.add(textRec.at.Sub(speechEnd))
		}
		if textOK && audioOK {
			ttsTextToAudio.add(audioRec.at.Sub(textRec.at))
		}
		if audioOK {
			providerToClient.add(audioAt.Sub(audioRec.at))
		}
		progress("e2e", turn+1, turns)
		time.Sleep(500 * time.Millisecond) // let the session settle between turns
	}

	note := fmt.Sprintf("One full voice turn per sample through the gateway, server_vad with silence_duration_ms=%d,\n"+
		"tts flush nudge "+fmt.Sprint(!ttsOpts.NoFlushNudge)+".\n"+
		"\"speech end\" is the last audio frame of the utterance, so every number below the VAD hold\n"+
		"includes it; \"commit→first playable audio\" is the gateway's own work with the hold removed.\n"+
		"The acceptance metric is \"speech end→first playable audio\".", silenceMs)
	return summarizeAll("E2E — full voice turn through Cascade", note,
		speechToCommit, toASR, toToken, toTTSText, ttsTextToAudio, providerToClient,
		toAudio, toPlayable, commitToPlayable, toDone), nil
}

// pushTurn streams the utterance in real time, then keeps streaming silence
// so server VAD sees the turn end and the session stays fed until stop.
func pushTurn(ctx context.Context, c *websocket.Conn, pcm []byte, endOfSpeech chan<- time.Time, stop <-chan struct{}) error {
	frame := audio.MsToBytes(20)
	send := func(b []byte) error {
		msg := fmt.Sprintf(`{"type":"input_audio_buffer.append","audio":%q}`, base64.StdEncoding.EncodeToString(b))
		return c.Write(ctx, websocket.MessageText, []byte(msg))
	}
	silence := make([]byte, frame)
	// A short lead-in of silence gives the VAD a noise floor.
	for i := 0; i < 15; i++ {
		if err := send(silence); err != nil {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
	for off := 0; off < len(pcm); off += frame {
		end := min(off+frame, len(pcm))
		if err := send(pcm[off:end]); err != nil {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
	endOfSpeech <- time.Now()
	for {
		select {
		case <-stop:
			return nil
		default:
		}
		if err := send(silence); err != nil {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// stampedEvent is a gateway event with the moment it reached the client.
type stampedEvent struct {
	frame map[string]any
	at    time.Time
}

// readLoop timestamps every event as it arrives so no measurement depends on
// when the benchmark happens to read.
func readLoop(ctx context.Context, c *websocket.Conn, out chan<- stampedEvent) error {
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			return err
		}
		at := time.Now()
		var fr map[string]any
		if err := json.Unmarshal(data, &fr); err != nil {
			continue
		}
		select {
		case out <- stampedEvent{frame: fr, at: at}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func dialGateway(ctx context.Context, base string) (*websocket.Conn, error) {
	url := "ws" + strings.TrimPrefix(base, "http") + "/v1/realtime"
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + gatewayKey}},
	})
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(32 << 20)
	// Drain the session preamble.
	for {
		fr, err := readEvent(ctx, conn, 10*time.Second)
		if err != nil {
			return nil, err
		}
		if fr["type"] == "conversation.created" {
			return conn, nil
		}
	}
}

func readEvent(ctx context.Context, c *websocket.Conn, timeout time.Duration) (map[string]any, error) {
	if timeout <= 0 {
		return nil, fmt.Errorf("deadline exceeded waiting for an event")
	}
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, data, err := c.Read(rctx)
	if err != nil {
		return nil, err
	}
	var fr map[string]any
	if err := json.Unmarshal(data, &fr); err != nil {
		return nil, err
	}
	return fr, nil
}
