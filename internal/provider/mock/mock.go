// Package mock provides deterministic, scriptable ASR, LLM and TTS providers
// for session and protocol tests. Thresholds are expressed in audio
// milliseconds or token counts, never wall-clock time, so tests are
// reproducible; optional delays exist only to exercise timeouts.
//
// Layering: mock imports provider and audio only.
package mock

import (
	"encoding/json"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// Name is the registry name of all three mocks.
const Name = "mock"

// Internal queue sizes; the mocks are test doubles, not tunables.
const (
	asrInputQueue   = 256
	asrEventQueue   = 16
	llmChunkQueue   = 16
	ttsSegmentQueue = 64
)

func init() {
	provider.Register(provider.KindASR, Name, func(_ string, raw json.RawMessage) (provider.Provider, error) {
		var s ASRScript
		if err := parse(raw, &s); err != nil {
			return nil, err
		}
		return NewASR(s), nil
	})
	provider.Register(provider.KindLLM, Name, func(_ string, raw json.RawMessage) (provider.Provider, error) {
		var s LLMScript
		if err := parse(raw, &s); err != nil {
			return nil, err
		}
		return NewLLM(s), nil
	})
	provider.Register(provider.KindTTS, Name, func(_ string, raw json.RawMessage) (provider.Provider, error) {
		var s TTSScript
		if err := parse(raw, &s); err != nil {
			return nil, err
		}
		return NewTTS(s), nil
	})
}

func parse(raw json.RawMessage, into any) error {
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, into)
}

// Duration is a JSON-friendly time.Duration for script options.
type Duration time.Duration

// UnmarshalJSON accepts a Go duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}
