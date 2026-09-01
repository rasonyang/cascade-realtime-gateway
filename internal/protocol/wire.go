package protocol

import (
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/session"
)

// Wire-level constants fixed by the GA profile.
const (
	objectSession      = "realtime.session"
	objectConversation = "realtime.conversation"
	objectItem         = "realtime.item"
	objectResponse     = "realtime.response"
	sessionTypeGA      = "realtime"
	defaultModel       = "cascade"
	eventIDMaxLen      = 512
	metadataMaxPairs   = 16
	metadataKeyMaxLen  = 64
	metadataValMaxLen  = 512
)

type base struct {
	EventID string `json:"event_id"`
	Type    string `json:"type"`
}

// ---- session ---------------------------------------------------------------

type wireFormat struct {
	Type string `json:"type"`
	Rate int    `json:"rate"`
}

type wireTranscription struct {
	Model    string `json:"model,omitempty"`
	Language string `json:"language,omitempty"`
	Prompt   string `json:"prompt,omitempty"`
}

type wireServerVAD struct {
	Type              string  `json:"type"`
	Threshold         float64 `json:"threshold"`
	PrefixPaddingMs   int     `json:"prefix_padding_ms"`
	SilenceDurationMs int     `json:"silence_duration_ms"`
	CreateResponse    bool    `json:"create_response"`
	InterruptResponse bool    `json:"interrupt_response"`
	IdleTimeoutMs     any     `json:"idle_timeout_ms"`
}

type wireSemanticVAD struct {
	Type              string `json:"type"`
	Eagerness         string `json:"eagerness"`
	CreateResponse    bool   `json:"create_response"`
	InterruptResponse bool   `json:"interrupt_response"`
}

type wireAudioInput struct {
	Format         wireFormat `json:"format"`
	Transcription  any        `json:"transcription"`
	TurnDetection  any        `json:"turn_detection"`
	NoiseReduction any        `json:"noise_reduction"`
}

type wireAudioOutput struct {
	Format wireFormat `json:"format"`
	Voice  string     `json:"voice"`
	Speed  float64    `json:"speed"`
}

type wireAudio struct {
	Input  wireAudioInput  `json:"input"`
	Output wireAudioOutput `json:"output"`
}

type wireSession struct {
	ID               string                 `json:"id"`
	Object           string                 `json:"object"`
	Type             string                 `json:"type"`
	Model            string                 `json:"model"`
	Instructions     string                 `json:"instructions"`
	OutputModalities []string               `json:"output_modalities"`
	Audio            wireAudio              `json:"audio"`
	MaxOutputTokens  config.MaxOutputTokens `json:"max_output_tokens"`
	Tools            []any                  `json:"tools"`
	ToolChoice       string                 `json:"tool_choice"`
	Truncation       string                 `json:"truncation"`
	Tracing          any                    `json:"tracing"`
	Prompt           any                    `json:"prompt"`
}

func pcmFormat() wireFormat {
	return wireFormat{Type: config.AudioFormatPCM, Rate: config.AudioSampleRate}
}

func sessionObject(id, model string, cfg config.SessionDefaults) wireSession {
	ws := wireSession{
		ID: id, Object: objectSession, Type: sessionTypeGA, Model: model,
		Instructions:     cfg.Instructions,
		OutputModalities: append([]string{}, cfg.OutputModalities...),
		Audio: wireAudio{
			Input:  wireAudioInput{Format: pcmFormat()},
			Output: wireAudioOutput{Format: pcmFormat(), Voice: cfg.Audio.Output.Voice, Speed: cfg.Audio.Output.Speed},
		},
		MaxOutputTokens: cfg.MaxOutputTokens,
		Tools:           []any{},
		ToolChoice:      "auto",
		Truncation:      "auto",
	}
	if tr := cfg.Audio.Input.Transcription; tr != nil {
		ws.Audio.Input.Transcription = wireTranscription{Model: tr.Model, Language: tr.Language, Prompt: tr.Prompt}
	}
	ws.Audio.Input.TurnDetection = turnDetectionObject(cfg.Audio.Input.TurnDetection)
	return ws
}

func turnDetectionObject(td *config.TurnDetection) any {
	if td == nil {
		return nil
	}
	switch td.Type {
	case config.TurnDetectionServerVAD:
		return wireServerVAD{
			Type: td.Type, Threshold: deref(td.Threshold, config.DefaultVADThreshold),
			PrefixPaddingMs:   deref(td.PrefixPaddingMs, config.DefaultVADPrefixPaddingMs),
			SilenceDurationMs: deref(td.SilenceDurationMs, config.DefaultVADSilenceDurationMs),
			CreateResponse:    td.CreateResponse, InterruptResponse: td.InterruptResponse,
		}
	case config.TurnDetectionSemanticVAD:
		return wireSemanticVAD{
			Type: td.Type, Eagerness: deref(td.Eagerness, "auto"),
			CreateResponse: td.CreateResponse, InterruptResponse: td.InterruptResponse,
		}
	}
	return nil
}

func deref[T any](p *T, def T) T {
	if p == nil {
		return def
	}
	return *p
}

type sessionEvent struct {
	base
	Session wireSession `json:"session"`
}

type conversationCreatedEvent struct {
	base
	Conversation struct {
		ID     string `json:"id"`
		Object string `json:"object"`
	} `json:"conversation"`
}

// ---- errors ----------------------------------------------------------------

type wireError struct {
	Type    string  `json:"type"`
	Code    string  `json:"code"`
	Message string  `json:"message"`
	Param   *string `json:"param"`
	EventID *string `json:"event_id"`
}

type errorEvent struct {
	base
	Error wireError `json:"error"`
}

// ---- items -----------------------------------------------------------------

type wireTextPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type wireInputAudioPart struct {
	Type       string  `json:"type"`
	Transcript *string `json:"transcript"`
}

type wireOutputAudioPart struct {
	Type       string `json:"type"`
	Transcript string `json:"transcript"`
}

type wireItem struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Type    string `json:"type"`
	Role    string `json:"role"`
	Status  string `json:"status"`
	Content []any  `json:"content"`
}

func itemObject(id string, it session.Item) wireItem {
	w := wireItem{ID: id, Object: objectItem, Type: "message", Role: string(it.Role), Status: string(it.Status)}
	switch {
	case it.Role == session.RoleAssistant && it.Content == session.ContentAudio:
		w.Content = []any{wireOutputAudioPart{Type: "output_audio", Transcript: it.Text}}
	case it.Role == session.RoleAssistant:
		w.Content = []any{wireTextPart{Type: "output_text", Text: it.Text}}
	case it.Content == session.ContentAudio:
		var tr *string
		if it.TranscriptDone {
			t := it.Text
			tr = &t
		}
		w.Content = []any{wireInputAudioPart{Type: "input_audio", Transcript: tr}}
	default:
		w.Content = []any{wireTextPart{Type: "input_text", Text: it.Text}}
	}
	return w
}

type itemEvent struct {
	base
	PreviousItemID *string  `json:"previous_item_id"`
	Item           wireItem `json:"item"`
}

type itemIDEvent struct {
	base
	ItemID string `json:"item_id"`
}

type itemTruncatedEvent struct {
	base
	ItemID       string `json:"item_id"`
	ContentIndex int    `json:"content_index"`
	AudioEndMs   int    `json:"audio_end_ms"`
}

type committedEvent struct {
	base
	PreviousItemID *string `json:"previous_item_id"`
	ItemID         string  `json:"item_id"`
}

type speechStartedEvent struct {
	base
	AudioStartMs int    `json:"audio_start_ms"`
	ItemID       string `json:"item_id"`
}

type speechStoppedEvent struct {
	base
	AudioEndMs int    `json:"audio_end_ms"`
	ItemID     string `json:"item_id"`
}

type transcriptDeltaEvent struct {
	base
	ItemID       string `json:"item_id"`
	ContentIndex int    `json:"content_index"`
	Delta        string `json:"delta"`
}

type transcriptUsage struct {
	Type    string  `json:"type"`
	Seconds float64 `json:"seconds"`
}

type transcriptCompletedEvent struct {
	base
	ItemID       string          `json:"item_id"`
	ContentIndex int             `json:"content_index"`
	Transcript   string          `json:"transcript"`
	Usage        transcriptUsage `json:"usage"`
}

// ---- responses -------------------------------------------------------------

type wireStatusDetails struct {
	Type   string         `json:"type"`
	Reason string         `json:"reason,omitempty"`
	Error  *wireStatusErr `json:"error,omitempty"`
}

type wireStatusErr struct {
	Type string `json:"type"`
	Code string `json:"code"`
}

type wireUsage struct {
	TotalTokens       int `json:"total_tokens"`
	InputTokens       int `json:"input_tokens"`
	OutputTokens      int `json:"output_tokens"`
	InputTokenDetails struct {
		TextTokens   int `json:"text_tokens"`
		AudioTokens  int `json:"audio_tokens"`
		CachedTokens int `json:"cached_tokens"`
	} `json:"input_token_details"`
	OutputTokenDetails struct {
		TextTokens  int `json:"text_tokens"`
		AudioTokens int `json:"audio_tokens"`
	} `json:"output_token_details"`
}

type wireResponseAudio struct {
	Output struct {
		Format wireFormat `json:"format"`
		Voice  string     `json:"voice"`
	} `json:"output"`
}

type wireResponse struct {
	ID               string                 `json:"id"`
	Object           string                 `json:"object"`
	Status           string                 `json:"status"`
	StatusDetails    *wireStatusDetails     `json:"status_details"`
	Output           []wireItem             `json:"output"`
	Usage            *wireUsage             `json:"usage"`
	ConversationID   string                 `json:"conversation_id"`
	OutputModalities []string               `json:"output_modalities"`
	Audio            wireResponseAudio      `json:"audio"`
	MaxOutputTokens  config.MaxOutputTokens `json:"max_output_tokens"`
	Metadata         map[string]string      `json:"metadata"`
}

type responseEvent struct {
	base
	Response wireResponse `json:"response"`
}

type outputItemEvent struct {
	base
	ResponseID  string   `json:"response_id"`
	OutputIndex int      `json:"output_index"`
	Item        wireItem `json:"item"`
}

type wireContentPart struct {
	Type       string  `json:"type"`
	Text       *string `json:"text,omitempty"`
	Transcript *string `json:"transcript,omitempty"`
}

type contentPartEvent struct {
	base
	ResponseID   string          `json:"response_id"`
	ItemID       string          `json:"item_id"`
	OutputIndex  int             `json:"output_index"`
	ContentIndex int             `json:"content_index"`
	Part         wireContentPart `json:"part"`
}

type outputDeltaEvent struct {
	base
	ResponseID   string `json:"response_id"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Delta        string `json:"delta"`
}

type outputDoneEvent struct {
	base
	ResponseID   string  `json:"response_id"`
	ItemID       string  `json:"item_id"`
	OutputIndex  int     `json:"output_index"`
	ContentIndex int     `json:"content_index"`
	Text         *string `json:"text,omitempty"`
	Transcript   *string `json:"transcript,omitempty"`
}
