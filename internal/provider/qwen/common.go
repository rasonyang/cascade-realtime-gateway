package qwen

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// Name is the registry name of all three providers.
const Name = "qwen"

// DefaultHost is the public Model Studio endpoint Cascade targets by default.
// Both the HTTP and the WebSocket URL are derived from it and either can be
// overridden per provider (`options.base_url` for the LLM, `options.url` for
// ASR and TTS) — which is how a dedicated Model Studio deployment, whose host
// is account-specific, is pointed at.
const DefaultHost = "dashscope.aliyuncs.com"

const (
	defaultLLMBaseURL = "https://" + DefaultHost + "/compatible-mode/v1"
	defaultWSURL      = "wss://" + DefaultHost + "/api-ws/v1/inference"

	defaultConnectTimeout = 10 * time.Second
	defaultIdleTimeout    = 30 * time.Second

	// wsReadLimit bounds a single inbound frame. Synthesized audio arrives
	// as binary frames of a few tens of kilobytes; the limit is generous so
	// a large frame is never mistaken for a protocol error.
	wsReadLimit = 8 << 20
)

// ---- shared WebSocket options ----------------------------------------------

// wsOptions are the fields shared by the ASR and TTS option blocks.
type wsOptions struct {
	URL            string          `json:"url"`
	ConnectTimeout config.Duration `json:"connect_timeout"`
	// IdleTimeout bounds the gap between inbound frames once a task is
	// running; exceeding it tears the socket down.
	IdleTimeout config.Duration `json:"idle_timeout"`
}

func (o *wsOptions) applyDefaults() {
	if o.URL == "" {
		o.URL = defaultWSURL
	}
	if o.ConnectTimeout == 0 {
		o.ConnectTimeout = config.Duration(defaultConnectTimeout)
	}
	if o.IdleTimeout == 0 {
		o.IdleTimeout = config.Duration(defaultIdleTimeout)
	}
}

// dial opens the DashScope duplex WebSocket. The authorization scheme is
// lowercase "bearer", which is what the service expects.
func dial(ctx context.Context, o wsOptions, apiKey string) (*websocket.Conn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, o.ConnectTimeout.Std())
	defer cancel()
	conn, resp, err := websocket.Dial(dialCtx, o.URL, &websocket.DialOptions{
		HTTPHeader: http.Header{
			"Authorization":              []string{"bearer " + apiKey},
			"X-DashScope-DataInspection": []string{"enable"},
		},
	})
	if err != nil {
		if resp != nil {
			return nil, provider.ErrorFromStatus(Name, resp.StatusCode, err.Error())
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &provider.Error{Provider: Name, Kind: provider.ErrTransient, Err: err}
	}
	conn.SetReadLimit(wsReadLimit)
	return conn, nil
}

// ---- DashScope framing -----------------------------------------------------

// Actions Cascade sends.
const (
	actionRunTask      = "run-task"
	actionContinueTask = "continue-task"
	actionFinishTask   = "finish-task"
)

// Events DashScope sends.
const (
	eventTaskStarted     = "task-started"
	eventResultGenerated = "result-generated"
	eventTaskFinished    = "task-finished"
	eventTaskFailed      = "task-failed"
)

// outHeader is the header of every frame Cascade sends. streaming is always
// "duplex": text and audio flow in both directions for the task's lifetime.
type outHeader struct {
	Action    string `json:"action"`
	TaskID    string `json:"task_id"`
	Streaming string `json:"streaming"`
}

func newOutHeader(action, taskID string) outHeader {
	return outHeader{Action: action, TaskID: taskID, Streaming: "duplex"}
}

// runFrame starts a task. P is the task-specific parameter block.
type runFrame[P any] struct {
	Header  outHeader     `json:"header"`
	Payload runPayload[P] `json:"payload"`
}

type runPayload[P any] struct {
	TaskGroup  string   `json:"task_group"`
	Task       string   `json:"task"`
	Function   string   `json:"function"`
	Model      string   `json:"model"`
	Parameters P        `json:"parameters"`
	Input      struct{} `json:"input"`
}

// continueFrame appends text to a running synthesis task.
type continueFrame struct {
	Header  outHeader       `json:"header"`
	Payload continuePayload `json:"payload"`
}

type continuePayload struct {
	Input textInput `json:"input"`
}

type textInput struct {
	Text string `json:"text"`
}

// finishFrame ends a task: the service flushes whatever is pending and
// replies with task-finished.
type finishFrame struct {
	Header  outHeader     `json:"header"`
	Payload finishPayload `json:"payload"`
}

type finishPayload struct {
	Input struct{} `json:"input"`
}

// inHeader is the header of every frame DashScope sends.
type inHeader struct {
	TaskID       string `json:"task_id"`
	Event        string `json:"event"`
	ErrorCode    string `json:"error_code"`
	ErrorMessage string `json:"error_message"`
}

// taskID returns a fresh 32-character hex task identifier.
func taskID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand never fails on the supported platforms; fall back to
		// a time-derived value rather than panicking in a provider.
		return fmt.Sprintf("%032x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// writeJSON marshals v and writes it as one text frame.
func writeJSON(ctx context.Context, conn *websocket.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return &provider.Error{Provider: Name, Kind: provider.ErrFatal, Err: err}
	}
	return conn.Write(ctx, websocket.MessageText, b)
}

// ---- errors ----------------------------------------------------------------

// taskFailed converts a task-failed header into a classified provider error.
func taskFailed(h inHeader) *provider.Error {
	return &provider.Error{
		Provider: Name,
		Kind:     classify(h.ErrorCode),
		Err:      fmt.Errorf("task failed: %s: %s", h.ErrorCode, h.ErrorMessage),
	}
}

// classify maps a DashScope error code to a retry class. Codes are dotted
// and case-inconsistent across services, so matching is on lowercase
// substrings.
func classify(code string) provider.ErrorKind {
	c := strings.ToLower(code)
	switch {
	case strings.Contains(c, "throttling"), strings.Contains(c, "quota"), strings.Contains(c, "limit"):
		return provider.ErrRateLimit
	case strings.Contains(c, "auth"), strings.Contains(c, "apikey"), strings.Contains(c, "forbidden"),
		strings.Contains(c, "accessdenied"), strings.Contains(c, "invalidapi"):
		return provider.ErrAuth
	case strings.Contains(c, "server_error"), strings.Contains(c, "internal"),
		strings.Contains(c, "unavailable"), strings.Contains(c, "timeout"):
		return provider.ErrTransient
	}
	return provider.ErrFatal
}

func fatalf(format string, args ...any) *provider.Error {
	return &provider.Error{Provider: Name, Kind: provider.ErrFatal, Err: fmt.Errorf(format, args...)}
}

func transientf(format string, args ...any) *provider.Error {
	return &provider.Error{Provider: Name, Kind: provider.ErrTransient, Err: fmt.Errorf(format, args...)}
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
