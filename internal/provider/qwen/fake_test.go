package qwen

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// recordedFrame is one text frame the fake service received, flattened to
// the fields the tests assert on.
type recordedFrame struct {
	Action string
	TaskID string
	Model  string
	Text   string         // continue-task input text
	Params map[string]any // run-task parameters
	At     time.Time
}

// reply is one scripted message the fake sends back. Exactly one of text,
// bin or closeCode is set.
type reply struct {
	text      string
	bin       []byte
	delay     time.Duration
	closeCode websocket.StatusCode
}

func text(s string) reply { return reply{text: s} }

func bin(n int) reply { return reply{bin: make([]byte, n)} }

func closeWith(code websocket.StatusCode) reply { return reply{closeCode: code} }

// fakeWS is a scripted DashScope /api-ws/v1/inference endpoint.
type fakeWS struct {
	t *testing.T
	// onFrame scripts the reply to each inbound text frame.
	onFrame func(f recordedFrame) []reply
	// onAudio scripts the reply to each inbound binary frame; total is the
	// cumulative byte count.
	onAudio func(total int) []reply

	mu sync.Mutex
	// startedTasks are the task ids the fake has acknowledged.
	startedTasks     []string
	frames           []recordedFrame
	audioBytes       int
	audioBeforeStart int
	auth             string
	inspection       string
	closeCode        websocket.StatusCode
	connected        chan struct{}
}

func newFakeWS(t *testing.T) (*fakeWS, *httptest.Server) {
	f := &fakeWS{t: t, connected: make(chan struct{})}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.auth = r.Header.Get("Authorization")
		f.inspection = r.Header.Get("X-DashScope-DataInspection")
		f.mu.Unlock()
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		conn.SetReadLimit(wsReadLimit)
		close(f.connected)
		ctx := context.Background()
		// Replies go out on their own goroutine so that a scripted delay
		// never stops the fake from reading: only then can the recorded
		// "audio before task-started" count mean what it says.
		out := make(chan reply, 64)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for rp := range out {
				if rp.delay > 0 {
					time.Sleep(rp.delay)
				}
				switch {
				case rp.closeCode != 0:
					conn.Close(rp.closeCode, "scripted")
					return
				case rp.bin != nil:
					if conn.Write(ctx, websocket.MessageBinary, rp.bin) != nil {
						return
					}
				default:
					if strings.Contains(rp.text, `"event":"task-started"`) {
						f.mu.Lock()
						f.startedTasks = append(f.startedTasks, startedTaskID(rp.text))
						f.mu.Unlock()
					}
					if conn.Write(ctx, websocket.MessageText, []byte(rp.text)) != nil {
						return
					}
				}
			}
		}()
		defer func() { close(out); <-done }()
		for {
			typ, data, err := conn.Read(ctx)
			if err != nil {
				f.mu.Lock()
				f.closeCode = websocket.CloseStatus(err)
				f.mu.Unlock()
				return
			}
			var replies []reply
			if typ == websocket.MessageBinary {
				f.mu.Lock()
				f.audioBytes += len(data)
				if len(f.startedTasks) == 0 {
					f.audioBeforeStart += len(data)
				}
				total := f.audioBytes
				f.mu.Unlock()
				if f.onAudio != nil {
					replies = f.onAudio(total)
				}
			} else {
				rec := parseFrame(data)
				f.mu.Lock()
				f.frames = append(f.frames, rec)
				f.mu.Unlock()
				if f.onFrame != nil {
					replies = f.onFrame(rec)
				}
			}
			for _, rp := range replies {
				select {
				case out <- rp:
				case <-done:
					return
				}
			}
		}
	}))
	return f, srv
}

func parseFrame(data []byte) recordedFrame {
	var m struct {
		Header struct {
			Action string `json:"action"`
			TaskID string `json:"task_id"`
		} `json:"header"`
		Payload struct {
			Model      string         `json:"model"`
			Parameters map[string]any `json:"parameters"`
			Input      struct {
				Text string `json:"text"`
			} `json:"input"`
		} `json:"payload"`
	}
	json.Unmarshal(data, &m)
	return recordedFrame{
		Action: m.Header.Action, TaskID: m.Header.TaskID, Model: m.Payload.Model,
		Text: m.Payload.Input.Text, Params: m.Payload.Parameters, At: time.Now(),
	}
}

// ackStarted returns the task-started frame for taskID. The fake records
// the acknowledgement when the frame actually goes out on the wire, not
// when the script is built.
func (f *fakeWS) ackStarted(taskID string) reply {
	return text(`{"header":{"task_id":"` + taskID + `","event":"task-started"},"payload":{}}`)
}

// startedTaskID pulls the task id back out of a scripted task-started frame.
func startedTaskID(s string) string {
	var m struct {
		Header struct {
			TaskID string `json:"task_id"`
		} `json:"header"`
	}
	json.Unmarshal([]byte(s), &m)
	return m.Header.TaskID
}

func (f *fakeWS) snapshot() ([]recordedFrame, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedFrame(nil), f.frames...), f.audioBytes, f.audioBeforeStart
}

func (f *fakeWS) actions() []string {
	frames, _, _ := f.snapshot()
	var out []string
	for _, fr := range frames {
		out = append(out, fr.Action)
	}
	return out
}

// wsURL converts an httptest URL into the ws:// form the provider dials.
func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// asrSentenceFrame builds a result-generated frame for one recognition sentence.
func asrSentenceFrame(taskID, sentence string, beginMs, endMs int, final bool) reply {
	body := map[string]any{
		"header": map[string]any{"task_id": taskID, "event": "result-generated"},
		"payload": map[string]any{
			"output": map[string]any{
				"sentence": map[string]any{
					"sentence_id": 1, "begin_time": beginMs, "end_time": nil,
					"text": sentence, "sentence_end": final, "words": []any{},
				},
			},
		},
	}
	if final {
		body["payload"].(map[string]any)["output"].(map[string]any)["sentence"].(map[string]any)["end_time"] = endMs
	}
	b, _ := json.Marshal(body)
	return reply{text: string(b)}
}

func taskFinishedFrame(taskID string) reply {
	return text(`{"header":{"task_id":"` + taskID + `","event":"task-finished"},"payload":{"output":{}}}`)
}

func taskFailedFrame(taskID, code, msg string) reply {
	b, _ := json.Marshal(map[string]any{
		"header":  map[string]any{"task_id": taskID, "event": "task-failed", "error_code": code, "error_message": msg},
		"payload": map[string]any{},
	})
	return reply{text: string(b)}
}
