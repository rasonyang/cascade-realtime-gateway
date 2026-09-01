package protocol

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider/mock"
	"github.com/rasonyang/cascade-realtime-gateway/internal/session"
)

const waitTimeout = 3 * time.Second

type frame map[string]any

func (f frame) typ() string { s, _ := f["type"].(string); return s }

func (f frame) str(path string) string {
	v := f.get(path)
	s, _ := v.(string)
	return s
}

func (f frame) get(path string) any {
	var cur any = map[string]any(f)
	for _, p := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[p]
	}
	return cur
}

type scripts struct {
	asr mock.ASRScript
	llm mock.LLMScript
	tts mock.TTSScript
}

func defaultScripts() scripts {
	return scripts{
		asr: mock.ASRScript{Utterances: []string{"hello there", "second utterance"}},
		llm: mock.LLMScript{Tokens: []string{"Hello", " world.", " Second", " sentence", " here."}, Usage: provider.Usage{InputTokens: 7, OutputTokens: 5}},
		tts: mock.TTSScript{MsPerRune: 10},
	}
}

// rig wires an Adapter to a real Session over the mocks and records every
// server frame in order.
type rig struct {
	t   *testing.T
	a   *Adapter
	s   *session.Session
	asr *mock.ASR
	llm *mock.LLM
	tts *mock.TTS

	mu     sync.Mutex
	frames []frame
	raw    [][]byte
	done   chan struct{}
}

func newRig(t *testing.T, sc scripts, model string, tweak func(*session.Options)) *rig {
	t.Helper()
	def := config.DefaultConfig()
	opts := session.Options{ID: "sess_test", Session: config.DefaultProfile().SessionDefaults(), Limits: def.Limits}
	opts.Limits.ASRFinalTimeout = config.Duration(time.Second)
	opts.Limits.ClientWriteTimeout = config.Duration(200 * time.Millisecond)
	if tweak != nil {
		tweak(&opts)
	}
	r := &rig{t: t, asr: mock.NewASR(sc.asr), llm: mock.NewLLM(sc.llm), tts: mock.NewTTS(sc.tts), done: make(chan struct{})}
	opts.ASR, opts.LLM, opts.TTS = r.asr, r.llm, r.tts
	r.s = session.New(opts)
	r.a = New(Options{SessionID: opts.ID, Model: model, Defaults: opts.Session, Session: r.s})
	if err := r.s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.add(r.a.Hello())
	go func() {
		defer close(r.done)
		for ev := range r.s.Events() {
			r.add(r.a.Outbound(ev))
		}
	}()
	t.Cleanup(func() {
		r.s.Close(session.CloseClient)
		select {
		case <-r.done:
		case <-time.After(waitTimeout):
			t.Error("session did not finish")
		}
	})
	return r
}

func (r *rig) add(frames [][]byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, b := range frames {
		var f frame
		if err := json.Unmarshal(b, &f); err != nil {
			r.t.Fatalf("server frame is not JSON: %s", b)
		}
		r.frames = append(r.frames, f)
		r.raw = append(r.raw, b)
	}
}

func (r *rig) send(js string) {
	r.t.Helper()
	r.add(r.a.Inbound([]byte(js)))
}

func (r *rig) sendf(format string, args ...any) { r.send(fmt.Sprintf(format, args...)) }

func (r *rig) append(pcm []byte) {
	frameBytes := audio.MsToBytes(20)
	for off := 0; off < len(pcm); off += frameBytes {
		end := min(off+frameBytes, len(pcm))
		r.sendf(`{"type":"input_audio_buffer.append","audio":%q}`, base64.StdEncoding.EncodeToString(pcm[off:end]))
	}
}

func (r *rig) all() []frame {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]frame(nil), r.frames...)
}

func (r *rig) count(typ string) int {
	n := 0
	for _, f := range r.all() {
		if f.typ() == typ {
			n++
		}
	}
	return n
}

func (r *rig) waitFor(desc string, pred func(frame) bool) frame {
	r.t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		for _, f := range r.all() {
			if pred(f) {
				return f
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	r.t.Fatalf("timeout waiting for %s; frames: %v", desc, r.types())
	return nil
}

func (r *rig) waitCount(typ string, n int) {
	r.t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if r.count(typ) >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	r.t.Fatalf("timeout waiting for %d × %s (have %d); frames: %v", n, typ, r.count(typ), r.types())
}

func (r *rig) waitType(typ string) frame {
	r.t.Helper()
	return r.waitFor(typ, func(f frame) bool { return f.typ() == typ })
}

func (r *rig) types() []string {
	out := []string{}
	for _, f := range r.all() {
		out = append(out, f.typ())
	}
	return out
}

func (r *rig) index(pred func(frame) bool) int {
	for i, f := range r.all() {
		if pred(f) {
			return i
		}
	}
	return -1
}

func isType(typ string) func(frame) bool {
	return func(f frame) bool { return f.typ() == typ }
}

func tone(ms int) []byte {
	n := ms * audio.SampleRate / 1000
	buf := make([]byte, n*audio.BytesPerSample)
	for i := 0; i < n; i++ {
		audio.PutSample(buf, i, int16(0.5*math.MaxInt16*math.Sin(2*math.Pi*440*float64(i)/audio.SampleRate)))
	}
	return buf
}

func silence(ms int) []byte { return make([]byte, audio.MsToBytes(ms)) }

func manual(o *session.Options) { o.Session.Audio.Input.TurnDetection = nil }

func serverVAD(create, interrupt bool) func(*session.Options) {
	return func(o *session.Options) {
		td := &config.TurnDetection{Type: config.TurnDetectionServerVAD, CreateResponse: create, InterruptResponse: interrupt}
		th, pre, sil := 0.5, 100, 200
		td.Threshold, td.PrefixPaddingMs, td.SilenceDurationMs = &th, &pre, &sil
		o.Session.Audio.Input.TurnDetection = td
	}
}

// ---- normalization for golden traces --------------------------------------

var idRe = regexp.MustCompile(`^(sess|conv|item|resp|call)_[a-z0-9]{16}$|^(event)_[a-z0-9]{12}$`)

// normalizer rewrites server-generated ids to <prefix:n> in first-seen order
// so traces are stable across runs.
type normalizer struct {
	seen map[string]string
	next map[string]int
}

func newNormalizer() *normalizer {
	return &normalizer{seen: map[string]string{}, next: map[string]int{}}
}

func (n *normalizer) frame(f frame) frame {
	return n.walk(map[string]any(f)).(map[string]any)
}

func (n *normalizer) walk(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = n.walk(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = n.walk(val)
		}
		return out
	case string:
		if m := idRe.FindStringSubmatch(x); m != nil {
			if rep, ok := n.seen[x]; ok {
				return rep
			}
			prefix := m[1] + m[2] // exactly one group matches
			n.next[prefix]++
			rep := fmt.Sprintf("<%s:%d>", prefix, n.next[prefix])
			n.seen[x] = rep
			return rep
		}
	}
	return v
}

// canonical renders a frame as JSON with sorted keys (encoding/json sorts
// map keys), one line.
func canonical(f frame) string {
	var buf strings.Builder
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(map[string]any(f))
	return strings.TrimSuffix(buf.String(), "\n")
}

func assertMonotonicEventIDs(t *testing.T, frames []frame) {
	t.Helper()
	ids := make([]string, 0, len(frames))
	for _, f := range frames {
		id := f.str("event_id")
		if !strings.HasPrefix(id, "event_") {
			t.Fatalf("frame without event_id: %v", f)
		}
		ids = append(ids, id)
	}
	if !sort.StringsAreSorted(ids) {
		t.Fatal("event ids are not monotonic")
	}
	set := map[string]bool{}
	for _, id := range ids {
		if set[id] {
			t.Fatalf("duplicate event_id %s", id)
		}
		set[id] = true
	}
}
