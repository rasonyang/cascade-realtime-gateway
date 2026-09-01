// Command qwenbench measures the latency of Cascade's Qwen providers against
// the live Alibaba Cloud Model Studio endpoints, per provider and end to end
// through the gateway.
//
//	ALIYUN_API_KEY=… go run ./hack/qwenbench -samples 20 -turns 20
//
// It is opt-in and never runs as part of the test suite: every sample is a
// real, billed request. The API key is read from the environment and is
// never logged.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider/qwen"
)

var stderr = os.Stderr

// utterance is what every ASR and E2E sample speaks: a question short enough
// to keep a run quick, and one the model answers in a sentence or two.
const utterance = "What is the largest ocean on Earth?"

func main() {
	os.Exit(run())
}

func run() int {
	samples := flag.Int("samples", 20, "warm samples per provider")
	turns := flag.Int("turns", 20, "complete end-to-end voice turns")
	only := flag.String("only", "asr,llm,tts,e2e", "comma-separated subset of the benchmarks to run")
	jsonOut := flag.String("json", "", "also write the report as JSON to this path")
	label := flag.String("label", "", "label recorded in the report, e.g. \"before\" or \"after\"")
	noNudge := flag.Bool("no-flush-nudge", false, "disable the TTS flush nudge, i.e. measure the unoptimized path")
	flag.Parse()

	key := os.Getenv("ALIYUN_API_KEY")
	if key == "" {
		fmt.Fprintln(stderr, "qwenbench: ALIYUN_API_KEY is not set")
		return 2
	}
	// Provider lifecycle facts arrive as debug logs; capture them rather
	// than printing them, so the report stays readable.
	cap := &logCapture{}
	slog.SetDefault(slog.New(cap))

	ctx := context.Background()
	want := map[string]bool{}
	for _, s := range strings.Split(*only, ",") {
		want[strings.TrimSpace(s)] = true
	}

	fmt.Fprintf(stderr, "qwenbench: synthesizing the fixture utterance…\n")
	pcm, err := synthesize(ctx, key, utterance)
	if err != nil {
		fmt.Fprintf(stderr, "qwenbench: %v\n", err)
		return 1
	}
	pcm = trimSilence(pcm)
	fmt.Fprintf(stderr, "  %q → %d ms of 24 kHz PCM (silence trimmed)\n", utterance, audio.BytesToMs(len(pcm)))

	report := struct {
		Label      string    `json:"label,omitempty"`
		StartedAt  time.Time `json:"started_at"`
		Utterance  string    `json:"utterance"`
		FlushNudge bool      `json:"tts_flush_nudge"`
		Sections   []section `json:"sections"`
	}{Label: *label, StartedAt: time.Now(), Utterance: utterance, FlushNudge: !*noNudge}

	type job struct {
		name string
		fn   func() (section, error)
	}
	ttsOpts := qwen.TTSOptions{NoFlushNudge: *noNudge}
	jobs := []job{
		{"asr", func() (section, error) { return benchASR(ctx, key, pcm, *samples, cap) }},
		{"llm", func() (section, error) { return benchLLM(ctx, key, *samples) }},
		{"tts", func() (section, error) { return benchTTS(ctx, key, ttsOpts, *samples, cap) }},
		{"e2e", func() (section, error) { return benchE2E(ctx, key, pcm, *turns, ttsOpts, cap) }},
	}
	for _, j := range jobs {
		if !want[j.name] {
			continue
		}
		fmt.Fprintf(stderr, "running %s…\n", j.name)
		sec, err := j.fn()
		if err != nil {
			fmt.Fprintf(stderr, "qwenbench: %s: %v\n", j.name, err)
			return 1
		}
		report.Sections = append(report.Sections, sec)
	}

	var out strings.Builder
	fmt.Fprintf(&out, "qwenbench — all values in milliseconds")
	if *label != "" {
		fmt.Fprintf(&out, " (%s)", *label)
	}
	fmt.Fprintf(&out, "\nhost %s, %s, tts flush nudge %v\n",
		"llm-…maas.aliyuncs.com", report.StartedAt.Format(time.RFC3339), report.FlushNudge)
	for _, sec := range report.Sections {
		out.WriteString(sec.String())
	}
	fmt.Println(out.String())

	if *jsonOut != "" {
		b, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fmt.Fprintf(stderr, "qwenbench: %v\n", err)
			return 1
		}
		if err := os.WriteFile(*jsonOut, append(b, '\n'), 0o644); err != nil {
			fmt.Fprintf(stderr, "qwenbench: %v\n", err)
			return 1
		}
		fmt.Fprintf(stderr, "wrote %s\n", *jsonOut)
	}
	return 0
}
