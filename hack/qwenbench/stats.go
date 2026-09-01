package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// series is one measured latency, in milliseconds.
type series struct {
	Name   string
	Values []float64
}

func (s *series) add(d time.Duration) { s.Values = append(s.Values, d.Seconds()*1000) }

// summary is the reported shape of a series.
type summary struct {
	Name string  `json:"name"`
	N    int     `json:"n"`
	Min  float64 `json:"min_ms"`
	Avg  float64 `json:"avg_ms"`
	P50  float64 `json:"p50_ms"`
	P90  float64 `json:"p90_ms"`
	P95  float64 `json:"p95_ms"`
	Max  float64 `json:"max_ms"`
	// Samples travels with the summary so a JSON report can be re-derived
	// and audited, not just read.
	Samples []float64 `json:"samples_ms"`
}

// percentile uses the nearest-rank method, which needs no interpolation and
// always returns a value that was actually measured.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(p/100*float64(len(sorted)) + 0.5)
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func (s *series) summarize() summary {
	out := summary{Name: s.Name, N: len(s.Values), Samples: s.Values}
	if len(s.Values) == 0 {
		return out
	}
	sorted := append([]float64(nil), s.Values...)
	sort.Float64s(sorted)
	var sum float64
	for _, v := range sorted {
		sum += v
	}
	out.Min = sorted[0]
	out.Max = sorted[len(sorted)-1]
	out.Avg = sum / float64(len(sorted))
	out.P50 = percentile(sorted, 50)
	out.P90 = percentile(sorted, 90)
	out.P95 = percentile(sorted, 95)
	return out
}

// section is a named group of series printed as one table.
type section struct {
	Title   string    `json:"title"`
	Note    string    `json:"note,omitempty"`
	Metrics []summary `json:"metrics"`
}

func (sec section) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n%s\n%s\n", sec.Title, strings.Repeat("=", len(sec.Title)))
	if sec.Note != "" {
		fmt.Fprintf(&b, "%s\n", sec.Note)
	}
	width := 10
	for _, m := range sec.Metrics {
		if len(m.Name) > width {
			width = len(m.Name)
		}
	}
	fmt.Fprintf(&b, "%-*s  %3s %8s %8s %8s %8s %8s %8s\n", width, "metric", "n", "min", "avg", "p50", "p90", "p95", "max")
	fmt.Fprintf(&b, "%s\n", strings.Repeat("-", width+2+3+6*9))
	for _, m := range sec.Metrics {
		if m.N == 0 {
			fmt.Fprintf(&b, "%-*s  %3d %8s\n", width, m.Name, 0, "n/a")
			continue
		}
		fmt.Fprintf(&b, "%-*s  %3d %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
			width, m.Name, m.N, m.Min, m.Avg, m.P50, m.P90, m.P95, m.Max)
	}
	return b.String()
}

func summarizeAll(title, note string, all ...*series) section {
	sec := section{Title: title, Note: note}
	for _, s := range all {
		sec.Metrics = append(sec.Metrics, s.summarize())
	}
	return sec
}
