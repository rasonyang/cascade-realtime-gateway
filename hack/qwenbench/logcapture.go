package main

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// logRecord is one captured slog record with the timestamp it was emitted at.
type logRecord struct {
	at    time.Time
	msg   string
	attrs map[string]slog.Value
}

// logCapture records slog records so provider lifecycle facts (task-started
// round trips, the first text handed to TTS, the first audio frame back) can
// be correlated with what the benchmark observes at the interface.
type logCapture struct {
	mu   sync.Mutex
	recs []logRecord
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *logCapture) WithGroup(string) slog.Handler            { return c }

func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	rec := logRecord{at: r.Time, msg: r.Message, attrs: map[string]slog.Value{}}
	r.Attrs(func(a slog.Attr) bool { rec.attrs[a.Key] = a.Value; return true })
	c.mu.Lock()
	c.recs = append(c.recs, rec)
	c.mu.Unlock()
	return nil
}

// mark returns the index one past the last record, so a later lookup can be
// restricted to records produced after this moment.
func (c *logCapture) mark() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.recs)
}

// after returns the first record with msg at or after index from.
func (c *logCapture) after(from int, msg string) (logRecord, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := from; i < len(c.recs); i++ {
		if c.recs[i].msg == msg {
			return c.recs[i], true
		}
	}
	return logRecord{}, false
}

// waitAfter polls for a record, which the reader goroutines produce
// asynchronously.
func (c *logCapture) waitAfter(from int, msg string, timeout time.Duration) (logRecord, bool) {
	deadline := time.Now().Add(timeout)
	for {
		if r, ok := c.after(from, msg); ok {
			return r, true
		}
		if time.Now().After(deadline) {
			return logRecord{}, false
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (r logRecord) float(key string) (float64, bool) {
	v, ok := r.attrs[key]
	if !ok {
		return 0, false
	}
	switch v.Kind() {
	case slog.KindFloat64:
		return v.Float64(), true
	case slog.KindInt64:
		return float64(v.Int64()), true
	}
	return 0, false
}
