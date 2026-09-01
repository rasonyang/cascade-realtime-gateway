// Package recorder is the narrow hook through which a finished session's
// summary leaves the gateway. v1 ships a slog implementation only.
//
// Layering: recorder must not import session, protocol or server.
package recorder

import (
	"context"
	"log/slog"
	"time"
)

// Summary describes one ended session. It carries no audio and no
// transcripts, only counts and timings.
type Summary struct {
	SessionID string
	StartedAt time.Time
	EndedAt   time.Time
	EndReason string
	Items     int
	Responses int
}

// Recorder receives session summaries. Implementations must be safe to call
// from any goroutine; the session calls SessionEnded asynchronously after it
// has finished, off the hot path.
type Recorder interface {
	SessionEnded(ctx context.Context, s Summary)
}

// Slog logs each summary at info level.
type Slog struct {
	Log *slog.Logger
}

// SessionEnded implements Recorder.
func (r Slog) SessionEnded(_ context.Context, s Summary) {
	r.Log.Info("session ended",
		"session_id", s.SessionID,
		"duration_ms", s.EndedAt.Sub(s.StartedAt).Milliseconds(),
		"end_reason", s.EndReason,
		"items", s.Items,
		"responses", s.Responses,
	)
}
