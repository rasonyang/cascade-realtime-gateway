package observability

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// Redacted replaces the value of every secret-bearing attribute.
const Redacted = "[REDACTED]"

// secretKeyFragments are matched case-insensitively as substrings of attribute
// keys. Any match is redacted regardless of nesting group.
var secretKeyFragments = []string{"api_key", "apikey", "authorization", "token", "secret", "password"}

// ParseLevel maps a config log level string to a slog.Level.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("unknown log level %q", s)
}

// NewLogger returns a JSON slog.Logger writing to w at the given level. Every
// attribute whose key looks like a credential is replaced by Redacted before
// it is written; raw audio is never passed to the logger by convention.
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: redactAttr,
	})
	return slog.New(h)
}

// IsSecretKey reports whether an attribute key must be redacted.
func IsSecretKey(key string) bool {
	k := strings.ToLower(key)
	for _, frag := range secretKeyFragments {
		if strings.Contains(k, frag) {
			return true
		}
	}
	return false
}

func redactAttr(_ []string, a slog.Attr) slog.Attr {
	if a.Value.Kind() == slog.KindGroup {
		return a // members are visited individually
	}
	if IsSecretKey(a.Key) {
		return slog.String(a.Key, Redacted)
	}
	return a
}
