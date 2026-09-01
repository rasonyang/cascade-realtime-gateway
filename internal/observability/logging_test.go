package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestRedactsSecretKeys(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, slog.LevelInfo)
	log.Info("cfg",
		"api_key", "sk-live-123",
		"listen", ":8080",
		slog.Group("providers",
			slog.Group("llm", "type", "openai", "Api_Key", "sk-nested"),
			"authorization", "Bearer x",
		),
		"token", "t0k",
		"password", "p",
		"secret", "s",
	)
	out := buf.String()
	for _, leaked := range []string{"sk-live-123", "sk-nested", "Bearer x", "t0k", `"p"`, `"s"`} {
		if strings.Contains(out, leaked) {
			t.Fatalf("secret %q leaked in log line: %s", leaked, out)
		}
	}
	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("log line is not JSON: %v", err)
	}
	if rec["api_key"] != Redacted {
		t.Fatalf("api_key = %v, want %q", rec["api_key"], Redacted)
	}
	if rec["listen"] != ":8080" {
		t.Fatalf("non-secret attr altered: %v", rec["listen"])
	}
	llm := rec["providers"].(map[string]any)["llm"].(map[string]any)
	if llm["type"] != "openai" || llm["Api_Key"] != Redacted {
		t.Fatalf("nested group not handled: %v", llm)
	}
}

func TestParseLevel(t *testing.T) {
	for in, want := range map[string]slog.Level{"debug": slog.LevelDebug, "info": slog.LevelInfo, "WARN": slog.LevelWarn, "error": slog.LevelError} {
		got, err := ParseLevel(in)
		if err != nil || got != want {
			t.Fatalf("ParseLevel(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	if _, err := ParseLevel("verbose"); err == nil {
		t.Fatal("expected error for unknown level")
	}
}

func TestLevelFilter(t *testing.T) {
	var buf bytes.Buffer
	NewLogger(&buf, slog.LevelWarn).Info("hidden")
	if buf.Len() != 0 {
		t.Fatalf("info record written at warn level: %s", buf.String())
	}
}
