// Command cascade is the Cascade realtime gateway binary.
//
// Phase 0: it loads, expands and validates the configuration, initializes
// logging and prints a redacted summary. The HTTP/WebSocket server arrives
// in Phase 3.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/observability"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"

	// Providers register themselves with the registry from init.
	_ "github.com/rasonyang/cascade-realtime-gateway/internal/provider/mock"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("cascade", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "config.json", "path to the configuration file")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cascade: %s: %v\n", *configPath, err)
		return 1
	}
	if err := cfg.Validate(provider.Known); err != nil {
		fmt.Fprintf(os.Stderr, "cascade: %s: %v\n", *configPath, err)
		return 1
	}

	level, err := observability.ParseLevel(cfg.Observability.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cascade: %v\n", err)
		return 1
	}
	log := observability.NewLogger(os.Stdout, level)

	turnDetection := "null"
	if td := cfg.SessionDefaults.Audio.Input.TurnDetection; td != nil {
		turnDetection = td.Type
	}
	log.Info("configuration loaded",
		"config", *configPath,
		"listen", cfg.Listen,
		"providers.asr", cfg.Providers.ASR.Type,
		"providers.llm", cfg.Providers.LLM.Type,
		"providers.tts", cfg.Providers.TTS.Type,
		"output_modalities", cfg.SessionDefaults.OutputModalities,
		"turn_detection", turnDetection,
		"transcription_enabled", cfg.SessionDefaults.Audio.Input.Transcription != nil,
		"max_sessions", cfg.Limits.MaxSessions,
		"log_level", cfg.Observability.LogLevel,
		"otel_enabled", cfg.Observability.OTelEndpoint != "",
	)
	return 0
}
