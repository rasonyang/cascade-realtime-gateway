// Command cascade is the Cascade realtime gateway binary: load and validate
// the configuration, build the providers, serve /v1/realtime, and shut down
// gracefully on SIGINT / SIGTERM.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/observability"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
	"github.com/rasonyang/cascade-realtime-gateway/internal/recorder"
	"github.com/rasonyang/cascade-realtime-gateway/internal/server"

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
	checkOnly := fs.Bool("check", false, "validate the configuration and exit")
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
	if *checkOnly {
		return 0
	}

	providers, err := server.Build(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cascade: providers: %v\n", err)
		return 1
	}
	srv := server.New(server.Options{Config: cfg, Providers: providers, Logger: log, Recorder: recorder.Slog{Log: log}})
	httpSrv := &http.Server{Addr: cfg.Listen, Handler: srv.Handler()}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	errCh := make(chan error, 1)
	go func() { errCh <- httpSrv.ListenAndServe() }()
	log.Info("listening", "addr", cfg.Listen, "path", server.RealtimePath)

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "cascade: listen: %v\n", err)
			return 1
		}
	case <-ctx.Done():
		log.Info("shutting down")
		// Stop accepting, then end sessions; both are bounded by the
		// configured client_write_timeout through session cleanup.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*cfg.Limits.ClientWriteTimeout.Std())
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Warn("shutdown incomplete", "err", err)
		}
	}
	return 0
}
