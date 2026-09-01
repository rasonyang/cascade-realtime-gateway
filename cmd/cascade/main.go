// Command cascade is the Cascade realtime gateway binary: load and validate
// the static configuration, open the runtime configuration (state file, or the
// bootstrap seed on a first start), serve /v1/realtime and — when an admin key
// is configured — /admin/v1, and shut down gracefully on SIGINT / SIGTERM.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/rasonyang/cascade-realtime-gateway/internal/admin"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/observability"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
	"github.com/rasonyang/cascade-realtime-gateway/internal/recorder"
	"github.com/rasonyang/cascade-realtime-gateway/internal/server"

	// Providers register themselves with the registry from init.
	_ "github.com/rasonyang/cascade-realtime-gateway/internal/provider/deepgram"
	_ "github.com/rasonyang/cascade-realtime-gateway/internal/provider/mock"
	_ "github.com/rasonyang/cascade-realtime-gateway/internal/provider/openai"
	_ "github.com/rasonyang/cascade-realtime-gateway/internal/provider/qwen"
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
	slog.SetDefault(log) // providers log lifecycle facts through the default logger

	adminEnabled := cfg.Auth.AdminAPIKey != ""
	log.Info("configuration loaded",
		"config", *configPath,
		"listen", cfg.Listen,
		"admin_enabled", adminEnabled,
		"admin.listen", cfg.Admin.Listen,
		"admin.state_file", cfg.Admin.StateFile,
		"bootstrap", len(cfg.Bootstrap) > 0,
		"max_sessions", cfg.Limits.MaxSessions,
		"log_level", cfg.Observability.LogLevel,
		"otel_enabled", cfg.Observability.OTelEndpoint != "",
	)
	if *checkOnly {
		return 0
	}

	tel, err := observability.Setup(context.Background(), cfg.Observability.OTelEndpoint)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cascade: otel: %v\n", err)
		return 1
	}
	store, err := admin.Open(admin.Options{
		StateFile: cfg.Admin.StateFile,
		Bootstrap: cfg.Bootstrap,
		Known:     provider.Known,
		Logger:    log,
		Telemetry: tel,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "cascade: runtime config: %v\n", err)
		return 1
	}
	srv := server.New(server.Options{Config: cfg, Resolver: store, Logger: log, Recorder: recorder.Slog{Log: log}, Telemetry: tel})
	httpSrv := &http.Server{Addr: cfg.Listen, Handler: srv.Handler()}

	// The Admin API listens on its own address and is absent entirely when no
	// admin key is configured.
	var adminSrv *http.Server
	if adminEnabled {
		adminSrv = &http.Server{Addr: cfg.Admin.Listen, Handler: store.Handler(cfg.Auth.AdminAPIKey)}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	errCh := make(chan error, 1)
	go func() { errCh <- httpSrv.ListenAndServe() }()
	log.Info("listening", "addr", cfg.Listen, "path", server.RealtimePath)
	if adminSrv != nil {
		go func() { errCh <- adminSrv.ListenAndServe() }()
		log.Info("admin listening", "addr", cfg.Admin.Listen, "path", admin.BasePath)
	}

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
		if adminSrv != nil {
			_ = adminSrv.Shutdown(shutdownCtx)
		}
		_ = httpSrv.Shutdown(shutdownCtx)
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Warn("shutdown incomplete", "err", err)
		}
	}
	// Flush spans and metrics before exit.
	flushCtx, cancel := context.WithTimeout(context.Background(), cfg.Limits.ClientWriteTimeout.Std())
	defer cancel()
	if err := tel.Shutdown(flushCtx); err != nil {
		log.Warn("telemetry flush incomplete", "err", err)
	}
	return 0
}
