// Package server terminates WebSocket connections: HTTP auth and upgrade,
// one Session + Adapter per connection, a read loop that never blocks on the
// session, a single writer goroutine per connection, timeout disconnects,
// the concurrent-session limit and graceful shutdown.
//
// Layering: server imports protocol, session, config, provider, recorder.
// Nothing imports server.
package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/coder/websocket"

	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/observability"
	"github.com/rasonyang/cascade-realtime-gateway/internal/protocol"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
	"github.com/rasonyang/cascade-realtime-gateway/internal/recorder"
	"github.com/rasonyang/cascade-realtime-gateway/internal/session"
)

// Providers are the three provider instances shared by every session.
type Providers struct {
	ASR provider.ASR
	LLM provider.LLM
	TTS provider.TTS
}

// Options configures a Server.
type Options struct {
	Config    *config.Config
	Providers Providers
	Logger    *slog.Logger
	Recorder  recorder.Recorder
	Telemetry *observability.Telemetry // nil records nothing
}

// Server owns the connection lifecycle. Its root context is cancelled by
// Shutdown, which ends every session.
type Server struct {
	cfg  *config.Config
	prov Providers
	log  *slog.Logger
	rec  recorder.Recorder
	tel  *observability.Telemetry

	ctx    context.Context
	cancel context.CancelFunc
	conns  sync.WaitGroup
	active atomic.Int64
}

// RealtimePath is the only route.
const RealtimePath = "/v1/realtime"

// Subprotocol is negotiated when the client offers it.
const Subprotocol = "realtime"

// New builds a server; call Handler to mount it and Shutdown to stop it.
func New(opts Options) *Server {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{cfg: opts.Config, prov: opts.Providers, log: log, rec: opts.Recorder, tel: opts.Telemetry, ctx: ctx, cancel: cancel}
}

// Handler returns the HTTP handler serving RealtimePath.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(RealtimePath, s.handleRealtime)
	return mux
}

// ActiveSessions returns the number of live connections.
func (s *Server) ActiveSessions() int { return int(s.active.Load()) }

// Shutdown ends every session and waits for their connections to finish or
// ctx to expire. Session cleanup itself is bounded by the configured
// timeouts, so a generous ctx normally returns well before its deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	s.cancel()
	done := make(chan struct{})
	go func() { s.conns.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type httpError struct {
	Error struct {
		Type    string  `json:"type"`
		Code    *string `json:"code"`
		Message string  `json:"message"`
	} `json:"error"`
}

func writeHTTPError(w http.ResponseWriter, status int, typ, msg string) {
	var e httpError
	e.Error.Type, e.Error.Message = typ, msg
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(e)
}

func (s *Server) authorized(r *http.Request) bool {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return false
	}
	token := strings.TrimSpace(h[len(prefix):])
	return subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.Auth.APIKey)) == 1
}

func (s *Server) handleRealtime(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeHTTPError(w, http.StatusMethodNotAllowed, "invalid_request_error", "GET with a WebSocket upgrade is required.")
		return
	}
	if !s.authorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="cascade"`)
		writeHTTPError(w, http.StatusUnauthorized, "invalid_request_error", "Missing or invalid Authorization header; expected 'Bearer <REALTIME_API_KEY>'.")
		return
	}
	if s.ctx.Err() != nil {
		writeHTTPError(w, http.StatusServiceUnavailable, "server_error", "The gateway is shutting down.")
		return
	}
	// Reserve a slot before upgrading so the limit is never exceeded.
	if n := s.active.Add(1); n > int64(s.cfg.Limits.MaxSessions) {
		s.active.Add(-1)
		writeHTTPError(w, http.StatusServiceUnavailable, "server_error", "max_sessions reached; try again later.")
		return
	}
	s.conns.Add(1)
	defer s.conns.Done()
	defer s.active.Add(-1)

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{Subprotocol},
		// Bearer auth already gates the endpoint and browsers cannot attach
		// it cross-origin, so the Origin check adds nothing here.
		InsecureSkipVerify: true,
	})
	if err != nil {
		s.log.Warn("websocket accept failed", "err", err)
		return
	}
	conn.SetReadLimit(int64(s.cfg.Limits.ClientMaxMessageBytes))
	s.serve(conn, r.URL.Query().Get("model"))
}

// outMsg is either a frame to write or the final close.
type outMsg struct {
	frame  []byte
	close  bool
	code   websocket.StatusCode
	reason string
}

func (s *Server) serve(conn *websocket.Conn, model string) {
	ctx, cancel := context.WithTimeout(s.ctx, s.cfg.Limits.SessionTimeout.Std())
	defer cancel()
	defer conn.CloseNow() //nolint:errcheck // last resort after the writer's Close

	id := protocol.NewSessionID()
	log := s.log.With("session_id", id)
	sess := session.New(session.Options{
		ID: id, Session: s.cfg.SessionDefaults, Limits: s.cfg.Limits,
		ASR: s.prov.ASR, LLM: s.prov.LLM, TTS: s.prov.TTS, Logger: s.log, Recorder: s.rec, Telemetry: s.tel,
	})
	if err := sess.Start(ctx); err != nil {
		log.Error("session start failed", "err", err)
		_ = conn.Close(websocket.StatusInternalError, string(session.CloseProviderError))
		return
	}
	adapter := protocol.New(protocol.Options{SessionID: id, Model: model, Defaults: s.cfg.SessionDefaults, Session: sess})
	log.Info("connection opened", "model", model, "active", s.active.Load())

	// The read loop uses its own context: cancelling the session context
	// must not tear the socket down before the writer has sent the close
	// frame, so reads are cancelled only once the writer has finished.
	readCtx, cancelRead := context.WithCancel(context.Background())
	defer cancelRead()

	out := make(chan outMsg, s.cfg.Limits.OutputEventQueue)
	writerDone := make(chan struct{})
	send := func(m outMsg) {
		select {
		case out <- m:
		case <-writerDone:
		}
	}

	// Single writer: the only goroutine that touches conn.Write / Close.
	go func() {
		defer cancelRead()
		defer close(writerDone)
		for m := range out {
			if m.close {
				_ = conn.Close(m.code, m.reason)
				return
			}
			// Bounded by client_write_timeout, not by the session context:
			// frames queued before a close (the fatal error, for instance)
			// must still reach the client.
			wctx, wcancel := context.WithTimeout(context.Background(), s.cfg.Limits.ClientWriteTimeout.Std())
			err := conn.Write(wctx, websocket.MessageText, m.frame)
			wcancel()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					log.Warn("client write timeout; disconnecting")
				}
				sess.Close(session.CloseClient)
				return
			}
		}
	}()

	// Event pump: session facts → wire frames → writer.
	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		closeCode, closeReason := websocket.StatusNormalClosure, ""
		for _, f := range adapter.Hello() {
			send(outMsg{frame: f})
		}
		for ev := range sess.Events() {
			switch e := ev.(type) {
			case session.EvError:
				if e.Fatal {
					closeCode, closeReason = websocket.StatusInternalError, e.Code
				}
			case session.EvSessionClosed:
				switch e.Reason {
				case session.CloseContextCancelled:
					closeCode, closeReason = websocket.StatusGoingAway, "shutting_down"
				case session.CloseSessionTimeout:
					closeCode, closeReason = websocket.StatusNormalClosure, string(e.Reason)
				case session.CloseInputOverflow, session.CloseBufferOverflow, session.CloseProviderError:
					closeCode, closeReason = websocket.StatusInternalError, string(e.Reason)
				}
			}
			for _, f := range adapter.Outbound(ev) {
				send(outMsg{frame: f})
			}
		}
		send(outMsg{close: true, code: closeCode, reason: closeReason})
		close(out)
	}()

	// Read loop: never blocks on the session beyond the bounded queues.
	for {
		typ, data, err := conn.Read(readCtx)
		if err != nil {
			break
		}
		if typ != websocket.MessageText {
			for _, f := range adapter.Reject("Binary frames are not supported; send one JSON client event per text frame.") {
				send(outMsg{frame: f})
			}
			continue
		}
		for _, f := range adapter.Inbound(data) {
			send(outMsg{frame: f})
		}
	}
	sess.Close(session.CloseClient) // no-op if the session already ended for another reason
	<-sess.Done()
	<-pumpDone
	<-writerDone
	log.Info("connection closed", "active", s.active.Load()-1)
}

// Build constructs the three providers named in cfg from the registry.
func Build(cfg *config.Config) (Providers, error) {
	var p Providers
	mk := func(kind provider.Kind, pc config.Provider) (provider.Provider, error) {
		return provider.New(kind, pc.Type, pc.APIKey, pc.Options)
	}
	asr, err := mk(provider.KindASR, cfg.Providers.ASR)
	if err != nil {
		return p, err
	}
	llm, err := mk(provider.KindLLM, cfg.Providers.LLM)
	if err != nil {
		return p, err
	}
	tts, err := mk(provider.KindTTS, cfg.Providers.TTS)
	if err != nil {
		return p, err
	}
	var ok bool
	if p.ASR, ok = asr.(provider.ASR); !ok {
		return p, errors.New("asr provider does not implement provider.ASR")
	}
	if p.LLM, ok = llm.(provider.LLM); !ok {
		return p, errors.New("llm provider does not implement provider.LLM")
	}
	if p.TTS, ok = tts.(provider.TTS); !ok {
		return p, errors.New("tts provider does not implement provider.TTS")
	}
	return p, nil
}
