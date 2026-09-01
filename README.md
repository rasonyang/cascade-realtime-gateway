# Cascade

OpenAI Realtime-compatible gateway over a cascaded ASR → LLM → TTS pipeline.

```sh
go build -o cascade ./cmd/cascade
REALTIME_API_KEY=... DEEPGRAM_API_KEY=... OPENAI_API_KEY=... ./cascade -config config.example.json
./cascade -config config.example.json -check   # validate the configuration and exit
```

Clients connect to `ws://<listen>/v1/realtime?model=<any>` with `Authorization: Bearer $REALTIME_API_KEY`
and speak the OpenAI Realtime GA protocol (`session.type = "realtime"`). Cascade serves plain WebSocket;
put a TLS terminator in front of it for `wss://` (the official OpenAI SDKs require `wss:`).

Architecture rules live in `CLAUDE.md`; the implementation plan in `docs/implementation-brief.md`;
the protocol compatibility profile in `docs/protocol-profile.md` (Phase 2).

## Observability

Set `observability.otel_endpoint` to an OTLP/HTTP collector (`"127.0.0.1:4318"`) to export spans
(`session → response → llm/tts`) and the core latency metrics (`cascade.*`); leave it empty to disable.
Logs are JSON on stdout with secret fields redacted; provider lifecycle facts are logged at `debug`.

## Tests

```sh
go test -race ./...                                   # unit, session, protocol conformance, server
CASCADE_E2E_LLM_MODEL=gpt-5-nano \
  DEEPGRAM_API_KEY=... OPENAI_API_KEY=... \
  go test -tags e2e -run TestE2E -v ./internal/server  # one real voice turn + interrupts (network)
```
