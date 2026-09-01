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

## Providers

`providers.llm.options` for `openai`: `model`, `base_url`, `request_timeout`, `stream_idle_timeout`, and
`reasoning_effort` (sent only when set; e.g. `"minimal"` on gpt-5 models, `"none"` where the model supports it —
non-reasoning models reject the field). `providers.tts.options`: `model`, `base_url`, timeouts.
`providers.asr.options` for `deepgram`: `model`, `language`, `smart_format`, `endpointing_ms`, `utterance_end_ms`,
`connect_timeout`, `finalize_timeout`, `keepalive_interval`, `audio_queue_frames`.

A manual check against the official OpenAI SDK lives in `hack/sdkcheck/`.

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
