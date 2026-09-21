# Cascade

A self-hosted voice gateway that speaks the OpenAI Realtime (GA) WebSocket protocol to clients and
runs a cascaded ASR → LLM → TTS pipeline behind it.

The client-facing protocol stays fixed. The ASR, LLM and TTS vendors behind it are configuration, and
each can be replaced independently without touching a client.

```
client (any OpenAI Realtime GA client)
   │  ws://…/v1/realtime        — the stable contract
   ▼
Cascade    protocol adapter → session actor → per-response pipeline
   │  provider interfaces       — the replaceable part
   ▼
ASR (deepgram | qwen)    LLM (openai-compatible | qwen)    TTS (openai | qwen)
```

## Why it exists

A speech-to-speech API such as OpenAI Realtime gives a client one clean protocol, but ties the
application to one vendor's model, voices, languages, pricing and regions. A cascaded pipeline lets
you choose the best ASR, LLM and TTS for each job — but then the realtime behaviour is yours to build.

Wiring ASR → LLM → TTS together takes an afternoon. The rest of a voice agent is what takes months:

- **Turn detection** — deciding when the user has finished speaking, and committing the right slice of
  audio, including the padding before speech was detected.
- **Barge-in** — when the user interrupts, cancelling the LLM and TTS streams immediately so neither
  keeps generating (or billing), and emitting a consistent terminal state.
- **Context after an interrupt** — trimming the assistant's turn to what the user actually heard, so
  the next turn's context does not contain sentences that were never played.
- **Streaming end to end** — synthesizing sentence by sentence while the LLM is still generating,
  without ever speaking tool-call arguments.
- **Ordering and races** — a late delta from a cancelled response must never reach the client; a slow
  client must never block cancellation or shutdown.
- **A client protocol** — event names, ids, lifecycles and error shapes that client code can rely on.

Cascade implements that layer once, behind a protocol that already has SDKs, documentation and
existing clients. The result:

- Clients are written against the OpenAI Realtime GA protocol. A client built for OpenAI Realtime can
  point at Cascade, within the [supported subset](#protocol-compatibility).
- The backend stack is a named *profile* — one ASR, one LLM, one TTS instance — changed through an
  Admin REST API at runtime. New calls pick up the change; calls in progress are unaffected.
- Provider specifics (wire formats, tool-call fragment reassembly, keep-alives, vendor quirks) are
  normalized at the provider boundary and never reach the client.

## Who it is for

Use it if you:

- build voice agents or telephony bridges and want to choose ASR / LLM / TTS vendors per language,
  region, cost or latency — or change them later without shipping new clients;
- already have OpenAI Realtime clients and need to run them on a different or self-hosted backend;
- want a small, inspectable gateway: one Go binary, one JSON config file, no database.

Do not use it if you:

- want native speech-to-speech model behaviour (paralinguistics, emotion, non-text audio
  understanding). A cascade passes only text between stages;
- need full OpenAI Realtime parity. Cascade implements a documented subset and rejects the rest with
  an `error` event — see [Not supported](#not-supported);
- need WebRTC, SIP, G.711, image input, or server-side tool execution;
- need a multi-tenant platform: auth is one static API key, and there is no clustering or persistence.

## Quick start

No API keys needed — this runs the full pipeline on the built-in deterministic `mock` providers:

```sh
go build -o cascade ./cmd/cascade
cd hack/sdkcheck   # the state file is written to the working directory; it is git-ignored here
REALTIME_API_KEY=sk-test ../../cascade -config config.mock.json
# ws://127.0.0.1:18080/v1/realtime   Authorization: Bearer sk-test
```

With real providers (Deepgram ASR + OpenAI LLM/TTS, or Qwen for all three), from the repository root:

```sh
REALTIME_API_KEY=... ADMIN_API_KEY=... DEEPGRAM_API_KEY=... OPENAI_API_KEY=... \
  ./cascade -config config.example.json
REALTIME_API_KEY=... ADMIN_API_KEY=... ALIYUN_API_KEY=... \
  ./cascade -config config.qwen.example.json

./cascade -config config.example.json -check   # validate the configuration and exit
```

Clients connect to `ws://<listen>/v1/realtime?model=<any>` with `Authorization: Bearer $REALTIME_API_KEY`.
`?model=` is accepted and echoed, not used for routing — the default profile decides the stack.

Cascade serves plain WebSocket. Put a TLS terminator in front of it for `wss://`; the official OpenAI
SDKs require `wss:`. [`hack/sdkcheck/`](hack/sdkcheck/) drives Cascade with the official `openai-node`
GA realtime client through a small TLS proxy and doubles as a client example.

Requires Go 1.25+. Direct dependencies are a WebSocket library and OpenTelemetry.

## Protocol compatibility

The target is a documented GA compatibility profile, not full parity. The authoritative reference —
every event, field, ordering and rejection — is [`docs/protocol-profile.md`](docs/protocol-profile.md).

| | |
|---|---|
| Protocol | GA only (`session.type = "realtime"`). Beta session fields are rejected, not translated. |
| Client events | `session.update`, `input_audio_buffer.append` / `commit` / `clear`, `conversation.item.create` / `delete` / `truncate`, `response.create`, `response.cancel` |
| Audio | `audio/pcm`, 24 kHz, mono, 16-bit, in and out |
| Input | audio, or text message items (`input_text`) |
| Output | `["audio"]` (with transcript) or `["text"]` (TTS never opened) |
| Turn detection | `server_vad`, `semantic_vad` (approximated from ASR transcripts), or `null` (client-driven) |
| Interruption | barge-in cancels the response; `conversation.item.truncate` trims context to the played audio |
| Function calling | `tools` + `tool_choice` on `session.update`; executed by the client; one call per response |

Anything outside the profile is answered with an `error` event naming the field. Nothing is silently
ignored.

### Not supported

WebRTC and SIP transports, `client_secrets` / `calls` endpoints, G.711 and other non-PCM formats,
image and audio message parts, MCP tools, parallel tool calls, out-of-band responses,
`conversation.item.retrieve`, input noise reduction, server-side output pacing, session resumption,
conversation persistence, multi-tenant auth, clustering, in-process TLS, and health or Prometheus
endpoints (metrics leave over OTLP only).

Text trimming on `truncate` is approximate: cascaded TTS has no exact text↔audio alignment, so Cascade
trims by provider alignment when available, otherwise proportionally per sentence. Approximations like
this are recorded in [`docs/decisions.md`](docs/decisions.md).

### Function calling

Declare tools on the session and the model can call them; Cascade never executes one. It forwards the
call, takes the result back as an opaque string, and feeds it into the next generation.

```jsonc
// session.update
{"type":"session.update","session":{"type":"realtime",
  "tools":[{"type":"function","name":"transfer_to_agent","description":"Hand off to a human.",
            "parameters":{"type":"object","properties":{"department":{"type":"string"}},"required":["department"]}}],
  "tool_choice":"auto"}}

// ← response.function_call_arguments.done {call_id, name, arguments}
// → the client executes the tool and returns its result, then asks for the next turn
{"type":"conversation.item.create","item":{"type":"function_call_output",
  "call_id":"call_…","output":"no agent available"}}
{"type":"response.create"}
```

`tool_choice` is `"auto"` (the default), `"none"`, `"required"` or `{"type":"function","name":…}`.
`arguments` arrives as one complete delta followed by `.done`, never as fragments — the fragments are
reassembled at the provider boundary, so a call cancelled or truncated mid-arguments is never
delivered. `arguments` is a raw JSON string Cascade never parses, so a client must still handle a
parse failure.

## Providers

| Type | ASR | LLM | TTS | Notes |
|---|:-:|:-:|:-:|---|
| `deepgram` | ✓ | | | streaming ASR |
| `openai` | | ✓ | ✓ | Chat Completions and `/audio/speech`; `base_url` is configurable |
| `qwen` | ✓ | ✓ | ✓ | Alibaba Cloud Model Studio (DashScope) |
| `mock` | ✓ | ✓ | ✓ | deterministic and scriptable; no network, no keys |

A provider *instance* is a named `(type, api_key, options)` triple; a *profile* references one instance
per role. Providers are three narrow Go interfaces plus a registry map in
[`internal/provider`](internal/provider/provider.go); adding a vendor means implementing the role's
interface and registering it.

`options` per type:

- `openai` LLM: `model`, `base_url`, `request_timeout`, `stream_idle_timeout`, `reasoning_effort` (sent
  only when set; non-reasoning models reject the field). `openai` TTS: `model`, `base_url`, timeouts.
- `deepgram`: `model`, `base_url`, `language`, `smart_format`, `endpointing_ms`, `utterance_end_ms`,
  `connect_timeout`, `finalize_timeout`, `keepalive_interval`, `audio_queue_frames`.
- `qwen`: see [`internal/provider/qwen`](internal/provider/qwen) and
  [`docs/qwen-latency.md`](docs/qwen-latency.md).

### Latency

Cascade itself adds under 1 ms between provider audio and client audio (p50); a turn's latency is the
providers' plus the VAD silence window. Measured on the Qwen stack over 20 turns: end of user speech
→ first playable audio p50 1223 ms, p95 1854 ms, of which 391 ms is the VAD hold. Those figures come
from a dedicated Model Studio deployment; the public endpoint has not been measured. Method and the
per-stage budget are in [`docs/qwen-latency.md`](docs/qwen-latency.md).

## Configuration

Configuration is split by lifetime.

**Static** — the JSON config file, read once at startup: `listen` (default `:8080`), `auth.api_key`,
`auth.admin_api_key`, `admin.listen` (default `127.0.0.1:8081`), `admin.state_file`, `limits`
(`max_sessions`, timeouts, queue sizes), `observability`, and the optional `bootstrap` seed.
`{env.NAME}` placeholders are expanded at load, except inside `bootstrap`. Unknown fields and unset
variables fail startup and name the offending field.

**Runtime** — provider instances, profiles and `settings.default_profile`, owned by the Admin REST API
and stored in a single JSON state file (`admin.state_file`, mode `0600`). There is no database. The
state file wins when it exists; `bootstrap` seeds it only on a first start. Without a `default_profile`
the gateway starts but answers `/v1/realtime` with `503`.

A session takes a configuration snapshot when its connection opens, so an Admin write affects only
connections opened after it. One connection is one call; `limits.max_sessions` caps concurrent calls,
and a connection over the limit gets `503`.

## Admin API

`ADMIN_API_KEY` (Bearer) guards every route; it is a different credential from `REALTIME_API_KEY`, and
neither key works on the other endpoint. Leaving `auth.admin_api_key` empty disables the Admin API
entirely — no listener at all. **Bind `admin.listen` to localhost or a private network**: these routes
decide what every new call runs on.

| Route | Method | |
|---|---|---|
| `/admin/v1/config` | `GET` `PUT` | the whole runtime configuration |
| `/admin/v1/providers` | `GET` | every provider instance |
| `/admin/v1/providers/{name}` | `GET` `PUT` `DELETE` | one provider instance |
| `/admin/v1/profiles` | `GET` | every profile |
| `/admin/v1/profiles/{name}` | `GET` `PUT` `DELETE` | one profile |
| `/admin/v1/settings` | `GET` `PUT` | `default_profile` |

```sh
curl -s -H "Authorization: Bearer $ADMIN_API_KEY" http://127.0.0.1:8081/admin/v1/config

curl -s -X PUT -H "Authorization: Bearer $ADMIN_API_KEY" \
  http://127.0.0.1:8081/admin/v1/providers/qwen-llm \
  -d '{"name":"qwen-llm","type":"qwen","api_key":"{env.ALIYUN_API_KEY}","options":{"model":"qwen3.6-flash"}}'

curl -s -X PUT -H "Authorization: Bearer $ADMIN_API_KEY" \
  http://127.0.0.1:8081/admin/v1/settings -d '{"default_profile":"default"}'
```

A profile is flat and complete; omitted fields take their defaults:

```json
{
  "name": "default",
  "asr": "qwen-asr", "llm": "qwen-llm", "tts": "qwen-tts",
  "instructions": "You are a concise voice assistant.",
  "temperature": 0.8,
  "max_output_tokens": "inf",
  "output_modalities": ["audio"],
  "voice": "alloy",
  "speed": 1.0,
  "asr_language": "zh",
  "asr_model": "",
  "transcription": {},
  "turn_detection": { "type": "server_vad", "threshold": 0.5, "prefix_padding_ms": 300,
                      "silence_duration_ms": 500, "create_response": true, "interrupt_response": true }
}
```

`temperature` is optional: omit it (or send `null`) to leave sampling to the LLM provider's own
default — some models reject the parameter outright. `asr_language` / `asr_model` configure the ASR
stream. `transcription` is the protocol-visible toggle (`null` disables the
`conversation.item.input_audio_transcription.*` events; the internal ASR runs either way) and carries
`prompt`; its `language` / `model` are rejected in a profile so there is one place to look.

Writes are all-or-nothing: `copy → apply → validate the whole configuration → build providers →
persist → swap`. A rejected or unpersistable write changes neither memory nor the file. Rejections
name the offending field:

```json
{ "error": { "type": "invalid_request_error", "message": "must be within [0.25, 1.5], got 9",
             "param": "profiles.default.speed" } }
```

`400` for validation, `404` for a missing resource, `409` for deleting a provider a profile still
references (or the profile `default_profile` points at), `500` if the state file cannot be written.

Secrets: `api_key` accepts a literal or `{env.NAME}` and is stored verbatim (placeholders resolve when
a provider is built, so the state file can be inspected safely). Every read returns `"***"`; writing
`"***"` back keeps the stored secret. Request bodies are never logged.

## Observability

Set `observability.otel_endpoint` to an OTLP/HTTP collector (`"127.0.0.1:4318"`) to export spans
(`session → response → llm/tts`) and the core latency metrics (`cascade.*`: ASR first/final transcript,
commit→final, LLM TTFT, TTS first audio, end-to-end, interrupt latency, provider errors, session end
reasons); leave it empty to disable. Session spans and session metrics carry `cascade.profile` and the
`cascade.asr` / `cascade.llm` / `cascade.tts` instance names;
`cascade.admin.config_writes{cascade.result}` counts Admin writes by outcome. Logs are JSON on stdout
with secret fields redacted; provider lifecycle facts are logged at `debug`. Audio and transcripts are
never persisted; a session-end summary goes through the `Recorder` interface, which logs by default.

## Tests

```sh
go test -race ./...                                   # unit, admin, session, protocol conformance, server
CASCADE_E2E_LLM_MODEL=gpt-5-nano \
  DEEPGRAM_API_KEY=... OPENAI_API_KEY=... \
  go test -tags e2e -run TestE2E -v ./internal/server  # one real voice turn + interrupts (network)

# Qwen / Model Studio, real endpoints (network, billed)
ALIYUN_API_KEY=... go test -tags qwenreal -v ./internal/provider/qwen        # provider behaviour
ALIYUN_API_KEY=... go test -tags e2e -run TestE2EQwen -v ./internal/server   # Admin API → profile → voice turn,
                                                                            # and function calling end to end
ALIYUN_API_KEY=... go run ./hack/qwenbench -only e2e -turns 20               # gateway latency
```

Protocol conformance has two layers: deterministic control sequences are compared against full-order
golden traces; interleaved audio and transcript streams are checked against causal invariants
(`created` before any delta, in-order within a stream, every stream closed by `done`, no delta after
`cancelled`).

## Further reading

- [`docs/protocol-profile.md`](docs/protocol-profile.md) — the compatibility profile
- [`docs/decisions.md`](docs/decisions.md) — design decisions and known approximations
- [`docs/implementation-brief.md`](docs/implementation-brief.md) — scope, internal design, test strategy
- [`docs/qwen-latency.md`](docs/qwen-latency.md) — measured latency and the per-stage budget
- [`CLAUDE.md`](CLAUDE.md) — architecture rules

## License

Apache License 2.0. See [LICENSE](LICENSE).
