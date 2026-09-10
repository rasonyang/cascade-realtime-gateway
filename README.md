# Cascade

OpenAI Realtime-compatible gateway over a cascaded ASR → LLM → TTS pipeline.

```sh
go build -o cascade ./cmd/cascade
REALTIME_API_KEY=... ADMIN_API_KEY=... DEEPGRAM_API_KEY=... OPENAI_API_KEY=... ./cascade -config config.example.json
./cascade -config config.example.json -check   # validate the configuration and exit
```

Clients connect to `ws://<listen>/v1/realtime?model=<any>` with `Authorization: Bearer $REALTIME_API_KEY`
and speak the OpenAI Realtime GA protocol (`session.type = "realtime"`). Cascade serves plain WebSocket;
put a TLS terminator in front of it for `wss://` (the official OpenAI SDKs require `wss:`).

## Function calling

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
reassembled at the provider boundary, which also means Cascade emits no `arguments.done` for a call
that was cancelled or truncated mid-arguments. At most one call per response. `arguments` is a raw
JSON string Cascade never parses, so a client must still handle a parse failure. The full rules,
including every rejection's field path, are in `docs/protocol-profile.md`.

Architecture rules live in `CLAUDE.md`; the implementation plan in `docs/implementation-brief.md`;
the protocol compatibility profile in `docs/protocol-profile.md`.

## Configuration

Configuration is split by lifetime.

**Static** — the config file, read once at startup: `listen`, `auth.api_key`, `auth.admin_api_key`,
`admin.listen`, `admin.state_file`, `limits`, `observability`, and the optional `bootstrap` seed.
`{env.NAME}` placeholders are expanded at load, except inside `bootstrap`.

**Runtime** — provider instances, profiles and `settings.default_profile`, owned by the Admin REST API
and stored in a single JSON state file (`admin.state_file`, mode `0600`). There is no database. The
state file wins when it exists; `bootstrap` seeds it only on a first start. Without a `default_profile`
the gateway starts but answers `/v1/realtime` with `503`.

A session takes a configuration snapshot when its connection opens, so an Admin write affects only
connections opened after it.

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

`temperature` is optional: omit it (or send `null`) to leave sampling to the LLM provider's own default,
which is what Cascade did before profiles existed — some models reject the parameter outright.
`asr_language` / `asr_model` configure the ASR stream. `transcription` is the protocol-visible toggle
(`null` disables the `conversation.item.input_audio_transcription.*` events; the internal ASR runs
either way) and carries `prompt`; its `language` / `model` are rejected in a profile so there is one
place to look.

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

## Providers

Each provider instance names a registered type and carries that type's own `options` block.

`options` for `openai` LLM: `model`, `base_url`, `request_timeout`, `stream_idle_timeout`, and
`reasoning_effort` (sent only when set; e.g. `"minimal"` on gpt-5 models, `"none"` where the model
supports it — non-reasoning models reject the field). `openai` TTS: `model`, `base_url`, timeouts.
`deepgram` ASR: `model`, `language`, `smart_format`, `endpointing_ms`, `utterance_end_ms`,
`connect_timeout`, `finalize_timeout`, `keepalive_interval`, `audio_queue_frames`.
`qwen` (all three roles): see `internal/provider/qwen` and `docs/qwen-latency.md`.

A manual check against the official OpenAI SDK lives in `hack/sdkcheck/`.

## Observability

Set `observability.otel_endpoint` to an OTLP/HTTP collector (`"127.0.0.1:4318"`) to export spans
(`session → response → llm/tts`) and the core latency metrics (`cascade.*`); leave it empty to disable.
Session spans and session metrics carry `cascade.profile` and the `cascade.asr` / `cascade.llm` /
`cascade.tts` instance names; `cascade.admin.config_writes{cascade.result}` counts Admin writes by
outcome. Logs are JSON on stdout with secret fields redacted; provider lifecycle facts are logged at
`debug`.

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

## License

Apache License 2.0. See [LICENSE](LICENSE).
