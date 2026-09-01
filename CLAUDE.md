# Cascade

Repository: `cascade-realtime-gateway` · Module: `github.com/rasonyang/cascade-realtime-gateway` · Binary: `cascade`

OpenAI Realtime **GA**-compatible voice gateway. Speaks the OpenAI Realtime WebSocket protocol externally; runs a Go-native session engine internally with a cascaded ASR → LLM → TTS pipeline. The compatibility target is the GA compatibility profile defined in `docs/protocol-profile.md`, not full parity.

## Language

English is the default for everything — code, identifiers, comments, docs, commit messages, log messages, and conversation with the user — unless explicitly told otherwise.

## Stack

Go, net/http, WebSocket, slog, OpenTelemetry. Single binary, single config file, zero external dependencies. Databases, Redis, Kafka, microservices: not allowed without a proven need.

## Architecture

```
Admin REST /admin/v1 → RuntimeConfig (atomic snapshot) → resolved per new connection
                                  ↓
WebSocket
  ↓
Protocol Adapter   ← protocol state: event/item/response IDs, serialization, profile validation, truncate conversion
  ↓ internal Command / Event (Go types)
Session (actor)    ← session state: Conversation, InputAudioBuffer, TurnManager, Response lifecycle
  ↓ spawned per generation
Response Pipeline  ← one goroutine group per response: LLM → sentencer → TTS, discarded when done
  ↓
Providers (ASR / LLM / TTS interfaces)
```

Iron rules:

- The OpenAI protocol must not become the internal domain model. Internal state is authoritative; protocol events are a projection. `internal/session` must not import the protocol or admin packages; `internal/provider` must not import session, protocol or admin; `internal/admin` must not import session, protocol or server. Only `internal/server` and `cmd/cascade` import admin.
- Only one protocol version is targeted: GA (`session.type="realtime"`, `output_modalities`, `session.audio.input/output`, `response.output_audio.delta`, etc.). Beta shapes are not supported. Event names, fields, ordering, and lifecycle follow the official docs — never guess. Unsupported fields/events are rejected with an `error` event by default, never silently ignored.
- Protocol conformance testing has two layers: deterministic control sequences are checked by full-order golden-trace comparison; non-deterministically interleaved streams (audio delta / transcript delta) are checked only against causal invariants — in-order within each stream, `created` before any delta, every stream closed by `done`, no new delta after `cancelled`.

## Concurrency model

- The Session is a single-goroutine actor that owns all mutable state. Callers post Commands; the actor emits Events. No locks, no shared state.
- Command (into the actor, expresses intent) and Event (out of the actor, states a fact) are two separate type sets; never mixed.
- Data plane uses typed channels; control plane is out-of-band (context cancellation + actor commands) and never queues behind back-pressured audio.
- Generation stamps apply only to events produced by pipeline goroutines (deltas, progress facts); the actor filters stale generations at ingress. Response terminal events (`response.done`, etc.) are emitted synchronously by the actor on FSM transition, carry no generation, and are never filtered. Inbound audio and control commands are outside generation filtering.
- Interrupt order is fixed: cancel ctx → mark FSM cancelled → actor emits terminal events synchronously → bump generation. Cancellation must propagate immediately and close provider streams; no stream may keep burning tokens. Generation filtering is only a race backstop, not a substitute for prompt cleanup.
- All WebSocket writes go through a single writer goroutine.

## Input audio

- Transport layer: the WS read loop never blocks; audio enters a bounded queue; a full queue is an explicit error and disconnect, never a silent drop.
- Domain layer: `InputAudioBuffer` holds uncommitted audio and owns append / clear / commit slicing, prefix-padding rollback, `audio_start_ms` / `audio_end_ms` computation, and the memory cap. All millisecond values come from it; VAD only reports relative offsets.

## Turns and interruption

- TurnManager is a pure state machine called by the actor: it takes VAD/ASR facts and client commands, and returns EmitSpeech / Interrupt / Commit / Trigger decisions. It starts no goroutines and never touches the conversation.
- turn_detection modes: `server_vad` / `semantic_vad` / `null`. Under `null`, VAD does not run, no speech events are emitted, and commit and response creation are entirely client-driven.
- In VAD modes, `create_response` and `interrupt_response` are orthogonal: `create_response=false` disables only auto-Trigger — **Commit still happens** and `input_audio_buffer.committed` is still emitted; `interrupt_response=false` disables only auto-interrupt — speech events are still emitted.
- Speech-start detection (the interrupt trigger) uses the engine's own acoustic VAD ahead of ASR and does not depend on the ASR vendor. Under `semantic_vad`, end-of-turn consumes ASR transcripts; this is an approximation and is recorded in `docs/decisions.md`.
- Cascade timing: Trigger emits `response.created` immediately, but the LLM must not start until the ASR Final for that turn's user item arrives (the `awaiting` phase). Commit calls `Finalize()` on the ASR stream to shorten the wait; on timeout, use the Partial if one exists, otherwise fail the response.
- Providers report facts only; they never mutate session state directly.
- After an interrupt, wait for the client's `conversation.item.truncate` with `audio_end_ms` and trim the item so the next turn's context contains only what the user actually heard. Cascaded TTS has no exact text↔audio alignment; text trimming is approximated by capability tier (provider alignment > per-sentence proportional > whole-response proportional), documented in `docs/decisions.md`.
- Distinguish generated / sent / played audio; played is known only via truncate reports.

## Response lifecycle

- The FSM uses only the five protocol-aligned states: `in_progress / completed / cancelled / incomplete / failed`.
- Pipeline progress is expressed with orthogonal flags (awaiting, llmDone, ttsDone, audioFlushed); all must be set for `completed`. No invented states such as `speaking`.
- `incomplete` maps to the protocol's "finished but not an error" outcomes (max_output_tokens, content_filter); the reason enum follows the official docs. Interruption always maps to `cancelled`, including interruption during the awaiting phase.

## Providers

- Three narrow interfaces: ASR (session-scoped long stream, with `Finalize` and an EndOfTurn event), LLM (request-scoped unidirectional stream), TTS (streaming audio output is mandatory; incremental text input and character alignment are optional capabilities — providers without them are downgraded to per-sentence requests inside their adapter). ctx is the only cancellation mechanism.
- Plain interfaces plus a registry map. No plugin or reflection system. A provider *instance* is a named (type, api_key, options) triple in the runtime config; a *type* is a registry entry, and the roles it is registered for decide which profile slots may reference it.
- Provider specifics are normalized at the boundary and must never leak into the protocol layer.

## Streaming and back-pressure

- Fully streaming end to end: the LLM generates, the sentencer splits, and TTS synthesizes concurrently; never wait for the full reply.
- No server-side output pacing; deltas are sent as soon as available and playback is the client's responsibility.
- Back-pressure is handled per segment: inbound audio per the section above; LLM chunk stalls are bounded by ctx; a stalled outbound event queue means the client is gone — disconnect after a timeout.
- Slow output never blocks cancellation or shutdown.

## Configuration (Caddy-style)

Configuration is split in two, by lifetime:

**Static config** — one file, the sole source of everything fixed for the life of the process: `listen`, `auth.api_key`, `auth.admin_api_key`, `admin.listen`, `admin.state_file`, limits/timeouts, logging, OTel, and the optional `bootstrap` seed. Mapped to one complete Go struct, validated as a whole at startup; a bad config fails fast and points at the offending field. Secrets are injected through `{env.XXX}` placeholders so the file can live in git; no encrypted-storage subsystem. The `bootstrap` block is exempt from placeholder expansion — it is runtime config, and the state file must keep `{env.XXX}` verbatim.

**Runtime config** — provider instances, profiles and `settings`, owned exclusively by the Admin REST API. Never edited through the config file (except the one-time `bootstrap` seed) and never through the Realtime WebSocket.

- No database. One immutable `RuntimeConfig` behind an `atomic.Pointer` (lock-free reads), writes serialized by a mutex, persisted to a single JSON state file (temp → fsync → rename, mode `0600`).
- Every write is `copy current → apply → validate the whole RuntimeConfig → build providers → persist → atomic swap`. A failure at any step leaves both memory and the file untouched.
- The state file wins when it exists; `bootstrap` is applied only on a first start, and is persisted immediately. A state file that cannot be read, decoded, validated or built is a startup failure, never a silent fallback.
- Validation is shared with the session/protocol layers, not duplicated: unknown provider type → 400, profile referencing a missing instance → 400, provider that does not fill the role → 400, unknown `default_profile` → 400, deleting a referenced resource → 409. Every error names a dotted field path.
- Secrets: `api_key` is a literal or `{env.X}`, stored verbatim; every read returns `"***"`; writing `"***"` back keeps the stored secret. Secrets never reach logs or the Recorder, and request bodies are never logged.
- A profile's JSON shape is flat (`instructions`, `temperature`, `max_output_tokens`, `output_modalities`, `voice`, `speed`, `asr_language`, `asr_model`, `transcription`, `turn_detection`); `temperature` is nullable and unset by default, so nothing is sent unless a profile asks for it — that is the Admin resource model, not the GA session object. `Profile.SessionDefaults()` projects it onto the GA-shaped snapshot the session and protocol layers consume; audio formats are fixed constants.
- Without a `default_profile` the gateway starts and refuses new `/v1/realtime` connections with 503.
- A Session takes a configuration snapshot when the connection opens; later Admin writes affect only later sessions.
- The Admin API is guarded by `ADMIN_API_KEY` (Bearer), a credential distinct from `REALTIME_API_KEY`; unset disables the Admin API entirely. Bind `admin.listen` to localhost or a private network.
- Provider-specific options are nested free-form blocks (json.RawMessage) parsed and validated by each provider adapter; they never leak into the generic interfaces.
- The Realtime protocol is unchanged by any of this: `?model=` stays accept-and-echo and `session.update` gains no fields.

## State and persistence

- One call = one Realtime WebSocket connection = exactly one Session actor. `session_id` is the call identifier; `max_sessions` is the concurrent-call limit. Nothing is shared across sessions. Telephony bridges (SIP/PSTN) are external clients that open one connection per call.
- Session state lives in memory only; lifetime = connection; nothing is written to disk (the protocol's sessions are not resumable anyway).
- No persistence in v1. Recording needs go through the narrow `Recorder.SessionEnded(ctx, summary)` interface; the v1 implementation logs via slog, called asynchronously after session end, off the hot path.

## Security

- Gateway auth: static `REALTIME_API_KEY` Bearer token.
- Logs redact secret fields; raw audio and plaintext credentials are never logged.

## Reliability

- Every resource has explicit ownership and cleanup. On disconnect: cancel session ctx → close provider streams → reclaim all goroutines. No orphan goroutines.
- Goroutine count is bounded and lifetime-bound: a few fixed session-level roles (actor loop, ASR reader, WS reader/writer); response-level goroutines are spawned with the pipeline and reclaimed with the response. No hard-coded counts, no resident pools.

## Observability

- Spans stop at the response level (session → response → llm/tts children); frame-level activity is metrics only, with frame logging off by default.
- Core metrics: ASR first/final transcript latency, commit→final latency, LLM TTFT, TTS first-audio latency, E2E latency, interrupt latency, provider errors, session end reason.
- Stable identifiers: session_id, response_id, item_id, provider. Session spans and session metrics also carry the profile and the three provider instance names.
- Admin writes log one INFO line (method, path, resource, status, `X-Request-ID`) and never the request body; `cascade.admin.config_writes{cascade.result}` counts them by outcome.
