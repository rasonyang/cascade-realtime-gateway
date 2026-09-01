# Cascade

Repository: `cascade-realtime-gateway` · Module: `github.com/rasonyang/cascade-realtime-gateway` · Binary: `cascade`

OpenAI Realtime **GA**-compatible voice gateway. Speaks the OpenAI Realtime WebSocket protocol externally; runs a Go-native session engine internally with a cascaded ASR → LLM → TTS pipeline. The compatibility target is the GA compatibility profile defined in `docs/protocol-profile.md`, not full parity.

## Language

English is the default for everything — code, identifiers, comments, docs, commit messages, log messages, and conversation with the user — unless explicitly told otherwise.

## Stack

Go, net/http, WebSocket, slog, OpenTelemetry. Single binary, single config file, zero external dependencies. Databases, Redis, Kafka, microservices: not allowed without a proven need.

## Architecture

```
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

- The OpenAI protocol must not become the internal domain model. Internal state is authoritative; protocol events are a projection. `internal/session` must not import the protocol package; `internal/provider` must not import session or protocol.
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
- Plain interfaces plus a registry map. No plugin or reflection system.
- Provider specifics are normalized at the boundary and must never leak into the protocol layer.

## Streaming and back-pressure

- Fully streaming end to end: the LLM generates, the sentencer splits, and TTS synthesizes concurrently; never wait for the full reply.
- No server-side output pacing; deltas are sent as soon as available and playback is the client's responsibility.
- Back-pressure is handled per segment: inbound audio per the section above; LLM chunk stalls are bounded by ctx; a stalled outbound event queue means the client is gone — disconnect after a timeout.
- Slow output never blocks cancellation or shutdown.

## Configuration (Caddy-style)

- One config file is the sole source of configuration, mapped to one complete Go struct, validated as a whole at startup; a bad config fails fast and points at the offending field.
- `session_defaults` uses a subset of the GA session object shape.
- Secrets are injected through `{env.XXX}` placeholders so the config file can live in git; no encrypted-storage subsystem.
- Config is an immutable snapshot at Session creation; reload (optional, SIGHUP + atomic.Pointer swap) affects new sessions only.
- Provider-specific options are nested free-form blocks (json.RawMessage) parsed by each provider.

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
- Stable identifiers: session_id, response_id, item_id, provider.
