# Cascade — Implementation Brief (v2)

> Usage: the repository root already contains `CLAUDE.md` (architecture iron rules, authoritative). This file is the implementation brief, given to Claude Code as the first prompt. Where the two conflict, `CLAUDE.md` wins.
>
> v2 changes: freeze the GA protocol version and rewrite wire/config shapes; fix TurnManager commit and manual-mode semantics; LLM start now waits for ASR Final; add `Finalize` to ASRStream; introduce a logical InputAudioBuffer; resolve the Generation vs. `response.done` contradiction; define the truncate text approximation; replace "any SDK, just change the base URL" with a compatibility profile.

---

## 0. Your role and working rules

You are the implementation engineer on this project. The architecture is already designed; your job is to turn it into runnable, tested Go code.

Rules:

1. **Read `CLAUDE.md` first, then this file, then start.** At the beginning of each phase, restate that phase's constraints; at the end, self-check against its acceptance criteria.
2. **English by default** for code, comments, docs, commit messages, logs, and replies to me, unless I say otherwise.
3. **The protocol target is OpenAI Realtime GA, and only that version.** Markers: `session.type = "realtime"`, `output_modalities`, audio config under `session.audio.input / session.audio.output`, event names such as `response.output_audio.delta` / `response.output_audio_transcript.delta` / `response.output_text.delta`. **Beta shapes are not supported** (`modalities`, top-level `voice`, top-level `turn_detection`, `response.audio.delta`). The first step of Phase 2 is to transcribe the official client/server events and session object reference into `docs/protocol-profile.md` (§5.4). If any field is uncertain, write `// PROTOCOL-VERIFY:` and stop to ask me. Never guess.
4. **Minimalism first.** Any abstraction, config option, dependency, or goroutine not required by this file or `CLAUDE.md` is forbidden. Before adding one, ask: "which proven need forces this?"
5. **Idiomatic Go.** Standard library first. Allowed third-party dependencies: one WebSocket library (`github.com/coder/websocket` or `gorilla/websocket`, pick one) and the OTel SDK. Ask before adding anything else.
6. **Every phase must compile, test, and be committable.** `go build ./... && go vet ./... && go test -race ./...` must be green before a phase is considered done.
7. **No magic numbers.** Queue sizes, buffer caps, timeouts all come from config, with defaults defined in config.
8. **Package doc comments** state the package's position in the layering and which packages it must not import.

---

## 1. Goal and scope

### Naming

- Product name: **Cascade**
- Repository: `cascade-realtime-gateway`
- Go module: `github.com/rasonyang/cascade-realtime-gateway`
- Binary: `cascade` (built from `cmd/cascade`)
- Tagline (README first line): "OpenAI Realtime-compatible gateway over a cascaded ASR → LLM → TTS pipeline"

### Goal

A single-binary Go service exposing the OpenAI Realtime **GA** WebSocket protocol (`/v1/realtime`), driving voice conversations internally through a cascaded ASR → LLM → TTS pipeline. The target is an **OpenAI Realtime GA compatibility profile** (the supported subset defined in §5.4), not full parity.

### In scope (v1)

- Transport: WebSocket, Bearer auth; the `?model=` query parameter is accepted and echoed, not used for routing.
- Audio: input and output are `audio/pcm`, 24 kHz, mono, base64 (GA default).
- Modalities: `output_modalities` is `["audio"]` or `["text"]` (mutually exclusive in GA).
- turn_detection (under `session.audio.input.turn_detection`): `server_vad`, `semantic_vad` (approximated, §5.3), `null`. Includes the `create_response` / `interrupt_response` switches.
- Input transcription: when `session.audio.input.transcription` is enabled, emit `conversation.item.input_audio_transcription.*` events.
- Interruption and `conversation.item.truncate`.
- Session defaults from the config file; `session.update` may override within the profile.
- Providers: mock (all three) plus one real provider of each kind (§6).
- Observability: slog, OTel spans down to response level, core latency metrics.

### Out of scope (v1)

- Tools / function calling, out-of-band responses, image input, `prompt` references, `tracing`, `truncation` policy config.
- Other audio encodings (g711, etc.); the `format.type` field is reserved but only `audio/pcm` is implemented.
- WebRTC, SIP, `/v1/realtime/client_secrets`, `/v1/realtime/calls`.
- Persistence, multi-instance, hot reload.
- Server-side audio pacing.
- Plugin systems, reflection-based registration.

---

## 2. Repository layout

```
cmd/cascade/main.go              # entry: load config → validate → start http server
internal/config/                 # Config struct, {env.*} expansion, Validate
internal/protocol/               # GA protocol: wire types, codec, Adapter, ID management, profile validation
internal/session/                # domain core: Session actor, Conversation, InputAudioBuffer, TurnManager, Response FSM, Pipeline
internal/audio/                  # pcm utilities, ms↔byte conversion
internal/vad/                    # VAD interface + energy VAD (pure Go)
internal/provider/               # ASR/LLM/TTS interfaces, registry, shared types
internal/provider/mock/          # deterministic mock implementations
internal/provider/<name>/        # one package per real provider
internal/recorder/               # Recorder interface + slog implementation
internal/observability/          # slog setup, redaction, OTel setup, metric definitions
internal/server/                 # http handler, auth, WS upgrade, connection lifecycle, single writer
docs/protocol-profile.md         # compatibility profile: supported / rejected / ignored events and fields
docs/decisions.md                # decisions made during implementation (one line each)
config.example.json
```

**Import direction iron rule** (enforced by an `internal/arch_test.go` that scans the import graph):

```
server → protocol → session → provider / vad / audio
session must not import protocol or server
provider must not import session or protocol
```

---

## 3. Core types

### 3.1 Command / Event (`internal/session`)

Two sealed type sets, each behind an unexported-marker interface.

Command (into the actor, expresses intent):

```
CmdUpdateSession{Patch SessionPatch}
CmdAppendAudio{PCM []byte}            // travels on the dedicated bounded audio queue, not the command channel
CmdCommitAudio
CmdClearAudio
CmdCreateItem{Item ItemSpec, PreviousItemID ItemRef}
CmdDeleteItem{ID ItemRef}
CmdTruncateItem{ID ItemRef, AudioEndMs int}
CmdCreateResponse{Overrides ResponseOverrides}
CmdCancelResponse
CmdClose{Reason CloseReason}
```

Event (out of the actor, states a fact):

```
EvSessionUpdated{Config SessionConfig}
EvError{Code, Message string, Fatal bool}
EvAudioBufferCommitted{Item ItemRef, PreviousItem ItemRef}
EvAudioBufferCleared
EvSpeechStarted{AudioStartMs int, Item ItemRef}
EvSpeechStopped{AudioEndMs int, Item ItemRef}
EvItemAdded{Item Item} / EvItemDone{Item Item}
EvItemDeleted{ID ItemRef}
EvItemTruncated{ID ItemRef, ContentIndex int, AudioEndMs int}
EvInputTranscriptDelta / EvInputTranscriptDone{Item ItemRef, Text string}
EvResponseCreated{Resp ResponseRef}
EvOutputItemAdded{Resp ResponseRef, Item Item}
EvOutputTextDelta / EvOutputAudioTranscriptDelta{Resp, Item, Gen, Delta string}
EvOutputAudioDelta{Resp, Item, Gen, PCM []byte}
EvOutputAudioDone / EvOutputTextDone / EvOutputItemDone{...}
EvResponseDone{Resp ResponseRef, Status ResponseStatus, Reason StatusReason, Usage Usage}
EvSessionClosed{Reason CloseReason}
```

`ItemRef` / `ResponseRef` are opaque internal handles; the protocol-side `item_xxx` / `resp_xxx` strings are maintained in the adapter's mapping tables.

### 3.2 Generation and terminal events (corrected)

```go
type Generation uint64
```

Exact rules:

- **Gen is stamped only on events produced by pipeline goroutines** (text/transcript/audio deltas and progress facts such as llmDone/ttsDone/audioDone). They flow back through `s.respEvents`; the actor compares `ev.Gen == s.activeGen` at ingress and drops mismatches.
- **Response terminal events (`EvResponseDone`, `EvOutputItemDone`, etc.) are always emitted synchronously by the actor on FSM transition**, bypass `respEvents`, carry no Gen, and are therefore never filtered.
- Fixed interrupt order: `resp.cancel()` → FSM marks Cancelled → actor synchronously emits `EvOutputAudioDone / EvOutputItemDone / EvResponseDone{Cancelled}` → `s.activeGen++`. Any late pipeline deltas are then dropped by the Gen mismatch.
- Inbound audio and control commands carry no Gen.

### 3.3 Provider interfaces (`internal/provider`)

```go
// ASR: session-scoped long-lived stream
type ASR interface {
    OpenStream(ctx context.Context, cfg ASRConfig) (ASRStream, error)
}
type ASRStream interface {
    PushAudio(pcm []byte) error
    Finalize() error            // "the current utterance has ended, produce a Final ASAP" (Deepgram: Finalize message; providers without it may flush/send silence or no-op, but MUST guarantee a subsequent Final or an explicit EndOfTurn event)
    Events() <-chan ASREvent    // Partial{Text} | Final{Text, StartMs, EndMs} | EndOfTurn | Error{Err}
    Close() error
}

// LLM: request-scoped unidirectional stream
type LLM interface {
    Chat(ctx context.Context, req ChatRequest) (<-chan LLMChunk, error)
    // LLMChunk: TextDelta | Done{FinishReason, Usage} | Error
}

// TTS: streaming audio output is mandatory; incremental text input and alignment are optional
type TTS interface {
    Synthesize(ctx context.Context, cfg TTSConfig) (TTSStream, error)
    Capabilities() TTSCaps      // {IncrementalText bool, Alignment bool}
}
type TTSStream interface {
    WriteText(s string) error   // providers without incremental input: the adapter issues one request per sentence internally and concatenates the output
    EndInput() error
    ReadAudio() (AudioChunk, error) // AudioChunk{PCM []byte, Alignment []CharTiming (may be nil)}; io.EOF on completion; must return err promptly after ctx cancellation
    Close() error
}
```

Registry: `map[string]Factory`, `Factory func(raw json.RawMessage) (Provider, error)`, `Provider` exposes `Kind()`. `Register()` from `init()` is acceptable; no reflection.

### 3.4 VAD (`internal/vad`)

```go
type Detector interface {
    Process(pcm []byte) []Event // SpeechStart{OffsetMs} | SpeechEnd{OffsetMs}
    Reset()
}
```

v1: energy/RMS with hysteresis and a silence window, pure Go, parameters from `turn_detection` (`threshold`, `prefix_padding_ms`, `silence_duration_ms`). **VAD is not instantiated in manual mode.**

### 3.5 InputAudioBuffer (new, `internal/session`)

The transport-level bounded queue `audioIn` exists only so the WS read loop never blocks; the domain layer has a separate logical buffer:

```go
type inputAudioBuffer struct {
    pcm      []byte   // all uncommitted audio since the last commit/clear
    baseMs   int      // offset of the buffer start on the session timeline, used for speech_started.audio_start_ms
    maxBytes int      // from config.limits.input_audio_buffer_max_ms
}
```

Responsibilities: `append`; `clear`; `commit` turns `[start, end)` into a user item and clears; on VAD `SpeechStart`, roll the start back by `prefix_padding_ms`; on VAD `SpeechEnd`, advance the end by `silence_duration_ms`; exceeding `maxBytes` → `EvError{Code: input_audio_buffer_overflow, Fatal: true}` and close the session (never silently drop). All millisecond event values are computed by this buffer, not by VAD directly.

### 3.6 TurnManager (corrected)

Pure state machine; starts no goroutines, never touches the conversation:

```go
type TurnInput interface{} // vadSpeechStart | vadSpeechEnd | asrFinal | asrEndOfTurn | clientCommit | clientCreateResponse | responseStarted | responseEnded
type TurnDecision struct {
    EmitSpeechStarted, EmitSpeechStopped bool
    Interrupt bool // cancel the current in_progress response
    Commit    bool // commit InputAudioBuffer to a user item and call Finalize on ASR
    Trigger   bool // create a response (the LLM still waits for ASR Final, see §4)
}
func (t *TurnManager) Step(in TurnInput) TurnDecision
```

Mode table (must be fully covered by table-driven tests):

| mode | create_response | interrupt_response | VAD start | VAD end |
|---|---|---|---|---|
| manual (`null`) | — | — | VAD not running, no speech events | — ; Commit/Trigger only in response to client `input_audio_buffer.commit` / `response.create` |
| server_vad | true | true | EmitSpeechStarted + Interrupt (if a response is in_progress) | EmitSpeechStopped + **Commit** + Trigger |
| server_vad | true | false | EmitSpeechStarted, no Interrupt | EmitSpeechStopped + Commit + Trigger |
| server_vad | false | true | EmitSpeechStarted + Interrupt | EmitSpeechStopped + **Commit, no Trigger** |
| server_vad | false | false | EmitSpeechStarted | EmitSpeechStopped + Commit, no Trigger |
| semantic_vad | same switches | same | same as server_vad | EmitSpeechStopped; Commit/Trigger deferred until `asrEndOfTurn` (§5.3) |

Key points: **`create_response=false` disables only auto-Trigger, not Commit**; `input_audio_buffer.committed` is still emitted. `interrupt_response` is meaningless in manual mode. A client `response.create` while a response is already in_progress returns an error per GA semantics (recorded in the profile).

### 3.7 Response FSM

```go
type ResponseStatus int // InProgress | Completed | Cancelled | Incomplete | Failed
type response struct {
    ref      ResponseRef
    gen      Generation
    status   ResponseStatus
    awaiting bool                          // created, waiting for the user item's transcript Final before starting the LLM
    llmDone, ttsDone, audioFlushed bool    // all true and no error → Completed
    cancel   context.CancelFunc
    item     ItemRef
    usage    Usage
    segments []audioSegment               // for truncate: each {textStart, textEnd int; audioStartByte, audioEndByte int}
}
```

- Interrupt / `response.cancel` → Cancelled, never Incomplete; interruption during `awaiting` is also Cancelled.
- LLM `finish_reason=length` → Incomplete{max_output_tokens}; content_filter → Incomplete{content_filter} (reason enum values copied from the GA reference).
- Provider error → Failed, with an `error` event.
- With `output_modalities=["text"]`, ttsDone/audioFlushed are considered satisfied.

---

## 4. Session actor main loop

```go
func (s *Session) run(ctx context.Context) {
    defer s.cleanup()
    for {
        select {
        case <-ctx.Done(): return
        case cmd := <-s.cmds:       s.handleCommand(cmd)
        case pcm := <-s.audioIn:    s.handleAudio(pcm)    // buffer.append → VAD → ASR push → TurnManager.Step
        case ev  := <-s.asrEvents:  s.handleASR(ev)       // transcript → item text → TurnManager.Step / start awaiting response
        case ev  := <-s.respEvents: s.handleResponseEvent(ev) // Gen filter first
        }
    }
}
```

Requirements:

- All mutable state lives in `Session` fields and is read/written only from the `run` goroutine. Other goroutines may only post to channels.
- `s.audioIn` is bounded (config). If the WS read loop finds it full: post `CmdClose{Reason: InputOverflow}` and disconnect — no drop, no block.
- **Response start is two-phase (fixes the timing hole)**:
  1. `Trigger` or `CmdCreateResponse` → immediately allocate a ResponseRef, emit `EvResponseCreated`, FSM enters InProgress with `awaiting=true`.
  2. When the transcript of the latest user item this response depends on reaches Final (or the item is a text item) → `awaiting=false`; only then `s.activeGen++` and spawn the pipeline goroutines. On wait timeout (config `asr_final_timeout`): start with the Partial if one exists and log a warning; otherwise Failed.
  3. Commit calls `Finalize()` on ASR to shorten the wait.
- `startPipeline`: `context.WithCancel(s.ctx)`, spawn the LLM / sentencer / TTS goroutines, events flow back via `s.respEvents`; a `sync.WaitGroup` guarantees all three have exited after any terminal state (tests assert the goroutine count returns to baseline).
- `interrupt`: fixed order per §3.2.
- `cleanup`: cancel the active response → close the ASR stream → wait for all child goroutines → call `Recorder.SessionEnded` asynchronously.

Sentencer: split on sentence-ending punctuation plus a minimum length; emit text/transcript deltas at LLM-chunk granularity without waiting for a full sentence. Record each sentence's `[textStart, textEnd)` in the full text and pass it to the TTS stage for §5.5 alignment. No NLP dependencies.

---

## 5. Protocol Adapter (`internal/protocol`)

### 5.1 Responsibilities

1. Inbound: JSON → wire struct → profile validation → internal Command. `input_audio_buffer.append` decodes base64 and posts to the audio queue.
2. Outbound: internal Event → one or more GA wire events.
3. IDs: generate `event_id` / `item_xxx` / `resp_xxx`, maintain bidirectional mapping tables.
4. Truncate: `audio_end_ms` → byte offset (24000 × 2 × ms / 1000), post `CmdTruncateItem`.
5. Errors: protocol validation failures emit an `error` event (with `event_id` back-reference) without disconnecting; fatal errors emit `error` then disconnect.

### 5.2 Single writer

Only one goroutine in the server layer calls `conn.Write`. A full outbound channel in the adapter means the client is gone; disconnect after the configured timeout.

### 5.3 semantic_vad approximation

Official semantic_vad uses a model to judge whether the user has finished. v1 approximation: after VAD end, wait for the ASR `Final`; if the text ends with sentence-ending punctuation, declare `asrEndOfTurn` immediately; otherwise apply an additional silence threshold mapped from `eagerness` (low/medium/high/auto). Record in `docs/decisions.md`; `session.updated` echoes `semantic_vad` faithfully.

### 5.4 Compatibility profile (`docs/protocol-profile.md`, first deliverable of Phase 2)

Three tables, one row per field/event:

- **Client events**: `session.update`, `input_audio_buffer.append/commit/clear`, `conversation.item.create/delete/truncate/retrieve`, `response.create/cancel` → each marked Supported / Rejected (error code) / Accepted-but-ignored.
- **Session fields**: `type`, `model`, `instructions`, `output_modalities`, `audio.input.{format,transcription,turn_detection,noise_reduction}`, `audio.output.{format,voice,speed}`, `max_output_tokens`, `tools`, `tool_choice`, `prompt`, `tracing`, `truncation`, `include` → each marked with supported range / rejected / ignored, and what `session.updated` echoes.
- **Server events**: every event name v1 emits and its trigger condition; explicitly list events never emitted (tool-related, whether `rate_limits.updated` is sent, etc.).

A `session.update` carrying unsupported fields is rejected with `error` by default, never silently ignored; exceptions go in `docs/decisions.md`.

### 5.5 Truncate text-trimming strategy (explicit approximation)

Cascaded TTS has no exact text↔audio alignment. Three tiers by capability, all driven by `segments` in the session:

1. **Provider alignment available** (`TTSCaps.Alignment=true`, e.g. ElevenLabs character timestamps): locate the exact character at `audio_end_ms` and trim there.
2. **Per-sentence request mode** (`IncrementalText=false`): each sentence's audio range is known. Keep every sentence with `audioEndByte ≤ cut point`; for the sentence straddling the cut, trim its text proportionally by rune count using `(cut − audioStartByte) / (audioEndByte − audioStartByte)`.
3. **Incremental input without alignment**: treat the whole response as one segment and trim generated text proportionally to generated audio.

All three tiers are declared approximations in `docs/decisions.md`. Tests assert only: the trimmed text is a prefix of the original; its length is monotonic in `audio_end_ms`; no trimming when `audio_end_ms ≥ total duration`.

---

## 6. Providers

Implementation order:

1. **mock**:
   - ASR: scripted; emits Partial/Final after N ms of audio; `Finalize()` emits Final immediately; injectable errors and delays; records the moment ctx is cancelled.
   - LLM: scripted token-by-token; configurable finish_reason, per-token delay, and blocking at token k until ctx is cancelled.
   - TTS: fixed-length pcm per sentence; configurable first-chunk delay; both `IncrementalText` and `Alignment` are toggleable to cover all three §5.5 tiers.
2. **LLM: `openai`** (OpenAI-compatible Chat Completions streaming, configurable `base_url`).
3. **ASR: `deepgram`** (WebSocket streaming; `Finalize` maps to Deepgram's `Finalize` message; `EndOfTurn` maps to its `speech_final` / `UtteranceEnd`).
4. **TTS: `openai`** (HTTP streaming, `IncrementalText=false, Alignment=false`). Later: `elevenlabs` (WS, `IncrementalText=true, Alignment=true`).

Each real provider: parses only its own `json.RawMessage` inside its package; timeouts from config; ctx cancellation closes connections immediately; errors normalized to `provider.Error{Provider, Kind(Auth|RateLimit|Transient|Fatal), Err}`; provider response structs never leave the package.

---

## 7. Configuration

`session_defaults` uses a subset of the GA session object shape directly, saving a translation layer:

```json
{
  "listen": ":8080",
  "auth": { "api_key": "{env.REALTIME_API_KEY}" },
  "providers": {
    "asr": { "type": "deepgram", "api_key": "{env.DEEPGRAM_API_KEY}", "options": { "model": "nova-3" } },
    "llm": { "type": "openai",   "api_key": "{env.OPENAI_API_KEY}",   "options": { "model": "gpt-4o-mini", "base_url": "https://api.openai.com/v1" } },
    "tts": { "type": "openai",   "api_key": "{env.OPENAI_API_KEY}",   "options": { "model": "tts-1" } }
  },
  "session_defaults": {
    "instructions": "You are a helpful assistant.",
    "output_modalities": ["audio"],
    "audio": {
      "input": {
        "format": { "type": "audio/pcm", "rate": 24000 },
        "transcription": { "enabled": true },
        "turn_detection": {
          "type": "server_vad", "threshold": 0.5, "prefix_padding_ms": 300, "silence_duration_ms": 500,
          "create_response": true, "interrupt_response": true
        }
      },
      "output": { "format": { "type": "audio/pcm", "rate": 24000 }, "voice": "alloy" }
    },
    "max_output_tokens": "inf"
  },
  "limits": {
    "max_sessions": 100,
    "session_timeout": "30m",
    "input_audio_queue_frames": 200,
    "input_audio_buffer_max_ms": 60000,
    "asr_final_timeout": "2s",
    "output_event_queue": 256,
    "client_write_timeout": "10s"
  },
  "observability": { "log_level": "info", "otel_endpoint": "" }
}
```

- `config.Load(path)`: read file → expand `{env.X}` (a missing variable is an error naming the field path) → unmarshal into the complete struct with `DisallowUnknownFields` → `Validate()` returns errors of the form `field "providers.llm.type": unknown provider "foo"`.
- On Session creation, deep-copy `session_defaults` into an immutable snapshot; `session.update` patches the snapshot with GA merge semantics.
- No hot reload in v1; leave an `atomic.Pointer[Config]` hook only.

---

## 8. Testing strategy

### 8.1 Unit

- `TurnManager`: table tests over every §3.6 mode × event sequence; specifically assert that `create_response=false` still Commits and that manual mode emits no speech events.
- Response FSM: every terminal path, including interruption during `awaiting`.
- Sentencer: punctuation, abbreviations, long streams without punctuation, correct `[textStart, textEnd)`.
- `inputAudioBuffer`: prefix-padding rollback, clear, commit slicing, overflow.
- Energy VAD: silence / burst / hysteresis.
- Truncate three-tier strategy (§5.5 invariants).
- Config expansion and Validate error messages.

### 8.2 Session level (mock providers, no network)

- Normal turn: commit → `Finalize` called → `response.created` precedes LLM start → LLM request messages contain the ASR Final text → completed with all flags set.
- Interrupt: VAD start while the LLM is blocked at token k → assert Cancelled, **`response.done{cancelled}` was emitted and not Gen-filtered**, stale-gen deltas dropped, all three goroutines exited, provider ctx cancelled.
- Interrupt during `awaiting`: no pipeline started, straight to Cancelled.
- After truncate, the conversation item's text/audio is trimmed and the next LLM request's messages contain only the trimmed text.
- Audio queue overflow and buffer overflow → session closes with the corresponding reason, nothing dropped.
- Disconnect cleanup: goroutine count returns to baseline.

### 8.3 Protocol conformance (`internal/protocol`)

- **Golden trace**: deterministic control sequences (`session.update` → `conversation.item.create` → `response.create` (text-only, fixed mock output) → `response.cancel`, etc.) recorded as `testdata/*.golden.jsonl`, compared in full order (IDs/timestamps normalized).
- **Causal invariants**: for streams with deltas, assert only: in-order within each stream; `response.created` precedes any delta; every `*.delta` stream has a matching `*.done`; no new delta for the same response_id after `response.done{cancelled}`; `error.event_id` back-references correctly; `input_audio_buffer.committed` still appears with `create_response=false`.
- **Profile conformance**: for every "rejected" field/event in the profile table, send one and assert the corresponding error code; for every "ignored" field, assert the `session.updated` echo matches the table.

Golden files are generated only after I have approved `docs/protocol-profile.md`.

### 8.4 End to end (`-tags e2e`)

Real providers, secrets from env, one "hello" round trip. Skipped in CI by default.

---

## 9. Phases and acceptance

Execute in order. At the end of each phase, stop and show me a diff summary and test output; wait for my confirmation before starting the next phase.

**Phase 0 — Skeleton**
- Directory layout, `go.mod`, `arch_test.go`, complete config package (Load/expansion/Validate + tests), slog + redaction, `cmd/cascade` starts and rejects bad configs.
- Acceptance: `./cascade -config bad.json` prints a field-pointing error and exits 1.

**Phase 1 — Domain core (no network)**
- Provider interfaces + the three mocks; session package: Command/Event, Conversation, InputAudioBuffer, TurnManager, Response FSM (with awaiting), Pipeline, actor loop, cleanup, three-tier truncate; energy VAD.
- Acceptance: all of §8.1 + §8.2 pass, `-race` clean.

**Phase 2 — Protocol layer**
- First produce `docs/protocol-profile.md` for my approval → wire types → bidirectional Adapter → ID tables → truncate conversion → golden and invariant tests.
- Acceptance: all three §8.3 layers pass; a text-only conversation works manually with the official GA SDK (beta header removed) or `websocat`.

**Phase 3 — Server**
- http handler, Bearer auth, `?model=` handling, WS upgrade, read loop (never blocks), single writer, timeout disconnect, max_sessions, session_timeout, graceful shutdown.
- Acceptance: connection-leak test; 100 concurrent mock sessions interrupted 1000 times with no goroutine growth.

**Phase 4 — Real providers**
- `openai` LLM → `deepgram` ASR → `openai` TTS.
- Acceptance: one real e2e voice turn; provider connections close within ≤ 200 ms after an interrupt (proven by log timestamps); commit → ASR Final latency is a metric.

**Phase 5 — Observability wrap-up**
- OTel spans: session → response → llm/tts; metrics: ASR first/final transcript latency, commit→final latency, LLM TTFT, TTS first audio, E2E, interrupt latency, provider error count, session end reason; `Recorder` slog implementation.
- Acceptance: the span tree and metrics are visible in a local collector; no frame-level spans, no frame-level logging by default.

---

## 10. Start now

Begin with Phase 0. First step: read `CLAUDE.md`, then list the files you intend to create and the complete field definition of the `Config` struct for my approval before writing code.

---

## Addendum — Phase 0 approval adjustments (2026-09-01)

1. Adopt the GA shape for `audio.input.transcription`; remove the non-GA `enabled` field. Treat it as nullable: `null` disables protocol transcription events, while an object enables them. Do not freeze the exact field set yet; Phase 2 must verify it against the official GA reference and define the supported subset in `docs/protocol-profile.md`. For the Phase 0 example, use `"transcription": {}`. Cascade's internal ASR always runs for the ASR → LLM pipeline regardless of this field; this field controls only whether `conversation.item.input_audio_transcription.*` events are exposed to the client.
2. Add `audio.output.speed` to the config with default `1.0`, matching the GA session shape. Validate the GA-supported range and update semantics when the protocol profile is frozen.
3. Initialize Git and persist this brief verbatim as `docs/implementation-brief.md`. `CLAUDE.md` remains authoritative when they conflict.
4. Standardize on `github.com/coder/websocket`, but do not add the dependency until the phase that actually needs WebSocket support.
5. In Phase 0, a valid config should: load → expand env → validate → initialize slog → print a small allowlisted/redacted configuration summary → exit 0. Do not start the HTTP/WebSocket server until Phase 3.

### Second approval round (2026-09-01)

6. Validate `audio.output.speed` against the currently documented GA range `[0.25, 1.5]` in Phase 0 rather than only checking `> 0`. Re-verify the range when freezing the Phase 2 protocol profile.
7. Make `turn_detection` validation type-specific: `threshold`, `prefix_padding_ms` and `silence_duration_ms` apply only to `server_vad`; `eagerness` applies only to `semantic_vad`. Fields that belong to the other mode are rejected rather than accepted as mixed shapes.
8. The proposed `Transcription` subset (`model`, `language`, `prompt`) is acceptable for Phase 0; keep the `PROTOCOL-VERIFY` marker because the GA object contains additional model-specific fields; Phase 2 freezes Cascade's exact supported subset.
