# Cascade — OpenAI Realtime GA compatibility profile

Status: **APPROVED 2026-09-01** (Phase 2). Golden traces in `internal/protocol/testdata` are generated from the sequences in §7; any change to this document requires regenerating them.

## 0. Sources and scope

| Source | Used for |
|---|---|
| `openai-node` v7.8.0, `src/resources/realtime/realtime.ts` (generated from the official OpenAPI spec) | Exact field names, types, enums and per-event documentation of every GA client event, server event, session object and response object. |
| https://developers.openai.com/api/docs/guides/realtime-conversations | Event lists per flow (session init, text response, audio response, audio input, interruption/truncate), `error` event shape and the `event_id` back-reference rule. |

Only the GA protocol is targeted: `session.type = "realtime"`, `output_modalities`, `session.audio.input/output`, `response.output_audio.*`, `response.output_audio_transcript.*`, `response.output_text.*`. Beta shapes (`modalities`, top-level `voice` / `turn_detection` / `input_audio_format`, `response.audio.*`, `response.text.*`, `OpenAI-Beta` header) are rejected, never translated.

Marker conventions used below:

- **Supported** — accepted and implemented as documented.
- **Rejected** — an `error` event is emitted (§8) and the whole client event is discarded; the session stays open.
- **Ignored** — accepted without effect; the echo column says what `session.updated` reports.
- `PROTOCOL-VERIFY` — the official reference does not pin this down; Cascade's choice is stated and was approved with the profile.

## 1. Transport

| Item | Profile |
|---|---|
| Endpoint | `GET /v1/realtime` upgraded to WebSocket. |
| Auth | `Authorization: Bearer <REALTIME_API_KEY>`. Missing/invalid → HTTP 401 before upgrade. |
| `?model=` | Accepted and echoed as `session.model`; not used for routing. Absent → echo `"cascade"`. |
| `OpenAI-Beta` header | Ignored (beta header does not select a beta protocol; the GA shape is the only one served). |
| Sub-protocols | `realtime` accepted if offered; none required. |
| Message framing | One JSON object per text frame. Binary frames → Rejected (`invalid_request_error` / `invalid_event`). |
| First server events | `session.created` then `conversation.created`, in that order, before any client event is processed. |
| Max inbound frame | `limits.client_max_message_bytes` (new config field, default 16 MiB; GA allows up to 15 MiB of base64 per `input_audio_buffer.append`). |

## 2. Client events

| Event | Status | Notes |
|---|---|---|
| `session.update` | Supported | Body validated against §3; any Rejected field or unknown field rejects the whole event. Success → `session.updated` with the full effective session (no `event_id` back-reference, per spec). |
| `input_audio_buffer.append` | Supported | `audio` base64 of `audio/pcm` 24 kHz mono 16-bit LE. No confirmation event (per spec). Odd trailing byte → Rejected (`invalid_value`, `param: "audio"`). |
| `input_audio_buffer.commit` | Supported | Empty buffer → Rejected (`invalid_event`, message "input audio buffer is empty"). Success → `input_audio_buffer.committed` → `conversation.item.added` (user `input_audio` item, `status: "in_progress"`, `transcript: null`); `conversation.item.done` (`status: "completed"`, transcript filled) follows once the ASR final arrives. Cascade defers `done` because the transcript gates the LLM (approximation, `docs/decisions.md`). Allowed in VAD modes too. |
| `input_audio_buffer.clear` | Supported | → `input_audio_buffer.cleared`. |
| `conversation.item.create` | Supported (subset, §5) | → `conversation.item.added` then `conversation.item.done`. `conversation.item.created` is never emitted. Unknown `previous_item_id` → Rejected (`invalid_value`, `param: "previous_item_id"`). Also accepts `function_call_output`. |
| `conversation.item.retrieve` | Rejected | Cascade does not retain user audio and the spec says the retrieved item "includes audio data"; returning an item without it would be a silent divergence. Error: `invalid_event`, `param: "type"`. |
| `conversation.item.delete` | Supported | Unknown id → Rejected (`invalid_value`, `param: "item_id"`). Deleting the output item of an in-progress response → Rejected (`invalid_value`, `param: "item_id"`). |
| `conversation.item.truncate` | Supported | Assistant audio message items only (`invalid_value`, `param: "item_id"` otherwise), `content_index` must be `0` (`param: "content_index"`). `audio_end_ms` greater than the generated audio → Rejected (`invalid_value`, `param: "audio_end_ms"`), never clamped. Success → `conversation.item.truncated`. Text trimming per `docs/decisions.md` three-tier approximation. |
| `response.create` | Supported (subset, §4) | Response already in progress → Rejected (`invalid_event`, message "a response is already in progress"). |
| `response.cancel` | Supported | No active response, or `response_id` not the active one → Rejected (spec: "If there is no response to cancel, the server will respond with an error"; `invalid_event`, or `invalid_value` with `param: "response_id"` when a foreign id was given). Success → terminal events with `status: "cancelled"`, `status_details.reason: "client_cancelled"`. |
| `output_audio_buffer.clear` | Rejected | WebRTC/SIP only per spec; `invalid_event`, `param: "type"`. |
| `transcription_session.update` | Rejected | Transcription sessions are not served; `invalid_event`, `param: "type"`. |
| any other `type` | Rejected | `invalid_value`, `param: "type"` (mirrors the official `scooby.dooby.doo` example). |
| missing `type` | Rejected | `invalid_event` (`"The 'type' field is missing."`). |
| `event_id` on client events | Supported | Echoed as `error.event_id` on any error the event causes. Max length 512 (spec). |

## 3. Session object (`session.update.session`, echoed by `session.created` / `session.updated`)

Echo shape: the full effective session in GA request shape plus `id` (`sess_…`) and `object: "realtime.session"` (`PROTOCOL-VERIFY`: the spec types omit `id`/`object` on the create-request shape, but clients commonly read `session.id`; approved).

| Field | Status | Accepted values | Echo |
|---|---|---|---|
| `type` | Supported | required; must be `"realtime"`; anything else → Rejected (`invalid_value`, `param: "session.type"`) | `"realtime"` |
| `model` | Ignored | any string | last value sent, else `?model=`, else `"cascade"` |
| `instructions` | Supported | any string (empty clears) | effective value |
| `output_modalities` | Supported | exactly `["audio"]` or `["text"]` | effective value |
| `audio.input.format` | Supported | `{type:"audio/pcm", rate:24000}` (both keys optional, defaults apply); `audio/pcmu`, `audio/pcma` → Rejected | `{type:"audio/pcm", rate:24000}` |
| `audio.input.transcription` | Supported (subset) | `null` disables; object enables. `model` (echoed only), `language`, `prompt` accepted. `delay`, `keywords`, `languages` → Rejected | effective object or `null` |
| `audio.input.noise_reduction` | Rejected unless `null` | `null` accepted (means off) | `null` |
| `audio.input.turn_detection` | Supported | `null` (manual); `server_vad` with `threshold` [0,1], `prefix_padding_ms` ≥0, `silence_duration_ms` >0, `create_response`, `interrupt_response`; `semantic_vad` with `eagerness` low/medium/high/auto, `create_response`, `interrupt_response`. Fields of the other mode → Rejected. `idle_timeout_ms` non-null → Rejected (`null` accepted) | effective object with all applicable fields filled (defaults 0.5 / 300 / 500 / true / true; eagerness `"auto"`), or `null` |
| `audio.output.format` | Supported | `{type:"audio/pcm", rate:24000}` only | same |
| `audio.output.voice` | Supported | any string (passed to TTS as-is; not validated against the OpenAI voice list); custom voice object `{id}` → Rejected. Per GA, the voice cannot be changed once the session has produced audio: a `session.update` (or `response.create` override) with a different voice after the first `response.output_audio.delta` → Rejected (`invalid_value`, `param: "session.audio.output.voice"` / `"response.audio.output.voice"`) | effective string |
| `audio.output.speed` | Supported | number in [0.25, 1.5] | effective number |
| `max_output_tokens` | Supported | `"inf"` or integer in [1, 4096] | effective value |
| `tools` | **Supported** | array of `{type:"function", name, description?, parameters?}`. `type` and `name` are required (`invalid_value`, `param: "session.tools[i].type"` / `[i].name`); `parameters` is an opaque JSON Schema **object** passed to the provider verbatim and never interpreted by Cascade, and an omitted `parameters` means a no-argument tool. Duplicate `name` → Rejected (`param: "session.tools[i].name"`). MCP tools → Rejected (`param: "session.tools[i].type"`). | effective array; `[]` when never set |
| `tool_choice` | **Supported** | `"none"` \| `"auto"` \| `"required"`, or `{type:"function", name}`. `ToolChoiceMcp` → Rejected. A forced function whose `name` is not in `tools` → Rejected (`param: "session.tool_choice.name"`). | effective value |
| `parallel_tool_calls` | Rejected | | not echoed |

**`tool_choice` default and canonicalization.** Cascade's session model owns the default: `tool_choice` defaults to `"auto"` and the effective value is **always** sent to the LLM provider. No code path omits the field and inherits a provider default. This is a Cascade decision, not a GA claim — the reference documents the four accepted forms but states no default. The three tool fields are serialized into a provider request only together with a non-empty `tools` array, because the Chat Completions API rejects `tool_choice` and `parallel_tool_calls` on a request that declares no tools.

Making `tools` / `tool_choice` effective changes **no existing session object on the wire**: §10.3 already had Cascade echoing `tools: []` and `tool_choice: "auto"` for every session.
| `reasoning` | Rejected | | not echoed |
| `prompt` | Ignored if `null`, else Rejected | | `null` |
| `tracing` | Ignored if `null`, else Rejected | | `null` |
| `truncation` | Ignored if `"auto"`, else Rejected | | `"auto"` |
| `include` | Ignored if `[]`/`null`, else Rejected | | not echoed |
| unknown field | Rejected | `invalid_value`, `param` = dotted path, message "Unknown parameter" | |
| beta top-level fields (`modalities`, `voice`, `turn_detection`, `input_audio_format`, `output_audio_format`, `input_audio_transcription`, `temperature`, `max_response_output_tokens`) | Rejected | `invalid_value`, `param` = the field, message "Unknown parameter … (beta shape; the GA session shape is required)" | |

Merge semantics: only fields present are updated; nested objects merge field-wise; `null` clears nullable fields (`transcription`, `turn_detection`, `noise_reduction`, `prompt`, `tracing`).

Effect on in-flight state: `session.created` echoes the configured `session_defaults` (the config→wire contract), never a hard-coded shape. Changing `turn_detection` mid-session rebuilds the VAD and turn state (uncommitted audio stays in the buffer, any VAD speech segment mark is dropped, an in-progress response keeps running). `instructions`, `output_modalities`, `voice`, `speed` and `max_output_tokens` apply from the next response; an in-progress response is unaffected.

## 4. `response.create.response`

| Field | Status | Notes |
|---|---|---|
| `instructions` | Supported | overrides for this response only |
| `output_modalities` | Supported | `["audio"]` or `["text"]` |
| `max_output_tokens` | Supported | `"inf"` or [1, 4096] |
| `audio.output.voice` | Supported | string only; per-response voice, subject to the same "not after first audio" rule as the session voice |
| `audio.output.format` | Supported | `audio/pcm` 24000 only |
| `conversation` | Ignored if `"auto"`, else Rejected | out-of-band responses are out of scope |
| `input` | Rejected | out-of-band context is out of scope |
| `metadata` | Supported | ≤16 string pairs, echoed on the response object |
| `tools`, `tool_choice`, `parallel_tool_calls`, `prompt`, `reasoning` | Rejected | Tools are session-level only; the consumer sends a bare `response.create` after returning a tool result, and per-response overrides can be added later without breaking anything. |
| unknown field | Rejected | `invalid_value`, `param` = dotted path |

## 5. Items accepted by `conversation.item.create`

| Item | Status |
|---|---|
| `{type:"message", role:"system", content:[{type:"input_text", text}]}` | Supported |
| `{type:"message", role:"user", content:[{type:"input_text", text}]}` | Supported |
| `role:"user"` with `input_audio` | Rejected (`invalid_value`, `param: "item.content[0].type"`): audio items enter only through the input audio buffer |
| `role:"user"` with `input_image` | Rejected |
| `{type:"message", role:"assistant", content:[{type:"output_text", text}]}` (also `type:"text"`, which the spec lists for client-created assistant items) | Supported |
| `role:"assistant"` with `output_audio` | Rejected (spec: cannot populate assistant audio messages) |
| multiple content parts | Rejected (`invalid_value`, `param: "item.content"`; one part per message in v1) |
| `{type:"function_call_output", call_id, output}` | **Supported**; `call_id` and `output` are required (`invalid_value`, `param: "item.call_id"` / `"item.output"`) |
| `function_call`, `item_reference`, `mcp_*` | Rejected (`invalid_value`, `param: "item.type"`); `function_call` is server-emitted only |
| client-supplied `id` | Supported; must be unique in the session (`invalid_value`, `param: "item.id"`) |
| `status`, `object` | Ignored |
| `previous_item_id` | Supported: absent = append, `"root"` = first, id = insert after |

Item shape on the wire (all server-emitted items): `{id, object:"realtime.item", type:"message", role, status, content:[part]}` where `part` is `{type:"input_text", text}`, `{type:"input_audio", transcript}` (audio omitted), `{type:"output_text", text}` or `{type:"output_audio", transcript}` (audio omitted).

The two tool items carry neither `role` nor `content`:

```
function_call         {id, object:"realtime.item", type:"function_call",
                       name, call_id, arguments, status}
function_call_output  {id, object:"realtime.item", type:"function_call_output",
                       call_id, output, status}
```

`arguments` and `output` are JSON-encoded strings. Cascade never parses either one; `output` is free text and may be empty.

**`call_id` validation.** The reference states that on a `function_call_output` the server checks that a `function_call` with the same id exists in the conversation history. Cascade does the same: an unknown `call_id` → Rejected (`invalid_value`, `param: "item.call_id"`). A **second** `function_call_output` for a `call_id` that has already been answered is also Rejected on `item.call_id` — a Cascade profile decision, since GA does not specify duplicate-output semantics, and two results for one call would put contradictory tool output into the LLM context with no rule for which wins.

## 6. Server events

| Event | Emitted when |
|---|---|
| `session.created` | first event after upgrade |
| `conversation.created` | immediately after `session.created` (`{id:"conv_…", object:"realtime.conversation"}`) |
| `session.updated` | after a successful `session.update` |
| `error` | any Rejected event or runtime failure; §8 |
| `input_audio_buffer.committed` | client or VAD commit; `item_id`, `previous_item_id` |
| `input_audio_buffer.cleared` | after `input_audio_buffer.clear` |
| `input_audio_buffer.speech_started` | VAD modes only; `audio_start_ms` includes prefix padding; `item_id` of the item that the commit will create |
| `input_audio_buffer.speech_stopped` | VAD modes only; `audio_end_ms` includes the silence window |
| `conversation.item.added` | client-created item, committed audio item, response output item (status `in_progress`, empty content) |
| `conversation.item.done` | client-created item (right after added), committed audio item (after its transcript is final), response output item (with `response.output_item.done`) |
| `conversation.item.input_audio_transcription.delta` | transcription enabled; one delta per ASR final segment received after commit |
| `conversation.item.input_audio_transcription.completed` | transcription enabled; `transcript`, `usage: {type:"duration", seconds}` |
| `conversation.item.deleted` | after delete |
| `conversation.item.truncated` | after truncate |
| `response.created` | immediately on trigger (client or `server_vad`), before the LLM starts; under `semantic_vad` after the committed item's transcript is final, and not at all when it is empty (§7) |
| `response.output_item.added` | when the pipeline starts (after the user item's transcript is final) |
| `response.content_part.added` | right after `response.output_item.added`; `part: {type:"audio", transcript:""}` or `{type:"text", text:""}` |
| `response.output_audio_transcript.delta` / `.done` | audio responses |
| `response.output_audio.delta` / `.done` | audio responses; base64 PCM |
| `response.output_text.delta` / `.done` | text responses |
| `response.content_part.done` | before `response.output_item.done`; `part` carries final `transcript`/`text` |
| `response.function_call_arguments.delta` | once per function call, carrying the **complete** arguments |
| `response.function_call_arguments.done` | immediately after, with the final arguments |
| `response.output_item.done` | terminal, with the full item (audio omitted) |
| `response.done` | always, with `status` ∈ completed/cancelled/incomplete/failed, `status_details`, `usage`, `output` |

Never emitted: `conversation.item.created` (superseded by `conversation.item.added` / `conversation.item.done`), `rate_limits.updated` (no real limits exist; fabricated values would mislead), `input_audio_buffer.timeout_triggered`, `input_audio_buffer.dtmf_event_received`, `output_audio_buffer.*`, `conversation.item.retrieved`, `conversation.item.input_audio_transcription.failed` (ASR failure is fatal → `error` + close), `conversation.item.input_audio_transcription.segment`, `response.mcp_call*`, `mcp_list_tools.*`, `transcription_session.updated`.

Field details:

- Every server event carries `event_id` (`event_…`, unique per session, monotonic).
- `response` object: `{id, object:"realtime.response", status, status_details, output, usage, conversation_id, output_modalities, audio:{output:{format, voice}}, max_output_tokens, metadata}`. In `response.created`: `status:"in_progress"`, `status_details:null`, `output:[]`, `usage:null`.
- `status_details`: `{type:"cancelled", reason:"turn_detected"|"client_cancelled"}`, `{type:"incomplete", reason:"max_output_tokens"|"content_filter"}`, `{type:"failed", error:{type, code}}`, `null` for completed. (Enums copied from the spec.)
- `usage`: `{total_tokens, input_tokens, output_tokens, input_token_details:{text_tokens, audio_tokens:0, cached_tokens:0}, output_token_details:{text_tokens, audio_tokens:0}}` — LLM text tokens only; audio tokens are always 0.
- `content_index` is always `0` (one part per message item) and does not apply to function_call items. `output_index` is **not** always `0`: the message item is `0` and a function_call after it is `1`; on a call-only turn the function_call item is at `0`.
- The two `response.function_call_arguments.*` events carry
  `{event_id, type, response_id, item_id, output_index, call_id, delta}` and
  `{event_id, type, response_id, item_id, output_index, call_id, name, arguments}`.
- A `function_call` item is a response output item, so it flows through the existing lifecycle — `response.output_item.added` → `conversation.item.added` → `arguments.delta` → `arguments.done` → `response.output_item.done` → `conversation.item.done` — and appears in `response.done.output`. It has no `response.content_part.*` events.
- `response.function_call_arguments.delta` is emitted **once**, with the complete arguments. Fragment accumulation belongs to the provider adapter ("provider specifics are normalized at the boundary"); the same precedent already exists for input transcription, which emits one delta per ASR final segment rather than per token.

**Documented GA deviation — `arguments.done` on an interrupted, cancelled or incomplete call.** The reference documents `.done` as also emitted when a response is interrupted, incomplete or cancelled, carrying whatever partial arguments accumulated. **Cascade does not.** Normalizing argument fragments at the provider boundary means partial arguments never cross into `internal/session`, so there is nothing to emit, and a truncated arguments string could only be mis-parsed. The resulting contract:

> `response.function_call_arguments.done` means **the tool call was completely delivered**. It does **not** guarantee that `arguments` is valid JSON — a model can emit syntactically invalid arguments in a call that completed normally, and Cascade never parses them.

A client must still handle a parse failure on `arguments`; what it no longer has to handle is a *truncated* payload arriving under `.done`. On cancellation before a complete call arrives, Cascade emits no `arguments.*` at all and no function_call item. Once a complete call has been delivered, cancelling does not retract it. `max_output_tokens` reached mid-arguments is the same case: `incomplete` / `max_output_tokens`, and no function_call item.

**Message-item ordering on a turn that both spoke and called** (a Cascade consequence, not a GA claim). The function_call item is delivered the instant the provider yields a complete call, but the message item cannot close until its audio has been flushed, so its `output_text.done` / `content_part.done` / `output_item.done` follow the function_call item's whole lifecycle rather than preceding it. The items keep their `output_index` (message `0`, call `1`); only the closing frames are late. A cascaded pipeline has no way to close the message item earlier.

## 7. Event ordering (golden sequences)

Terminal-event order between `response.output_audio.done` and `response.output_audio_transcript.done` is not fixed by the reference; Cascade uses the order in which the GA guide lists them: `response.output_audio.done` first. `rate_limits.updated` never appears (§6).

**Connect**
```
session.created → conversation.created
```

**Client text item + text response** (`output_modalities:["text"]`)
```
conversation.item.added → conversation.item.done
response.created → response.output_item.added → conversation.item.added → response.content_part.added
→ response.output_text.delta × n → response.output_text.done
→ response.content_part.done → response.output_item.done → conversation.item.done → response.done
```

**Audio response**
```
response.created → response.output_item.added → conversation.item.added → response.content_part.added
→ { response.output_audio_transcript.delta | response.output_audio.delta } interleaved
→ response.output_audio.done → response.output_audio_transcript.done
→ response.content_part.done → response.output_item.done → conversation.item.done → response.done
```

**server_vad turn** (`create_response: true`, transcription enabled)
```
input_audio_buffer.speech_started → input_audio_buffer.speech_stopped
→ input_audio_buffer.committed → conversation.item.added
→ response.created
→ conversation.item.input_audio_transcription.delta × n → conversation.item.input_audio_transcription.completed → conversation.item.done
→ response.output_item.added → … (audio response as above)
```
With `create_response: false` the sequence stops after `conversation.item.done`; `input_audio_buffer.committed` is still emitted.

**semantic_vad turn** (`create_response: true`, transcription enabled; a Cascade profile decision)
```
input_audio_buffer.speech_started → input_audio_buffer.speech_stopped
→ input_audio_buffer.committed → conversation.item.added
→ conversation.item.input_audio_transcription.delta × n → conversation.item.input_audio_transcription.completed → conversation.item.done
→ response.created → response.output_item.added → … (audio response as above)
```
`response.created` follows the transcript rather than the commit (the LLM starts at the same moment as under `server_vad`). When the transcript is empty or whitespace (non-speech noise) the sequence stops after `conversation.item.done`: no response is created and no error is sent. If no Final arrives within `asr_final_timeout`, a Partial starts the response; without one there is no response and no error. The end of turn waits at most max(`asr_final_timeout`, eagerness delay) after `speech_stopped` for a Final to judge. A `speech_started` during an active response still interrupts it per `interrupt_response`, even if that speech turns out to be noise.

**Interruption** (`interrupt_response: true`, response in progress)
```
input_audio_buffer.speech_started
→ response.output_audio.done → response.output_audio_transcript.done → response.content_part.done
→ response.output_item.done → conversation.item.done → response.done{cancelled, turn_detected}
```
Interruption during the awaiting phase (no output item yet): `speech_started → response.done{cancelled}` only.

**Truncate**
```
conversation.item.truncate → conversation.item.truncated
```

**Call-only turn** (the model called a tool and said nothing)
```
response.created
→ response.output_item.added (function_call, output_index 0) → conversation.item.added
→ response.function_call_arguments.delta → response.function_call_arguments.done
→ response.output_item.done → conversation.item.done → response.done{completed}
```
No message item and no audio: the message item is created lazily, on the first text delta.

**Text + call turn**
```
response.created
→ response.output_item.added (message, 0) → conversation.item.added → response.content_part.added
→ response.output_text.delta × n
→ response.output_item.added (function_call, 1) → conversation.item.added
→ response.function_call_arguments.delta → response.function_call_arguments.done
→ response.output_item.done (1) → conversation.item.done
→ response.output_text.done → response.content_part.done
→ response.output_item.done (0) → conversation.item.done → response.done{completed}
```
The message item's closing frames follow the call's, per the ordering note in §6.

**Tool round trip**
```
… call-only turn …
conversation.item.create{function_call_output} → conversation.item.added → conversation.item.done
response.create → … a spoken response …
```

**Cancel before a complete call**
```
response.created → response.done{cancelled}
```
No function_call item anywhere in the trace, and `output: []`.

**Cancel after a delivered call**
```
response.created
→ response.output_item.added (function_call) → conversation.item.added
→ response.function_call_arguments.delta → .done
→ response.output_item.done → conversation.item.done
→ response.done{cancelled}
```
The function_call item stays in the conversation and in `response.done.output`; its own lifecycle closed at delivery and is not replayed. The client may legally send its `function_call_output` and a `response.create` against the now-cancelled response, and both are accepted.

**Errors**: an `error` is emitted in place of the events the rejected client event would have produced; nothing else changes.

## 8. `error` event

Shape (spec): `{type:"error", event_id, error:{type, code, message, param, event_id}}` where `error.event_id` is the client `event_id` that caused it (`null` for server-originated errors).

Only the two codes shown in the official material are used for protocol validation; the reason is carried by `message` and `param`. Cascade's own runtime failures use `type: "server_error"` with Cascade codes that make no claim to be part of the OpenAI enum.

| `error.type` | `error.code` | When | `param` |
|---|---|---|---|
| `invalid_request_error` | `invalid_event` | unparsable JSON, binary frame, missing `type`, unknown or unsupported event `type`, or an event that is not valid in the current state: commit with an empty buffer, `response.create` while a response is in progress, `response.cancel` with nothing to cancel | `"type"` for type problems, otherwise `null` |
| `invalid_request_error` | `invalid_value` | a field is missing, has the wrong type, an out-of-profile value, or is unknown (message "Unknown parameter"); references an unknown/duplicate item or a foreign `response_id`; `audio_end_ms` beyond the generated audio; voice change after the first audio | dotted path of the field (`"session.audio.output.speed"`, `"item.content[0].type"`, `"item_id"`, `"audio_end_ms"`, …) |
| `server_error` | `provider_error` | LLM/TTS failure (response → `failed`), ASR failure (fatal), a provider returning more than one tool call in one response | `null` |
| `server_error` | `transcript_timeout` | no transcript before `asr_final_timeout` (response → `failed`) | `null` |
| `server_error` | `input_audio_buffer_overflow` | buffer exceeds `input_audio_buffer_max_ms` (fatal) | `null` |
| `server_error` | `input_queue_overflow` | transport queue full (fatal) | `null` |

Function-calling rejections, all `invalid_value` under `invalid_request_error`:

| Cause | `param` |
|---|---|
| tool entry missing `type` / not `"function"` (MCP included) | `session.tools[i].type` |
| tool entry missing `name`; duplicate name | `session.tools[i].name` |
| `parameters` not an object | `session.tools[i].parameters` |
| `tool_choice` not one of the four forms | `session.tool_choice` |
| forced function not present in `tools` | `session.tool_choice.name` |
| client-created `function_call` item | `item.type` |
| `function_call_output` with unknown `call_id` | `item.call_id` |
| second `function_call_output` for one call | `item.call_id` |
| `function_call_output` missing `call_id` / `output` | `item.call_id` / `item.output` |

Fatal errors are followed by WebSocket close code `1011` with the Cascade code as the close reason.

## 9. Identifiers

`sess_` session, `conv_` conversation, `item_` items (unless client-supplied), `resp_` responses, `event_` server events; suffixes are 16 lowercase alphanumerics. Golden traces replace every id with `<sess>`, `<conv>`, `<item:n>`, `<resp:n>`, `<call:n>`, `<event:n>`; timestamps never appear in events.

**All conversation items, function_call items included, use `item_` ids.** Tool-invocation linkage uses a separate `call_` id, minted by the protocol adapter for each function call. The LLM provider's own call id never reaches the client: it stays inside the session, where it is the key that links a result back to the call it answers in history replay. A `function_call_output` echoes the same wire `call_id` as the call it answers.

General rule: **GA example prefixes such as `fc_` are not copied unless the reference explicitly requires them.** The ids in the official docs (`fc_001`, `call_001`, `resp_002`) are illustrative payloads, not a spec constraint, and the only consumer of the linkage reads `call_id`.

## 10. Decisions taken at approval (2026-09-01)

1. `conversation.item.created` is never emitted; `conversation.item.added` → `conversation.item.done` only.
2. Terminal order: `response.output_audio.done` → `response.output_audio_transcript.done`.
3. Defaulted-but-unsupported fields are echoed: `tools: []`, `tool_choice: "auto"`, `truncation: "auto"`, `tracing: null`, `prompt: null`, `noise_reduction: null`.
4. `conversation.item.retrieve` is rejected; no audio-less item is returned.
5. `session.model` echoes `"cascade"` when `?model=` is absent.
6. `voice` follows GA: no change after the session has produced audio.
7. Protocol validation uses only `invalid_event` / `invalid_value`; Cascade runtime errors keep Cascade codes under `server_error`.
8. `usage` counts LLM text tokens only; audio tokens are `0`.
9. `rate_limits.updated` is never emitted.
10. `limits.client_max_message_bytes` added (default 16 MiB, `> 0`).
11. Phase 1 behavior changes: truncate beyond generated audio is an error, not a clamp; committed audio items stay `in_progress` until their transcript is final; terminal emit order per item 2.

Phase 7 (function calling), approved 2026-09-01:

12. `tools` / `tool_choice` become effective rather than echoed-only. `tool_choice` defaults to `"auto"` in Cascade's session model and is always sent to the provider; no provider default is ever inherited. The existing echoes are unchanged, so no existing trace moved on their account.
13. At most one function call per response; `parallel_tool_calls` stays Rejected and is sent to providers as `false`. A provider returning more than one call is a `provider.Error`, never a silent drop.
14. `response.function_call_arguments.delta` is emitted **once**, with the complete arguments; fragment accumulation belongs to the provider adapter.
15. Cascade does **not** emit `arguments.done` for an interrupted, cancelled or incomplete call, where GA does with partial arguments — a documented deviation caused by normalizing argument fragments at the provider boundary. `arguments.done` therefore means the call was completely delivered; it does not guarantee `arguments` is valid JSON, which Cascade never parses.
16. The message output item is created lazily, on the first text delta, so a call-only turn emits no empty message item (GA-faithful). On a turn that both spoke and called, the message item's closing frames follow the call's, because a cascaded pipeline cannot close it before its audio is flushed (§6).
17. Tools are executed by the client, never by the gateway. Cascade forwards the call, accepts the result as an opaque string, and feeds it back into the next generation. It interprets no tool semantics — including refusals, which are ordinary results.
18. A second `function_call_output` for an already-answered `call_id` is Rejected (`invalid_value`, `param: "item.call_id"`). A Cascade profile decision: GA does not specify duplicate-output semantics.
19. GA example id prefixes (`fc_`, `call_001`, …) are not copied. All conversation items use `item_`; tool linkage uses a `call_` id minted by the adapter, never the provider's own (§9).
