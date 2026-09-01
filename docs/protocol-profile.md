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
| `conversation.item.create` | Supported (subset, §5) | → `conversation.item.added` then `conversation.item.done`. `conversation.item.created` is never emitted. Unknown `previous_item_id` → Rejected (`invalid_value`, `param: "previous_item_id"`). |
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
| `tools` | Ignored if `[]`, else Rejected | | `[]` |
| `tool_choice` | Ignored if `"auto"` or `"none"`, else Rejected | | `"auto"` |
| `parallel_tool_calls` | Rejected | | not echoed |
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
| `tools`, `tool_choice`, `parallel_tool_calls`, `prompt`, `reasoning` | Rejected | |
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
| `function_call`, `function_call_output`, `item_reference`, `mcp_*` | Rejected (`invalid_value`, `param: "item.type"`) |
| client-supplied `id` | Supported; must be unique in the session (`invalid_value`, `param: "item.id"`) |
| `status`, `object` | Ignored |
| `previous_item_id` | Supported: absent = append, `"root"` = first, id = insert after |

Item shape on the wire (all server-emitted items): `{id, object:"realtime.item", type:"message", role, status, content:[part]}` where `part` is `{type:"input_text", text}`, `{type:"input_audio", transcript}` (audio omitted), `{type:"output_text", text}` or `{type:"output_audio", transcript}` (audio omitted).

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
| `response.created` | immediately on trigger (client or VAD), before the LLM starts |
| `response.output_item.added` | when the pipeline starts (after the user item's transcript is final) |
| `response.content_part.added` | right after `response.output_item.added`; `part: {type:"audio", transcript:""}` or `{type:"text", text:""}` |
| `response.output_audio_transcript.delta` / `.done` | audio responses |
| `response.output_audio.delta` / `.done` | audio responses; base64 PCM |
| `response.output_text.delta` / `.done` | text responses |
| `response.content_part.done` | before `response.output_item.done`; `part` carries final `transcript`/`text` |
| `response.output_item.done` | terminal, with the full item (audio omitted) |
| `response.done` | always, with `status` ∈ completed/cancelled/incomplete/failed, `status_details`, `usage`, `output` |

Never emitted: `conversation.item.created` (superseded by `conversation.item.added` / `conversation.item.done`), `rate_limits.updated` (no real limits exist; fabricated values would mislead), `input_audio_buffer.timeout_triggered`, `input_audio_buffer.dtmf_event_received`, `output_audio_buffer.*`, `conversation.item.retrieved`, `conversation.item.input_audio_transcription.failed` (ASR failure is fatal → `error` + close), `conversation.item.input_audio_transcription.segment`, `response.function_call_arguments.*`, `response.mcp_call*`, `mcp_list_tools.*`, `transcription_session.updated`.

Field details:

- Every server event carries `event_id` (`event_…`, unique per session, monotonic).
- `response` object: `{id, object:"realtime.response", status, status_details, output, usage, conversation_id, output_modalities, audio:{output:{format, voice}}, max_output_tokens, metadata}`. In `response.created`: `status:"in_progress"`, `status_details:null`, `output:[]`, `usage:null`.
- `status_details`: `{type:"cancelled", reason:"turn_detected"|"client_cancelled"}`, `{type:"incomplete", reason:"max_output_tokens"|"content_filter"}`, `{type:"failed", error:{type, code}}`, `null` for completed. (Enums copied from the spec.)
- `usage`: `{total_tokens, input_tokens, output_tokens, input_token_details:{text_tokens, audio_tokens:0, cached_tokens:0}, output_token_details:{text_tokens, audio_tokens:0}}` — LLM text tokens only; audio tokens are always 0.
- `output_index` is always `0`, `content_index` always `0` (one item, one part per response).

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

**Errors**: an `error` is emitted in place of the events the rejected client event would have produced; nothing else changes.

## 8. `error` event

Shape (spec): `{type:"error", event_id, error:{type, code, message, param, event_id}}` where `error.event_id` is the client `event_id` that caused it (`null` for server-originated errors).

Only the two codes shown in the official material are used for protocol validation; the reason is carried by `message` and `param`. Cascade's own runtime failures use `type: "server_error"` with Cascade codes that make no claim to be part of the OpenAI enum.

| `error.type` | `error.code` | When | `param` |
|---|---|---|---|
| `invalid_request_error` | `invalid_event` | unparsable JSON, binary frame, missing `type`, unknown or unsupported event `type`, or an event that is not valid in the current state: commit with an empty buffer, `response.create` while a response is in progress, `response.cancel` with nothing to cancel | `"type"` for type problems, otherwise `null` |
| `invalid_request_error` | `invalid_value` | a field is missing, has the wrong type, an out-of-profile value, or is unknown (message "Unknown parameter"); references an unknown/duplicate item or a foreign `response_id`; `audio_end_ms` beyond the generated audio; voice change after the first audio | dotted path of the field (`"session.audio.output.speed"`, `"item.content[0].type"`, `"item_id"`, `"audio_end_ms"`, …) |
| `server_error` | `provider_error` | LLM/TTS failure (response → `failed`), ASR failure (fatal) | `null` |
| `server_error` | `transcript_timeout` | no transcript before `asr_final_timeout` (response → `failed`) | `null` |
| `server_error` | `input_audio_buffer_overflow` | buffer exceeds `input_audio_buffer_max_ms` (fatal) | `null` |
| `server_error` | `input_queue_overflow` | transport queue full (fatal) | `null` |

Fatal errors are followed by WebSocket close code `1011` with the Cascade code as the close reason.

## 9. Identifiers

`sess_` session, `conv_` conversation, `item_` items (unless client-supplied), `resp_` responses, `event_` server events; suffixes are 16 lowercase alphanumerics. Golden traces replace every id with `<sess>`, `<conv>`, `<item:n>`, `<resp:n>`, `<event:n>`; timestamps never appear in events.

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
