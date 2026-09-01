# Protocol profile delta — Phase 7 (function calling)

Status: **APPROVED 2026-09-01.** All three open items are resolved (see "Approved resolutions" at the
end); every `PROTOCOL-VERIFY` from the phase brief is closed. Implementation may begin.

This delta stays a separate file **until the phase lands**, rather than being folded into
`docs/protocol-profile.md` now. The profile documents shipped behaviour and its §7 sequences are what
the golden traces in `internal/protocol/testdata` are generated from; merging unimplemented sequences
into it would leave the approved profile describing code that does not exist and implying goldens that
have not been generated. It is applied verbatim into the numbered sections below — and this file
deleted — in the same change that implements it and regenerates the traces.

Every wire shape here is **transcribed** from the pinned reference, not inferred. Where the reference
does not pin something down it is marked, and Cascade's choice is stated for approval.

---

## §0 — sources for this delta

| Source | Used for |
|---|---|
| `openai-node` v7.8.0 `src/resources/realtime/realtime.ts` — `RealtimeConversationItemFunctionCall` (L1583), `RealtimeConversationItemFunctionCallOutput` (L1623), `RealtimeFunctionTool` (L1848), `RealtimeToolChoiceConfig` (L3268), `ResponseFunctionCallArgumentsDeltaEvent` (L4757), `ResponseFunctionCallArgumentsDoneEvent` (L4798), `ConversationItemWithReference.call_id` (L716) | Exact field names, types and required/optional status of both item types, the tool definition, the tool-choice union and both argument events. |
| `openai-node` v7.8.0 `src/resources/responses/responses.ts` — `ToolChoiceOptions` (L9561), `ToolChoiceFunction` (L9518) | The `tool_choice` enum and the forced-function shape. |
| https://developers.openai.com/api/docs/guides/realtime-mcp, .../api/reference/resources/realtime/server-events | A worked `session.update` with `tools` + `tool_choice`, and a literal `response.function_call_arguments.done` payload confirming the field set. |

---

## §2 — client events (rows changed)

| Event | Status | Notes |
|---|---|---|
| `session.update` | Supported | `tools` and `tool_choice` move from Rejected/Ignored to Supported; see §3. |
| `conversation.item.create` | Supported (subset, §5) | Now also accepts `function_call_output`. `function_call` items remain Rejected: seeding a call the model never made has no consumer. |

---

## §3 — session object (rows changed)

| Field | Status | Accepted values | Echo |
|---|---|---|---|
| `tools` | **Supported** | array of `{type:"function", name, description?, parameters?}`. `type` and `name` required (`invalid_value`, `param: "session.tools[i].type"` / `[i].name`); `parameters` is an opaque JSON Schema **object**, passed to the provider verbatim and never interpreted by Cascade; omitted `parameters` means a no-argument tool. Duplicate `name` → Rejected (`param: "session.tools[i].name"`). MCP tools (`RealtimeToolsConfigUnion.Mcp`) → Rejected (`param: "session.tools[i].type"`). | effective array; `[]` when never set |
| `tool_choice` | **Supported** | `"none"` \| `"auto"` \| `"required"`, or `{type:"function", name}`. `ToolChoiceMcp` → Rejected. A forced function whose `name` is not in `tools` → Rejected (`param: "session.tool_choice.name"`). | effective value |

**Default and canonicalization.** Cascade's session model owns the default: `tool_choice` defaults to
`"auto"` and the effective value is **always** sent to the LLM provider. No code path omits the field
and inherits a provider default. This is a Cascade decision, not a GA claim — the reference documents
the four accepted forms but states no default.

**Continuity.** §10.3 already has Cascade echoing `tools: []` and `tool_choice: "auto"` for every
session. Making them effective therefore changes **no existing session object on the wire**, and no
golden trace's `session.created` / `session.updated` frame moves.

`response.create.response.tools` / `.tool_choice` stay **Rejected** (§4 unchanged): the only consumer
sends a bare `response.create` after returning a tool result, and per-response tool overrides can be
added later without breaking anything.

---

## §5 — items

Two rows added to the acceptance table:

| Item | Status |
|---|---|
| `{type:"function_call_output", call_id, output}` | **Supported** via `conversation.item.create`. `call_id` and `output` required. |
| `{type:"function_call", …}` | **Rejected** for client creation (`invalid_value`, `param: "item.type"`); emitted by the server only. |

**Wire shapes** (added to the "Item shape on the wire" paragraph):

```
function_call         {id, object:"realtime.item", type:"function_call",
                       name, call_id, arguments, status}
function_call_output  {id, object:"realtime.item", type:"function_call_output",
                       call_id, output, status}
```

`arguments` and `output` are JSON-encoded strings. Cascade never parses either one; `output` is free
text per the reference and may be empty.

**`call_id` validation.** The reference is explicit (`ConversationItemWithReference.call_id`): *"If
passed on a `function_call_output` item, the server will check that a `function_call` item with the
same ID exists in the conversation history."* Cascade does the same — unknown `call_id` → Rejected
(`invalid_value`, `param: "item.call_id"`).

**Duplicate output — a Cascade profile decision** (approved 2026-09-01). The reference does not
specify duplicate-output semantics, so this is Cascade's rule, not a GA claim: a second
`function_call_output` for a `call_id` that has already been answered is **Rejected**,
`invalid_value`, `param: "item.call_id"`, message "a function_call_output already exists for this
call". Rationale: two results for one call would put contradictory tool output into the LLM context
with no rule for which wins.

`status` on client-created items stays Ignored, as it already is for messages.

---

## §6 — server events

Two events added:

| Event | Emitted when |
|---|---|
| `response.function_call_arguments.delta` | once per function call, carrying the **complete** arguments (see the divergence below) |
| `response.function_call_arguments.done` | immediately after, with the final arguments |

Field sets, transcribed from `ResponseFunctionCallArgumentsDeltaEvent` / `…DoneEvent` and confirmed
against the literal payload in the server-events reference:

```
.delta  {event_id, type, response_id, item_id, output_index, call_id, delta}
.done   {event_id, type, response_id, item_id, output_index, call_id, name, arguments}
```

A `function_call` item is a response output item, so it flows through the existing lifecycle:
`response.output_item.added` → `conversation.item.added` → `arguments.delta` → `arguments.done` →
`response.output_item.done` → `conversation.item.done`, and it appears in `response.done.output`.

**`output_index` is no longer always `0`.** The §6 note "`output_index` is always `0`,
`content_index` always `0`" becomes: `content_index` is always `0` (one part per message item);
`output_index` is `0` for the message item and `1` for the function_call item on a turn that both
spoke and called. On a call-only turn the function_call item is at `0`. `content_index` does not apply
to function_call items.

**Documented GA deviation — `arguments.done` on an interrupted, cancelled or incomplete call**
(approved 2026-09-01). The reference documents `…DoneEvent` as *"Also emitted when a Response is
interrupted, incomplete, or cancelled"*, i.e. GA emits `.done` carrying whatever partial arguments
accumulated. **Cascade intentionally does not.** The deviation is caused by normalizing argument
fragments at the provider boundary: partial arguments never cross into `internal/session`, so there is
nothing to emit, and a truncated arguments string could only be mis-parsed by the client.

The resulting contract, which clients should be held to:

> `response.function_call_arguments.done` means **the tool call was completely delivered**. It does
> **not** guarantee that `arguments` is valid JSON — a model can emit syntactically invalid arguments
> in a call that completed normally, and Cascade never parses them.

So a client must still handle a parse failure on `arguments`; what it no longer has to handle is a
*truncated* payload arriving under `.done`. On cancellation before a complete call arrives, Cascade
emits no `arguments.*` at all and no function_call item. Once a complete call has been delivered,
cancelling does not retract it.

**Never emitted** (added to that list): `response.mcp_call*`, `mcp_list_tools.*` — already listed;
plus `conversation.item.input_audio_transcription.*` behaviour is unchanged.

---

## §7 — golden sequences

Five sequences added. (4) and (5) replace the "cancel mid-arguments" case from the phase brief: mid-
argument cancellation is invisible at this layer and is tested on the provider adapters instead.

1. **Call-only turn** — `response.created` → `response.output_item.added`(function_call, index 0) →
   `conversation.item.added` → `arguments.delta` → `arguments.done` → `response.output_item.done` →
   `conversation.item.done` → `response.done{completed}`. No message item, no audio.
2. **Text + call turn** — the message item at index 0 with its transcript/audio events, then the
   function_call item at index 1.
3. **Full round trip** — sequence 1, then `conversation.item.create{function_call_output}` →
   `conversation.item.added` → `conversation.item.done`, then `response.create` → a spoken response.
4. **Cancel before a complete call** — no function_call item anywhere in the trace;
   `response.done{cancelled}` with `output: []`.
5. **Cancel after a delivered call** — the function_call item is present and stays in
   `response.done{cancelled}.output`.

**Normalizer.** `<call:n>` is added alongside `<item:n>` / `<resp:n>` so `call_id` is stable across
runs.

---

## §8 — error event

New rejection rows, all `invalid_value` with `type: "invalid_request_error"`:

| Cause | `param` |
|---|---|
| tool entry missing `type` / not `"function"` | `session.tools[i].type` |
| tool entry missing `name`; duplicate name | `session.tools[i].name` |
| `parameters` not an object | `session.tools[i].parameters` |
| `tool_choice` not one of the four forms | `session.tool_choice` |
| forced function not present in `tools` | `session.tool_choice.name` |
| client-created `function_call` item | `item.type` |
| `function_call_output` with unknown `call_id` | `item.call_id` |
| second `function_call_output` for one call | `item.call_id` (pending the `PROTOCOL-VERIFY` above) |
| `function_call_output` missing `output` | `item.output` |

---

## §9 — identifiers

**All conversation items, function_call items included, use `item_` ids.** Tool-invocation linkage
uses a separate `call_` id: `call_` + 16 lowercase alphanumerics, generated by the adapter for each
function call. Golden traces normalize it to `<call:n>`.

General rule, now explicit: **GA example prefixes such as `fc_` are not copied unless the reference
explicitly requires them.** §9 already declares id formats to be Cascade's own; the ids in the
official docs (`fc_001`, `call_001`, `resp_002`) are illustrative payloads, not a spec constraint, and
the only consumer of the linkage reads `call_id`.

---

## §10 — decisions to append

12. `tools` / `tool_choice` become effective rather than echoed-only. `tool_choice` defaults to
    `"auto"` in Cascade's session model and is always sent to the provider; no provider default is
    ever inherited. Existing echoes (`tools: []`, `tool_choice: "auto"`) are unchanged, so no existing
    trace moves.
13. At most one function call per response; `parallel_tool_calls` stays Rejected and is sent to
    providers as `false`. A provider returning more than one call is a `provider.Error`, never a
    silent drop.
14. `response.function_call_arguments.delta` is emitted **once** with the complete arguments. Fragment
    accumulation belongs to the provider adapter ("provider specifics are normalized at the
    boundary"), and the same precedent already exists for input transcription, which emits one delta
    per ASR final segment rather than per token.
15. Cascade does **not** emit `arguments.done` for an interrupted, cancelled or incomplete call, where
    GA does with partial arguments — a documented deviation caused by normalizing argument fragments
    at the provider boundary. `arguments.done` therefore means the call was completely delivered; it
    does **not** guarantee `arguments` is valid JSON, which Cascade never parses.
18. A second `function_call_output` for an already-answered `call_id` is Rejected
    (`invalid_value`, `param: "item.call_id"`). A Cascade profile decision: GA does not specify
    duplicate-output semantics.
19. GA example id prefixes (`fc_`, `call_001`, …) are not copied unless the reference explicitly
    requires them. All conversation items use `item_`; tool linkage uses `call_`.
16. The message output item is created lazily, on the first text delta, so a call-only turn emits no
    empty message item (GA-faithful). Any existing golden trace this moves is reported individually
    before regeneration.
17. Tools are executed by the client, never by the gateway. Cascade forwards the call, accepts the
    result as an opaque string, and feeds it back into the next generation. It interprets no tool
    semantics — including refusals, which are ordinary results.

---

## Resolved `PROTOCOL-VERIFY` items from the phase brief §3

| # | Question | Resolution |
|---|---|---|
| 1 | Exact wire shape of both item types | **Resolved** — transcribed from `RealtimeConversationItemFunctionCall` / `…FunctionCallOutput`; see §5 |
| 2 | Does `arguments.delta` carry `call_id`? | **Resolved — yes**, plus `event_id`, `item_id`, `output_index`, `response_id`, `delta` |
| 3 | Does GA validate `function_call_output.call_id`? | **Resolved — yes**, the reference states the server checks a matching `function_call` exists. Duplicate output is unspecified by GA and is settled as a Cascade profile decision (§5, decision 18) |
| 4 | `response.done.status` for a tool-call turn | **Resolved by elimination** — the status enum is `completed / cancelled / failed / incomplete / in_progress`; a tool call is none of the failure states, and `…DoneEvent`'s own wording contrasts the normal case with "interrupted, incomplete, or cancelled". `finish_reason: "tool_calls"` → `FinishStop` → `completed` |
| 5 | Is `tools` echoed when never set? | **Resolved** — §10.3 already echoes `[]`; unchanged |

---

## Approved resolutions (2026-09-01)

1. **Duplicate `function_call_output`** — reject the second output for an already-answered `call_id`
   with `invalid_value`, `param: "item.call_id"`. A Cascade profile decision, because GA does not
   specify duplicate-output semantics.
2. **Incomplete tool-call arguments** — Cascade intentionally does not emit
   `response.function_call_arguments.done` for interrupted, cancelled or incomplete tool calls. A
   documented GA deviation caused by normalizing argument fragments at the provider boundary.
   `arguments.done` therefore means the tool call was completely delivered, but does **not** guarantee
   that `arguments` is valid JSON.
3. **Ids** — all conversation items, including function_call items, use `item_` ids; tool-invocation
   linkage uses a separate `call_` id; golden traces normalize call ids as `<call:n>`. GA example
   prefixes such as `fc_` are not copied unless the reference explicitly requires them.

Everything else in this delta is transcription from the pinned reference, not a judgement call.
