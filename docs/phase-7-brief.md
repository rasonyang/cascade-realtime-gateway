# Phase 7 — Function calling (tools)

Status: **approved 2026-09-01 — ready to implement.** Direction approved with the corrections in
§5.2 / §5.5 / §6 folded in; the protocol delta (`docs/protocol-profile-phase7-delta.md`) is approved
and every `PROTOCOL-VERIFY` is closed. Acceptance item 1 is satisfied.

Approved at direction sign-off:

| Decision | Outcome |
|---|---|
| §4 lazy message-item creation | **(a) approved** — defer to the first text delta; regenerate a golden trace only if it actually changes, and report each change individually |
| One function call per response, `parallel_tool_calls: false` | Approved |
| One complete `arguments.delta` instead of fragments | Approved |
| Phase goal | Unblock aicc's `transfer_to_agent`, `take_message`, `hangup` — **not** full GA tool parity |
| `tool_choice` default | Resolved in Cascade's canonical session model (§5.1), never left to a provider default |

`docs/implementation-brief.md` ends at Phase 5 and is preserved verbatim; Phase 6 (Admin REST API)
and this phase are additions to it. The working rules of that brief §0 apply unchanged — in
particular rule 3 (**GA only, never guess, mark `PROTOCOL-VERIFY` and stop to ask**), rule 4
(minimalism), and rule 6 (`go build ./... && go vet ./... && go test -race ./...` green before the
phase is done). `CLAUDE.md` remains authoritative where they conflict.

---

## 1. Why this phase exists

`aicc` (the FreeSWITCH call-centre service on the same host) cannot route any flow-bound DID to
Cascade without function calling. Its Flow DSL v2 gives the model the conversation and the flow the
phases, and every phase transition is the result of a tool call: `transfer_to_agent`,
`take_message`, `hangup`. None of the three is decided by text-intent classification. So the current
`session.tools` rejection is not a feature gap for them, it is a hard block — only free-chat DIDs can
use the gateway today.

Their constraints, which this design must satisfy:

- Tools are **executed by the client**, never by the gateway. The gateway is a conduit: the LLM emits
  a call, the gateway forwards it, the client returns the result, the gateway feeds it back into the
  next generation. The gateway interprets no tool semantics.
- **A tool may refuse, and a refusal is part of the conversation, not an error.** aicc folds steering
  hints into the tool result itself so the model changes course in the same turn. For the gateway
  this is transparent — the result is an opaque string — but it is the reason the gateway must not
  "absorb" tool calls internally.
- They consume exactly one inbound event: `response.function_call_arguments.done` (`call_id`, `name`,
  `arguments`), and reply with `conversation.item.create` carrying `function_call_output`, followed
  by a plain `response.create`.

This phase deliberately supersedes the Phase 6 constraint "no new `session.update` fields": `tools`
**is** the new field, and adding it is the phase.

---

## 2. Scope

**In scope**

- `session.tools` / `session.tool_choice` accepted, validated, echoed.
- One `function_call` output item per response, with its `response.function_call_arguments.*` events.
- `conversation.item.create` with `function_call_output`, fed back into the next generation.
- `provider.LLM` carries tool definitions out and complete tool calls back; `openai` and `qwen`
  adapters implement it.
- The cascade pipeline never speaks tool-call arguments.

**Explicitly out of scope** (each is a deliberate cut, not an oversight)

| Cut | Reason |
|---|---|
| Parallel tool calls (>1 per response) | `parallel_tool_calls` stays Rejected and is sent to providers as `false`. aicc's three tools are mutually exclusive actions; nothing is lost, and a single call keeps the response FSM simple. |
| `tools` / `tool_choice` as `response.create` per-response overrides | aicc sends a bare `response.create` after the tool output. Session-level only (brief §0 rule 4). |
| Client-created `function_call` items via `conversation.item.create` | Seeding a call the model never made has no consumer. Stays Rejected. |
| MCP tools, `item_reference`, remote tool servers | Out of the v1 compatibility profile. |
| Streaming `arguments` fragments to the client | See §5.4. |

---

## 3. Step 1 — protocol profile delta (approval gate)

**Produce the `docs/protocol-profile.md` delta first and stop for approval, before writing code.**
This mirrors Phase 2's discipline and is the only way the wire shapes get checked against the
official GA reference rather than guessed.

Sections to extend: §2 (client events), §3 (session object), §5 (items), §6 (server events),
§7 (golden sequences), §8 (error event).

Carry `PROTOCOL-VERIFY` markers on at least these, and stop to ask rather than guess:

1. The exact wire shape of the `function_call` item (`{id, object, type, name, call_id, arguments,
   status}`?) and of `function_call_output` (`{id, object, type, call_id, output}`?).
2. Whether `response.function_call_arguments.delta` carries `call_id` in addition to `item_id` /
   `output_index` / `response_id`.
3. Whether GA validates `function_call_output.call_id` against a known call, and what it does with a
   duplicate output for the same `call_id`.
4. `response.done.status` for a turn that ended in a tool call (expected `"completed"`, with the
   function_call item in `output`) and the `finish_reason` mapping.
5. Whether `tools` is echoed in `session.created` for a session that never set it (today it echoes
   `[]`; that must not change for non-tool sessions).

---

## 4. The one decision that can touch existing golden traces

Today `response.output_item.added` + `response.content_part.added` are emitted at **pipeline start**,
before any LLM output. On a call-only turn — aicc's common case, `transfer_to_agent` with no spoken
preamble — that produces an **empty message item** in `response.done.output`, which GA does not do:
a tool-call turn's output contains only the function_call item.

| Option | Behaviour | Cost |
|---|---|---|
| **(a) Defer message-item creation to the first text delta** | GA-faithful; a call-only turn emits no message item at all | May reorder events in existing deterministic traces → golden regeneration |
| (b) Keep creation at pipeline start | Traces certainly stable | An empty message item on every call-only turn; a documented divergence from GA |

**Recommendation: (a).** Verify empirically before assuming a cost: run the Phase 2–5 golden tests
first, and regenerate with `-update` **only if** the ordering actually changes, reporting exactly
which traces moved and why. Given the Phase 6 precedent ("traces byte-identical"), regenerating any
trace needs explicit sign-off.

**This is the top approval question of the phase.**

---

## 5. Design

### 5.1 `internal/provider` — interface changes

Typed structs only; no `map[string]any` crosses the interface.

```go
// ToolDef is one function the model may call. Parameters is an opaque JSON
// Schema object, passed through verbatim; Cascade does not interpret it.
type ToolDef struct {
    Name        string
    Description string
    Parameters  json.RawMessage
}

// ToolChoice mirrors the GA field: "auto" | "none" | "required", or a
// specific function by name.
type ToolChoice struct {
    Mode string // "auto" | "none" | "required" | "function"
    Name string // set only when Mode == "function"
}

// ToolCall is one complete call the model asked for. Arguments is the raw
// JSON string the model produced; Cascade never parses it.
type ToolCall struct {
    ID        string
    Name      string
    Arguments string
}
```

`ChatRequest` gains `Tools []ToolDef` and `ToolChoice *ToolChoice` (nil = provider default).

`Message` gains the two shapes history replay needs:

```go
type Message struct {
    Role       Role
    Content    string
    ToolCalls  []ToolCall // assistant turn that called tools
    ToolCallID string     // Role == RoleTool: which call this answers
}

const RoleTool Role = "tool"
```

`LLMChunk` gains `LLMToolCall` with a `ToolCall` field.

**Fragment accumulation happens inside each adapter, never in the session.** OpenAI streams
`delta.tool_calls` as `{index, id, function:{name, arguments}}` with `arguments` split across chunks;
the adapter accumulates by index and emits **one complete `LLMToolCall` chunk before `LLMDone`**.
This is the "provider specifics are normalized at the boundary" rule — the session must never see a
partial call. Adapters send `parallel_tool_calls: false`; if a provider returns more than one call
anyway, the adapter emits the first and returns a `provider.Error` for the rest rather than silently
dropping them.

`finish_reason: "tool_calls"` maps to `FinishStop` (a tool call is a completed turn, not a truncated
one).

**`tool_choice` is resolved in Cascade's canonical session model, never left to a provider default.**
`config.Profile` / `SessionDefaults` carry an explicit `tool_choice` whose default is `"auto"` — the
value profile §10.3 already echoes today — and the effective value is **always** serialized into
`ChatRequest.ToolChoice` and sent to the provider. No code path may omit the field and inherit
whatever OpenAI or DashScope happens to default to. `ToolChoice` is therefore a value, not a pointer,
in `ChatRequest`.

**`qwen` needs live verification, not assumption.** DashScope's compatible-mode endpoint has already
deviated twice in ways no document mentioned (`reasoning_content` leaking into deltas, the TTS flush
stall). Its `tool_calls` streaming shape must be checked against the live endpoint under
`-tags qwenreal` before the adapter is considered done.

### 5.2 `internal/session` — conversation and FSM

**Item model.** `ContentKind` gains `ContentToolCall` and `ContentToolOutput`. The `Item` struct
gains `CallID string` and reuses `Text` for the arguments / output payload. No new item container.

**`messages()` replay.** An assistant turn that spoke *and* called is stored as two items but must be
replayed as **one** `provider.Message` (`Role: assistant`, `Content: <text>`, `ToolCalls: [call]`) —
merge an assistant message item with an immediately following `function_call` item. A
`function_call_output` item becomes `{Role: RoleTool, ToolCallID: call_id, Content: output}`. State
this explicitly; it is where history replay breaks silently if left implicit.

**Response FSM.** `response` gains `toolCall *provider.ToolCall` and `spoke bool` (whether any text
delta arrived). `done()` becomes: `llmDone && (textOnly || !spoke || (ttsDone && audioFlushed))` — a
call-only turn has no audio to flush. `outcome()` is unchanged: a tool call completes.

**Item creation stays in the actor, and `pipeToolCall` is the single delivery path.** The pipeline
sends exactly one `pipeToolCall{Gen, Call}` the moment the adapter yields a complete call; the actor
commits the `function_call` item and emits its events **immediately** on receipt, not at
`pipeLLMDone`. `pipeLLMDone` must **not** carry the `ToolCall` as well — one fact, one path. Two
paths would mean two places that can create the item and an ordering question between them
(`arguments.done` before or after the LLM finishes), which is exactly the kind of ambiguity the
actor model exists to prevent. `response.toolCall` is recorded for `response.done.output`; the
delivery itself has already happened.

`SessionPatch` gains `Tools []ToolDef` and `ToolChoice`. Both apply from the next response; an
in-progress response is unaffected, exactly like `instructions`.

### 5.3 `internal/session/pipeline.go`

`llmStage` already switches on `c.Kind`. Add `provider.LLMToolCall`: record it on the pipeline and
send `pipeToolCall{Gen, Call}` to the actor. **It is never written to `chunks`**, so the sentencer
never sees it and TTS never speaks JSON arguments.

Text and tool-call deltas arrive on the same stream and are distinguishable per chunk, so **no
buffering and no lookahead are needed** — text keeps flowing to TTS as it arrives. This preserves the
"never wait for the full reply" rule, and it means text + call in one response is not a design choice
but a consequence: a spoken preamble ("Let me check that for you.") is already half-synthesized
before the call is known. The brief therefore *states* text+call coexistence rather than asking about
it, and `output_index` is no longer always `0` (message at 0 when text exists, function_call after
it). §6 of the protocol profile must be updated accordingly.

### 5.4 `internal/protocol`

**Inbound.** `session.tools` accepted as an array of `{type:"function", name, description,
parameters}` — the flat GA shape aicc already sends. `session.tool_choice` accepts all four GA forms
and passes them through (one decode branch; refusing three of them only buys a second round trip
later). `conversation.item.create` accepts `{type:"function_call_output", call_id, output}`.

**Outbound.** On a tool call the adapter emits, in order:

```
response.output_item.added        (function_call item, status in_progress)
conversation.item.added
response.function_call_arguments.delta   (complete arguments, one delta)
response.function_call_arguments.done    (call_id, name, arguments)
response.output_item.done         (function_call item, status completed)
conversation.item.done
```

**One delta carrying the complete arguments**, not fragments. The provider layer normalizes fragments
away (§5.1), aicc consumes only `.done`, and Cascade already has this precedent — input transcription
emits one delta per ASR final segment rather than per token. Record it as a profile decision, not an
accident.

`response.done.output` carries the function_call item (and the message item before it, when the turn
also spoke).

### 5.5 Rules for the awkward cases

These need explicit answers in the profile; they are the ones that bite in production.

| Case | Rule |
|---|---|
| `function_call_output.call_id` names no known call | Rejected: `invalid_value`, `param: "item.call_id"`. `PROTOCOL-VERIFY` whether GA does the same. |
| A second `function_call_output` for the same `call_id` | Rejected: `invalid_value`, `param: "item.call_id"`. `PROTOCOL-VERIFY`. |
| Interrupt **while** arguments are still streaming | The response is cancelled and **no** function_call item is delivered; the client is never asked to answer a call it did not see. Argument fragments are normalized away inside the adapter (§5.1), so this case is **invisible to Session** and is tested at the provider-adapter layer, not with a session golden. GA emits `arguments.done` with partial arguments here; Cascade does not (see the delta doc). |
| Interrupt **after** `arguments.done` was emitted | The item was delivered, so it **stays in the conversation**. The client may legally send its `function_call_output` and a `response.create` against the now-cancelled response; both are accepted. `response.done.output` for the cancelled turn still lists the delivered function_call item. |
| The model calls a tool with `output_modalities: ["text"]` | Identical handling; only the message item's part type differs. |
| `max_output_tokens` reached mid-arguments | `incomplete` / `max_output_tokens`; no function_call item is delivered (arguments are unusable). |

---

## 6. Testing

Layered exactly as `CLAUDE.md` requires — deterministic control sequences by full-order golden
comparison, non-deterministic streams by causal invariants only.

- **Mock LLM** gains a tool script: emit N text tokens, then a tool call, then Done — so call-only,
  text+call and text-only turns are all deterministic.
- **New golden traces** (session/protocol layer only — argument fragments never reach it):
  (1) call-only turn, (2) text + call turn, (3) full round trip — call → `function_call_output` →
  second response that speaks, (4) **cancel before a complete call arrives** (no function_call item
  in the trace), (5) **cancel after the call was delivered** (the item is in the trace and in
  `response.done.output`).
- **Provider-adapter tests** own the fragment layer: accumulation across chunks, a stream cancelled
  mid-arguments yielding no `LLMToolCall`, and more-than-one-call handling. These are unit tests on
  `openai` / `qwen`, not session goldens.
- **Invariants**: no audio delta ever carries tool-call text; `arguments.done` never precedes
  `output_item.added`; no `arguments.*` after `cancelled`.
- **Existing Phase 2–5 traces byte-identical**, except as approved under §4.
- **Session tests**: `messages()` replay shape for text+call and for the tool result; FSM `done()` on
  a call-only turn; the two interrupt cases in §5.5.
- **Protocol tests**: field paths for every rejection in §5.5; `tools` echo unchanged (`[]`) for a
  session that never sets it.
- **Live provider check** (`-tags qwenreal`): qwen `tool_calls` streaming shape, fragment
  accumulation, `finish_reason` mapping.
- **Real e2e** (`-tags e2e`, alongside `TestE2EQwenAdminProfile`): through an admin-resolved profile —
  the model calls a tool, the test returns a `function_call_output`, and the second response is
  spoken. This is the test that proves the whole path, not just the protocol layer.

---

## 7. Acceptance

1. ~~`docs/protocol-profile.md` delta approved before any code, with every `PROTOCOL-VERIFY` from §3
   resolved by the official reference rather than inference.~~ **Done** —
   `docs/protocol-profile-phase7-delta.md`, approved 2026-09-01. It is folded into
   `docs/protocol-profile.md` (and this file deleted) in the same change that implements the phase and
   regenerates the traces.
2. The §4 item-timing decision made explicitly, and any golden regeneration reported trace by trace.
3. `go build ./... && go vet ./... && go test -race ./...` green, plus `go vet -tags e2e` and
   `-tags qwenreal`.
4. All four new golden traces pass; all Phase 2–5 traces byte-identical except as approved.
5. Real e2e passes: tool call → client-supplied output → spoken second response, on live providers
   through the Admin-configured profile.
6. TTS never receives tool-call arguments — asserted, not assumed.
7. `docs/decisions.md`, `README.md` and the `CLAUDE.md` provider/protocol sections updated when the
   phase lands (not before).
8. aicc can route a flow-bound DID at the end of it. That is the actual acceptance test; §1's three
   tools working end to end is what "done" means.

---

## 8. Approval state

All four direction questions are answered in the table at the top of this document. The remaining
gate is the protocol delta: `docs/protocol-profile-phase7-delta.md` must be approved before any
implementation code is written. Every `PROTOCOL-VERIFY` item from §3 is resolved there against the
pinned GA reference (`openai-node` v7.8.0), with the one unresolved item and the one deliberate
divergence called out explicitly.
