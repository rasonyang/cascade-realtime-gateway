# Decisions

One line per decision, newest last. Format: `YYYY-MM-DD — <decision> (<why>)`.

- 2026-09-01 — Replies to the user are in Chinese per the user's harness language setting; code, comments, docs, commits and logs stay English per `CLAUDE.md`.
- 2026-09-01 — `session_defaults.audio.input.transcription` uses the GA nullable-object shape (no `enabled` field); `null` disables `conversation.item.input_audio_transcription.*` events, an object enables them. The internal ASR always runs for the ASR → LLM pipeline regardless. Exact field subset is frozen in Phase 2 (`PROTOCOL-VERIFY`).
- 2026-09-01 — `session_defaults.audio.output.speed` is in config with default `1.0`, validated against the currently documented GA range `[0.25, 1.5]`; re-verified when the Phase 2 profile is frozen.
- 2026-09-01 — `turn_detection` validation is type-specific: `threshold` / `prefix_padding_ms` / `silence_duration_ms` only for `server_vad`, `eagerness` only for `semantic_vad`; mixed shapes are rejected. Under `semantic_vad` the acoustic VAD (Phase 1) uses the `server_vad` defaults since GA exposes no tuning fields there.
- 2026-09-01 — Config load order: read → `{env.X}` expansion on the generic JSON tree (error names the field path) → strict decode (`DisallowUnknownFields`) over `DefaultConfig()` → mode-specific `turn_detection` defaults → `Validate`. Absent field = default; `"turn_detection": null` = manual mode.
- 2026-09-01 — `config.Validate(known ProviderLookup)` takes an injected lookup so `internal/config` never imports `internal/provider`; a nil lookup skips provider-name existence checks (Phase 0 only, registry wired in Phase 1).
- 2026-09-01 — WebSocket library: `github.com/coder/websocket`; added only when a phase needs it (Phase 2/3).
- 2026-09-01 — Phase 0 `cmd/cascade` with a valid config logs an allowlisted, redacted summary and exits 0; the HTTP/WebSocket server arrives in Phase 3.
