# Cascade

OpenAI Realtime-compatible gateway over a cascaded ASR → LLM → TTS pipeline.

```sh
go build -o cascade ./cmd/cascade
REALTIME_API_KEY=... DEEPGRAM_API_KEY=... OPENAI_API_KEY=... ./cascade -config config.example.json
```

Architecture rules live in `CLAUDE.md`; the implementation plan in `docs/implementation-brief.md`;
the protocol compatibility profile in `docs/protocol-profile.md` (Phase 2).
