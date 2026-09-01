# qwenbench — latency benchmark for the Qwen providers

Measures Cascade's Alibaba Cloud Model Studio providers against the live
endpoints: each provider on its own, then a complete voice turn through the
gateway. Every sample is a real, billed request, so the tool is opt-in and
never runs as part of `go test`.

```sh
# from the repository root
ALIYUN_API_KEY=… go run ./hack/qwenbench -samples 20 -turns 20 -json report.json

# the before/after pair used for docs/qwen-latency.md
ALIYUN_API_KEY=… go run ./hack/qwenbench -label before -no-flush-nudge -samples 20 -turns 20
ALIYUN_API_KEY=… go run ./hack/qwenbench -label after                  -samples 20 -turns 20
```

Flags: `-samples` warm samples per provider, `-turns` complete voice turns,
`-only asr,llm,tts,e2e` to run a subset, `-json` to also write the raw
samples, `-label` to tag the report, `-no-flush-nudge` to measure the TTS
path without the flush workaround (see `docs/decisions.md`).

The fixture utterance is synthesized once per run with the real TTS and its
leading and trailing silence is trimmed, so "end of user speech" is the
acoustic end of the utterance rather than the end of a synthesizer's tail.

All values are milliseconds; percentiles use the nearest-rank method, so
every reported number is a sample that was actually measured.

The API key is read from `ALIYUN_API_KEY` and is never logged or written to
the report.
