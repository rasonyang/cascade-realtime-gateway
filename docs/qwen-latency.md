# Qwen provider latency

Measured 2026-09-01 against the live Alibaba Cloud Model Studio endpoint
`llm-REDACTED.cn-beijing.maas.aliyuncs.com` from a machine in China,
with `hack/qwenbench`. Models: `qwen-audio-3.0-asr-flash-streaming`,
`qwen3.6-flash`, `qwen-audio-3.0-tts-flash` (voice `longanlingxi`).

Reproduce with:

```sh
ALIYUN_API_KEY=… go run ./hack/qwenbench -label before -no-flush-nudge -samples 20 -turns 20
ALIYUN_API_KEY=… go run ./hack/qwenbench -label after                  -samples 20 -turns 20
```

20 warm samples per provider and 20 complete voice turns per run. All values
are milliseconds; percentiles use the nearest-rank method, so every number
below is a sample that was actually measured. The fixture utterance is
*"What is the largest ocean on Earth?"*, synthesized with the real TTS and
trimmed of its leading and trailing silence (2.36 s), so "speech end" is the
acoustic end of the utterance rather than the end of a synthesizer's tail.

**Acceptance metric — end of user speech → first playable audio: p50 1223 ms,
p90 1613 ms, p95 1854 ms.**

## Summary

| | before | after | change |
| --- | ---: | ---: | ---: |
| speech end → first playable audio, p50 | 1423.6 | **1223.1** | −200.5 (−14 %) |
| speech end → first playable audio, p90 | 1915.6 | **1613.1** | −302.5 (−16 %) |
| speech end → first playable audio, p95 | 1935.1 | **1853.9** | −81.2 (−4 %) |
| TTS first text → first audio (in the pipeline), p50 | 399.3 | **233.5** | −165.8 (−42 %) |
| TTS first text → first audio (isolated, incremental), p50 | 543.3 | **258.4** | −284.9 (−52 %) |

The change between the two runs is one provider-local fix, described under
[Bottleneck](#bottleneck). Nothing else differs; both runs use the same
binary and the same flags apart from `-no-flush-nudge`.

## Cold connection versus warm steady state

The two transports behave differently and the distinction matters:

- **LLM (HTTP/2, pooled).** The first request on a fresh client pays a full
  TLS handshake: 232.9 ms cold against a warm p50 of 117.1 ms, so roughly
  **+115 ms once per process**, not once per turn. A long-lived gateway pays
  it at startup and never again.
- **ASR and TTS (WebSocket, not pooled).** Every stream opens its own
  connection, so every stream pays TCP + TLS. Cold and warm are therefore
  within noise of each other — TTS 79.6 ms cold against a warm p50 of
  80.8 ms, ASR 86.2 ms cold against 79.9 ms. The "cold" row is the first
  dial of its section, by which point DNS is already resolved; during
  protocol probing the very first dial of a fresh process took 170–220 ms
  against 65–85 ms afterwards, so first-resolution DNS is worth roughly
  another 100 ms, once.

Neither cost lands on the acceptance metric. The ASR socket is opened when
the session starts, and the TTS socket is opened at the top of the response
pipeline, concurrently with the LLM request — measured provider-side, the
TTS task is acknowledged and idle roughly 250 ms before the first sentence
arrives.

## Per-provider, warm (20 samples each, "after" run)

### ASR — `qwen-audio-3.0-asr-flash-streaming`

One turn per sample on one long-lived connection, audio pushed in real time.

| metric | min | avg | p50 | p90 | p95 | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| run-task → task-started | 14.3 | 34.3 | 20.2 | 31.8 | 36.1 | 262.9 |
| first audio → first result | 625.7 | 696.1 | 638.6 | 778.1 | 827.4 | 1318.3 |
| end of speech → final transcript | 174.3 | 408.7 | 227.5 | 733.7 | 865.8 | 1787.4 |
| end of speech → end_of_turn | 174.3 | 408.7 | 227.5 | 733.7 | 865.8 | 1787.5 |

`Finalize()` is issued the moment the last audio frame is pushed. The final
follows in 227 ms at the median, and the next task is running ~20 ms later on
the same socket. The tail is service-side variance, not queueing: it appears
in both runs and correlates with nothing the gateway does.

### LLM — `qwen3.6-flash`, `enable_thinking: false`

| metric | min | avg | p50 | p90 | p95 | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| request → first token (cold) | 232.9 | — | — | — | — | — |
| request → first token | 91.8 | 123.6 | 117.1 | 141.8 | 168.9 | 230.1 |
| request → first usable TTS chunk | 220.7 | 269.9 | 251.0 | 314.1 | 367.4 | 377.8 |
| request → generation done | 388.3 | 478.0 | 480.1 | 535.6 | 539.2 | 572.4 |

"First usable TTS chunk" is the first sentence long enough to synthesize,
using the same rule as `internal/session/sentencer.go`. Note that it costs
~134 ms beyond the first token: that is the model writing the rest of the
first sentence, and it is irreducible without splitting sentences earlier.

Leaving `enable_thinking` unset is not an option: the model then streams
`delta.reasoning_content` before any answer text, which both delays the first
usable chunk and produces text a voice turn must not speak.

### TTS — `qwen-audio-3.0-tts-flash`

| metric | min | avg | p50 | p90 | p95 | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| connect | 64.4 | 88.5 | 80.8 | 100.1 | 112.4 | 170.0 |
| run-task → task-started | 22.0 | 29.4 | 26.3 | 36.0 | 52.0 | 56.3 |
| first text → first audio (one-shot) | 226.1 | 265.3 | 265.9 | 273.3 | 303.2 | 334.5 |
| first text → first audio (incremental) | 231.9 | 266.9 | 258.4 | 291.5 | 302.5 | 387.9 |
| first text → 100 ms of audio | 226.1 | 265.3 | 265.9 | 273.3 | 303.2 | 334.5 |
| first text → last audio | 806.5 | 871.8 | 847.3 | 928.3 | 1073.0 | 1074.2 |

The first audio frame carries 19 200 bytes — 400 ms of 24 kHz PCM — so
"first audio" and "100 ms of playable audio" are the same instant. Before
the fix the incremental figure was 543.3 ms at the median against 267.9 ms
one-shot; after it, the two paths are indistinguishable.

## End to end through the gateway (20 turns)

Server VAD, `silence_duration_ms = 500`. Every row is measured from the last
audio frame of the utterance, so each includes the VAD hold; the hold is
broken out first so the gateway's own work can be read separately.

| metric | min | avg | p50 | p90 | p95 | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| speech end → commit (VAD hold) | 387.1 | 391.8 | 390.9 | 397.3 | 397.7 | 397.7 |
| speech end → ASR final | 570.3 | 790.2 | 634.6 | 1002.3 | 1326.0 | 2275.9 |
| speech end → LLM first token | 709.9 | 958.5 | 827.1 | 1231.8 | 1489.8 | 2401.1 |
| speech end → first TTS text | 811.4 | 1091.9 | 962.9 | 1368.0 | 1591.7 | 2511.3 |
| first TTS text → provider audio | 193.7 | 237.0 | 233.5 | 261.9 | 262.4 | 269.5 |
| provider audio → client audio | 0.1 | 0.7 | 0.6 | 1.1 | 1.4 | 1.7 |
| **speech end → first playable audio** | **1027.1** | **1329.5** | **1223.1** | **1613.1** | **1853.9** | **2735.8** |
| commit → first playable audio | 638.6 | 937.8 | 831.2 | 1225.9 | 1456.2 | 2341.6 |
| speech end → response.done | 1637.0 | 2235.6 | 2204.5 | 2798.0 | 2821.7 | 3387.4 |

Median budget, as consecutive legs:

| leg | p50 | share |
| --- | ---: | ---: |
| VAD hold (configuration) | 391 | 32 % |
| commit → ASR final | 244 | 20 % |
| ASR final → LLM first token | 192 | 16 % |
| LLM first token → first TTS text | 136 | 11 % |
| first TTS text → first audio | 234 | 19 % |
| Cascade itself (provider → client) | 0.6 | 0 % |

Cascade adds 0.6 ms between the provider's first audio frame and the client's
first delta. All of the latency is provider or configuration.

## Bottleneck

Before the fix the dominant leg was TTS time-to-first-audio at 399 ms p50 and
594 ms p95 — noticeably worse than the 268 ms the provider manages when
synthesizing on its own.

The cause is a service behavior that is not documented: **`qwen-audio-3.0-tts-flash`
holds a completed sentence unsynthesized until further input arrives on the
task.** In a cascaded pipeline the next input is the LLM's next sentence, so
the first audio frame waits for text nobody needs yet. Measured by writing
one sentence and then pausing before the next:

| pause after the sentence | first audio |
| --- | ---: |
| none (write everything, then `finish-task`) | 252 ms |
| 100 ms | 320 ms |
| 300 ms | 588 ms |
| 800 ms | 1054 ms |
| 800 ms, sentence split across two `continue-task` frames | 1051 ms |
| 800 ms, one whitespace `continue-task` after the sentence | **252 ms** |

The pause is paid in full. Splitting the sentence across two frames does not
help — what the service waits for is input *after* a sentence terminator, not
more bytes. A whitespace-only `continue-task` satisfies it.

**The fix** (`internal/provider/qwen/tts.go`): after writing a sentence, if
nothing else is queued, send one space as its own `continue-task`. It is
provider-local, adds no state, and costs one ~100-byte frame per sentence
when the service no longer needs it. `options.no_flush_nudge` turns it off,
because it does lean on undocumented behavior.

It was checked for damage rather than assumed safe: the synthesized audio was
transcribed back through the ASR and is word-for-word identical, and the
utterance is ~640 ms shorter because the inter-sentence pauses are tighter —
which is if anything better for a voice turn.

Two heavier options were rejected. Declaring `IncrementalText: false` would
route through the per-sentence path and get one-shot latency, but puts a
fresh ~80 ms dial on the critical path for every sentence. Opening a second
stream for the first sentence alone would split one utterance across two
synthesis tasks and risk a prosody seam at the join.

### After the fix

No single leg dominates any more: the VAD hold (391 ms, pure configuration),
the ASR final flush (244 ms) and the TTS first packet (234 ms) are within a
factor of 1.7 of each other. Further work has to be chosen, not just found:

- `silence_duration_ms` is the largest single line and is a tuning knob, not
  code. Dropping it to 300 ms would take ~90 ms off every turn at the cost of
  cutting in on speakers who pause mid-sentence. Not changed here: it is a
  product decision, not a latency bug.
- The 136 ms spent completing the first sentence could be cut by splitting
  the *first* segment on a comma. That changes `internal/session`, affects
  every provider, and risks unnatural prosody on the first clause — out of
  scope for a provider change.
- ASR and TTS first-packet times are the services' own; nothing on this side
  reaches them.

### Regressions

`speech end → first playable audio` max went from 2286 ms to 2736 ms. This is
upstream ASR variance, not the fix: in the same run the TTS leg's max improved
from 777 ms to 270 ms, while `speech end → ASR final` max went from 1583 ms to
2276 ms. The ASR tail is present in both runs at both p95 and max and moves
independently of anything the gateway does.

## For comparison

The same gateway on the previous provider set (`gpt-4o-mini` + `tts-1` +
Deepgram `nova-3`, measured 2026-09-01, `docs/decisions.md`) reached first
audio 5.9 s after commit. The Qwen set reaches it in 831 ms at the median.
The bulk of that gap is the network path: the Beijing endpoint answers in
tens of milliseconds from this machine, where the previous stack was paying
0.6–1.1 s TLS handshakes per provider.
