// Package qwen implements Cascade's three providers on Alibaba Cloud's
// Qwen / DashScope endpoints, all registered under the name "qwen":
//
//   - LLM: the OpenAI-compatible streaming Chat Completions API under
//     /compatible-mode/v1, with enable_thinking forced to false.
//   - ASR: qwen-audio-3.0-asr-flash-streaming over the DashScope duplex
//     WebSocket at /api-ws/v1/inference.
//   - TTS: qwen-audio-3.0-tts-flash over the same WebSocket endpoint.
//
// Layering: qwen imports provider, config and audio only. Every DashScope
// wire shape (the header/payload envelope, run-task / continue-task /
// finish-task actions and the task-started / result-generated /
// task-finished / task-failed events) stays inside this package; what
// leaves it is already normalized to the provider interfaces. ctx is the
// only cancellation mechanism: cancelling it closes the socket and reclaims
// every goroutine.
package qwen
