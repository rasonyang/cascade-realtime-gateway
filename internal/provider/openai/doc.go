// Package openai implements the LLM provider on the OpenAI-compatible Chat
// Completions streaming API and the TTS provider on OpenAI speech synthesis.
// Both are registered under the name "openai".
//
// The TTS provider has two transports chosen by model ID: a model containing
// "realtime" (the default, gpt-realtime-2.1-mini) speaks the Realtime
// WebSocket API, one connection per synthesis stream; any other model
// (tts-1, tts-1-hd, gpt-4o-mini-tts, all retired on 2027-01-06) uses POST
// /audio/speech.
//
// Layering: openai imports provider, config and audio only. Its wire structs
// never leave the package; ctx cancellation closes the HTTP response body or
// the WebSocket immediately, which aborts the request.
package openai
