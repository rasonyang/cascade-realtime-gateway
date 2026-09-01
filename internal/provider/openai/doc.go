// Package openai implements the LLM provider on the OpenAI-compatible Chat
// Completions streaming API and the TTS provider on the OpenAI speech
// endpoint. Both are registered under the name "openai".
//
// Layering: openai imports provider, config and audio only. Its wire structs
// never leave the package; ctx cancellation closes the HTTP response body
// immediately, which aborts the request.
package openai
