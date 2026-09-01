// Package session is Cascade's domain core: a single-goroutine Session actor
// owning the Conversation, InputAudioBuffer, TurnManager and Response FSM,
// with one goroutine group per response running the LLM → sentencer → TTS
// pipeline against the provider interfaces.
//
// Layering: session may import config, provider, vad, audio and recorder. It
// must not import protocol or server: the OpenAI protocol is a projection of
// the internal Command/Event types, never the other way round.
//
// Concurrency: all mutable state lives in Session fields touched only by the
// run goroutine. Other goroutines only post to channels. Commands (intent,
// inbound) and Events (facts, outbound) are two sealed type sets.
package session
