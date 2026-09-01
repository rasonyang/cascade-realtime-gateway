// Package protocol is the OpenAI Realtime GA wire layer: it decodes and
// validates client events against docs/protocol-profile.md, turns them into
// session Commands, and projects session Events onto GA server events. It
// owns every protocol identifier (event_id, item_xxx, resp_xxx, sess_xxx,
// conv_xxx) in its mapping tables; the session only knows opaque refs.
//
// Layering: protocol imports session, config and audio. It must not import
// server. The session never imports this package: the wire format is a
// projection of internal state, never the other way round.
package protocol
