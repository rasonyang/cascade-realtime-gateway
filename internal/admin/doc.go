// Package admin owns the runtime configuration: provider instances, profiles
// and settings. It holds them as an immutable snapshot behind an
// atomic.Pointer (lock-free reads for every new session), serializes writes
// with a mutex, persists them to a single JSON state file, and exposes the
// /admin/v1 REST API.
//
// Every write follows one path: copy the current configuration, apply the
// change, validate the whole result, build the providers it names, persist it,
// and only then swap the snapshot. A failure at any step leaves both memory
// and the state file untouched.
//
// Layering: admin imports config, provider and observability. Nothing but
// server and cmd imports admin; internal/session and internal/protocol never
// do.
package admin
