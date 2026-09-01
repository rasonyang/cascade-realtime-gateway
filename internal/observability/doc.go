// Package observability sets up structured logging with secret redaction and,
// from Phase 5 on, OpenTelemetry tracing and metrics.
//
// Layering: observability is a leaf package. It must not import any other
// package under internal/.
package observability
