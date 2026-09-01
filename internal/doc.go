// Package internal only hosts the import-direction architecture test.
//
// Layering (see CLAUDE.md and arch_test.go):
//
//	server → protocol → session → provider / vad / audio
//	session must not import protocol or server
//	provider must not import session or protocol
//	config and observability are leaves
package internal
