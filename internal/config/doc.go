// Package config loads, expands and validates the single Cascade configuration
// file into one complete Config struct.
//
// Layering: config is a leaf package. It must not import any other package
// under internal/ (in particular not provider, session, protocol or server).
// Provider-name existence is checked through an injected ProviderLookup so
// the registry never has to be imported here.
package config
