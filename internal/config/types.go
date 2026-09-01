package config

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// Duration is a time.Duration that is encoded in JSON as a Go duration
// string such as "30m" or "2s". Numbers are rejected to avoid unit ambiguity.
type Duration time.Duration

// Std returns the value as a time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// String returns the Go duration string form.
func (d Duration) String() string { return time.Duration(d).String() }

// MarshalJSON encodes the duration as a string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON decodes a duration string; anything else is an error.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string such as \"30m\", got %s", string(b))
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q", s)
	}
	*d = Duration(v)
	return nil
}

// MaxOutputTokens mirrors the GA session field: either the string "inf" or a
// positive integer.
type MaxOutputTokens struct {
	Inf bool
	N   int
}

// MarshalJSON encodes "inf" or the integer.
func (m MaxOutputTokens) MarshalJSON() ([]byte, error) {
	if m.Inf {
		return json.Marshal("inf")
	}
	return json.Marshal(m.N)
}

// UnmarshalJSON accepts "inf" or an integer.
func (m *MaxOutputTokens) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		if s != "inf" {
			return fmt.Errorf("max_output_tokens must be \"inf\" or an integer, got %q", s)
		}
		*m = MaxOutputTokens{Inf: true}
		return nil
	}
	n, err := strconv.Atoi(string(b))
	if err != nil {
		return fmt.Errorf("max_output_tokens must be \"inf\" or an integer, got %s", string(b))
	}
	*m = MaxOutputTokens{N: n}
	return nil
}

// FieldError reports a configuration problem at a dotted field path, e.g.
// `field "providers.llm.type": unknown provider "foo"`.
type FieldError struct {
	Field string
	Msg   string
}

func (e *FieldError) Error() string {
	return fmt.Sprintf("field %q: %s", e.Field, e.Msg)
}

func fieldErrorf(field, format string, args ...any) error {
	return &FieldError{Field: field, Msg: fmt.Sprintf(format, args...)}
}
