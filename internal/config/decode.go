package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// DecodeRuntimeConfig decodes a whole runtime configuration document. field is
// the dotted path the document sits at ("" for a state file or an Admin
// request body, "bootstrap" for the seed block) and prefixes every reported
// field path. Unknown fields are errors; each profile is decoded over
// DefaultProfile so omitted fields take their documented defaults.
func DecodeRuntimeConfig(raw []byte, field string) (*RuntimeConfig, error) {
	var doc struct {
		Providers map[string]json.RawMessage `json:"providers"`
		Profiles  map[string]json.RawMessage `json:"profiles"`
		Settings  Settings                   `json:"settings"`
	}
	if err := strictDecode(raw, field, &doc); err != nil {
		return nil, err
	}
	out := &RuntimeConfig{
		Providers: make(map[string]*ProviderInstance, len(doc.Providers)),
		Profiles:  make(map[string]*Profile, len(doc.Profiles)),
		Settings:  doc.Settings,
	}
	for name, body := range doc.Providers {
		p, err := DecodeProviderInstance(body, joinPath(field, "providers."+name), name)
		if err != nil {
			return nil, err
		}
		out.Providers[name] = p
	}
	for name, body := range doc.Profiles {
		p, err := DecodeProfile(body, joinPath(field, "profiles."+name), name)
		if err != nil {
			return nil, err
		}
		out.Profiles[name] = p
	}
	return out, nil
}

// DecodeProviderInstance decodes one provider instance. name fills in an
// omitted `name` field so a caller addressing the resource by key or URL path
// need not repeat it; a name that is present and different is left alone for
// Validate to reject.
func DecodeProviderInstance(raw []byte, field, name string) (*ProviderInstance, error) {
	var p ProviderInstance
	if err := strictDecode(raw, field, &p); err != nil {
		return nil, err
	}
	if p.Name == "" {
		p.Name = name
	}
	return &p, nil
}

// DecodeProfile decodes one profile over DefaultProfile and fills the
// mode-specific turn_detection defaults, exactly as the configuration file
// loader does.
func DecodeProfile(raw []byte, field, name string) (*Profile, error) {
	p := DefaultProfile()
	if err := strictDecode(raw, field, p); err != nil {
		return nil, err
	}
	if p.Name == "" {
		p.Name = name
	}
	if p.TurnDetection != nil {
		p.TurnDetection.ApplyDefaults()
	}
	return p, nil
}

// MarshalRuntimeConfig encodes a runtime configuration for the state file:
// indented, newline-terminated and byte-stable across restarts (Go sorts map
// keys). Secrets are written verbatim, including unexpanded {env.NAME}.
func MarshalRuntimeConfig(c *RuntimeConfig) ([]byte, error) {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode runtime config: %w", err)
	}
	return append(b, '\n'), nil
}

// strictDecode decodes one JSON object into `into`, rejecting unknown fields
// and reporting problems as *FieldError paths rooted at field.
func strictDecode(raw []byte, field string, into any) error {
	var tree any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&tree); err != nil {
		return fieldErrorf(orRoot(field), "invalid JSON: %v", err)
	}
	if _, err := dec.Token(); err == nil {
		return fieldErrorf(orRoot(field), "unexpected data after the JSON value")
	}
	if _, ok := tree.(map[string]any); !ok {
		return fieldErrorf(orRoot(field), "must be an object")
	}
	strict := json.NewDecoder(bytes.NewReader(raw))
	strict.DisallowUnknownFields()
	if err := strict.Decode(into); err != nil {
		return prefixFieldError(decodeError(err, tree), field)
	}
	return nil
}

func orRoot(field string) string {
	if field == "" {
		return "(root)"
	}
	return field
}

// prefixFieldError re-roots a *FieldError under prefix.
func prefixFieldError(err error, prefix string) error {
	var fe *FieldError
	if prefix == "" || !errors.As(err, &fe) {
		return err
	}
	return &FieldError{Field: joinPath(prefix, fe.Field), Msg: fe.Msg}
}

// DecodeSettings decodes the settings object, rejecting unknown fields.
func DecodeSettings(raw []byte, field string) (Settings, error) {
	var s Settings
	if err := strictDecode(raw, field, &s); err != nil {
		return Settings{}, err
	}
	return s, nil
}
