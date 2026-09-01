package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"strconv"
)

// envPlaceholder matches {env.NAME} placeholders inside string values.
var envPlaceholder = regexp.MustCompile(`\{env\.([A-Za-z_][A-Za-z0-9_]*)\}`)

// Load reads the configuration file at path and returns a complete Config:
// the file is decoded strictly (unknown fields are errors) on top of
// DefaultConfig, {env.NAME} placeholders are expanded with an error that
// names the field path when a variable is unset, and mode-specific
// turn_detection defaults are filled in. Load does not call Validate.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		// The caller reports the path; strip it from the OS error to avoid
		// printing it twice.
		var pe *fs.PathError
		if errors.As(err, &pe) {
			err = pe.Err
		}
		return nil, fmt.Errorf("cannot read file: %w", err)
	}
	return Parse(raw, os.LookupEnv)
}

// Parse is Load without the file read. lookupEnv resolves {env.NAME}
// placeholders; a nil lookup treats every variable as unset.
func Parse(raw []byte, lookupEnv func(string) (string, bool)) (*Config, error) {
	if lookupEnv == nil {
		lookupEnv = func(string) (string, bool) { return "", false }
	}

	// Decode into a generic tree first so env expansion can report the
	// dotted field path of the offending value.
	var tree any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&tree); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := ensureEOF(dec); err != nil {
		return nil, err
	}
	if _, ok := tree.(map[string]any); !ok {
		return nil, errors.New("parse config: top-level value must be an object")
	}

	expanded, err := expandEnv(tree, "", lookupEnv)
	if err != nil {
		return nil, err
	}
	normalized, err := json.Marshal(expanded)
	if err != nil {
		return nil, fmt.Errorf("re-encode config: %w", err)
	}

	cfg := DefaultConfig()
	strict := json.NewDecoder(bytes.NewReader(normalized))
	strict.DisallowUnknownFields()
	if err := strict.Decode(cfg); err != nil {
		return nil, decodeError(err, expanded)
	}

	if td := cfg.SessionDefaults.Audio.Input.TurnDetection; td != nil {
		td.applyDefaults()
	}
	return cfg, nil
}

func ensureEOF(dec *json.Decoder) error {
	if _, err := dec.Token(); err == nil {
		return errors.New("parse config: unexpected data after top-level object")
	}
	return nil
}

// expandEnv walks the generic JSON tree and replaces {env.NAME} placeholders
// in every string. path is the dotted path of node, used in errors.
func expandEnv(node any, path string, lookupEnv func(string) (string, bool)) (any, error) {
	switch v := node.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, child := range v {
			expandedChild, err := expandEnv(child, joinPath(path, k), lookupEnv)
			if err != nil {
				return nil, err
			}
			out[k] = expandedChild
		}
		return out, nil
	case []any:
		out := make([]any, len(v))
		for i, child := range v {
			expandedChild, err := expandEnv(child, path+"["+strconv.Itoa(i)+"]", lookupEnv)
			if err != nil {
				return nil, err
			}
			out[i] = expandedChild
		}
		return out, nil
	case string:
		var missing string
		expanded := envPlaceholder.ReplaceAllStringFunc(v, func(m string) string {
			name := envPlaceholder.FindStringSubmatch(m)[1]
			val, ok := lookupEnv(name)
			if !ok && missing == "" {
				missing = name
			}
			return val
		})
		if missing != "" {
			return nil, fieldErrorf(path, "environment variable %s is not set", missing)
		}
		return expanded, nil
	default:
		return node, nil
	}
}

func joinPath(base, key string) string {
	if base == "" {
		return key
	}
	return base + "." + key
}

var unknownFieldRe = regexp.MustCompile(`json: unknown field "([^"]+)"`)

// decodeError rewrites encoding/json errors into FieldError values that name
// the dotted path where possible.
func decodeError(err error, tree any) error {
	if m := unknownFieldRe.FindStringSubmatch(err.Error()); m != nil {
		if path, ok := findKeyPath(tree, "", m[1]); ok {
			return fieldErrorf(path, "unknown field")
		}
		return fieldErrorf(m[1], "unknown field")
	}
	var ute *json.UnmarshalTypeError
	if errors.As(err, &ute) {
		path := ute.Field
		if path == "" {
			path = "(root)"
		}
		return fieldErrorf(path, "expected %s, got %s", ute.Type, ute.Value)
	}
	return fmt.Errorf("decode config: %w", err)
}

// findKeyPath returns the dotted path of the first object key equal to name.
func findKeyPath(node any, path, name string) (string, bool) {
	switch v := node.(type) {
	case map[string]any:
		if _, ok := v[name]; ok {
			return joinPath(path, name), true
		}
		for k, child := range v {
			if p, ok := findKeyPath(child, joinPath(path, k), name); ok {
				return p, true
			}
		}
	case []any:
		for i, child := range v {
			if p, ok := findKeyPath(child, path+"["+strconv.Itoa(i)+"]", name); ok {
				return p, true
			}
		}
	}
	return "", false
}
