package admin

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
)

// SecretMask is what a secret reads as over the API. Writing it back means
// "keep the stored secret".
const SecretMask = "***"

// apiError is one Admin API failure: an HTTP status, the GA-shaped error type
// in Code, and an optional dotted field path.
type apiError struct {
	Status  int
	Code    string
	Message string
	Param   string
}

func (e *apiError) Error() string { return e.Message }

// Result is the value the admin config-write metric is labelled with.
func (e *apiError) Result() string {
	switch e.Status {
	case http.StatusConflict:
		return "conflict"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusInternalServerError:
		return "persist_error"
	default:
		return "invalid"
	}
}

// badRequest turns a validation or decode failure into a 400 that keeps the
// offending field path.
func badRequest(err error) error {
	var ae *apiError
	if errors.As(err, &ae) {
		return ae
	}
	var fe *config.FieldError
	if errors.As(err, &fe) {
		return &apiError{Status: http.StatusBadRequest, Code: "invalid_request_error",
			Message: fe.Msg, Param: fe.Field}
	}
	return &apiError{Status: http.StatusBadRequest, Code: "invalid_request_error", Message: err.Error()}
}

func notFound(kind, name string) error {
	return &apiError{Status: http.StatusNotFound, Code: "invalid_request_error",
		Message: "No such " + kind + ": " + name + "."}
}

func conflict(msg string) error {
	return &apiError{Status: http.StatusConflict, Code: "invalid_request_error", Message: msg}
}

// wireError is the response body for a failed request; it matches the shape
// the realtime endpoint uses, plus `param`.
type wireError struct {
	Error struct {
		Type    string  `json:"type"`
		Code    *string `json:"code"`
		Message string  `json:"message"`
		Param   *string `json:"param,omitempty"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, err error) {
	ae := &apiError{Status: http.StatusInternalServerError, Code: "server_error", Message: err.Error()}
	var got *apiError
	if errors.As(err, &got) {
		ae = got
	}
	var body wireError
	body.Error.Type, body.Error.Message = ae.Code, ae.Message
	if ae.Param != "" {
		p := ae.Param
		body.Error.Param = &p
	}
	writeJSON(w, ae.Status, body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// ---- redaction -------------------------------------------------------------

// redactProvider returns a copy whose api_key reads as SecretMask.
func redactProvider(p *config.ProviderInstance) *config.ProviderInstance {
	out := p.Clone()
	out.APIKey = SecretMask
	return out
}

// redactConfig returns a copy of the whole configuration with every secret
// masked.
func redactConfig(c *config.RuntimeConfig) *config.RuntimeConfig {
	out := c.Clone()
	for _, p := range out.Providers {
		p.APIKey = SecretMask
	}
	return out
}

// unmask resolves SecretMask back to the stored secret for one instance. A
// mask on an instance that does not exist yet has nothing to keep.
func unmask(next *config.ProviderInstance, cur *config.RuntimeConfig, field string) error {
	if next.APIKey != SecretMask {
		return nil
	}
	prev, ok := cur.Providers[next.Name]
	if !ok {
		return &config.FieldError{Field: field + ".api_key",
			Msg: "cannot keep the current secret: no provider named " + next.Name + " exists"}
	}
	next.APIKey = prev.APIKey
	return nil
}
