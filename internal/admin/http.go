package admin

import (
	"crypto/subtle"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
)

// BasePath is the Admin API root.
const BasePath = "/admin/v1"

// maxBodyBytes caps an Admin request body. Runtime configuration documents are
// small; anything larger is a mistake.
const maxBodyBytes = 1 << 20

// Handler returns the Admin API mounted under BasePath, guarded by apiKey.
// The caller must not mount it when the admin key is unset: the Admin API is
// then disabled entirely.
func (s *Store) Handler(apiKey string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+BasePath+"/config", s.getConfig)
	mux.HandleFunc("PUT "+BasePath+"/config", s.putConfig)
	mux.HandleFunc("GET "+BasePath+"/providers", s.listProviders)
	mux.HandleFunc("GET "+BasePath+"/providers/{name}", s.getProvider)
	mux.HandleFunc("PUT "+BasePath+"/providers/{name}", s.putProvider)
	mux.HandleFunc("DELETE "+BasePath+"/providers/{name}", s.deleteProvider)
	mux.HandleFunc("GET "+BasePath+"/profiles", s.listProfiles)
	mux.HandleFunc("GET "+BasePath+"/profiles/{name}", s.getProfile)
	mux.HandleFunc("PUT "+BasePath+"/profiles/{name}", s.putProfile)
	mux.HandleFunc("DELETE "+BasePath+"/profiles/{name}", s.deleteProfile)
	mux.HandleFunc("GET "+BasePath+"/settings", s.getSettings)
	mux.HandleFunc("PUT "+BasePath+"/settings", s.putSettings)
	return s.authenticate(apiKey, mux)
}

// authenticate gates every route on the admin Bearer token. The realtime key
// is a different credential and is never accepted here.
func (s *Store) authenticate(apiKey string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		h := r.Header.Get("Authorization")
		ok := apiKey != "" && strings.HasPrefix(h, prefix) &&
			subtle.ConstantTimeCompare([]byte(strings.TrimSpace(h[len(prefix):])), []byte(apiKey)) == 1
		if !ok {
			w.Header().Set("WWW-Authenticate", `Bearer realm="cascade-admin"`)
			writeError(w, &apiError{Status: http.StatusUnauthorized, Code: "invalid_request_error",
				Message: "Missing or invalid Authorization header; expected 'Bearer <ADMIN_API_KEY>'."})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		next.ServeHTTP(w, r)
	})
}

// ---- reads -----------------------------------------------------------------

func (s *Store) getConfig(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, redactConfig(s.Config()))
}

func (s *Store) listProviders(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"providers": redactConfig(s.Config()).Providers})
}

func (s *Store) getProvider(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	p, ok := s.Config().Providers[name]
	if !ok {
		writeError(w, notFound("provider", name))
		return
	}
	writeJSON(w, http.StatusOK, redactProvider(p))
}

func (s *Store) listProfiles(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"profiles": s.Config().Profiles})
}

func (s *Store) getProfile(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	p, ok := s.Config().Profiles[name]
	if !ok {
		writeError(w, notFound("profile", name))
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Store) getSettings(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Config().Settings)
}

// ---- writes ----------------------------------------------------------------

func (s *Store) putConfig(w http.ResponseWriter, r *http.Request) {
	s.write(w, r, "config", func(body []byte, cur *config.RuntimeConfig) error {
		doc, err := config.DecodeRuntimeConfig(body, "")
		if err != nil {
			return badRequest(err)
		}
		for name, p := range doc.Providers {
			if err := unmask(p, cur, "providers."+name); err != nil {
				return badRequest(err)
			}
		}
		*cur = *doc
		return nil
	}, func(cur *config.RuntimeConfig) any { return redactConfig(cur) })
}

func (s *Store) putProvider(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	s.write(w, r, "provider "+name, func(body []byte, cur *config.RuntimeConfig) error {
		field := "providers." + name
		p, err := config.DecodeProviderInstance(body, field, name)
		if err != nil {
			return badRequest(err)
		}
		if err := unmask(p, cur, field); err != nil {
			return badRequest(err)
		}
		if cur.Providers == nil {
			cur.Providers = map[string]*config.ProviderInstance{}
		}
		cur.Providers[name] = p
		return nil
	}, func(cur *config.RuntimeConfig) any { return redactProvider(cur.Providers[name]) })
}

func (s *Store) deleteProvider(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	s.write(w, r, "provider "+name, func(_ []byte, cur *config.RuntimeConfig) error {
		if _, ok := cur.Providers[name]; !ok {
			return notFound("provider", name)
		}
		for _, pn := range cur.ProfileNames() {
			p := cur.Profiles[pn]
			if p.ASR == name || p.LLM == name || p.TTS == name {
				return conflict("Provider " + name + " is still referenced by profile " + pn + ".")
			}
		}
		delete(cur.Providers, name)
		return nil
	}, nil)
}

func (s *Store) putProfile(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	s.write(w, r, "profile "+name, func(body []byte, cur *config.RuntimeConfig) error {
		p, err := config.DecodeProfile(body, "profiles."+name, name)
		if err != nil {
			return badRequest(err)
		}
		if cur.Profiles == nil {
			cur.Profiles = map[string]*config.Profile{}
		}
		cur.Profiles[name] = p
		return nil
	}, func(cur *config.RuntimeConfig) any { return cur.Profiles[name] })
}

func (s *Store) deleteProfile(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	s.write(w, r, "profile "+name, func(_ []byte, cur *config.RuntimeConfig) error {
		if _, ok := cur.Profiles[name]; !ok {
			return notFound("profile", name)
		}
		if cur.Settings.DefaultProfile == name {
			return conflict("Profile " + name + " is still referenced by settings.default_profile.")
		}
		delete(cur.Profiles, name)
		return nil
	}, nil)
}

func (s *Store) putSettings(w http.ResponseWriter, r *http.Request) {
	s.write(w, r, "settings", func(body []byte, cur *config.RuntimeConfig) error {
		st, err := config.DecodeSettings(body, "settings")
		if err != nil {
			return badRequest(err)
		}
		cur.Settings = st
		return nil
	}, func(cur *config.RuntimeConfig) any { return cur.Settings })
}

// write runs one mutation end to end: read the body, apply it under the store
// lock, and render the result. respond is nil for deletions (204).
func (s *Store) write(w http.ResponseWriter, r *http.Request, resource string,
	apply func(body []byte, cur *config.RuntimeConfig) error, respond func(*config.RuntimeConfig) any) {

	var body []byte
	if r.Method != http.MethodDelete {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			s.logWrite(r, resource, http.StatusBadRequest)
			writeError(w, &apiError{Status: http.StatusBadRequest, Code: "invalid_request_error",
				Message: "The request body could not be read."})
			return
		}
		body = b
	}
	live, err := s.update(func(cur *config.RuntimeConfig) error { return apply(body, cur) })
	if err != nil {
		status := http.StatusInternalServerError
		var ae *apiError
		if errors.As(err, &ae) {
			status = ae.Status
		}
		s.logWrite(r, resource, status)
		writeError(w, err)
		return
	}
	if respond == nil {
		s.logWrite(r, resource, http.StatusNoContent)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.logWrite(r, resource, http.StatusOK)
	writeJSON(w, http.StatusOK, respond(live))
}

// logWrite records one admin write attempt. The request body is never logged:
// it carries provider secrets.
func (s *Store) logWrite(r *http.Request, resource string, status int) {
	s.log.Info("admin write",
		"method", r.Method,
		"path", r.URL.Path,
		"resource", resource,
		"status", status,
		"request_id", r.Header.Get("X-Request-ID"),
	)
}
