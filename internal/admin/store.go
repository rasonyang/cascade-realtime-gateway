package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/metric"

	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/observability"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// ErrNoDefaultProfile is returned by Resolve when settings.default_profile is
// empty. The gateway runs, but it cannot serve a realtime connection yet.
var ErrNoDefaultProfile = errors.New("admin: no default profile configured")

// StateFileMode is the permission the state file is created with: it holds
// provider secrets in the clear.
const StateFileMode os.FileMode = 0o600

// Options configures a Store.
type Options struct {
	// StateFile is the single JSON file the runtime configuration lives in.
	StateFile string
	// Bootstrap seeds the configuration when StateFile does not exist. It is
	// the raw `bootstrap` block from the configuration file.
	Bootstrap json.RawMessage
	// Known reports whether a provider type is registered for a role.
	Known config.ProviderLookup
	// LookupEnv resolves {env.NAME} in provider API keys; nil means os.LookupEnv.
	LookupEnv func(string) (string, bool)
	Logger    *slog.Logger
	// Telemetry is optional; nil records nothing.
	Telemetry *observability.Telemetry
}

// Store owns the runtime configuration.
type Store struct {
	path      string
	known     config.ProviderLookup
	lookupEnv func(string) (string, bool)
	log       *slog.Logger
	tel       *observability.Telemetry

	mu  sync.Mutex // serializes writes; reads never take it
	cur atomic.Pointer[snapshot]
}

// snapshot is one immutable runtime configuration together with the provider
// instances it names. Sessions hold a reference to the providers they were
// given, so a later swap never disturbs them.
type snapshot struct {
	cfg   *config.RuntimeConfig
	built map[string]provider.Provider // "<kind>/<instance>"
}

// Runtime is the configuration one new session is started with.
type Runtime struct {
	Profile                   string
	ASRName, LLMName, TTSName string
	Session                   config.SessionDefaults
	ASRLanguage, ASRModel     string
	Temperature               *float64
	ASR                       provider.ASR
	LLM                       provider.LLM
	TTS                       provider.TTS
}

// Open builds a Store. The state file wins when it exists: a bootstrap block
// is applied only on the very first start, and is then persisted so later
// starts read the state file. A state file that cannot be read, decoded,
// validated or built is a startup failure, never a silent fallback.
func Open(opts Options) (*Store, error) {
	s := &Store{
		path:      opts.StateFile,
		known:     opts.Known,
		lookupEnv: opts.LookupEnv,
		log:       opts.Logger,
		tel:       opts.Telemetry,
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	if s.tel == nil {
		s.tel = observability.Noop()
	}
	if s.lookupEnv == nil {
		s.lookupEnv = os.LookupEnv
	}

	raw, err := os.ReadFile(s.path)
	switch {
	case err == nil:
		snap, err := s.prepare(raw, "")
		if err != nil {
			return nil, fmt.Errorf("state file %s: %w", s.path, err)
		}
		s.cur.Store(snap)
		s.log.Info("runtime configuration loaded", "source", "state_file", "path", s.path,
			"providers", len(snap.cfg.Providers), "profiles", len(snap.cfg.Profiles),
			"default_profile", snap.cfg.Settings.DefaultProfile)
	case errors.Is(err, os.ErrNotExist) && len(opts.Bootstrap) > 0:
		snap, err := s.prepare(opts.Bootstrap, "bootstrap")
		if err != nil {
			return nil, err
		}
		if err := s.persist(snap.cfg); err != nil {
			return nil, err
		}
		s.cur.Store(snap)
		s.log.Info("runtime configuration loaded", "source", "bootstrap", "path", s.path,
			"providers", len(snap.cfg.Providers), "profiles", len(snap.cfg.Profiles),
			"default_profile", snap.cfg.Settings.DefaultProfile)
	case errors.Is(err, os.ErrNotExist):
		s.cur.Store(&snapshot{cfg: emptyRuntimeConfig(), built: map[string]provider.Provider{}})
		s.log.Warn("no runtime configuration; realtime connections are refused until a default profile is set",
			"path", s.path)
	default:
		return nil, fmt.Errorf("read state file %s: %w", s.path, err)
	}
	return s, nil
}

func emptyRuntimeConfig() *config.RuntimeConfig {
	return &config.RuntimeConfig{
		Providers: map[string]*config.ProviderInstance{},
		Profiles:  map[string]*config.Profile{},
	}
}

// prepare decodes, validates and builds a runtime configuration document.
func (s *Store) prepare(raw []byte, field string) (*snapshot, error) {
	rc, err := config.DecodeRuntimeConfig(raw, field)
	if err != nil {
		return nil, err
	}
	return s.compile(rc)
}

// compile validates a candidate configuration and builds its providers.
func (s *Store) compile(rc *config.RuntimeConfig) (*snapshot, error) {
	if err := rc.Validate(s.known); err != nil {
		return nil, err
	}
	built, err := s.build(rc)
	if err != nil {
		return nil, err
	}
	return &snapshot{cfg: rc, built: built}, nil
}

// build constructs every (role, instance) pair a profile references. Instances
// nothing references are validated but not built: an instance has no role
// until a profile gives it one.
func (s *Store) build(rc *config.RuntimeConfig) (map[string]provider.Provider, error) {
	out := map[string]provider.Provider{}
	for _, name := range rc.ProfileNames() {
		p := rc.Profiles[name]
		for _, ref := range []struct {
			kind     provider.Kind
			instance string
		}{{provider.KindASR, p.ASR}, {provider.KindLLM, p.LLM}, {provider.KindTTS, p.TTS}} {
			key := string(ref.kind) + "/" + ref.instance
			if _, done := out[key]; done {
				continue
			}
			inst := rc.Providers[ref.instance]
			field := "providers." + inst.Name
			apiKey, err := config.ExpandEnv(field+".api_key", inst.APIKey, s.lookupEnv)
			if err != nil {
				return nil, err
			}
			built, err := provider.New(ref.kind, inst.Type, apiKey, inst.Options)
			if err != nil {
				return nil, &config.FieldError{Field: field + ".options", Msg: err.Error()}
			}
			out[key] = built
		}
	}
	return out, nil
}

// Config returns a deep copy of the current runtime configuration. Secrets are
// returned as stored; redaction is the HTTP layer's job.
func (s *Store) Config() *config.RuntimeConfig { return s.cur.Load().cfg.Clone() }

// Resolve returns the configuration a new session runs with. The snapshot is
// taken once per connection: later Admin writes affect only later sessions.
func (s *Store) Resolve() (Runtime, error) {
	snap := s.cur.Load()
	name := snap.cfg.Settings.DefaultProfile
	if name == "" {
		return Runtime{}, ErrNoDefaultProfile
	}
	p := snap.cfg.Profiles[name]
	if p == nil { // unreachable: Validate rejects a dangling default_profile
		return Runtime{}, ErrNoDefaultProfile
	}
	rt := Runtime{
		Profile: p.Name, ASRName: p.ASR, LLMName: p.LLM, TTSName: p.TTS,
		Session:     p.SessionDefaults(),
		ASRLanguage: p.ASRLanguage, ASRModel: p.ASRModel,
	}
	if p.Temperature != nil {
		v := *p.Temperature
		rt.Temperature = &v
	}
	var ok bool
	if rt.ASR, ok = snap.built[string(provider.KindASR)+"/"+p.ASR].(provider.ASR); !ok {
		return Runtime{}, fmt.Errorf("admin: provider %q does not implement ASR", p.ASR)
	}
	if rt.LLM, ok = snap.built[string(provider.KindLLM)+"/"+p.LLM].(provider.LLM); !ok {
		return Runtime{}, fmt.Errorf("admin: provider %q does not implement LLM", p.LLM)
	}
	if rt.TTS, ok = snap.built[string(provider.KindTTS)+"/"+p.TTS].(provider.TTS); !ok {
		return Runtime{}, fmt.Errorf("admin: provider %q does not implement TTS", p.TTS)
	}
	return rt, nil
}

// update runs one write: copy → apply → validate → build → persist → swap, and
// returns the configuration that is now live. Nothing is swapped in and
// nothing is written unless every step succeeds.
func (s *Store) update(apply func(*config.RuntimeConfig) error) (*config.RuntimeConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := s.cur.Load().cfg.Clone()
	if err := apply(next); err != nil {
		s.record(err)
		return nil, err
	}
	snap, err := s.compile(next)
	if err != nil {
		err = badRequest(err)
		s.record(err)
		return nil, err
	}
	if err := s.persist(next); err != nil {
		s.log.Error("admin state file write failed; configuration unchanged", "path", s.path, "err", err)
		err = &apiError{Status: http.StatusInternalServerError, Code: "server_error",
			Message: "The runtime configuration could not be persisted; nothing was changed."}
		s.record(err)
		return nil, err
	}
	s.cur.Store(snap)
	s.record(nil)
	return next, nil
}

// record counts one write attempt by outcome.
func (s *Store) record(err error) {
	result := "ok"
	var ae *apiError
	if errors.As(err, &ae) {
		result = ae.Result()
	} else if err != nil {
		result = "invalid"
	}
	s.tel.Metrics.AdminConfigWrites.Add(context.Background(), 1,
		metric.WithAttributes(observability.KeyResult.String(result)))
}

// persist writes the configuration atomically: a temporary file in the same
// directory, fsync, then rename over the target.
func (s *Store) persist(c *config.RuntimeConfig) error {
	body, err := config.MarshalRuntimeConfig(c)
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	f, err := os.CreateTemp(dir, ".cascade-state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp state file: %w", err)
	}
	tmp := f.Name()
	defer os.Remove(tmp) // no-op once the rename succeeded
	if err := f.Chmod(StateFileMode); err != nil {
		f.Close() //nolint:errcheck // the write already failed
		return fmt.Errorf("chmod temp state file: %w", err)
	}
	if _, err := f.Write(body); err != nil {
		f.Close() //nolint:errcheck // the write already failed
		return fmt.Errorf("write temp state file: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close() //nolint:errcheck // the write already failed
		return fmt.Errorf("fsync temp state file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp state file: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename state file: %w", err)
	}
	// Best effort: the rename is durable only once the directory entry is.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
