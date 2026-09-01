package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/provider"
)

// Name is the registry name of both providers.
const Name = "openai"

const (
	defaultBaseURL           = "https://api.openai.com/v1"
	defaultRequestTimeout    = 30 * time.Second
	defaultStreamIdleTimeout = 30 * time.Second
)

// httpOptions are the fields shared by both providers' option blocks.
type httpOptions struct {
	BaseURL           string          `json:"base_url"`
	RequestTimeout    config.Duration `json:"request_timeout"`     // until response headers arrive
	StreamIdleTimeout config.Duration `json:"stream_idle_timeout"` // max gap between streamed chunks
}

func (o *httpOptions) applyDefaults() {
	if o.BaseURL == "" {
		o.BaseURL = defaultBaseURL
	}
	o.BaseURL = strings.TrimRight(o.BaseURL, "/")
	if o.RequestTimeout == 0 {
		o.RequestTimeout = config.Duration(defaultRequestTimeout)
	}
	if o.StreamIdleTimeout == 0 {
		o.StreamIdleTimeout = config.Duration(defaultStreamIdleTimeout)
	}
}

func newClient(o httpOptions) *http.Client {
	return &http.Client{Transport: &http.Transport{
		ResponseHeaderTimeout: o.RequestTimeout.Std(),
		ForceAttemptHTTP2:     true,
	}}
}

// postJSON issues an authenticated POST and returns the response, or a
// classified provider.Error for non-2xx statuses.
func postJSON(ctx context.Context, client *http.Client, apiKey, url string, body any) (*http.Response, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, &provider.Error{Provider: Name, Kind: provider.ErrFatal, Err: err}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return nil, &provider.Error{Provider: Name, Kind: provider.ErrFatal, Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &provider.Error{Provider: Name, Kind: provider.ErrTransient, Err: err}
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, provider.ErrorFromStatus(Name, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return resp, nil
}

// idleWatchdog cancels a stream when no progress is reported within d.
type idleWatchdog struct {
	timer *time.Timer
	d     time.Duration
}

func newIdleWatchdog(d time.Duration, onIdle func()) *idleWatchdog {
	return &idleWatchdog{timer: time.AfterFunc(d, onIdle), d: d}
}

func (w *idleWatchdog) progress() { w.timer.Reset(w.d) }

func (w *idleWatchdog) stop() { w.timer.Stop() }

func fatalf(format string, args ...any) *provider.Error {
	return &provider.Error{Provider: Name, Kind: provider.ErrFatal, Err: fmt.Errorf(format, args...)}
}
