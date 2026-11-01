package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// HealthCheckConfig configures active health checking for a route's upstream targets.
// A probe is a GET request to Path on each target; a 2xx response marks the target up and
// any other outcome marks it down.
type HealthCheckConfig struct {
	Path     string
	Interval time.Duration
	Timeout  time.Duration
}

// Validate reports whether the health check settings are usable.
func (h *HealthCheckConfig) Validate() error {
	if h == nil {
		return nil
	}
	if h.Path == "" || h.Path[0] != '/' {
		return fmt.Errorf("path must start with %q, got %q", "/", h.Path)
	}
	if h.Interval <= 0 {
		return fmt.Errorf("interval must be positive, got %s", h.Interval)
	}
	if h.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive, got %s", h.Timeout)
	}
	if h.Timeout >= h.Interval {
		return fmt.Errorf("timeout %s must be shorter than interval %s", h.Timeout, h.Interval)
	}
	return nil
}

// runHealthChecks probes every target of the route until ctx is cancelled. The first
// round runs immediately so a target that is already down is removed before client
// traffic arrives; later rounds run on the configured interval.
func (g *Gateway) runHealthChecks(ctx context.Context, entry *routeEntry) {
	hc := entry.config.HealthCheck
	client := &http.Client{Timeout: hc.Timeout}

	probeAll := func() {
		var wg sync.WaitGroup
		for i, target := range entry.pool.targets {
			wg.Add(1)
			go func(index int, target *url.URL) {
				defer wg.Done()
				entry.pool.SetActiveHealth(index, probeTarget(ctx, client, target, hc.Path))
			}(i, target)
		}
		wg.Wait()
	}

	probeAll()

	ticker := time.NewTicker(hc.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			probeAll()
		}
	}
}

// probeTarget sends one health request to target and reports whether it answered 2xx.
// The response body is drained (bounded) so the connection can be reused.
func probeTarget(ctx context.Context, client *http.Client, target *url.URL, path string) bool {
	probeURL := *target
	if probeURL.Path == "" || probeURL.Path == "/" {
		probeURL.Path = path
	} else {
		probeURL.Path = singleJoiningSlash(probeURL.Path, path)
	}
	probeURL.RawPath = ""
	probeURL.RawQuery = ""

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL.String(), nil)
	if err != nil {
		return false
	}

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	return resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
}
