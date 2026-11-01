// Package readiness answers the one question an orchestrator asks before it
// sends traffic to an instance: are the dependencies this configuration uses
// actually reachable right now.
//
// A Set is built from the running configuration, so a feature that is switched
// off contributes no check and cannot fail readiness. The set is rebuilt with
// the handler, which is what makes /admin/reload re-collect the dependencies.
package readiness

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// DefaultTimeout bounds a whole probe. Short enough that an orchestrator's own
// probe deadline still wins, long enough for one round trip to a local service.
const DefaultTimeout = 500 * time.Millisecond

// Check is one dependency and the probe that proves it is reachable.
type Check struct {
	// Name is the key the failure is reported under, e.g. "redis".
	Name string
	// Probe returns nil when the dependency is usable. It should honour ctx.
	Probe func(context.Context) error
}

// Set is the collection of checks that apply to the running configuration.
// The zero value is usable and always ready.
type Set struct {
	checks []Check
}

// NewSet returns a set holding the given checks.
func NewSet(checks ...Check) *Set {
	return &Set{checks: checks}
}

// Add appends a check. Callers add only what the configuration enables.
func (s *Set) Add(name string, probe func(context.Context) error) {
	s.checks = append(s.checks, Check{Name: name, Probe: probe})
}

// Len reports how many dependencies are being watched. Zero means the
// configuration uses nothing external, so readiness is always true.
func (s *Set) Len() int { return len(s.checks) }

// Run probes every dependency concurrently and returns the failures keyed by
// name. A probe that overruns ctx is reported as a failure rather than being
// waited on: a slow dependency must fail the probe, not hang it.
func (s *Set) Run(ctx context.Context) map[string]string {
	if len(s.checks) == 0 {
		return nil
	}

	var (
		mu       sync.Mutex
		failures = make(map[string]string, len(s.checks))
		wg       sync.WaitGroup
	)

	record := func(name, detail string) {
		mu.Lock()
		failures[name] = detail
		mu.Unlock()
	}

	for _, c := range s.checks {
		wg.Add(1)
		go func(c Check) {
			defer wg.Done()

			// The probe runs in its own goroutine so this one can stop waiting
			// the moment ctx expires. A probe that ignores ctx leaks its
			// goroutine until the dependency answers; that is the only way to
			// keep a hung dependency from hanging the handler.
			done := make(chan error, 1)
			go func() { done <- c.Probe(ctx) }()

			select {
			case err := <-done:
				if err != nil {
					record(c.Name, "error: "+err.Error())
				}
			case <-ctx.Done():
				record(c.Name, "error: "+ctx.Err().Error())
			}
		}(c)
	}

	wg.Wait()

	if len(failures) == 0 {
		return nil
	}
	return failures
}

// Handler serves the readiness endpoint. It returns 200 with {"status":"ok"}
// when every check passes, and 503 naming each failed dependency otherwise:
//
//	{"status":"not ready","checks":{"redis":"error: dial tcp: connection refused"}}
//
// A timeout of zero or less means DefaultTimeout.
func (s *Set) Handler(timeout time.Duration) http.HandlerFunc {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}

	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		w.Header().Set("Content-Type", "application/json")
		// A probe result is only true at the moment it was taken.
		w.Header().Set("Cache-Control", "no-store")

		if failures := s.Run(ctx); len(failures) > 0 {
			body, err := json.Marshal(struct {
				Status string            `json:"status"`
				Checks map[string]string `json:"checks"`
			}{Status: "not ready", Checks: failures})
			if err != nil {
				http.Error(w, "readiness check failed", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write(body)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}
}
