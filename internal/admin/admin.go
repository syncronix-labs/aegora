// Package admin provides a dedicated administrative HTTP server for Aegora,
// exposing health, routing metadata, running configuration snapshots,
// and hot-reload triggers.
package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// Version represents the build version of Aegora.
// It can be overridden at link time using:
//
//	-ldflags "-X 'github.com/syncronix-labs/aegora/internal/admin.Version=v1.0.0'"
var Version = "dev"

// Reloader defines the interface for reloading the gateway configuration at runtime.
type Reloader interface {
	Reload() (applied []string, skipped []string, err error)
}

// ConfigSnapshot represents the active gateway configuration snapshot.
//
// Secret Redaction Rule:
// Aegora currently does not store credentials, certificates, or tokens in its
// config file. When secrets (such as Redis passwords, TLS private keys, or API tokens)
// are introduced in future phases, ConfigSnapshot serialization must sanitize or
// mask those fields (e.g., "[REDACTED]") before returning the JSON payload.
type ConfigSnapshot any

// RouteInfo provides JSON-friendly metadata for a configured proxy route.
type RouteInfo struct {
	Name       string   `json:"name"`
	PathPrefix string   `json:"path_prefix,omitempty"`
	PathRegex  string   `json:"path_regex,omitempty"`
	Targets    []string `json:"targets"`
	LBPolicy   string   `json:"lb_policy"`

	// Live state, merged in by the caller that owns the gateway. A route under an open
	// breaker and a route running on one leg look identical in configuration, and those are
	// the two things an operator needs first during an incident.
	BreakerState        string         `json:"breaker_state"`
	ConsecutiveFailures int            `json:"consecutive_failures"`
	TargetHealth        []TargetHealth `json:"target_health"`
}

// TargetHealth reports whether the load balancer currently considers one upstream target
// available. A target that failed recently stays unhealthy until its cooldown elapses.
type TargetHealth struct {
	URL     string `json:"url"`
	Healthy bool   `json:"healthy"`
}

// Server is the dedicated administrative HTTP server for Aegora.
//
// Architectural Separation Rationale:
// The admin API runs on a distinct HTTP listener and TCP port (default 127.0.0.1:9091)
// isolated from the data plane (127.0.0.1:8080) and metrics endpoint (127.0.0.1:9090).
// This guarantees:
//  1. Fault and contention isolation: administrative operations (such as configuration reloads)
//     cannot monopolize worker goroutines or starve customer-facing data plane traffic.
//  2. Network-level access control: the admin port can be bound to localhost or private management
//     VPCs and protected by strict firewall rules, ingress controllers, or mutual TLS, preventing
//     unauthorized external access without complicating edge proxy routing.
type Server struct {
	httpServer *http.Server
	mux        *http.ServeMux
	reloader   Reloader
	configFn   func() ConfigSnapshot
	routesFn   func() []RouteInfo
}

// NewServer constructs a new administrative HTTP Server configured with the provided
// network address, hot-reloader, and query providers.
func NewServer(addr string, reloader Reloader, configFn func() ConfigSnapshot, routesFn func() []RouteInfo) *Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/admin/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{
			"status":  "ok",
			"version": Version,
		})
	})

	mux.HandleFunc("/admin/routes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var routes []RouteInfo
		if routesFn != nil {
			routes = routesFn()
		}
		if routes == nil {
			routes = []RouteInfo{}
		}
		writeJSON(w, http.StatusOK, routes)
	})

	mux.HandleFunc("/admin/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var snap ConfigSnapshot
		if configFn != nil {
			snap = configFn()
		}
		if snap == nil {
			snap = map[string]any{}
		}
		writeJSON(w, http.StatusOK, snap)
	})

	mux.HandleFunc("/admin/reload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		if reloader == nil {
			writeJSONError(w, http.StatusInternalServerError, "reloader not configured")
			return
		}

		applied, skipped, err := reloader.Reload()
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}

		if applied == nil {
			applied = []string{}
		}
		if skipped == nil {
			skipped = []string{}
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"reloaded": true,
			"applied":  applied,
			"skipped":  skipped,
		})
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeJSONError(w, http.StatusNotFound, "not found")
	})

	httpServer := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	return &Server{
		httpServer: httpServer,
		mux:        mux,
		reloader:   reloader,
		configFn:   configFn,
		routesFn:   routesFn,
	}
}

// Handler returns the root HTTP handler for testing and middleware decoration.
func (s *Server) Handler() http.Handler {
	return s.mux
}

// ListenAndServe starts the administrative HTTP server on its configured address.
func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the administrative HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// Close immediately closes the administrative HTTP server.
func (s *Server) Close() error {
	return s.httpServer.Close()
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{
		"error": msg,
	})
}
