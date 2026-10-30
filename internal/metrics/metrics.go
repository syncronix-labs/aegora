// Package metrics provides Prometheus RED method telemetry for Aegora.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/syncronix-labs/aegora/internal/middleware"
	"github.com/syncronix-labs/aegora/internal/proxy"
)

// RouteMatcher defines the route-matching interface used by metrics middleware.
// It matches incoming HTTP requests to their configured RouteConfig.
// *proxy.Router satisfies this interface.
type RouteMatcher interface {
	Match(r *http.Request) *proxy.RouteConfig
}

// Metrics encapsulates Prometheus collectors for Aegora's RED method telemetry:
//   - Rate: aegora_requests_total (CounterVec: route, method, code)
//   - Errors: captured by status code label in aegora_requests_total (4xx/5xx)
//   - Duration: aegora_request_duration_seconds (HistogramVec: route, method)
//   - Concurrency: aegora_inflight_requests (GaugeVec: route)
//   - Cache: aegora_cache_requests_total (CounterVec: route, result)
type Metrics struct {
	RequestsTotal   *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
	Inflight        *prometheus.GaugeVec
	// CacheRequestsTotal counts cache lookups by route and outcome, so hit
	// ratio is a division against aegora_requests_total on the same label.
	CacheRequestsTotal *prometheus.CounterVec
}

// New constructs and registers the gateway RED metrics on the provided registry.
//
// Registry Isolation Rationale:
// Metrics are explicitly registered on the supplied *prometheus.Registry rather than
// the global default registry (prometheus.DefaultRegisterer) to ensure complete test
// isolation and support future multi-tenant or multi-instance gateway runtimes.
//
// Latency Buckets Rationale:
// aegora_request_duration_seconds uses prometheus.DefBuckets
// (.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10). This default distribution is
// suitable for sub-second gateway latencies, capturing fast proxy round-trips while
// preserving headroom for downstream backend timeouts. Gateway operators can tune
// custom bucket distributions as operational telemetry needs evolve.
func New(reg *prometheus.Registry) *Metrics {
	if reg == nil {
		panic("metrics: registry must not be nil")
	}

	m := &Metrics{
		RequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "aegora_requests_total",
				Help: "Total number of HTTP requests processed by the gateway.",
			},
			[]string{"route", "method", "code"},
		),
		RequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "aegora_request_duration_seconds",
				Help:    "Duration of HTTP requests handled by the gateway in seconds.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"route", "method"},
		),
		Inflight: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "aegora_inflight_requests",
				Help: "Current number of HTTP requests being processed by the gateway.",
			},
			[]string{"route"},
		),
		CacheRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "aegora_cache_requests_total",
				Help: "Total number of cache lookups made by the gateway, by route and outcome.",
			},
			[]string{"route", "result"},
		),
	}

	if err := reg.Register(m.RequestsTotal); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if existing, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
				m.RequestsTotal = existing
			}
		} else {
			panic(err)
		}
	}
	if err := reg.Register(m.RequestDuration); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if existing, ok := are.ExistingCollector.(*prometheus.HistogramVec); ok {
				m.RequestDuration = existing
			}
		} else {
			panic(err)
		}
	}
	if err := reg.Register(m.Inflight); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if existing, ok := are.ExistingCollector.(*prometheus.GaugeVec); ok {
				m.Inflight = existing
			}
		} else {
			panic(err)
		}
	}
	if err := reg.Register(m.CacheRequestsTotal); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if existing, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
				m.CacheRequestsTotal = existing
			}
		} else {
			panic(err)
		}
	}
	return m
}

// ObserveCache records the outcome of a cache lookup, where result is "hit" or
// "miss". It satisfies the Observer interface declared in internal/cache, which
// is defined there rather than here so the cache package does not have to import
// this one: internal/metrics reaches internal/proxy, and internal/proxy imports
// internal/cache, so the reverse edge would be a cycle.
func (m *Metrics) ObserveCache(route, result string) {
	m.CacheRequestsTotal.WithLabelValues(route, result).Inc()
}

// Middleware constructs an HTTP middleware that observes RED metrics for all requests.
//
// Middleware Placement Rationale:
// Positioned right after RequestID in the gateway chain so that all requests, including
// shed requests (503), rate-limited requests (429), and unmatched routes (404), are
// observed in telemetry before early rejection occurs.
func Middleware(reg *prometheus.Registry, router RouteMatcher) middleware.Middleware {
	m := New(reg)
	return m.Middleware(router)
}

// Middleware returns the HTTP middleware using this Metrics instance.
func (m *Metrics) Middleware(router RouteMatcher) middleware.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			routeName := "unmatched"
			if router != nil {
				if matched := router.Match(r); matched != nil && matched.Name != "" {
					routeName = matched.Name
				}
			}

			m.Inflight.WithLabelValues(routeName).Inc()
			defer m.Inflight.WithLabelValues(routeName).Dec()

			rw := newStatusResponseWriter(w)
			start := time.Now()

			next.ServeHTTP(rw, r)

			duration := time.Since(start).Seconds()
			code := strconv.Itoa(rw.status)

			m.RequestsTotal.WithLabelValues(routeName, r.Method, code).Inc()
			m.RequestDuration.WithLabelValues(routeName, r.Method).Observe(duration)
		})
	}
}

// statusResponseWriter captures the HTTP status code written by downstream handlers.
//
// Architecture & Streaming Safety:
// This wrapper implements ONLY http.ResponseWriter (Header, Write, WriteHeader).
// It deliberately does NOT implement http.Flusher, http.Hijacker, or http.ResponseController's
// Unwrap() method. Advertising http.Flusher or exposing Unwrap causes httputil.ReverseProxy
// to treat the downstream response as streaming and trigger mid-body Flushes, which can break
// buffering middleware or prematurely commit response headers.
type statusResponseWriter struct {
	w           http.ResponseWriter
	status      int
	wroteHeader bool
}

func newStatusResponseWriter(w http.ResponseWriter) *statusResponseWriter {
	return &statusResponseWriter{
		w:      w,
		status: http.StatusOK,
	}
}

func (rw *statusResponseWriter) Header() http.Header {
	return rw.w.Header()
}

func (rw *statusResponseWriter) WriteHeader(code int) {
	if rw.wroteHeader {
		return
	}
	rw.status = code
	rw.wroteHeader = true
	rw.w.WriteHeader(code)
}

func (rw *statusResponseWriter) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.WriteHeader(http.StatusOK)
	}
	return rw.w.Write(b)
}
