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
//   - Drops: aegora_shed_total and aegora_queue_dropped_total (CounterVec: route)
type Metrics struct {
	RequestsTotal   *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
	Inflight        *prometheus.GaugeVec
	ShedTotal       *prometheus.CounterVec
	QueueDropped    *prometheus.CounterVec
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
		ShedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "aegora_shed_total",
				Help: "Total number of requests rejected by the AIMD load shedding controller.",
			},
			[]string{"route"},
		),
		QueueDropped: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "aegora_queue_dropped_total",
				Help: "Total number of requests dropped by the admission queue (full depth, CoDel, or shutdown).",
			},
			[]string{"route"},
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
	if err := reg.Register(m.ShedTotal); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if existing, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
				m.ShedTotal = existing
			}
		} else {
			panic(err)
		}
	}
	if err := reg.Register(m.QueueDropped); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if existing, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
				m.QueueDropped = existing
			}
		} else {
			panic(err)
		}
	}
	return m
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

			// Drop counters: shedding and queue mark their rejections with the
			// header below; reading it here keeps the route label consistent
			// with the rest of this middleware without threading a metrics
			// dependency through the shedding and queue packages.
			switch rw.Header().Get(dropHeader) {
			case dropShed:
				m.ShedTotal.WithLabelValues(routeName).Inc()
			case dropQueue:
				m.QueueDropped.WithLabelValues(routeName).Inc()
			}
		})
	}
}

// Header values that mark a request as dropped by a specific admission-control
// component. They are set on rejected responses by the shedding controller and
// the admission queue, and consumed by the metrics middleware above.
const (
	dropHeader = "X-Aegora-Drop"
	dropShed   = "shedding"
	dropQueue  = "queue"
)

// MarkShed sets the drop marker that tells the metrics middleware the request
// was rejected by the AIMD shedding controller. Call before writing the 503.
func MarkShed(h http.Header) {
	h.Set(dropHeader, dropShed)
}

// MarkQueueDropped sets the drop marker that tells the metrics middleware the
// request was dropped by the admission queue. Call before writing the 503.
func MarkQueueDropped(h http.Header) {
	h.Set(dropHeader, dropQueue)
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
