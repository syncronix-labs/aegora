package proxy

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/syncronix-labs/aegora/internal/httperr"
	"github.com/syncronix-labs/aegora/internal/middleware"
	"github.com/syncronix-labs/aegora/internal/reliability"
)

type contextKey int

const (
	upstreamContextKey contextKey = iota
)

type upstreamInfo struct {
	pool  *Pool
	index int
}

type routeEntry struct {
	config  RouteConfig
	pool    *Pool
	proxy   *httputil.ReverseProxy
	breaker *reliability.CircuitBreaker
}

// ReliabilityConfig specifies circuit breaker and retry policies for the gateway.
//
// Architectural distinction:
// - pool: tracks per-target connectivity for individual upstream server instances.
// - breaker: tracks per-route service health across the entire route to prevent cascading failures.
type ReliabilityConfig struct {
	BreakerFailureThreshold int
	BreakerCooldown         time.Duration
	BreakerHalfOpenProbes   int
	BreakerSuccessThreshold int
	RetryEnabled            bool
	RetryPolicy             reliability.RetryPolicy
	BaseTransport           http.RoundTripper
}

// Gateway routes incoming HTTP requests to upstream server pools via reverse proxies,
// enforcing route matching, path prefix stripping, load balancing, circuit breaking,
// automatic retries, and failure handling.
type Gateway struct {
	router *Router
	routes map[*RouteConfig]*routeEntry
}

// NewGateway constructs a Gateway with a compiled Router, one upstream Pool per route,
// and one reverse proxy per route utilizing Aegora's header sanitization and HTTP transport.
// Each route is configured with its own CircuitBreaker and, if enabled, a RetryTransport.
func NewGateway(routes []RouteConfig, rc ReliabilityConfig) (*Gateway, error) {
	if rc.BreakerFailureThreshold <= 0 {
		return nil, errors.New("gateway: breaker failure threshold must be positive")
	}
	if rc.BreakerCooldown <= 0 {
		return nil, errors.New("gateway: breaker cooldown must be positive")
	}
	if rc.BreakerHalfOpenProbes <= 0 {
		return nil, errors.New("gateway: breaker half-open probes must be positive")
	}
	if rc.BreakerSuccessThreshold <= 0 {
		return nil, errors.New("gateway: breaker success threshold must be positive")
	}
	if rc.RetryEnabled {
		if err := rc.RetryPolicy.Validate(); err != nil {
			return nil, fmt.Errorf("gateway: retry policy: %w", err)
		}
	}

	router, err := NewRouter(routes)
	if err != nil {
		return nil, fmt.Errorf("gateway: router init: %w", err)
	}

	// RetryTransport may be shared across routes since it is stateless beyond its policy.
	// When retry is disabled, BreakerTransport chains directly over the base transport
	// (rc.BaseTransport or NewTransport()).
	baseTransport := rc.BaseTransport
	if baseTransport == nil {
		baseTransport = NewTransport()
	}
	if rc.RetryEnabled {
		baseTransport = &reliability.RetryTransport{
			Base:   baseTransport,
			Policy: rc.RetryPolicy,
		}
	}

	routeMap := make(map[*RouteConfig]*routeEntry, len(router.routes))
	for i := range router.routes {
		r := &router.routes[i]

		pool, err := NewPool(r.Targets, r.LBPolicy)
		if err != nil {
			return nil, fmt.Errorf("gateway: route %q pool init: %w", r.Name, err)
		}

		// Breakers must be per-route instances to track service health independently.
		breaker := reliability.NewCircuitBreaker(
			rc.BreakerFailureThreshold,
			rc.BreakerCooldown,
			rc.BreakerHalfOpenProbes,
			rc.BreakerSuccessThreshold,
		)

		// Breaker sits outside retry: if open, it fails fast without wasting retries.
		// The retry loop's final outcome (exhausted attempts or failure) is what the breaker records.
		routeTransport := &reliability.BreakerTransport{
			Base:    baseTransport,
			Breaker: breaker,
		}

		proxy := newReverseProxy(
			func(pr *httputil.ProxyRequest) {
				pr.SetURL(&url.URL{
					Scheme: pr.In.URL.Scheme,
					Host:   pr.In.URL.Host,
				})
				sanitizeHeaders(pr)
			},
			func(w http.ResponseWriter, req *http.Request, err error) {
				if errors.Is(err, reliability.ErrCircuitOpen) {
					// The breaker knows exactly when it will admit a probe, so tell the
					// client when to come back instead of letting it retry immediately.
					// The header has to be in place before the shared error writer sends
					// the status line.
					if remaining := breaker.CooldownRemaining(); remaining > 0 {
						seconds := int(math.Ceil(remaining.Seconds()))
						if seconds < 1 {
							seconds = 1
						}
						w.Header().Set("Retry-After", strconv.Itoa(seconds))
					}
					httperr.Write(w, http.StatusServiceUnavailable, "service unavailable", middleware.GetRequestID(req))
					return
				}
				// A chunked body that exceeded server.max_request_body_bytes mid-stream is a
				// client fault: answer 413 and leave the upstream's passive health untouched.
				var maxBytesErr *http.MaxBytesError
				if errors.As(err, &maxBytesErr) {
					httperr.Write(w, http.StatusRequestEntityTooLarge, "request body too large", middleware.GetRequestID(req))
					return
				}
				if info, ok := req.Context().Value(upstreamContextKey).(*upstreamInfo); ok && info != nil {
					if info.pool != nil {
						info.pool.ReportFailure(info.index)
					}
				}
				httperr.Write(w, http.StatusBadGateway, "bad gateway", middleware.GetRequestID(req))
			},
		)
		proxy.Transport = routeTransport

		routeMap[r] = &routeEntry{
			config:  *r,
			pool:    pool,
			proxy:   proxy,
			breaker: breaker,
		}
	}

	return &Gateway{
		router: router,
		routes: routeMap,
	}, nil
}

// Router returns the underlying Router instance used by the gateway for route matching.
func (g *Gateway) Router() *Router {
	if g == nil {
		return nil
	}
	return g.router
}

// ServeHTTP matches incoming requests against configured routes, strips prefixes,
// selects an upstream target, and proxies the request.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if g == nil || g.router == nil {
		httperr.Write(w, http.StatusNotFound, "no route matched", middleware.GetRequestID(req))
		return
	}

	route := g.router.Match(req)
	if route == nil {
		httperr.Write(w, http.StatusNotFound, "no route matched", middleware.GetRequestID(req))
		return
	}

	entry := g.routes[route]
	if entry == nil {
		httperr.Write(w, http.StatusNotFound, "no route matched", middleware.GetRequestID(req))
		return
	}

	path := req.URL.Path
	if entry.config.StripPrefix != "" {
		path = strings.TrimPrefix(path, entry.config.StripPrefix)
		if path == "" || !strings.HasPrefix(path, "/") {
			path = "/" + strings.TrimPrefix(path, "/")
		}
	}
	if path == "" {
		path = "/"
	}

	targetURL, idx, ok := entry.pool.Next()
	if !ok {
		httperr.Write(w, http.StatusBadGateway, "all upstreams unavailable", middleware.GetRequestID(req))
		return
	}
	defer entry.pool.Done(idx)

	ctx := context.WithValue(req.Context(), upstreamContextKey, &upstreamInfo{
		pool:  entry.pool,
		index: idx,
	})

	outReq := req.Clone(ctx)
	outReq.URL.Scheme = targetURL.Scheme
	outReq.URL.Host = targetURL.Host
	if targetURL.Path == "" || targetURL.Path == "/" {
		outReq.URL.Path = path
	} else {
		outReq.URL.Path = singleJoiningSlash(targetURL.Path, path)
	}
	outReq.URL.RawPath = ""
	outReq.Host = targetURL.Host

	entry.proxy.ServeHTTP(w, outReq)
}

func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}
