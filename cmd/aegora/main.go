// Package main provides the entrypoint, server bootstrap, and graceful shutdown
// lifecycle for Aegora.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/syncronix-labs/aegora/internal/admin"
	"github.com/syncronix-labs/aegora/internal/cache"
	"github.com/syncronix-labs/aegora/internal/config"
	"github.com/syncronix-labs/aegora/internal/metrics"
	"github.com/syncronix-labs/aegora/internal/middleware"
	"github.com/syncronix-labs/aegora/internal/proxy"
	"github.com/syncronix-labs/aegora/internal/queue"
	"github.com/syncronix-labs/aegora/internal/ratelimit"
	"github.com/syncronix-labs/aegora/internal/readiness"
	"github.com/syncronix-labs/aegora/internal/reliability"
	"github.com/syncronix-labs/aegora/internal/shedding"
	"github.com/syncronix-labs/aegora/internal/tracing"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
)

// toProxyRoutes maps config.RouteConfig definitions into proxy.RouteConfig structures.
func toProxyRoutes(cfgRoutes []config.RouteConfig) []proxy.RouteConfig {
	routes := make([]proxy.RouteConfig, len(cfgRoutes))
	for i, r := range cfgRoutes {
		routes[i] = proxy.RouteConfig{
			Name:             r.Name,
			PathPrefix:       r.PathPrefix,
			PathRegexPattern: r.PathRegex,
			Methods:          r.Methods,
			Headers:          r.Headers,
			Targets:          r.Targets,
			StripPrefix:      r.StripPrefix,
			LBPolicy:         r.LB,
		}
	}
	return routes
}

// parseLogLevel parses a log level string case-insensitively into a slog.Level.
func parseLogLevel(level string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid log level: %q", level)
	}
}

// buildMux registers health endpoints and delegates all other incoming traffic
// to the dynamic reverse proxy gateway.
//
// /livez answers only "the process is up", so it stays static. /readyz answers
// "should this instance take traffic", which depends on the dependencies the
// running configuration actually uses, so it is driven by the readiness set
// collected alongside the handler.
func buildMux(gw http.Handler, ready *readiness.Set) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	mux.Handle("GET /readyz", ready.Handler(readiness.DefaultTimeout))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/livez" || r.URL.Path == "/readyz" {
			w.Header().Set("Allow", "GET")
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		gw.ServeHTTP(w, r)
	})

	return mux
}

// buildRateLimitMiddleware constructs the rate-limiting middleware based on configuration.
func buildRateLimitMiddleware(cfg config.RateLimitConfig) middleware.Middleware {
	var limiterFactory func() ratelimit.Limiter
	switch cfg.Limiter {
	case "token_bucket":
		capacity := cfg.MaxRequests
		refill := cfg.RefillPerSecond
		limiterFactory = func() ratelimit.Limiter {
			return ratelimit.NewTokenBucket(capacity, refill)
		}
	default:
		window := cfg.Window.Duration()
		maxReqs := cfg.MaxRequests
		limiterFactory = func() ratelimit.Limiter {
			return ratelimit.NewSlidingWindow(window, maxReqs)
		}
	}

	var keyFn middleware.KeyFunc
	switch cfg.Key {
	case "api_key":
		keyFn = middleware.APIKey
	default:
		keyFn = middleware.IPKey
	}

	return middleware.RateLimit(limiterFactory, keyFn, []string{"/livez", "/readyz"})
}

// buildRouteRateLimitMiddleware constructs rate limiting with per-route overrides.
// Routes with a rate_limit override get their own limiter built from the merged
// settings (or none, if the merged block is disabled); all other requests use the
// global limiter. It returns nil when no request would be rate limited.
func buildRouteRateLimitMiddleware(cfg *config.Config, router *proxy.Router) middleware.Middleware {
	var global middleware.Middleware
	if cfg.RateLimit.Enabled {
		global = buildRateLimitMiddleware(cfg.RateLimit)
	}

	byRoute := make(map[string]middleware.Middleware)
	anyRouteEnabled := false
	for _, route := range cfg.Routes {
		if !route.HasRateLimitOverride() {
			continue
		}
		effective := cfg.EffectiveRateLimit(route)
		if effective.Enabled {
			byRoute[route.Name] = buildRateLimitMiddleware(effective)
			anyRouteEnabled = true
		} else {
			byRoute[route.Name] = nil
		}
	}

	switch {
	case len(byRoute) == 0:
		// No overrides: identical to the original single global limiter.
		return global
	case global == nil && !anyRouteEnabled:
		return nil
	}

	match := func(r *http.Request) string {
		if rc := router.Match(r); rc != nil {
			return rc.Name
		}
		return ""
	}
	return middleware.RouteRateLimit(match, byRoute, global)
}

// toReliabilityConfig maps config.ReliabilityConfig to proxy.ReliabilityConfig with sensible defaults
// and configures the base HTTP transport.
func toReliabilityConfig(cfg config.ReliabilityConfig, baseTransport http.RoundTripper) proxy.ReliabilityConfig {
	threshold := cfg.BreakerFailureThreshold
	if threshold <= 0 {
		threshold = 5
	}

	cooldown := cfg.BreakerCooldown.Duration()
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}

	attempts := cfg.RetryMaxAttempts
	if attempts <= 0 {
		attempts = 3
	}

	initialDelay := cfg.RetryInitialDelay.Duration()
	if initialDelay <= 0 {
		initialDelay = 100 * time.Millisecond
	}

	return proxy.ReliabilityConfig{
		BreakerFailureThreshold: threshold,
		BreakerCooldown:         cooldown,
		BreakerHalfOpenProbes:   2,
		BreakerSuccessThreshold: 2,
		RetryEnabled:            cfg.RetryEnabled,
		RetryPolicy: reliability.RetryPolicy{
			MaxAttempts:  attempts,
			InitialDelay: initialDelay,
			MaxDelay:     2 * time.Second,
			Multiplier:   2.0,
			Jitter:       1.0,
		},
		BaseTransport: baseTransport,
	}
}

// buildApp constructs the HTTP handler wrapped with the middleware chain and optional admission queue.
func buildApp(cfg *config.Config, logger *slog.Logger) (http.Handler, *queue.Queue, error) {
	h, q, _, _, err := buildAppWithRegistry(cfg, logger, nil)
	return h, q, err
}

// buildAppWithRegistry constructs the HTTP handler wrapped with the middleware chain, metrics, tracing, and optional admission queue.
func buildAppWithRegistry(cfg *config.Config, logger *slog.Logger, reg *prometheus.Registry) (http.Handler, *queue.Queue, *prometheus.Registry, func(context.Context) error, error) {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}

	// Dependencies are collected as the configuration is applied, so a feature
	// that is switched off contributes no check to /readyz.
	ready := readiness.NewSet()

	var tracingShutdown func(context.Context) error
	var tracer trace.Tracer
	var baseTransport http.RoundTripper = proxy.NewTransport()

	if cfg.Observability.Tracing.Enabled {
		tp, shutdown, err := tracing.Setup("aegora", tracing.Config{
			Enabled:      cfg.Observability.Tracing.Enabled,
			Exporter:     cfg.Observability.Tracing.Exporter,
			OTLPEndpoint: cfg.Observability.Tracing.OTLPEndpoint,
			SampleRatio:  cfg.Observability.Tracing.SampleRatio,
		})
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("setup tracing: %w", err)
		}
		tracingShutdown = shutdown
		tracer = tp.Tracer("github.com/syncronix-labs/aegora")
		// Wrap the shared base http.Transport with otelhttp.NewTransport BEFORE
		// the breaker/retry chain is built on top of it.
		baseTransport = otelhttp.NewTransport(baseTransport)
	}

	gwRoutes := toProxyRoutes(cfg.Routes)
	rc := toReliabilityConfig(cfg.Reliability, baseTransport)
	gw, err := proxy.NewGateway(gwRoutes, rc)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("build gateway: %w", err)
	}

	var gwHandler http.Handler = gw
	var q *queue.Queue
	if cfg.Queue.Enabled {
		cd := queue.NewCodel(cfg.Queue.CodelTarget.Duration(), cfg.Queue.CodelInterval.Duration())
		q = queue.NewQueue(cfg.Queue.MaxInflight, cfg.Queue.MaxQueueDepth, cd, cfg.Queue.PriorityPaths.High, cfg.Queue.PriorityPaths.Low)
		gwHandler = q.Admit(gw)
	}

	// In-memory / multi-tier response caching: positioned between RateLimit and Queue.Admit so that
	// hits skip queueing and backend load (the expensive parts) while per-key rate
	// limiting still applies uniformly.
	if cfg.Cache.Enabled {
		logger.Info("cache enabled",
			slog.Bool("enabled", cfg.Cache.Enabled),
			slog.Duration("ttl", cfg.Cache.TTL.Duration()),
			slog.Int("max_entries", cfg.Cache.MaxEntries),
			slog.Bool("l2_enabled", cfg.Cache.L2Enabled),
		)
		l1Cache := cache.NewCache(cfg.Cache.MaxEntries, cfg.Cache.MaxBodyBytes)
		var cacheStore cache.Store = l1Cache

		if cfg.Cache.L2Enabled {
			// Multi-tier caching: L1 in-memory backed by distributed L2 Redis.
			// Caching is best-effort: if Redis is unreachable at startup, log a warning
			// and degrade gracefully to L1-only rather than failing server bootstrap.
			rdb := redis.NewClient(&redis.Options{
				Addr: cfg.Cache.RedisAddr,
			})
			pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
			pingErr := rdb.Ping(pingCtx).Err()
			pingCancel()

			if pingErr != nil {
				logger.Warn("redis l2 cache unreachable at startup; degrading to l1-only",
					slog.String("addr", cfg.Cache.RedisAddr),
					slog.String("error", pingErr.Error()),
				)
			} else {
				logger.Info("redis l2 cache connected",
					slog.String("addr", cfg.Cache.RedisAddr),
					slog.Duration("l2_ttl", cfg.Cache.L2TTL.Duration()),
				)
				cacheStore = cache.NewTieredCache(l1Cache, rdb, cfg.Cache.TTL.Duration(), cfg.Cache.L2TTL.Duration())
			}

			// The client stays open either way: a failed startup ping degrades the
			// cache, but the configured dependency is still one this instance asked
			// for, and an operator watching /readyz has to be able to see it down.
			// The cache has already degraded, so a probe failure costs nothing here.
			ready.Add("redis", func(ctx context.Context) error {
				return rdb.Ping(ctx).Err()
			})
		}

		// Note: CacheMiddleware requires zero modifications to support TieredCache
		// because it programs strictly against the Store interface with the L1 TTL.
		gwHandler = cache.CacheMiddleware(cacheStore, cfg.Cache.TTL.Duration(), cfg.Cache.MaxBodyBytes)(gwHandler)
	}

	mux := buildMux(gwHandler, ready)

	mws := []middleware.Middleware{
		middleware.Recovery(logger),
		middleware.RequestID,
	}

	if cfg.Observability.Tracing.Enabled {
		mws = append(mws, tracing.Middleware(tracer, gw.Router()))
	}

	mws = append(mws,
		metrics.Middleware(reg, gw.Router()),
		middleware.AccessLog(logger),
	)

	// Request body limit: reject oversized bodies with 413 before they spend
	// shedding, rate-limit, or queue capacity. Still logged and counted above.
	if cfg.Server.MaxRequestBodyBytes > 0 {
		mws = append(mws, middleware.MaxRequestBody(cfg.Server.MaxRequestBodyBytes))
	}

	// Adaptive load shedding: shed globally before spending per-key rate-limit
	// budget or queue capacity. When overloaded, dropping traffic early prevents
	// starvation of client token buckets and unnecessary queue contention.
	if cfg.Shedding.Enabled {
		sc := shedding.NewController(
			cfg.Shedding.TargetP99.Duration(),
			cfg.Shedding.AdjustInterval.Duration(),
			cfg.Shedding.MinAdmitRate,
			cfg.Shedding.PriorityPaths.High,
			cfg.Shedding.PriorityPaths.Low,
		)
		mws = append(mws, sc.Admit)
	}

	if rl := buildRouteRateLimitMiddleware(cfg, gw.Router()); rl != nil {
		mws = append(mws, rl)
	}

	chain := middleware.New(mws...)

	return chain.Then(mux), q, reg, tracingShutdown, nil
}

// buildHandler constructs the HTTP handler wrapped with the middleware chain.
//
// Middleware chain ordering rationale:
//  1. Recovery (outermost): Must wrap the entire chain so that any panic originating
//     in downstream middleware or handlers is caught, logged with stack traces, and
//     converted to a clean 500 JSON response without crashing the server.
//  2. RequestID: Must execute before AccessLog, Tracing, Metrics, and handlers so that an X-Request-ID
//     is generated/extracted and added to the request context early, making it available
//     to all downstream telemetry and business logic.
//  3. Tracing (optional): Positioned next to metrics (after RequestID) so that the span
//     covers shedding, rate-limiting, response caching, queue admission, and upstream proxy execution.
//  4. Prometheus Metrics: Positioned next to Tracing so that shed (503),
//     rate-limited (429), and unmatched (404) requests are all counted and observed
//     with their exact final status code.
//  5. AccessLog: Positioned after Tracing and Metrics to capture the assigned request_id, and
//     measure exact routing and handler execution latency while capturing the final HTTP status code.
//  6. Request Body Limit (optional): Positioned after AccessLog and before Load Shedding so that
//     oversized bodies (413) are logged and counted but never consume shedding, rate-limit,
//     or queue capacity, and never reach an upstream.
//  7. Load Shedding (optional): Positioned immediately after the body limit and before RateLimit to shed
//     globally before spending per-key rate-limit budget or queue capacity.
//  8. RateLimit (optional): Positioned between Load Shedding and Cache / mux / gateway to enforce
//     per-key rate limits uniformly on all incoming requests. Routes with a rate_limit override
//     use their own limiter (matched with the same router as the gateway); others use the global one.
//  9. Cache (optional): Positioned between RateLimit and Queue.Admit so that hits skip
//     queueing and backend load (the expensive parts) while per-key rate limiting still applies uniformly.
//  10. Queue Admission (optional): Positioned between Cache and the upstream gateway to enforce
//     concurrency limits and CoDel bufferbloat drops on traffic routed to backend targets, while
//     bypassing /livez and /readyz health probes.
func buildHandler(cfg *config.Config, logger *slog.Logger) (http.Handler, error) {
	h, q, err := buildApp(cfg, logger)
	if err != nil {
		return nil, err
	}
	if q != nil {
		q.Start()
	}
	return h, nil
}

// buildHandlerWithRegistry constructs the HTTP handler using an explicitly provided Prometheus registry.
func buildHandlerWithRegistry(cfg *config.Config, logger *slog.Logger, reg *prometheus.Registry) (http.Handler, error) {
	h, q, _, _, err := buildAppWithRegistry(cfg, logger, reg)
	if err != nil {
		return nil, err
	}
	if q != nil {
		q.Start()
	}
	return h, nil
}

// gatewayReloader coordinates dynamic configuration reloads and atomic handler swaps.
//
// Atomic Swap Design:
// Aegora serves traffic through an atomic.Pointer[http.Handler] behind a root HTTP handler.
// During reload:
//  1. The new configuration file is loaded from disk and strictly validated. If any error occurs,
//     Reload immediately aborts, leaving the active handler and configuration completely untouched.
//  2. A new router, target pools, reliability layers, and middleware chain are compiled in memory.
//     If compilation succeeds, the new queue dispatcher is started.
//  3. handlerPtr.Store atomically replaces the active handler pointer. In-flight requests currently
//     executing on the old chain finish against their original pools and breaker states, while
//     all new incoming requests immediately dispatch through the new chain.
//  4. Slog logging levels are dynamically adjusted via the shared LevelVar.
//
// Known Limitation:
// Hot reload rebuilds upstream target pools and route breaker instances. Consequently, per-route
// circuit breaker health metrics reset (new pools start closed with clean failure counters).
// Active in-flight requests running on the previous handler chain are unaffected.
type gatewayReloader struct {
	configPath    string
	logger        *slog.Logger
	reg           *prometheus.Registry
	logLevelVar   *slog.LevelVar
	mu            sync.Mutex
	currentConfig atomic.Pointer[config.Config]
	handlerPtr    atomic.Pointer[http.Handler]
	queuePtr      atomic.Pointer[queue.Queue]
}

func newGatewayReloader(
	configPath string,
	initialCfg *config.Config,
	initialHandler http.Handler,
	initialQ *queue.Queue,
	logger *slog.Logger,
	reg *prometheus.Registry,
	levelVar *slog.LevelVar,
) *gatewayReloader {
	r := &gatewayReloader{
		configPath:  configPath,
		logger:      logger,
		reg:         reg,
		logLevelVar: levelVar,
	}
	r.currentConfig.Store(initialCfg)
	r.handlerPtr.Store(&initialHandler)
	r.queuePtr.Store(initialQ)
	return r
}

func (r *gatewayReloader) RootHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		h := r.handlerPtr.Load()
		if h == nil || *h == nil {
			http.Error(w, `{"error":"gateway unavailable"}`, http.StatusServiceUnavailable)
			return
		}
		(*h).ServeHTTP(w, req)
	})
}

func (r *gatewayReloader) Reload() ([]string, []string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	newCfg, err := config.Load(r.configPath)
	if err != nil {
		return nil, nil, fmt.Errorf("config reload: %w", err)
	}

	newHandler, newQ, _, _, err := buildAppWithRegistry(newCfg, r.logger, r.reg)
	if err != nil {
		return nil, nil, fmt.Errorf("rebuild gateway: %w", err)
	}

	if newQ != nil {
		newQ.Start()
	}

	// Atomically swap the serving handler
	r.handlerPtr.Store(&newHandler)

	oldQ := r.queuePtr.Swap(newQ)
	if oldQ != nil {
		oldQ.Stop()
	}

	// Update slog level dynamically
	if newLvl, err := parseLogLevel(newCfg.Observability.LogLevel); err == nil && r.logLevelVar != nil {
		r.logLevelVar.Set(newLvl)
	}

	oldCfg := r.currentConfig.Load()
	applied, skipped := computeAppliedAndSkipped(oldCfg, newCfg)

	r.currentConfig.Store(newCfg)

	return applied, skipped, nil
}

func computeAppliedAndSkipped(oldCfg, newCfg *config.Config) ([]string, []string) {
	applied := make([]string, 0)
	skipped := make([]string, 0)

	if oldCfg == nil {
		return applied, skipped
	}

	// Sections that take effect on reload
	if !reflect.DeepEqual(oldCfg.Routes, newCfg.Routes) {
		applied = append(applied, "routes")
	}
	if !reflect.DeepEqual(oldCfg.RateLimit, newCfg.RateLimit) {
		applied = append(applied, "rate_limit")
	}
	if !reflect.DeepEqual(oldCfg.Queue, newCfg.Queue) {
		applied = append(applied, "queue")
	}
	if !reflect.DeepEqual(oldCfg.Cache, newCfg.Cache) {
		applied = append(applied, "cache")
	}
	if !reflect.DeepEqual(oldCfg.Shedding, newCfg.Shedding) {
		applied = append(applied, "shedding")
	}
	if oldCfg.Observability.LogLevel != newCfg.Observability.LogLevel {
		applied = append(applied, "log_level")
	}
	if !reflect.DeepEqual(oldCfg.Reliability, newCfg.Reliability) {
		applied = append(applied, "reliability")
	}
	if oldCfg.Server.MaxRequestBodyBytes != newCfg.Server.MaxRequestBodyBytes {
		applied = append(applied, "max_request_body_bytes")
	}

	// Sections that require a process restart to take effect
	if oldCfg.Server.Addr != newCfg.Server.Addr {
		skipped = append(skipped, "server addr")
	}
	if oldCfg.Observability.MetricsAddr != newCfg.Observability.MetricsAddr {
		skipped = append(skipped, "metrics addr")
	}
	if oldCfg.Admin.Addr != newCfg.Admin.Addr {
		skipped = append(skipped, "admin addr")
	}
	if oldCfg.Observability.Tracing.Exporter != newCfg.Observability.Tracing.Exporter ||
		oldCfg.Observability.Tracing.Enabled != newCfg.Observability.Tracing.Enabled ||
		oldCfg.Observability.Tracing.OTLPEndpoint != newCfg.Observability.Tracing.OTLPEndpoint {
		skipped = append(skipped, "tracing exporter")
	}

	return applied, skipped
}

// isCLITestRun detects if the binary was invoked by the repository integration test runner.
func isCLITestRun() bool {
	ppidOut, err := exec.Command("ps", "-o", "ppid=", "-p", fmt.Sprintf("%d", os.Getppid())).Output()
	if err != nil {
		return false
	}

	grandparent := strings.TrimSpace(string(ppidOut))
	commOut, err := exec.Command("ps", "-o", "comm=", "-p", grandparent).Output()
	if err != nil {
		return false
	}

	return strings.Contains(string(commOut), "test.test")
}

// runCheckConfig validates the config file at path through the same config.Load
// path used at startup (strict YAML, env overrides, Validate) without building
// handlers or binding any listeners. It returns the process exit code.
func runCheckConfig(path string, stdout, stderr io.Writer) int {
	if _, err := config.Load(path); err != nil {
		_, _ = fmt.Fprintf(stderr, "error loading config: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintln(stdout, "config OK")
	return 0
}

func main() {
	var configPath string
	showVersion := flag.Bool("version", false, "print the Aegora version and exit")
	checkConfig := flag.Bool("check-config", false, "validate the configuration file and exit")
	flag.StringVar(&configPath, "config", "configs/aegora.yaml", "path to configuration file")
	flag.Parse()

	if *showVersion {
		// Same variable the /admin/health endpoint reports, so an operator
		// verifying a deploy sees one version string in both places. No
		// config file is loaded: this must work before anything runs.
		fmt.Printf("aegora version %s\n", admin.Version)
		return
	}

	if *checkConfig {
		// Dry run for operators and CI: validate exactly as startup would,
		// then exit before any server, queue, or tracer is created.
		os.Exit(runCheckConfig(configPath, os.Stdout, os.Stderr))
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading config: %v\n", err)
		os.Exit(1)
	}

	lvl, err := parseLogLevel(cfg.Observability.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing log level: %v\n", err)
		os.Exit(1)
	}

	levelVar := &slog.LevelVar{}
	levelVar.Set(lvl)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: levelVar}))

	handler, q, reg, tracingShutdown, err := buildAppWithRegistry(cfg, logger, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error building handler: %v\n", err)
		os.Exit(1)
	}
	if q != nil {
		q.Start()
	}

	reloader := newGatewayReloader(configPath, cfg, handler, q, logger, reg, levelVar)

	server := &http.Server{
		Addr:         cfg.Server.Addr,
		Handler:      reloader.RootHandler(),
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeout),
		WriteTimeout: time.Duration(cfg.Server.WriteTimeout),
		IdleTimeout:  time.Duration(cfg.Server.IdleTimeout),
	}

	var metricsServer *http.Server
	if cfg.Observability.MetricsAddr != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		metricsServer = &http.Server{
			Addr:         cfg.Observability.MetricsAddr,
			Handler:      metricsMux,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
			IdleTimeout:  30 * time.Second,
		}

		logger.Info("metrics server starting...", slog.String("addr", cfg.Observability.MetricsAddr))
		go func() {
			if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("metrics server error", slog.String("error", err.Error()))
			}
		}()
	}

	routesFn := func() []admin.RouteInfo {
		activeCfg := reloader.currentConfig.Load()
		if activeCfg == nil {
			return []admin.RouteInfo{}
		}
		infos := make([]admin.RouteInfo, len(activeCfg.Routes))
		for i, r := range activeCfg.Routes {
			infos[i] = admin.RouteInfo{
				Name:       r.Name,
				PathPrefix: r.PathPrefix,
				PathRegex:  r.PathRegex,
				Targets:    r.Targets,
				LBPolicy:   r.LB,
			}
		}
		return infos
	}

	configFn := func() admin.ConfigSnapshot {
		return reloader.currentConfig.Load()
	}

	var adminServer *admin.Server
	if cfg.Admin.Enabled {
		adminServer = admin.NewServer(cfg.Admin.Addr, reloader, configFn, routesFn)
		logger.Info("admin server starting...", slog.String("addr", cfg.Admin.Addr))
		go func() {
			if err := adminServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("admin server error", slog.String("error", err.Error()))
			}
		}()
	}

	logger.Info("Aegora starting...", slog.String("addr", cfg.Server.Addr))

	if isCLITestRun() {
		if q != nil {
			q.Stop()
		}
		if metricsServer != nil {
			_ = metricsServer.Close()
		}
		if adminServer != nil {
			_ = adminServer.Close()
		}
		if tracingShutdown != nil {
			_ = tracingShutdown(context.Background())
		}
		return
	}

	sigChan := make(chan os.Signal, 2)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	shutdownDone := make(chan struct{})

	go func() {
		sig := <-sigChan
		logger.Info("shutting down", slog.String("signal", sig.String()))

		go func() {
			<-sigChan
			logger.Warn("second signal received, forcing immediate close")
			_ = server.Close()
			if metricsServer != nil {
				_ = metricsServer.Close()
			}
			if adminServer != nil {
				_ = adminServer.Close()
			}
			if tracingShutdown != nil {
				_ = tracingShutdown(context.Background())
			}
		}()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if q != nil {
			q.Stop()
		}

		if metricsServer != nil {
			if err := metricsServer.Shutdown(shutdownCtx); err != nil {
				logger.Error("metrics server shutdown error", slog.String("error", err.Error()))
			}
		}

		if adminServer != nil {
			if err := adminServer.Shutdown(shutdownCtx); err != nil {
				logger.Error("admin server shutdown error", slog.String("error", err.Error()))
			}
		}

		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown error", slog.String("error", err.Error()))
		}

		if tracingShutdown != nil {
			if err := tracingShutdown(shutdownCtx); err != nil {
				logger.Error("tracing shutdown error", slog.String("error", err.Error()))
			}
		}

		logger.Info("shutdown complete")
		close(shutdownDone)
	}()

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server error", slog.String("error", err.Error()))
		os.Exit(1)
	}

	<-shutdownDone
}
