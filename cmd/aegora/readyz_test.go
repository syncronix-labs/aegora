package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/syncronix-labs/aegora/internal/config"
)

func readyz(t *testing.T, cfg *config.Config) *httptest.ResponseRecorder {
	t.Helper()

	handler, err := buildHandler(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("buildHandler: %v", err)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	return rec
}

func redisCache(addr string) config.CacheConfig {
	return config.CacheConfig{
		Enabled:      true,
		TTL:          config.Duration(60 * time.Second),
		L2Enabled:    true,
		L2TTL:        config.Duration(5 * time.Minute),
		RedisAddr:    addr,
		MaxEntries:   100,
		MaxBodyBytes: 1024 * 1024,
	}
}

func failingChecks(t *testing.T, rec *httptest.ResponseRecorder) map[string]string {
	t.Helper()

	var body struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	if body.Status != "not ready" {
		t.Fatalf("status field = %q, want %q", body.Status, "not ready")
	}
	return body.Checks
}

// A gateway with no external dependencies has nothing to wait for, so it is
// ready the moment it starts.
func TestReadyz_NoDependenciesIsReady(t *testing.T) {
	rec := readyz(t, newTestConfig())

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if body := rec.Body.String(); body != `{"status":"ok"}` {
		t.Fatalf("body = %q, want the unchanged ok payload", body)
	}
}

func TestReadyz_ReachableRedisIsReady(t *testing.T) {
	s := miniredis.RunT(t)

	cfg := newTestConfig()
	cfg.Cache = redisCache(s.Addr())

	rec := readyz(t, cfg)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
}

// The cache degrades to L1-only when Redis is unreachable at startup, but the
// configured dependency is still down and an operator has to be able to see it.
func TestReadyz_UnreachableRedisIsNotReady(t *testing.T) {
	s := miniredis.RunT(t)
	addr := s.Addr()
	s.Close()

	cfg := newTestConfig()
	cfg.Cache = redisCache(addr)

	rec := readyz(t, cfg)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body = %s", rec.Code, rec.Body.String())
	}

	checks := failingChecks(t, rec)
	detail, reported := checks["redis"]
	if !reported {
		t.Fatalf("checks = %v, want the unreachable dependency keyed as redis", checks)
	}
	if len(checks) != 1 {
		t.Fatalf("checks = %v, want only the configured dependency", checks)
	}
	if len(detail) < len("error: ") || detail[:len("error: ")] != "error: " {
		t.Fatalf("detail = %q, want it to start with %q", detail, "error: ")
	}
}

// Redis going away after startup has to flip readiness over, not just the
// startup path.
func TestReadyz_RedisGoingAwayAfterStartupFlipsReadiness(t *testing.T) {
	s := miniredis.RunT(t)

	cfg := newTestConfig()
	cfg.Cache = redisCache(s.Addr())

	handler, err := buildHandler(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("buildHandler: %v", err)
	}

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if first.Code != http.StatusOK {
		t.Fatalf("status before the outage = %d, want 200; body = %s", first.Code, first.Body.String())
	}

	s.Close()

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if second.Code != http.StatusServiceUnavailable {
		t.Fatalf("status during the outage = %d, want 503; body = %s", second.Code, second.Body.String())
	}
	if _, reported := failingChecks(t, second)["redis"]; !reported {
		t.Fatalf("body = %s, want the dependency named", second.Body.String())
	}
}

// Readiness is about dependencies; liveness is about the process. A dead Redis
// must not make the process look dead, or an orchestrator restarts a healthy
// gateway.
func TestReadyz_AnUnreachableDependencyLeavesLivezAlone(t *testing.T) {
	s := miniredis.RunT(t)
	addr := s.Addr()
	s.Close()

	cfg := newTestConfig()
	cfg.Cache = redisCache(addr)

	handler, err := buildHandler(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("buildHandler: %v", err)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/livez", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("livez status = %d, want 200", rec.Code)
	}
	if body := rec.Body.String(); body != `{"status":"ok"}` {
		t.Fatalf("livez body = %q, want the static ok payload", body)
	}
}

// A disabled cache contributes no check, so an instance that never asked for
// Redis cannot be taken out of rotation by one.
func TestReadyz_DisabledCacheIgnoresRedis(t *testing.T) {
	cfg := newTestConfig()
	cfg.Cache = config.CacheConfig{
		Enabled:      false,
		TTL:          config.Duration(60 * time.Second),
		L2Enabled:    true,
		RedisAddr:    "127.0.0.1:65530",
		MaxEntries:   100,
		MaxBodyBytes: 1024 * 1024,
	}

	rec := readyz(t, cfg)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
}
