package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/syncronix-labs/aegora/internal/admin"
	"github.com/syncronix-labs/aegora/internal/config"
)

func newTestConfig() *config.Config {
	return &config.Config{
		Server: config.ServerConfig{
			Addr:         "127.0.0.1:8080",
			ReadTimeout:  config.Duration(15 * time.Second),
			WriteTimeout: config.Duration(15 * time.Second),
			IdleTimeout:  config.Duration(30 * time.Second),
		},
		Routes: []config.RouteConfig{
			{
				Name:       "test-route",
				PathPrefix: "/api",
				Targets:    []string{"http://127.0.0.1:8081"},
			},
		},
		Observability: config.ObservabilityConfig{
			LogLevel:    "info",
			MetricsAddr: "127.0.0.1:9090",
		},
	}
}

func TestBuildHandler_EndpointsAndMiddleware(t *testing.T) {
	cfg := newTestConfig()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler, err := buildHandler(cfg, logger)
	if err != nil {
		t.Fatalf("buildHandler: %v", err)
	}

	tests := []struct {
		name         string
		method       string
		path         string
		wantStatus   int
		wantBody     string
		checkJSONMsg bool
	}{
		{
			name:       "GET /livez returns 200 ok",
			method:     http.MethodGet,
			path:       "/livez",
			wantStatus: http.StatusOK,
			wantBody:   `{"status":"ok"}`,
		},
		{
			name:       "GET /readyz returns 200 ok",
			method:     http.MethodGet,
			path:       "/readyz",
			wantStatus: http.StatusOK,
			wantBody:   `{"status":"ok"}`,
		},
		{
			name:       "POST /livez returns 405 Method Not Allowed",
			method:     http.MethodPost,
			path:       "/livez",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "POST /readyz returns 405 Method Not Allowed",
			method:     http.MethodPost,
			path:       "/readyz",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "GET /unknown returns 404 Not Found",
			method:     http.MethodGet,
			path:       "/unknown",
			wantStatus: http.StatusNotFound,
			wantBody:   `{"error":"no route matched"}`,
		},
		{
			name:       "POST /other returns 404 Not Found",
			method:     http.MethodPost,
			path:       "/other",
			wantStatus: http.StatusNotFound,
			wantBody:   `{"error":"no route matched"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, tt.path, nil)

			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status mismatch: got %d, want %d", rec.Code, tt.wantStatus)
			}

			if tt.wantBody != "" && rec.Body.String() != tt.wantBody {
				t.Fatalf("body mismatch: got %q, want %q", rec.Body.String(), tt.wantBody)
			}

			// Verify that every response carries an X-Request-ID header
			reqID := rec.Header().Get("X-Request-ID")
			if reqID == "" {
				t.Fatal("expected X-Request-ID header on response, got none")
			}
			if len(reqID) != 32 {
				t.Fatalf("expected 32-character hex X-Request-ID, got %q", reqID)
			}
		})
	}
}

func TestBuildHandler_PropagatesExistingRequestID(t *testing.T) {
	cfg := newTestConfig()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler, err := buildHandler(cfg, logger)
	if err != nil {
		t.Fatalf("buildHandler: %v", err)
	}

	const incomingID = "custom-client-id-abc"
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/livez", nil)
	req.Header.Set("X-Request-ID", incomingID)

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if got := rec.Header().Get("X-Request-ID"); got != incomingID {
		t.Fatalf("expected echoed X-Request-ID %q, got %q", incomingID, got)
	}
}

func TestBuildHandler_MultiBackendRoutingAndStripPrefix(t *testing.T) {
	var backend1Path, backend2Path string
	var mu sync.Mutex

	backend1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		backend1Path = r.URL.Path
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"backend":"1"}`))
	}))
	defer backend1.Close()

	backend2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		backend2Path = r.URL.Path
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"backend":"2"}`))
	}))
	defer backend2.Close()

	cfg := &config.Config{
		Server: config.ServerConfig{
			Addr:         "127.0.0.1:8080",
			ReadTimeout:  config.Duration(15 * time.Second),
			WriteTimeout: config.Duration(15 * time.Second),
			IdleTimeout:  config.Duration(30 * time.Second),
		},
		Routes: []config.RouteConfig{
			{
				Name:        "users-route",
				PathPrefix:  "/users",
				Targets:     []string{backend1.URL},
				StripPrefix: "/users",
			},
			{
				Name:        "orders-route",
				PathPrefix:  "/orders",
				Targets:     []string{backend2.URL},
				StripPrefix: "/orders",
			},
		},
		Observability: config.ObservabilityConfig{
			LogLevel:    "info",
			MetricsAddr: "127.0.0.1:9090",
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler, err := buildHandler(cfg, logger)
	if err != nil {
		t.Fatalf("buildHandler: %v", err)
	}

	// 1. Request to /users/profile lands on backend 1 and strip prefix yields /profile
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/users/profile", nil)
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", rec1.Code, rec1.Body.String())
	}
	if rec1.Body.String() != `{"backend":"1"}` {
		t.Fatalf("expected backend 1 response, got %s", rec1.Body.String())
	}

	mu.Lock()
	gotPath1 := backend1Path
	mu.Unlock()
	if gotPath1 != "/profile" {
		t.Fatalf("expected stripped path /profile on backend 1, got %q", gotPath1)
	}

	// 2. Request to /orders/42 lands on backend 2 and strip prefix yields /42
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/orders/42", nil)
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", rec2.Code, rec2.Body.String())
	}
	if rec2.Body.String() != `{"backend":"2"}` {
		t.Fatalf("expected backend 2 response, got %s", rec2.Body.String())
	}

	mu.Lock()
	gotPath2 := backend2Path
	mu.Unlock()
	if gotPath2 != "/42" {
		t.Fatalf("expected stripped path /42 on backend 2, got %q", gotPath2)
	}
}

func TestBuildHandler_MaxRequestBody(t *testing.T) {
	const limit = 16

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	newHandler := func(t *testing.T, maxBody int64) (http.Handler, *atomic.Int32) {
		t.Helper()
		var hits atomic.Int32
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(backend.Close)

		cfg := newTestConfig()
		cfg.Routes[0].Targets = []string{backend.URL}
		cfg.Server.MaxRequestBodyBytes = maxBody
		h, err := buildHandlerWithRegistry(cfg, logger, prometheus.NewRegistry())
		if err != nil {
			t.Fatalf("buildHandler: %v", err)
		}
		return h, &hits
	}

	post := func(h http.Handler, size int) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/upload", strings.NewReader(strings.Repeat("a", size)))
		h.ServeHTTP(rec, req)
		return rec
	}

	t.Run("one byte over limit is rejected without upstream hit", func(t *testing.T) {
		h, hits := newHandler(t, limit)
		rec := post(h, limit+1)

		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("expected 413, got %d (body: %s)", rec.Code, rec.Body.String())
		}
		if got := rec.Body.String(); got != `{"error":"request body too large"}` {
			t.Fatalf("unexpected body: %s", got)
		}
		if n := hits.Load(); n != 0 {
			t.Fatalf("expected no upstream hits, got %d", n)
		}
	})

	t.Run("body at limit is proxied", func(t *testing.T) {
		h, hits := newHandler(t, limit)
		rec := post(h, limit)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d (body: %s)", rec.Code, rec.Body.String())
		}
		if n := hits.Load(); n != 1 {
			t.Fatalf("expected 1 upstream hit, got %d", n)
		}
	})

	t.Run("chunked body over limit returns 413", func(t *testing.T) {
		h, _ := newHandler(t, limit)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/upload", strings.NewReader(strings.Repeat("a", limit*4)))
		req.ContentLength = -1 // unknown length, as with chunked transfer encoding
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("expected 413, got %d (body: %s)", rec.Code, rec.Body.String())
		}
		if got := rec.Body.String(); got != `{"error":"request body too large"}` {
			t.Fatalf("unexpected body: %s", got)
		}
	})

	t.Run("unset limit allows large bodies", func(t *testing.T) {
		h, hits := newHandler(t, 0)
		rec := post(h, 1<<20)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d (body: %s)", rec.Code, rec.Body.String())
		}
		if n := hits.Load(); n != 1 {
			t.Fatalf("expected 1 upstream hit, got %d", n)
		}
	})
}

func TestComputeAppliedAndSkipped_MaxRequestBody(t *testing.T) {
	oldCfg := newTestConfig()
	newCfg := newTestConfig()
	newCfg.Server.MaxRequestBodyBytes = 1024

	applied, skipped := computeAppliedAndSkipped(oldCfg, newCfg)

	if len(applied) != 1 || applied[0] != "max_request_body_bytes" {
		t.Fatalf("applied = %v, want [max_request_body_bytes]", applied)
	}
	if len(skipped) != 0 {
		t.Fatalf("skipped = %v, want none", skipped)
	}
}

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		input   string
		want    slog.Level
		wantErr bool
	}{
		{input: "debug", want: slog.LevelDebug, wantErr: false},
		{input: "DEBUG", want: slog.LevelDebug, wantErr: false},
		{input: "info", want: slog.LevelInfo, wantErr: false},
		{input: "Info", want: slog.LevelInfo, wantErr: false},
		{input: "warn", want: slog.LevelWarn, wantErr: false},
		{input: "WARN", want: slog.LevelWarn, wantErr: false},
		{input: "error", want: slog.LevelError, wantErr: false},
		{input: "ERROR", want: slog.LevelError, wantErr: false},
		{input: "invalid", wantErr: true},
		{input: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseLogLevel(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseLogLevel(%q) error = %v, wantErr = %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("parseLogLevel(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestRunCheckConfig(t *testing.T) {
	testdata := filepath.Join("..", "..", "internal", "config", "testdata")

	tests := []struct {
		name       string
		path       string
		env        map[string]string
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		{
			name:       "valid config",
			path:       filepath.Join(testdata, "valid.yaml"),
			wantCode:   0,
			wantStdout: "config OK\n",
		},
		{
			name:       "unknown field is named",
			path:       filepath.Join(testdata, "invalid-unknown-field.yaml"),
			wantCode:   1,
			wantStderr: "extra_typo_field",
		},
		{
			name:       "invalid duration",
			path:       filepath.Join(testdata, "invalid-duration.yaml"),
			wantCode:   1,
			wantStderr: "bogus",
		},
		{
			name:       "missing file",
			path:       filepath.Join(t.TempDir(), "missing.yaml"),
			wantCode:   1,
			wantStderr: "read file",
		},
		{
			name:       "invalid env override",
			path:       filepath.Join(testdata, "valid.yaml"),
			env:        map[string]string{"AEGORA_SERVER_READ_TIMEOUT": "bogus"},
			wantCode:   1,
			wantStderr: "AEGORA_SERVER_READ_TIMEOUT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			var stdout, stderr bytes.Buffer
			code := runCheckConfig(tt.path, &stdout, &stderr)

			if code != tt.wantCode {
				t.Fatalf("runCheckConfig() = %d, want %d (stderr: %q)", code, tt.wantCode, stderr.String())
			}
			if tt.wantStdout != "" && stdout.String() != tt.wantStdout {
				t.Errorf("stdout = %q, want %q", stdout.String(), tt.wantStdout)
			}
			if tt.wantCode == 0 && stderr.Len() != 0 {
				t.Errorf("stderr = %q, want empty", stderr.String())
			}
			if tt.wantCode != 0 {
				if stdout.Len() != 0 {
					t.Errorf("stdout = %q, want empty on failure", stdout.String())
				}
				if !strings.Contains(stderr.String(), tt.wantStderr) {
					t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tt.wantStderr)
				}
			}
		})
	}
}

func TestBuildHandler_AccessLogging(t *testing.T) {
	cfg := newTestConfig()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	handler, err := buildHandler(cfg, logger)
	if err != nil {
		t.Fatalf("buildHandler: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/livez", nil)

	handler.ServeHTTP(rec, req)

	logOutput := buf.String()
	if logOutput == "" {
		t.Fatal("expected access log output, got empty")
	}
}

func TestBuildHandler_RouteRateLimitOverrides(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	intPtr := func(v int) *int { return &v }
	boolPtr := func(v bool) *bool { return &v }

	newHandler := func(t *testing.T, globalEnabled bool) http.Handler {
		t.Helper()
		cfg := newTestConfig()
		cfg.Routes = []config.RouteConfig{
			{
				Name: "login", PathPrefix: "/login", Targets: []string{backend.URL},
				RateLimit: &config.RouteRateLimitConfig{Enabled: boolPtr(true), MaxRequests: intPtr(2)},
			},
			{
				Name: "internal", PathPrefix: "/internal", Targets: []string{backend.URL},
				RateLimit: &config.RouteRateLimitConfig{Enabled: boolPtr(false)},
			},
			{Name: "web", PathPrefix: "/", Targets: []string{backend.URL}},
		}
		cfg.RateLimit = config.RateLimitConfig{
			Enabled: globalEnabled, Limiter: "sliding_window", MaxRequests: 4,
			Window: config.Duration(time.Minute), Key: "ip",
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		h, err := buildHandlerWithRegistry(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)), prometheus.NewRegistry())
		if err != nil {
			t.Fatalf("buildHandler: %v", err)
		}
		return h
	}

	do := func(h http.Handler, path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.RemoteAddr = "192.0.2.10:5555"
		h.ServeHTTP(rec, req)
		return rec
	}

	t.Run("route override has its own stricter limit", func(t *testing.T) {
		h := newHandler(t, true)

		for i := range 2 {
			rec := do(h, "/login")
			if rec.Code != http.StatusOK {
				t.Fatalf("login %d = %d, want 200", i+1, rec.Code)
			}
			if got := rec.Header().Get("X-RateLimit-Limit"); got != "2" {
				t.Fatalf("login X-RateLimit-Limit = %q, want 2", got)
			}
		}
		if rec := do(h, "/login"); rec.Code != http.StatusTooManyRequests {
			t.Fatalf("login 3 = %d, want 429", rec.Code)
		}

		// Global bucket is separate and untouched by login traffic.
		for i := range 4 {
			rec := do(h, "/home")
			if rec.Code != http.StatusOK {
				t.Fatalf("web %d = %d, want 200", i+1, rec.Code)
			}
			if got := rec.Header().Get("X-RateLimit-Limit"); got != "4" {
				t.Fatalf("web X-RateLimit-Limit = %q, want 4", got)
			}
		}
		if rec := do(h, "/home"); rec.Code != http.StatusTooManyRequests {
			t.Fatalf("web 5 = %d, want 429", rec.Code)
		}
	})

	t.Run("enabled false opts a route out", func(t *testing.T) {
		h := newHandler(t, true)
		for i := range 10 {
			rec := do(h, "/internal/status")
			if rec.Code != http.StatusOK {
				t.Fatalf("internal %d = %d, want 200", i+1, rec.Code)
			}
			if got := rec.Header().Get("X-RateLimit-Limit"); got != "" {
				t.Fatalf("internal route should not be rate limited, got X-RateLimit-Limit %q", got)
			}
		}
	})

	t.Run("route can enable limiting while global is off", func(t *testing.T) {
		h := newHandler(t, false)
		for range 2 {
			do(h, "/login")
		}
		if rec := do(h, "/login"); rec.Code != http.StatusTooManyRequests {
			t.Fatalf("login 3 = %d, want 429", rec.Code)
		}
		for i := range 10 {
			if rec := do(h, "/home"); rec.Code != http.StatusOK {
				t.Fatalf("web %d = %d, want 200 (global limiting disabled)", i+1, rec.Code)
			}
		}
	})

	// Documents a known limitation: routing matches the percent-encoded path, so
	// "/%6Cogin" does not match the /login route and gets the catch-all route's limit.
	// A backend that decodes paths would still see /login. Tracked as a follow-up.
	t.Run("percent-encoded path falls through to catch-all limit", func(t *testing.T) {
		h := newHandler(t, true)
		for range 2 {
			do(h, "/login")
		}
		rec := do(h, "/%6Cogin")
		if rec.Code != http.StatusOK {
			t.Fatalf("encoded login = %d, want 200 (catch-all limit applies)", rec.Code)
		}
		if got := rec.Header().Get("X-RateLimit-Limit"); got != "4" {
			t.Fatalf("encoded login X-RateLimit-Limit = %q, want 4 (catch-all route)", got)
		}
	})
}

func TestBuildHandler_RateLimiting(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"proxied"}`))
	}))
	defer backend.Close()

	cfg := &config.Config{
		Server: config.ServerConfig{
			Addr:         "127.0.0.1:8080",
			ReadTimeout:  config.Duration(15 * time.Second),
			WriteTimeout: config.Duration(15 * time.Second),
			IdleTimeout:  config.Duration(30 * time.Second),
		},
		Routes: []config.RouteConfig{
			{
				Name:       "api-route",
				PathPrefix: "/api",
				Targets:    []string{backend.URL},
			},
		},
		Observability: config.ObservabilityConfig{
			LogLevel: "info",
		},
		RateLimit: config.RateLimitConfig{
			Enabled:     true,
			Limiter:     "sliding_window",
			MaxRequests: 2,
			Window:      config.Duration(time.Minute),
			Key:         "ip",
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler, err := buildHandler(cfg, logger)
	if err != nil {
		t.Fatalf("buildHandler: %v", err)
	}

	// 1st request -> 200 OK
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/api/data", nil)
	req1.RemoteAddr = "192.168.1.50:1234"
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("request 1 expected 200, got %d", rec1.Code)
	}

	// 2nd request -> 200 OK
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/api/data", nil)
	req2.RemoteAddr = "192.168.1.50:1234"
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("request 2 expected 200, got %d", rec2.Code)
	}

	// 3rd (N+1th) rapid request -> 429 Too Many Requests
	rec3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodGet, "/api/data", nil)
	req3.RemoteAddr = "192.168.1.50:1234"
	handler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusTooManyRequests {
		t.Fatalf("N+1th request expected 429, got %d", rec3.Code)
	}

	if limit := rec3.Header().Get("X-RateLimit-Limit"); limit != "2" {
		t.Fatalf("expected X-RateLimit-Limit '2', got %q", limit)
	}
	if rem := rec3.Header().Get("X-RateLimit-Remaining"); rem != "0" {
		t.Fatalf("expected X-RateLimit-Remaining '0', got %q", rem)
	}
	if retryAfter := rec3.Header().Get("Retry-After"); retryAfter == "" {
		t.Fatalf("expected Retry-After header, got empty")
	}
	if body := rec3.Body.String(); body != `{"error":"rate limit exceeded"}` {
		t.Fatalf("expected body %q, got %q", `{"error":"rate limit exceeded"}`, body)
	}

	// Verify that /livez is never limited, even when client is rate-limited
	for i := 0; i < 5; i++ {
		recLivez := httptest.NewRecorder()
		reqLivez := httptest.NewRequest(http.MethodGet, "/livez", nil)
		reqLivez.RemoteAddr = "192.168.1.50:1234"
		handler.ServeHTTP(recLivez, reqLivez)
		if recLivez.Code != http.StatusOK {
			t.Fatalf("/livez call %d expected 200, got %d", i+1, recLivez.Code)
		}
	}
}

func TestBuildHandler_QueueAdmission(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"proxied"}`))
	}))
	defer backend.Close()

	cfg := &config.Config{
		Server: config.ServerConfig{
			Addr:         "127.0.0.1:8080",
			ReadTimeout:  config.Duration(15 * time.Second),
			WriteTimeout: config.Duration(15 * time.Second),
			IdleTimeout:  config.Duration(30 * time.Second),
		},
		Routes: []config.RouteConfig{
			{
				Name:       "api-route",
				PathPrefix: "/api",
				Targets:    []string{backend.URL},
			},
		},
		Observability: config.ObservabilityConfig{
			LogLevel: "info",
		},
		Queue: config.QueueConfig{
			Enabled:       true,
			MaxInflight:   2,
			MaxQueueDepth: 5,
			CodelTarget:   config.Duration(100 * time.Millisecond),
			CodelInterval: config.Duration(time.Second),
			PriorityPaths: config.PriorityPathsConfig{
				High: []string{"/api/priority"},
				Low:  []string{"/api/bulk"},
			},
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler, q, err := buildApp(cfg, logger)
	if err != nil {
		t.Fatalf("buildApp: %v", err)
	}
	if q == nil {
		t.Fatal("expected non-nil queue when queue is enabled")
	}
	q.Start()
	defer q.Stop()

	// 1. Livez bypasses queue completely
	recLivez := httptest.NewRecorder()
	reqLivez := httptest.NewRequest(http.MethodGet, "/livez", nil)
	handler.ServeHTTP(recLivez, reqLivez)
	if recLivez.Code != http.StatusOK {
		t.Fatalf("expected 200 for /livez, got %d", recLivez.Code)
	}

	// 2. Normal request through queue to backend succeeds
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/api/data", nil)
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 for /api/data, got %d (body: %s)", rec1.Code, rec1.Body.String())
	}

	// 3. High priority request through queue to backend succeeds
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/api/priority/pay", nil)
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 for /api/priority/pay, got %d", rec2.Code)
	}
}

func TestBuildHandler_Shedding(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"proxied"}`))
	}))
	defer backend.Close()

	cfg := &config.Config{
		Server: config.ServerConfig{
			Addr:         "127.0.0.1:8080",
			ReadTimeout:  config.Duration(15 * time.Second),
			WriteTimeout: config.Duration(15 * time.Second),
			IdleTimeout:  config.Duration(30 * time.Second),
		},
		Routes: []config.RouteConfig{
			{
				Name:       "api-route",
				PathPrefix: "/api",
				Targets:    []string{backend.URL},
			},
		},
		Observability: config.ObservabilityConfig{
			LogLevel: "info",
		},
		Shedding: config.SheddingConfig{
			Enabled:        true,
			TargetP99:      config.Duration(500 * time.Millisecond),
			MinAdmitRate:   0.05,
			AdjustInterval: config.Duration(time.Second),
			PriorityPaths: config.PriorityPathsConfig{
				High: []string{"/api/payments"},
				Low:  []string{"/api/analytics"},
			},
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler, err := buildHandler(cfg, logger)
	if err != nil {
		t.Fatalf("buildHandler: %v", err)
	}

	// 1. Initial requests pass through since initial admitRate is 1.0
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// 2. Health checks pass through without issue
	recLivez := httptest.NewRecorder()
	reqLivez := httptest.NewRequest(http.MethodGet, "/livez", nil)
	handler.ServeHTTP(recLivez, reqLivez)
	if recLivez.Code != http.StatusOK {
		t.Fatalf("expected 200 for /livez, got %d", recLivez.Code)
	}
}

func TestBuildHandler_Cache(t *testing.T) {
	var backendHits atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		backendHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"cached":true}`))
	}))
	defer backend.Close()

	cfg := &config.Config{
		Server: config.ServerConfig{
			Addr:         "127.0.0.1:8080",
			ReadTimeout:  config.Duration(15 * time.Second),
			WriteTimeout: config.Duration(15 * time.Second),
			IdleTimeout:  config.Duration(30 * time.Second),
		},
		Routes: []config.RouteConfig{
			{
				Name:       "cached-route",
				PathPrefix: "/api",
				Targets:    []string{backend.URL},
			},
		},
		Observability: config.ObservabilityConfig{
			LogLevel: "info",
		},
		Cache: config.CacheConfig{
			Enabled:      true,
			TTL:          config.Duration(60 * time.Second),
			MaxEntries:   100,
			MaxBodyBytes: 1024 * 1024,
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler, err := buildHandler(cfg, logger)
	if err != nil {
		t.Fatalf("buildHandler: %v", err)
	}

	// First GET: miss, backend called
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/api/resource", nil)
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first status = %d, want 200", rec1.Code)
	}
	if rec1.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("expected X-Cache MISS, got %q", rec1.Header().Get("X-Cache"))
	}
	if backendHits.Load() != 1 {
		t.Fatalf("expected 1 backend hit, got %d", backendHits.Load())
	}

	// Second GET: hit, backend NOT called
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/api/resource", nil)
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("second status = %d, want 200", rec2.Code)
	}
	if rec2.Header().Get("X-Cache") != "HIT" {
		t.Fatalf("expected X-Cache HIT, got %q", rec2.Header().Get("X-Cache"))
	}
	if backendHits.Load() != 1 {
		t.Fatalf("expected backend hits to remain 1 after cache hit, got %d", backendHits.Load())
	}

	// Health check bypasses cache and returns 200
	recLivez := httptest.NewRecorder()
	reqLivez := httptest.NewRequest(http.MethodGet, "/livez", nil)
	handler.ServeHTTP(recLivez, reqLivez)
	if recLivez.Code != http.StatusOK {
		t.Fatalf("expected 200 for /livez, got %d", recLivez.Code)
	}
}

func TestBuildHandler_TieredCache(t *testing.T) {
	s := miniredis.RunT(t)
	var backendHits atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		backendHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"tiered":true}`))
	}))
	defer backend.Close()

	cfg := &config.Config{
		Server: config.ServerConfig{
			Addr:         "127.0.0.1:8080",
			ReadTimeout:  config.Duration(15 * time.Second),
			WriteTimeout: config.Duration(15 * time.Second),
			IdleTimeout:  config.Duration(30 * time.Second),
		},
		Routes: []config.RouteConfig{
			{
				Name:       "cached-route",
				PathPrefix: "/api",
				Targets:    []string{backend.URL},
			},
		},
		Observability: config.ObservabilityConfig{
			LogLevel: "info",
		},
		Cache: config.CacheConfig{
			Enabled:      true,
			TTL:          config.Duration(60 * time.Second),
			L2Enabled:    true,
			L2TTL:        config.Duration(5 * time.Minute),
			RedisAddr:    s.Addr(),
			MaxEntries:   100,
			MaxBodyBytes: 1024 * 1024,
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler, err := buildHandler(cfg, logger)
	if err != nil {
		t.Fatalf("buildHandler: %v", err)
	}

	// First request: miss, calls backend and writes through to miniredis
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/api/tiered", nil)
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first status = %d, want 200", rec1.Code)
	}
	if rec1.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("expected X-Cache MISS, got %q", rec1.Header().Get("X-Cache"))
	}
	if backendHits.Load() != 1 {
		t.Fatalf("expected 1 backend hit, got %d", backendHits.Load())
	}

	// Verify key was written to Redis L2
	if !s.Exists("aegora:cache:v1:GET /api/tiered?") {
		t.Fatal("expected Redis to contain cached key")
	}

	// Second request: hit, backend NOT called
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/api/tiered", nil)
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("second status = %d, want 200", rec2.Code)
	}
	if rec2.Header().Get("X-Cache") != "HIT" {
		t.Fatalf("expected X-Cache HIT, got %q", rec2.Header().Get("X-Cache"))
	}
	if backendHits.Load() != 1 {
		t.Fatalf("expected backend hits to remain 1 after cache hit, got %d", backendHits.Load())
	}
}

func TestBuildHandler_TieredCache_UnreachableDegrades(t *testing.T) {
	var backendHits atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		backendHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"degraded":true}`))
	}))
	defer backend.Close()

	// Use an unreachable local port so Redis dial immediately fails
	cfg := &config.Config{
		Server: config.ServerConfig{
			Addr:         "127.0.0.1:8080",
			ReadTimeout:  config.Duration(15 * time.Second),
			WriteTimeout: config.Duration(15 * time.Second),
			IdleTimeout:  config.Duration(30 * time.Second),
		},
		Routes: []config.RouteConfig{
			{
				Name:       "cached-route",
				PathPrefix: "/api",
				Targets:    []string{backend.URL},
			},
		},
		Observability: config.ObservabilityConfig{
			LogLevel: "info",
		},
		Cache: config.CacheConfig{
			Enabled:      true,
			TTL:          config.Duration(60 * time.Second),
			L2Enabled:    true,
			L2TTL:        config.Duration(5 * time.Minute),
			RedisAddr:    "127.0.0.1:65530",
			MaxEntries:   100,
			MaxBodyBytes: 1024 * 1024,
		},
	}

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
	handler, err := buildHandler(cfg, logger)
	if err != nil {
		t.Fatalf("buildHandler should not fail when Redis unreachable: %v", err)
	}

	// Verify log contains warning about degrading to L1
	if !strings.Contains(logBuf.String(), "degrading to l1-only") {
		t.Errorf("expected warning log about degrading to L1, got: %s", logBuf.String())
	}

	// Verify L1 cache still works
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/api/degraded", nil)
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec1.Code)
	}
	if rec1.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("expected X-Cache MISS, got %q", rec1.Header().Get("X-Cache"))
	}

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/api/degraded", nil)
	handler.ServeHTTP(rec2, req2)
	if rec2.Header().Get("X-Cache") != "HIT" {
		t.Fatalf("expected X-Cache HIT on L1 cache, got %q", rec2.Header().Get("X-Cache"))
	}
}

func TestBuildHandler_PrometheusMetrics(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"metric":true}`))
	}))
	defer backend.Close()

	cfg := &config.Config{
		Server: config.ServerConfig{
			Addr:         "127.0.0.1:8080",
			ReadTimeout:  config.Duration(15 * time.Second),
			WriteTimeout: config.Duration(15 * time.Second),
			IdleTimeout:  config.Duration(30 * time.Second),
		},
		Routes: []config.RouteConfig{
			{
				Name:       "metrics-test-route",
				PathPrefix: "/api",
				Targets:    []string{backend.URL},
			},
		},
		Observability: config.ObservabilityConfig{
			LogLevel:    "info",
			MetricsAddr: "127.0.0.1:9090",
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	reg := prometheus.NewRegistry()
	handler, err := buildHandlerWithRegistry(cfg, logger, reg)
	if err != nil {
		t.Fatalf("buildHandlerWithRegistry: %v", err)
	}

	// Make request to matching route
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// Make request to unmatched route (404)
	rec404 := httptest.NewRecorder()
	req404 := httptest.NewRequest(http.MethodGet, "/unmatched-path", nil)
	handler.ServeHTTP(rec404, req404)
	if rec404.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec404.Code)
	}

	// Verify /metrics endpoint output using promhttp.HandlerFor
	metricsHandler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	metricsRec := httptest.NewRecorder()
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsHandler.ServeHTTP(metricsRec, metricsReq)

	if metricsRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /metrics, got %d", metricsRec.Code)
	}

	metricsOutput := metricsRec.Body.String()
	if !strings.Contains(metricsOutput, `aegora_requests_total{code="200",method="GET",route="metrics-test-route"} 1`) {
		t.Errorf("expected metrics to contain matching route 200 count, got:\n%s", metricsOutput)
	}
	if !strings.Contains(metricsOutput, `aegora_requests_total{code="404",method="GET",route="unmatched"} 1`) {
		t.Errorf("expected metrics to contain unmatched route 404 count, got:\n%s", metricsOutput)
	}
	if !strings.Contains(metricsOutput, `aegora_request_duration_seconds_count{method="GET",route="metrics-test-route"} 1`) {
		t.Errorf("expected duration seconds count in metrics output, got:\n%s", metricsOutput)
	}
}

func TestBuildHandler_OpenTelemetryTracing(t *testing.T) {
	var receivedTraceparent string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedTraceparent = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"proxied"}`))
	}))
	defer backend.Close()

	cfg := &config.Config{
		Server: config.ServerConfig{
			Addr:         "127.0.0.1:8080",
			ReadTimeout:  config.Duration(15 * time.Second),
			WriteTimeout: config.Duration(15 * time.Second),
			IdleTimeout:  config.Duration(30 * time.Second),
		},
		Routes: []config.RouteConfig{
			{
				Name:       "trace-test-route",
				PathPrefix: "/api",
				Targets:    []string{backend.URL},
			},
		},
		Observability: config.ObservabilityConfig{
			LogLevel: "info",
			Tracing: config.TracingConfig{
				Enabled:     true,
				Exporter:    "stdout",
				SampleRatio: 1.0,
			},
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler, err := buildHandler(cfg, logger)
	if err != nil {
		t.Fatalf("buildHandler: %v", err)
	}

	const parentTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const parentSpanID = "00f067aa0ba902b7"
	incomingTraceparent := fmt.Sprintf("00-%s-%s-01", parentTraceID, parentSpanID)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/users", nil)
	req.Header.Set("traceparent", incomingTraceparent)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if receivedTraceparent == "" {
		t.Fatalf("expected backend to receive traceparent header from upstream propagation, got empty")
	}

	// Verify upstream backend received the exact same trace ID
	if !strings.Contains(receivedTraceparent, parentTraceID) {
		t.Errorf("expected propagated traceparent %q to contain parent trace ID %q", receivedTraceparent, parentTraceID)
	}
}

func TestHotReload_ValidConfigSwap(t *testing.T) {
	backend1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("backend1 response"))
	}))
	defer backend1.Close()

	backend2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("backend2 response"))
	}))
	defer backend2.Close()

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "aegora.yaml")

	initialYAML := fmt.Sprintf(`
server:
  addr: "127.0.0.1:8080"
  read_timeout: "15s"
  write_timeout: "15s"
  idle_timeout: "30s"
routes:
  - name: "route-one"
    path_prefix: "/r1"
    targets:
      - "%s"
observability:
  log_level: "info"
  metrics_addr: "127.0.0.1:9090"
admin:
  enabled: true
  addr: "127.0.0.1:9091"
`, backend1.URL)

	if err := os.WriteFile(configPath, []byte(initialYAML), 0o600); err != nil {
		t.Fatalf("write initial config: %v", err)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	lvl, err := parseLogLevel(cfg.Observability.LogLevel)
	if err != nil {
		t.Fatalf("parseLogLevel: %v", err)
	}
	levelVar := &slog.LevelVar{}
	levelVar.Set(lvl)
	logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: levelVar}))

	handler, q, reg, _, err := buildAppWithRegistry(cfg, logger, nil)
	if err != nil {
		t.Fatalf("buildAppWithRegistry: %v", err)
	}
	if q != nil {
		q.Start()
	}

	reloader := newGatewayReloader(configPath, cfg, handler, q, logger, reg, levelVar)
	rootHandler := reloader.RootHandler()

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

	adminSrv := admin.NewServer(cfg.Admin.Addr, reloader, configFn, routesFn)
	adminHandler := adminSrv.Handler()

	// 1. Verify before reload:
	// GET /r1/test -> 200 from backend1
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/r1/test", nil)
	rootHandler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK || rec1.Body.String() != "backend1 response" {
		t.Fatalf("expected 200 backend1 response, got %d %q", rec1.Code, rec1.Body.String())
	}

	// GET /r2/test -> 404 (route does not exist yet)
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/r2/test", nil)
	rootHandler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unconfigured /r2, got %d", rec2.Code)
	}

	// Check routesFn output before reload
	initialRoutes := routesFn()
	if len(initialRoutes) != 1 || initialRoutes[0].Name != "route-one" {
		t.Fatalf("expected 1 route named 'route-one', got %+v", initialRoutes)
	}

	// 2. Modify config on disk to replace route-one with route-two and change log_level to debug
	updatedYAML := fmt.Sprintf(`
server:
  addr: "127.0.0.1:8080"
  read_timeout: "15s"
  write_timeout: "15s"
  idle_timeout: "30s"
routes:
  - name: "route-two"
    path_prefix: "/r2"
    targets:
      - "%s"
observability:
  log_level: "debug"
  metrics_addr: "127.0.0.1:9090"
admin:
  enabled: true
  addr: "127.0.0.1:9091"
`, backend2.URL)

	if err := os.WriteFile(configPath, []byte(updatedYAML), 0o600); err != nil {
		t.Fatalf("write updated config: %v", err)
	}

	// Trigger reload via admin POST /admin/reload
	reloadRec := httptest.NewRecorder()
	reloadReq := httptest.NewRequest(http.MethodPost, "/admin/reload", nil)
	adminHandler.ServeHTTP(reloadRec, reloadReq)

	if reloadRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /admin/reload, got %d: %s", reloadRec.Code, reloadRec.Body.String())
	}

	var reloadResp struct {
		Reloaded bool     `json:"reloaded"`
		Applied  []string `json:"applied"`
		Skipped  []string `json:"skipped"`
	}
	if err := json.Unmarshal(reloadRec.Body.Bytes(), &reloadResp); err != nil {
		t.Fatalf("unmarshal reload resp: %v", err)
	}

	if !reloadResp.Reloaded {
		t.Errorf("expected reloaded = true")
	}

	// Verify log level updated dynamically
	if levelVar.Level() != slog.LevelDebug {
		t.Errorf("expected levelVar updated to debug, got %v", levelVar.Level())
	}

	// 3. Verify after reload:
	// New route appears in routesFn output
	updatedRoutes := routesFn()
	if len(updatedRoutes) != 1 || updatedRoutes[0].Name != "route-two" {
		t.Fatalf("expected 1 route named 'route-two', got %+v", updatedRoutes)
	}

	// Traffic to /r2/test now succeeds through swapped handler!
	recNew := httptest.NewRecorder()
	reqNew := httptest.NewRequest(http.MethodGet, "/r2/test", nil)
	rootHandler.ServeHTTP(recNew, reqNew)
	if recNew.Code != http.StatusOK || recNew.Body.String() != "backend2 response" {
		t.Fatalf("expected 200 backend2 response after reload, got %d %q", recNew.Code, recNew.Body.String())
	}

	// Old route /r1/test now returns 404
	recOld := httptest.NewRecorder()
	reqOld := httptest.NewRequest(http.MethodGet, "/r1/test", nil)
	rootHandler.ServeHTTP(recOld, reqOld)
	if recOld.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for old route after reload, got %d", recOld.Code)
	}

	// 4. Verify admin endpoints: /admin/health, /admin/routes, /admin/config
	healthRec := httptest.NewRecorder()
	adminHandler.ServeHTTP(healthRec, httptest.NewRequest(http.MethodGet, "/admin/health", nil))
	if healthRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /admin/health, got %d", healthRec.Code)
	}
	var healthData map[string]string
	if err := json.Unmarshal(healthRec.Body.Bytes(), &healthData); err != nil || healthData["status"] != "ok" {
		t.Fatalf("invalid /admin/health payload: %v", healthData)
	}

	routesRec := httptest.NewRecorder()
	adminHandler.ServeHTTP(routesRec, httptest.NewRequest(http.MethodGet, "/admin/routes", nil))
	if routesRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /admin/routes, got %d", routesRec.Code)
	}
	var adminRoutes []admin.RouteInfo
	if err := json.Unmarshal(routesRec.Body.Bytes(), &adminRoutes); err != nil || len(adminRoutes) != 1 || adminRoutes[0].Name != "route-two" {
		t.Fatalf("invalid /admin/routes payload: %v", adminRoutes)
	}

	cfgRec := httptest.NewRecorder()
	adminHandler.ServeHTTP(cfgRec, httptest.NewRequest(http.MethodGet, "/admin/config", nil))
	if cfgRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /admin/config, got %d", cfgRec.Code)
	}
	var returnedCfg config.Config
	if err := json.Unmarshal(cfgRec.Body.Bytes(), &returnedCfg); err != nil || len(returnedCfg.Routes) != 1 || returnedCfg.Routes[0].Name != "route-two" {
		t.Fatalf("invalid /admin/config payload: %v", returnedCfg)
	}
}

func TestHotReload_InvalidConfigAtomicity(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("backend response"))
	}))
	defer backend.Close()

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "aegora.yaml")

	validYAML := fmt.Sprintf(`
server:
  addr: "127.0.0.1:8080"
  read_timeout: "15s"
  write_timeout: "15s"
  idle_timeout: "30s"
routes:
  - name: "stable-route"
    path_prefix: "/stable"
    targets:
      - "%s"
observability:
  log_level: "info"
  metrics_addr: "127.0.0.1:9090"
`, backend.URL)

	if err := os.WriteFile(configPath, []byte(validYAML), 0o600); err != nil {
		t.Fatalf("write valid config: %v", err)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler, q, reg, _, err := buildAppWithRegistry(cfg, logger, nil)
	if err != nil {
		t.Fatalf("buildAppWithRegistry: %v", err)
	}

	reloader := newGatewayReloader(configPath, cfg, handler, q, logger, reg, nil)
	rootHandler := reloader.RootHandler()

	// Initial request succeeds
	rec1 := httptest.NewRecorder()
	rootHandler.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/stable/item", nil))
	if rec1.Code != http.StatusOK || rec1.Body.String() != "backend response" {
		t.Fatalf("expected 200 before bad reload, got %d %q", rec1.Code, rec1.Body.String())
	}

	// Corrupt config file: routes empty (violates Validate requirement of at least 1 route)
	invalidYAML := `
server:
  addr: "127.0.0.1:8080"
  read_timeout: "15s"
  write_timeout: "15s"
  idle_timeout: "30s"
routes: []
observability:
  log_level: "info"
  metrics_addr: "127.0.0.1:9090"
`
	if err := os.WriteFile(configPath, []byte(invalidYAML), 0o600); err != nil {
		t.Fatalf("write invalid config: %v", err)
	}

	// Trigger reload -> must return error
	applied, skipped, err := reloader.Reload()
	if err == nil {
		t.Fatalf("expected reload to fail on invalid config, got nil (applied: %v, skipped: %v)", applied, skipped)
	}

	// Prove ATOMICITY: old handler and route still serve successfully!
	rec2 := httptest.NewRecorder()
	rootHandler.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/stable/item", nil))
	if rec2.Code != http.StatusOK || rec2.Body.String() != "backend response" {
		t.Fatalf("expected 200 after failed reload (atomicity preserved), got %d %q", rec2.Code, rec2.Body.String())
	}
}

func TestHotReload_SkippedRestartRequired(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "aegora.yaml")

	baseYAML := `
server:
  addr: "127.0.0.1:8080"
  read_timeout: "15s"
  write_timeout: "15s"
  idle_timeout: "30s"
routes:
  - name: "r1"
    path_prefix: "/r1"
    targets:
      - "http://127.0.0.1:8081"
observability:
  log_level: "info"
  metrics_addr: "127.0.0.1:9090"
  tracing:
    enabled: true
    exporter: "stdout"
    sample_ratio: 1.0
admin:
  enabled: true
  addr: "127.0.0.1:9091"
`
	if err := os.WriteFile(configPath, []byte(baseYAML), 0o600); err != nil {
		t.Fatalf("write base config: %v", err)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler, q, reg, _, err := buildAppWithRegistry(cfg, logger, nil)
	if err != nil {
		t.Fatalf("buildAppWithRegistry: %v", err)
	}

	reloader := newGatewayReloader(configPath, cfg, handler, q, logger, reg, nil)

	// Change restart-required sections: server.addr, metrics_addr, admin.addr, tracing exporter
	changedYAML := `
server:
  addr: "127.0.0.1:8888"
  read_timeout: "15s"
  write_timeout: "15s"
  idle_timeout: "30s"
routes:
  - name: "r1"
    path_prefix: "/r1"
    targets:
      - "http://127.0.0.1:8081"
observability:
  log_level: "info"
  metrics_addr: "127.0.0.1:9990"
  tracing:
    enabled: true
    exporter: "otlp"
    otlp_endpoint: "localhost:4318"
    sample_ratio: 1.0
admin:
  enabled: true
  addr: "127.0.0.1:9991"
`
	if err := os.WriteFile(configPath, []byte(changedYAML), 0o600); err != nil {
		t.Fatalf("write changed config: %v", err)
	}

	_, skipped, err := reloader.Reload()
	if err != nil {
		t.Fatalf("Reload failed: %v", err)
	}

	skippedMap := make(map[string]bool)
	for _, s := range skipped {
		skippedMap[s] = true
	}

	expectedSkipped := []string{"server addr", "metrics addr", "admin addr", "tracing exporter"}
	for _, exp := range expectedSkipped {
		if !skippedMap[exp] {
			t.Errorf("expected skipped to contain %q, got: %v", exp, skipped)
		}
	}
}
