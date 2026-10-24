package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/syncronix-labs/aegora/internal/ratelimit"
)

func TestKeyFuncs(t *testing.T) {
	t.Run("IPKey", func(t *testing.T) {
		// X-Forwarded-For present with multiple IPs
		req1 := httptest.NewRequest(http.MethodGet, "/", nil)
		req1.Header.Set("X-Forwarded-For", "203.0.113.195, 70.41.3.18, 150.172.238.178")
		req1.RemoteAddr = "10.0.0.1:12345"
		if got := IPKey(req1); got != "203.0.113.195" {
			t.Fatalf("expected leftmost XFF '203.0.113.195', got %q", got)
		}

		// Fallback to RemoteAddr host
		req2 := httptest.NewRequest(http.MethodGet, "/", nil)
		req2.RemoteAddr = "192.168.1.100:54321"
		if got := IPKey(req2); got != "192.168.1.100" {
			t.Fatalf("expected RemoteAddr host '192.168.1.100', got %q", got)
		}

		// RemoteAddr without port
		req3 := httptest.NewRequest(http.MethodGet, "/", nil)
		req3.RemoteAddr = "192.168.1.101"
		if got := IPKey(req3); got != "192.168.1.101" {
			t.Fatalf("expected raw RemoteAddr '192.168.1.101', got %q", got)
		}

		// Nil request
		if got := IPKey(nil); got != "" {
			t.Fatalf("expected empty for nil, got %q", got)
		}
	})

	t.Run("APIKey", func(t *testing.T) {
		// X-API-Key header present
		req1 := httptest.NewRequest(http.MethodGet, "/", nil)
		req1.Header.Set("X-API-Key", "secret-api-key-123")
		if got := APIKey(req1); got != "secret-api-key-123" {
			t.Fatalf("expected 'secret-api-key-123', got %q", got)
		}

		// Fallback to Bearer token in Authorization header
		req2 := httptest.NewRequest(http.MethodGet, "/", nil)
		req2.Header.Set("Authorization", "Bearer jwt-token-xyz")
		if got := APIKey(req2); got != "jwt-token-xyz" {
			t.Fatalf("expected 'jwt-token-xyz', got %q", got)
		}

		// Neither present returns empty string
		req3 := httptest.NewRequest(http.MethodGet, "/", nil)
		if got := APIKey(req3); got != "" {
			t.Fatalf("expected empty string, got %q", got)
		}

		// Nil request
		if got := APIKey(nil); got != "" {
			t.Fatalf("expected empty for nil, got %q", got)
		}
	})
}

func TestRateLimit_PerKeyIndependence(t *testing.T) {
	newLimiter := func() ratelimit.Limiter {
		return ratelimit.NewSlidingWindow(time.Minute, 2)
	}

	mw := RateLimit(newLimiter, IPKey, nil)
	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	handler := mw(dummyHandler)

	// Exhaust Key A's budget (2 requests allowed)
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api", nil)
		req.RemoteAddr = "10.0.0.1:1000"
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("key A request %d expected 200, got %d", i+1, rec.Code)
		}
	}

	// Key A request 3 should be denied with 429
	recA := httptest.NewRecorder()
	reqA := httptest.NewRequest(http.MethodGet, "/api", nil)
	reqA.RemoteAddr = "10.0.0.1:1000"
	handler.ServeHTTP(recA, reqA)
	if recA.Code != http.StatusTooManyRequests {
		t.Fatalf("key A request 3 expected 429, got %d", recA.Code)
	}

	// Key B should be unaffected and admitted
	recB := httptest.NewRecorder()
	reqB := httptest.NewRequest(http.MethodGet, "/api", nil)
	reqB.RemoteAddr = "10.0.0.2:2000"
	handler.ServeHTTP(recB, reqB)
	if recB.Code != http.StatusOK {
		t.Fatalf("key B expected 200, got %d", recB.Code)
	}
}

func TestRateLimit_429ResponseHeadersAndBody(t *testing.T) {
	newLimiter := func() ratelimit.Limiter {
		return ratelimit.NewSlidingWindow(10*time.Second, 1)
	}

	mw := RateLimit(newLimiter, IPKey, nil)
	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := mw(dummyHandler)

	// First request succeeds
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/resource", nil)
	req1.RemoteAddr = "1.2.3.4:5000"
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec1.Code)
	}
	if rec1.Header().Get("X-RateLimit-Limit") != "1" {
		t.Fatalf("expected X-RateLimit-Limit 1, got %q", rec1.Header().Get("X-RateLimit-Limit"))
	}
	if rec1.Header().Get("X-RateLimit-Remaining") != "0" {
		t.Fatalf("expected X-RateLimit-Remaining 0, got %q", rec1.Header().Get("X-RateLimit-Remaining"))
	}

	// Second request denied with 429
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/resource", nil)
	req2.RemoteAddr = "1.2.3.4:5000"
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", rec2.Code)
	}

	if ct := rec2.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %q", ct)
	}

	if limit := rec2.Header().Get("X-RateLimit-Limit"); limit != "1" {
		t.Fatalf("expected X-RateLimit-Limit '1', got %q", limit)
	}

	if rem := rec2.Header().Get("X-RateLimit-Remaining"); rem != "0" {
		t.Fatalf("expected X-RateLimit-Remaining '0', got %q", rem)
	}

	retryAfter := rec2.Header().Get("Retry-After")
	if retryAfter == "" || retryAfter == "0" {
		t.Fatalf("expected non-empty positive Retry-After header, got %q", retryAfter)
	}

	wantBody := `{"error":"rate limit exceeded"}`
	if rec2.Body.String() != wantBody {
		t.Fatalf("body mismatch: got %q, want %q", rec2.Body.String(), wantBody)
	}

	var jsonMap map[string]string
	if err := json.Unmarshal(rec2.Body.Bytes(), &jsonMap); err != nil {
		t.Fatalf("response body is not valid JSON: %v", err)
	}
	if jsonMap["error"] != "rate limit exceeded" {
		t.Fatalf("expected JSON error 'rate limit exceeded', got %q", jsonMap["error"])
	}
}

func TestRateLimit_SkipPathsBypass(t *testing.T) {
	newLimiter := func() ratelimit.Limiter {
		return ratelimit.NewSlidingWindow(time.Minute, 1)
	}

	mw := RateLimit(newLimiter, IPKey, []string{"/livez", "/readyz"})
	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	handler := mw(dummyHandler)

	// Repeated requests to /livez should never be rate limited
	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/livez", nil)
		req.RemoteAddr = "10.0.0.5:1234"
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d to /livez expected 200, got %d", i+1, rec.Code)
		}
	}

	// Repeated requests to /readyz should never be rate limited
	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		req.RemoteAddr = "10.0.0.5:1234"
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d to /readyz expected 200, got %d", i+1, rec.Code)
		}
	}
}

func TestRateLimit_EvictionWithSmallCap(t *testing.T) {
	newLimiter := func() ratelimit.Limiter {
		return ratelimit.NewSlidingWindow(time.Minute, 10)
	}

	rl := newRateLimiter(newLimiter, IPKey, nil)
	// Set small cap and short idle duration to test eviction
	rl.maxKeys = 2
	rl.idleSweep = 20 * time.Millisecond
	handler := rl.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// 1. Add key 1
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/", nil)
	req1.RemoteAddr = "10.0.0.1:1000"
	handler.ServeHTTP(rec1, req1)

	// 2. Add key 2
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.RemoteAddr = "10.0.0.2:1000"
	handler.ServeHTTP(rec2, req2)

	rl.mu.Lock()
	countBefore := len(rl.entries)
	rl.mu.Unlock()
	if countBefore != 2 {
		t.Fatalf("expected 2 keys, got %d", countBefore)
	}

	// Wait for idle sweep duration to elapse
	time.Sleep(30 * time.Millisecond)

	// 3. Add key 3, which triggers the idle sweep and evicts idle keys
	rec3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodGet, "/", nil)
	req3.RemoteAddr = "10.0.0.3:1000"
	handler.ServeHTTP(rec3, req3)

	rl.mu.Lock()
	countAfter := len(rl.entries)
	_, hasKey3 := rl.entries["10.0.0.3"]
	rl.mu.Unlock()

	if !hasKey3 {
		t.Fatal("expected key 3 to be registered")
	}
	// Idle keys 1 and 2 should have been swept, leaving key 3 (len <= maxKeys)
	if countAfter > 2 {
		t.Fatalf("expected registry size <= 2 after eviction, got %d", countAfter)
	}
}

func TestRouteRateLimit_Dispatch(t *testing.T) {
	limitTo := func(n int) Middleware {
		return RateLimit(func() ratelimit.Limiter { return ratelimit.NewSlidingWindow(time.Minute, n) }, IPKey, nil)
	}

	// Stand-in for the gateway router: maps exact paths to route names.
	match := func(r *http.Request) string {
		switch r.URL.Path {
		case "/login":
			return "login"
		case "/internal":
			return "internal"
		case "/unmatched":
			return ""
		default:
			return "api"
		}
	}

	handler := RouteRateLimit(match, map[string]Middleware{
		"login":    limitTo(1),
		"internal": nil, // explicitly unlimited
	}, limitTo(3))(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	do := func(path string) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.RemoteAddr = "192.0.2.1:1234"
		handler.ServeHTTP(rec, req)
		return rec.Code
	}

	// login: own bucket with limit 1
	if got := do("/login"); got != http.StatusOK {
		t.Fatalf("login #1 = %d, want 200", got)
	}
	if got := do("/login"); got != http.StatusTooManyRequests {
		t.Fatalf("login #2 = %d, want 429", got)
	}

	// api and unmatched share the fallback bucket (limit 3); login usage did not count.
	for i, path := range []string{"/api", "/unmatched", "/api"} {
		if got := do(path); got != http.StatusOK {
			t.Fatalf("fallback request %d (%s) = %d, want 200", i+1, path, got)
		}
	}
	if got := do("/api"); got != http.StatusTooManyRequests {
		t.Fatalf("fallback request 4 = %d, want 429", got)
	}

	// internal: nil entry means no limiting
	for i := range 10 {
		if got := do("/internal"); got != http.StatusOK {
			t.Fatalf("internal request %d = %d, want 200", i+1, got)
		}
	}
}

func TestRouteRateLimit_NilFallbackPassesThrough(t *testing.T) {
	handler := RouteRateLimit(func(*http.Request) string { return "" }, nil, nil)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for i := range 5 {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d = %d, want 200", i+1, rec.Code)
		}
	}
}
