package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/syncronix-labs/aegora/internal/cache"
)

// staticNamer stands in for the router-backed namer the gateway wires in, so a
// test can assert on a known route label.
type staticNamer struct{ route string }

func (n staticNamer) RouteName(*http.Request) string { return n.route }

func cacheCounter(t *testing.T, m *Metrics, route, result string) float64 {
	t.Helper()
	return testutil.ToFloat64(m.CacheRequestsTotal.WithLabelValues(route, result))
}

func TestCacheObserver_CountsAMissThenAHit(t *testing.T) {
	m := New(prometheus.NewRegistry())
	store := cache.NewCache(10, 1024)

	backend := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("payload"))
	})

	handler := cache.CacheMiddleware(store, time.Minute, 1024,
		cache.WithObserver(m, staticNamer{route: "api"}))(backend)

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/resource", nil))

	if got := cacheCounter(t, m, "api", "miss"); got != 1 {
		t.Fatalf("miss = %v after the first request, want 1", got)
	}
	if got := cacheCounter(t, m, "api", "hit"); got != 0 {
		t.Fatalf("hit = %v after the first request, want 0", got)
	}

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/resource", nil))

	if got := cacheCounter(t, m, "api", "hit"); got != 1 {
		t.Fatalf("hit = %v after the second request, want 1", got)
	}
	if got := cacheCounter(t, m, "api", "miss"); got != 1 {
		t.Fatalf("miss = %v after the second request, want it to stay at 1", got)
	}
}

func TestCacheObserver_UsesTheRouteItWasGiven(t *testing.T) {
	m := New(prometheus.NewRegistry())
	store := cache.NewCache(10, 1024)

	backend := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("payload"))
	})

	handler := cache.CacheMiddleware(store, time.Minute, 1024,
		cache.WithObserver(m, staticNamer{route: "catalog"}))(backend)

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/items", nil))

	if got := cacheCounter(t, m, "catalog", "miss"); got != 1 {
		t.Fatalf("miss on the catalog route = %v, want 1", got)
	}
	if got := cacheCounter(t, m, "api", "miss"); got != 0 {
		t.Fatalf("miss on the api route = %v, want 0", got)
	}
}

func TestCacheObserver_IgnoresRequestsTheCacheDoesNotServe(t *testing.T) {
	m := New(prometheus.NewRegistry())
	store := cache.NewCache(10, 1024)

	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cookie" {
			// A user-specific response: the cache must not store it, so there is
			// no lookup outcome to report.
			w.Header().Set("Set-Cookie", "session=abc")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("payload"))
	})

	handler := cache.CacheMiddleware(store, time.Minute, 1024,
		cache.WithObserver(m, staticNamer{route: "api"}))(backend)

	requests := []struct {
		name string
		req  *http.Request
	}{
		{"POST bypasses the cache", httptest.NewRequest(http.MethodPost, "/resource", nil)},
		{"response sets a cookie", httptest.NewRequest(http.MethodGet, "/cookie", nil)},
		{"request carries authorization", func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/private", nil)
			r.Header.Set("Authorization", "Bearer token")
			return r
		}()},
	}

	for _, tc := range requests {
		t.Run(tc.name, func(t *testing.T) {
			handler.ServeHTTP(httptest.NewRecorder(), tc.req)

			if got := cacheCounter(t, m, "api", "hit"); got != 0 {
				t.Fatalf("hit = %v, want 0", got)
			}
			if got := cacheCounter(t, m, "api", "miss"); got != 0 {
				t.Fatalf("miss = %v, want 0", got)
			}
		})
	}
}

func TestCacheObserver_DisabledWithoutAnObserver(t *testing.T) {
	m := New(prometheus.NewRegistry())
	store := cache.NewCache(10, 1024)

	backend := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("payload"))
	})

	// No WithObserver: the cache still works, it just reports nothing.
	handler := cache.CacheMiddleware(store, time.Minute, 1024)(backend)

	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/resource", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("call %d: status = %d, want 200", i, rec.Code)
		}
	}

	if got := cacheCounter(t, m, "api", "hit") + cacheCounter(t, m, "api", "miss"); got != 0 {
		t.Fatalf("counted %v lookups without an observer, want 0", got)
	}
}
