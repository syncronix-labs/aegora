package cache

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCacheHitAge(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		for _, conditional := range []bool{false, true} {
			t.Run(method+"/conditional="+map[bool]string{false: "false", true: "true"}[conditional], func(t *testing.T) {
				store := NewCache(10, 1024)
				store.Put(method+" /?", &Entry{
					Status: http.StatusOK, Header: http.Header{"Age": {"99"}}, Body: []byte("cached"),
					StoredAt: time.Now().Add(-5 * time.Second), ExpiresAt: time.Now().Add(time.Minute), ETag: `"test"`,
				})
				handler := Middleware(store, time.Minute, 1024)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					t.Fatal("cache hit reached the upstream handler")
				}))
				request := httptest.NewRequest(method, "/", nil)
				if conditional {
					request.Header.Set("If-None-Match", `"test"`)
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Header().Get("Age") != "5" || len(response.Header().Values("Age")) != 1 {
					t.Fatalf("unexpected Age header: %v", response.Header().Values("Age"))
				}
				if conditional && response.Code != http.StatusNotModified {
					t.Fatalf("conditional status = %d", response.Code)
				}
			})
		}
	}
}

func TestCacheMissStoresTimestamp(t *testing.T) {
	store := NewCache(10, 1024)
	handler := Middleware(store, time.Minute, 1024)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("fresh"))
	}))
	start := time.Now()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	entry, ok := store.Get("GET /?")
	if !ok || entry.StoredAt.Before(start) || entry.StoredAt.After(time.Now()) {
		t.Fatalf("missing or invalid stored timestamp: %+v", entry)
	}
	if response.Header().Get("Age") != "" {
		t.Fatal("cache miss should not add an Age header")
	}
	for _, storedAt := range []time.Time{{}, time.Now().Add(time.Minute)} {
		entry.StoredAt = storedAt
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
		if response.Header().Get("Age") != "0" {
			t.Fatalf("unexpected age for timestamp %v: %s", storedAt, response.Header().Get("Age"))
		}
	}
}
