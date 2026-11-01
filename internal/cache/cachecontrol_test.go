package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// recordingStore keeps whatever the middleware stored so a test can inspect the
// lifetime it was given, which the Cache itself does not expose.
type recordingStore struct {
	entries map[string]*Entry
}

func newRecordingStore() *recordingStore {
	return &recordingStore{entries: make(map[string]*Entry)}
}

func (s *recordingStore) Get(key string) (*Entry, bool) {
	entry, ok := s.entries[key]
	return entry, ok
}

func (s *recordingStore) Put(key string, e *Entry) {
	s.entries[key] = e
}

func cacheControlHeader(values ...string) http.Header {
	h := make(http.Header)
	for _, value := range values {
		h.Add("Cache-Control", value)
	}
	return h
}

func TestEffectiveTTL(t *testing.T) {
	const configured = time.Minute

	cases := []struct {
		name   string
		header http.Header
		want   time.Duration
	}{
		{"no header at all", nil, configured},
		{"max-age shorter than the configured TTL", cacheControlHeader("max-age=5"), 5 * time.Second},
		{"max-age longer than the configured TTL", cacheControlHeader("max-age=600"), configured},
		{"max-age equal to the configured TTL", cacheControlHeader("max-age=60"), configured},
		{"max-age zero", cacheControlHeader("max-age=0"), 0},
		{"other directives around it", cacheControlHeader("public, max-age=5, must-revalidate"), 5 * time.Second},
		{"quoted value", cacheControlHeader(`max-age="5"`), 5 * time.Second},
		{"directive name in a different case", cacheControlHeader("Max-Age=5"), 5 * time.Second},
		{"value that is not a number", cacheControlHeader("max-age=soon"), configured},
		{"negative value", cacheControlHeader("max-age=-1"), configured},
		{"no max-age directive", cacheControlHeader("no-store, private"), configured},
		{"split across two header lines", cacheControlHeader("public", "max-age=5"), 5 * time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveTTL(configured, tc.header); got != tc.want {
				t.Fatalf("effectiveTTL() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCacheMiddleware_CapsLifetimeAtBackendMaxAge(t *testing.T) {
	store := newRecordingStore()

	backend := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=5")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data"))
	})

	handler := CacheMiddleware(store, time.Minute, 1024)(backend)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/five-seconds", nil))

	if len(store.entries) != 1 {
		t.Fatalf("stored %d entries, want 1", len(store.entries))
	}
	for key, entry := range store.entries {
		if got := entry.ExpiresAt.Sub(entry.StoredAt); got != 5*time.Second {
			t.Fatalf("%s: lifetime = %v, want 5s", key, got)
		}
	}
}

func TestCacheMiddleware_ALongerMaxAgeDoesNotExtendTheConfiguredTTL(t *testing.T) {
	store := newRecordingStore()

	backend := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "max-age=86400")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data"))
	})

	handler := CacheMiddleware(store, 30*time.Second, 1024)(backend)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/forever", nil))

	if len(store.entries) != 1 {
		t.Fatalf("stored %d entries, want 1", len(store.entries))
	}
	for key, entry := range store.entries {
		if got := entry.ExpiresAt.Sub(entry.StoredAt); got != 30*time.Second {
			t.Fatalf("%s: lifetime = %v, want the configured 30s", key, got)
		}
	}
}

func TestCacheMiddleware_MaxAgeZeroIsServedButNotStored(t *testing.T) {
	store := newRecordingStore()
	var callCount int

	backend := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.Header().Set("Cache-Control", "max-age=0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data"))
	})

	handler := CacheMiddleware(store, time.Minute, 1024)(backend)

	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/never", nil))
		if rec.Code != http.StatusOK || rec.Body.String() != "data" {
			t.Fatalf("call %d: got %d %q, want 200 %q", i, rec.Code, rec.Body.String(), "data")
		}
		if got := rec.Header().Get("X-Cache"); got != "MISS" {
			t.Fatalf("call %d: X-Cache = %q, want MISS", i, got)
		}
	}

	if len(store.entries) != 0 {
		t.Fatalf("stored %d entries, want none", len(store.entries))
	}
	if callCount != 2 {
		t.Fatalf("backend calls = %d, want 2", callCount)
	}
}

func TestCacheMiddleware_ExpiresWhenTheBackendMaxAgeElapses(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for a real second to elapse")
	}

	store := NewCache(10, 1024)
	var callCount int

	backend := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.Header().Set("Cache-Control", "max-age=1")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data"))
	})

	// The configured TTL is far longer than the second the origin allows, so a
	// hit after that second can only come from ignoring max-age.
	handler := CacheMiddleware(store, time.Hour, 1024)(backend)

	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/short", nil))
	if got := rec1.Header().Get("X-Cache"); got != "MISS" {
		t.Fatalf("first call: X-Cache = %q, want MISS", got)
	}

	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/short", nil))
	if got := rec2.Header().Get("X-Cache"); got != "HIT" {
		t.Fatalf("second call: X-Cache = %q, want HIT", got)
	}

	time.Sleep(1100 * time.Millisecond)

	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, httptest.NewRequest(http.MethodGet, "/short", nil))
	if got := rec3.Header().Get("X-Cache"); got != "MISS" {
		t.Fatalf("call after max-age elapsed: X-Cache = %q, want MISS", got)
	}
	if callCount != 2 {
		t.Fatalf("backend calls = %d, want 2", callCount)
	}
}

func TestTieredCache_CapsBothTiersAtTheEntryLifetime(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	l1 := NewCache(10, 1024)
	tc := NewTieredCache(l1, rdb, time.Minute, 5*time.Minute)

	const key = "GET /api/short-lived"
	tc.Put(key, newSampleEntry(`{"ok":true}`, time.Now().Add(3*time.Second)))

	stored, hit := l1.Get(key)
	if !hit {
		t.Fatal("expected the entry in L1")
	}
	if remaining := time.Until(stored.ExpiresAt); remaining <= 0 || remaining > 3*time.Second {
		t.Fatalf("L1 remaining lifetime = %v, want at most 3s", remaining)
	}

	ttl, err := rdb.TTL(context.Background(), defaultTieredKeyPrefix+key).Result()
	if err != nil {
		t.Fatalf("redis TTL: %v", err)
	}
	if ttl <= 0 || ttl > 3*time.Second {
		t.Fatalf("L2 TTL = %v, want at most 3s", ttl)
	}
}

func TestTieredCache_DoesNotStoreAnExpiredEntry(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	l1 := NewCache(10, 1024)
	tc := NewTieredCache(l1, rdb, time.Minute, 5*time.Minute)

	const key = "GET /api/already-stale"
	tc.Put(key, newSampleEntry(`{"ok":true}`, time.Now().Add(-time.Second)))

	if _, hit := l1.Get(key); hit {
		t.Fatal("L1 stored an entry that had already expired")
	}

	exists, err := rdb.Exists(context.Background(), defaultTieredKeyPrefix+key).Result()
	if err != nil {
		t.Fatalf("redis exists: %v", err)
	}
	if exists != 0 {
		t.Fatal("L2 stored an entry that had already expired")
	}
}
