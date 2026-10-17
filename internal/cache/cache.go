// Package cache provides in-memory HTTP response caching middleware
// with TTL expiration, size-bounded buffering, and conditional validation.
package cache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/syncronix-labs/aegora/internal/middleware"
)

// Entry represents a cached HTTP response.
type Entry struct {
	Status    int
	Header    http.Header
	Body      []byte
	StoredAt  time.Time
	ExpiresAt time.Time
	ETag      string
}

// Store defines the storage abstraction for cached HTTP entries.
// The middleware programs against this interface so a multi-tier store
// (e.g. distributed L2 cache or Redis) can replace it later with no middleware changes.
type Store interface {
	Get(key string) (*Entry, bool)
	Put(key string, e *Entry)
}

// Cache implements an in-memory Store with mutex concurrency control,
// TTL-based lazy expiration, and size-based eviction.
type Cache struct {
	mu           sync.RWMutex
	entries      map[string]*Entry
	maxEntries   int
	maxBodyBytes int
}

// NewCache constructs an in-memory Cache with the specified capacity limits.
// It panics if maxEntries <= 0 or maxBodyBytes <= 0 per repo convention.
func NewCache(maxEntries, maxBodyBytes int) *Cache {
	if maxEntries <= 0 {
		panic("cache: maxEntries must be positive")
	}
	if maxBodyBytes <= 0 {
		panic("cache: maxBodyBytes must be positive")
	}

	return &Cache{
		entries:      make(map[string]*Entry),
		maxEntries:   maxEntries,
		maxBodyBytes: maxBodyBytes,
	}
}

// Get retrieves an unexpired entry for key.
// If an entry is present but expired, it is lazily deleted and returns nil, false.
func (c *Cache) Get(key string) (*Entry, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	if !ok {
		c.mu.RUnlock()
		return nil, false
	}

	if time.Now().Before(entry.ExpiresAt) {
		c.mu.RUnlock()
		return entry, true
	}
	c.mu.RUnlock()

	// Lazily delete expired entry on read.
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[key]; ok && !time.Now().Before(entry.ExpiresAt) {
		delete(c.entries, key)
	}
	return nil, false
}

// Put stores an entry as-is under the specified key.
// If the cache is at capacity (maxEntries) and inserting a new key, an arbitrary entry
// is evicted (LRU eviction policy is planned for future upgrades).
func (c *Cache) Put(key string, e *Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.entries[key]; !exists && len(c.entries) >= c.maxEntries {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}

	c.entries[key] = e
}

// Invalidate removes a specific entry by key from the cache.
func (c *Cache) Invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

// InvalidatePrefix scans the cache and removes all entries whose keys begin with prefix.
func (c *Cache) InvalidatePrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.entries {
		if strings.HasPrefix(k, prefix) {
			delete(c.entries, k)
		}
	}
}

// cachingResponseWriter buffers the response up to maxBodyBytes.
// If the response body exceeds maxBodyBytes, it flushes captured data to the underlying
// ResponseWriter and streams future writes uncached.
type cachingResponseWriter struct {
	underlying   http.ResponseWriter
	header       http.Header
	status       int
	wroteHeader  bool
	buf          bytes.Buffer
	maxBodyBytes int
	overflow     bool
	flushed      bool
}

func newCachingResponseWriter(w http.ResponseWriter, maxBodyBytes int) *cachingResponseWriter {
	return &cachingResponseWriter{
		underlying:   w,
		header:       make(http.Header),
		status:       http.StatusOK,
		maxBodyBytes: maxBodyBytes,
	}
}

func (rw *cachingResponseWriter) Header() http.Header {
	return rw.header
}

func (rw *cachingResponseWriter) WriteHeader(code int) {
	if rw.wroteHeader {
		return
	}
	rw.status = code
	rw.wroteHeader = true
}

func (rw *cachingResponseWriter) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.WriteHeader(http.StatusOK)
	}

	if rw.overflow {
		return rw.underlying.Write(b)
	}

	if rw.buf.Len()+len(b) > rw.maxBodyBytes {
		rw.overflow = true
		rw.flushToUnderlying()
		return rw.underlying.Write(b)
	}

	return rw.buf.Write(b)
}

func (rw *cachingResponseWriter) flushToUnderlying() {
	if rw.flushed {
		return
	}
	rw.flushed = true

	for k, vv := range rw.header {
		for _, v := range vv {
			rw.underlying.Header().Add(k, v)
		}
	}
	rw.underlying.WriteHeader(rw.status)
	if rw.buf.Len() > 0 {
		_, _ = rw.underlying.Write(rw.buf.Bytes())
		rw.buf.Reset()
	}
}

// etagMatches performs an HTTP ETag comparison for If-None-Match.
func etagMatches(ifNoneMatch, etag string) bool {
	if ifNoneMatch == "*" {
		return true
	}
	for _, tag := range strings.Split(ifNoneMatch, ",") {
		tag = strings.TrimSpace(tag)
		if tag == etag {
			return true
		}
		// Weak comparison fallback: W/"xyz" matches "xyz"
		trimmedClient := strings.TrimPrefix(tag, "W/")
		trimmedServer := strings.TrimPrefix(etag, "W/")
		if trimmedClient == trimmedServer {
			return true
		}
	}
	return false
}

// Middleware constructs an HTTP middleware that provides response caching.
func Middleware(store Store, ttl time.Duration, maxBodyBytes int) middleware.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Rule: GET and HEAD only; everything else bypasses untouched with no X-Cache header.
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				next.ServeHTTP(w, r)
				return
			}

			// Security Rule: A shared cache must never cache or serve responses for requests
			// containing an Authorization header to avoid mixing user sessions or leaking private data.
			if r.Header.Get("Authorization") != "" {
				next.ServeHTTP(w, r)
				return
			}

			cacheKey := r.Method + " " + r.URL.EscapedPath() + "?" + r.URL.RawQuery

			// Attempt cache hit
			if entry, hit := store.Get(cacheKey); hit {
				age := int64(0)
				if !entry.StoredAt.IsZero() {
					age = max(0, int64(time.Since(entry.StoredAt)/time.Second))
				}
				// Evaluate If-None-Match conditional request
				ifNoneMatch := r.Header.Get("If-None-Match")
				if entry.ETag != "" && ifNoneMatch != "" && etagMatches(ifNoneMatch, entry.ETag) {
					for k, vv := range entry.Header {
						for _, v := range vv {
							w.Header().Add(k, v)
						}
					}
					w.Header().Set("X-Cache", "HIT")
					w.Header().Set("Age", strconv.FormatInt(age, 10))
					w.WriteHeader(http.StatusNotModified)
					return
				}

				// Fresh cache hit: serve cached status, headers, and body
				for k, vv := range entry.Header {
					for _, v := range vv {
						w.Header().Add(k, v)
					}
				}
				w.Header().Set("X-Cache", "HIT")
				w.Header().Set("Age", strconv.FormatInt(age, 10))
				w.WriteHeader(entry.Status)
				if r.Method != http.MethodHead {
					_, _ = w.Write(entry.Body)
				}
				return
			}

			// Cache miss: execute downstream handler with response buffering
			rw := newCachingResponseWriter(w, maxBodyBytes)
			rw.Header().Set("X-Cache", "MISS")

			next.ServeHTTP(rw, r)

			// If the response already streamed due to buffer overflow, do not store.
			if rw.overflow {
				return
			}

			bodyBytes := rw.buf.Bytes()

			// Cache Eligibility Rules (all must hold):
			// 1. Status == 200: Only successful OK responses are safe to cache.
			// 2. Request Cache-Control: Request must not carry "no-store".
			// 3. Response Cache-Control: Response must not carry "no-store".
			// 4. No Set-Cookie: Responses setting cookies are user-specific and must not be shared.
			// 5. Body within maxBodyBytes: Prevent memory bloat from oversized payloads.
			reqNoStore := strings.Contains(strings.ToLower(r.Header.Get("Cache-Control")), "no-store")
			respNoStore := strings.Contains(strings.ToLower(rw.header.Get("Cache-Control")), "no-store")
			hasSetCookie := rw.header.Get("Set-Cookie") != ""
			isEligible := rw.status == http.StatusOK && !reqNoStore && !respNoStore && !hasSetCookie && len(bodyBytes) <= maxBodyBytes

			if isEligible {
				// Determine ETag: keep backend ETag if provided, else compute weak ETag
				// W/"<hex>" from first 16 hex chars (8 bytes) of SHA-256(body).
				etag := rw.header.Get("ETag")
				if etag == "" {
					sum := sha256.Sum256(bodyBytes)
					etag = fmt.Sprintf("W/\"%s\"", hex.EncodeToString(sum[:8]))
					rw.header.Set("ETag", etag)
				}

				now := time.Now()
				entry := &Entry{
					Status:    rw.status,
					Header:    rw.header.Clone(),
					Body:      bytes.Clone(bodyBytes),
					StoredAt:  now,
					ExpiresAt: now.Add(ttl),
					ETag:      etag,
				}
				store.Put(cacheKey, entry)
			}

			// Flush captured response to the client
			rw.flushToUnderlying()
		})
	}
}

// CacheMiddleware is an explicit alias for Middleware to support conventional naming.
//
//nolint:revive // name stutter is accepted for API clarity
func CacheMiddleware(store Store, ttl time.Duration, maxBodyBytes int) middleware.Middleware {
	return Middleware(store, ttl, maxBodyBytes)
}
