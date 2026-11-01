// Package cache provides in-memory HTTP response caching middleware
// with TTL expiration, size-bounded buffering, and conditional validation.
package cache

import (
	"bytes"
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/syncronix-labs/aegora/internal/middleware"
	"golang.org/x/sync/singleflight"
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

// cacheItem is the value stored in the recency list. It keeps the key alongside the
// entry so evicting a list element can also remove the matching index entry.
type cacheItem struct {
	key   string
	entry *Entry
}

// Cache implements an in-memory Store with mutex concurrency control,
// TTL-based lazy expiration, and LRU (least recently used) eviction at capacity.
//
// Recency is tracked with an intrusive doubly linked list: the front element is the
// least recently used and therefore the next eviction candidate, the back element is
// the most recently used. Both reads (Get hits) and writes (Put) move an entry to the
// back, so a key that keeps being requested survives while cold keys age out. Evicting
// a hot key is what the previous arbitrary eviction did under capacity pressure, which
// collapsed the hit ratio.
type Cache struct {
	mu           sync.RWMutex
	entries      map[string]*list.Element // key -> element whose Value is a *cacheItem
	order        *list.List               // front = least recently used, back = most recently used
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
		entries:      make(map[string]*list.Element),
		order:        list.New(),
		maxEntries:   maxEntries,
		maxBodyBytes: maxBodyBytes,
	}
}

// removeElement unlinks elem from both the recency list and the key index.
// Callers must hold c.mu for writing.
func (c *Cache) removeElement(elem *list.Element) {
	c.order.Remove(elem)
	delete(c.entries, elem.Value.(*cacheItem).key)
}

// evict drops the least recently used entry. Callers must hold c.mu for writing.
func (c *Cache) evict() {
	if front := c.order.Front(); front != nil {
		c.removeElement(front)
	}
}

// promote marks key as the most recently used entry and returns it.
// It re-checks freshness under the write lock, since the entry may have expired or been
// evicted while the caller was upgrading from its read-lock fast path.
func (c *Cache) promote(key string) (*Entry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	item := elem.Value.(*cacheItem)
	if !time.Now().Before(item.entry.ExpiresAt) {
		c.removeElement(elem)
		return nil, false
	}

	c.order.MoveToBack(elem)
	return item.entry, true
}

// Get retrieves an unexpired entry for key.
// A successful read counts as a use, so the entry is promoted to the most recently
// used position; entries that are never read are the ones evicted at capacity.
// If an entry is present but expired, it is lazily deleted and returns nil, false.
func (c *Cache) Get(key string) (*Entry, bool) {
	c.mu.RLock()
	elem, ok := c.entries[key]
	if !ok {
		c.mu.RUnlock()
		return nil, false
	}

	item := elem.Value.(*cacheItem)
	if time.Now().Before(item.entry.ExpiresAt) {
		// Fast path: the entry is already the most recently used, so no recency
		// update is needed and the read lock can be released without upgrading.
		// The entry is loaded before the unlock: a concurrent Put may replace the
		// field, so reading it after releasing the lock would be a data race.
		if c.order.Back() == elem {
			entry := item.entry
			c.mu.RUnlock()
			return entry, true
		}
		c.mu.RUnlock()
		return c.promote(key)
	}
	c.mu.RUnlock()

	// Lazily delete expired entry on read.
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.entries[key]; ok {
		if item := elem.Value.(*cacheItem); !time.Now().Before(item.entry.ExpiresAt) {
			c.removeElement(elem)
		}
	}
	return nil, false
}

// Put stores an entry as-is under the specified key.
// A write counts as a use, so the entry becomes the most recently used.
// If the cache is at capacity (maxEntries) and inserting a new key, the least recently
// used entry is evicted.
func (c *Cache) Put(key string, e *Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.entries[key]; ok {
		elem.Value.(*cacheItem).entry = e
		c.order.MoveToBack(elem)
		return
	}

	// Make room before inserting so the new key is never the eviction victim.
	if c.order.Len() >= c.maxEntries {
		c.evict()
	}

	c.entries[key] = c.order.PushBack(&cacheItem{key: key, entry: e})
}

// Invalidate removes a specific entry by key from the cache.
func (c *Cache) Invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.entries[key]; ok {
		c.removeElement(elem)
	}
}

// InvalidatePrefix scans the cache and removes all entries whose keys begin with prefix.
func (c *Cache) InvalidatePrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for elem := c.order.Front(); elem != nil; {
		next := elem.Next()
		if strings.HasPrefix(elem.Value.(*cacheItem).key, prefix) {
			c.removeElement(elem)
		}
		elem = next
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

// Observer records the outcome of the cache lookups the middleware makes. The
// implementation lives outside this package so the cache stays independent of
// the metrics library, which cannot be imported here: internal/metrics reaches
// internal/proxy, and internal/proxy already imports this package.
type Observer interface {
	ObserveCache(route, result string)
}

// RouteNamer resolves the configured route name for a request, so a cache
// observation carries the same label as the request metrics.
type RouteNamer interface {
	RouteName(r *http.Request) string
}

// options holds the optional wiring of Middleware.
type options struct {
	observer   Observer
	routeNamer RouteNamer
}

// Option configures optional behaviour of Middleware.
type Option func(*options)

// WithObserver reports every cache lookup the middleware makes. routeNamer
// supplies the route label, and both must be non-nil for anything to be
// reported. A lookup is reported as "hit" when it was served from the store and
// as "miss" when it went to the backend and produced a cacheable response;
// requests and responses the cache is not allowed to touch are not reported at
// all, because they never became a cache lookup in the first place.
func WithObserver(observer Observer, routeNamer RouteNamer) Option {
	return func(o *options) {
		o.observer = observer
		o.routeNamer = routeNamer
	}
}

// Middleware constructs an HTTP middleware that provides response caching.
//
// Request Coalescing (cache stampede protection):
// Concurrent misses for the same cache key are collapsed with singleflight. The first
// request (the leader) runs the downstream handler; requests for the same key arriving
// while it is in flight (followers) wait and are served the leader's response from the
// resulting cache entry, so N simultaneous misses produce one backend call.
//
// Only cache-eligible responses are shared. If the leader's response is not stored
// (non-200 status, Set-Cookie, Cache-Control no-store, body over maxBodyBytes) or the
// leader panics, each follower calls the downstream handler itself and observes its own
// outcome. Failures are therefore never fanned out to other callers. Requests carrying
// Cache-Control no-store are never cached, so they skip coalescing entirely.
func Middleware(store Store, ttl time.Duration, maxBodyBytes int, opts ...Option) middleware.Middleware {
	var cfg options
	for _, opt := range opts {
		opt(&cfg)
	}

	observe := func(r *http.Request, result string) {
		if cfg.observer == nil || cfg.routeNamer == nil {
			return
		}
		route := cfg.routeNamer.RouteName(r)
		if route == "" {
			route = "unmatched"
		}
		cfg.observer.ObserveCache(route, result)
	}

	return func(next http.Handler) http.Handler {
		var group singleflight.Group

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

			cacheKey := requestCacheKey(r)

			// Attempt cache hit
			if entry, hit := store.Get(cacheKey); hit {
				observe(r, "hit")
				serveEntry(w, r, entry, "HIT")
				return
			}

			// A no-store request is never cached, so there is nothing to share with others.
			if strings.Contains(strings.ToLower(r.Header.Get("Cache-Control")), "no-store") {
				fetchAndStore(w, r, next, store, cacheKey, ttl, maxBodyBytes)
				return
			}

			// Cache miss: coalesce concurrent misses for the same key. singleflight runs fn
			// synchronously in the leader's goroutine, so leader and leaderPanic are not shared.
			leader := false
			var leaderPanic any
			v, _, _ := group.Do(cacheKey, func() (result any, err error) {
				leader = true
				// Recover here so a leader panic (e.g. http.ErrAbortHandler from a disconnected
				// client) is re-raised only for the leader instead of in every follower.
				defer func() {
					if p := recover(); p != nil {
						leaderPanic = p
						result = (*Entry)(nil)
					}
				}()
				entry := fetchAndStore(w, r, next, store, cacheKey, ttl, maxBodyBytes)
				if entry != nil {
					observe(r, "miss")
				}
				return entry, nil
			})

			if leader {
				if leaderPanic != nil {
					panic(leaderPanic)
				}
				return
			}

			// Follower: share the leader's cacheable response, or fetch independently.
			if entry, _ := v.(*Entry); entry != nil {
				observe(r, "miss")
				serveEntry(w, r, entry, "MISS")
				return
			}
			if entry := fetchAndStore(w, r, next, store, cacheKey, ttl, maxBodyBytes); entry != nil {
				observe(r, "miss")
			}
		})
	}
}

// serveEntry writes a cached entry to w, answering 304 Not Modified when the request's
// If-None-Match matches the entry's ETag. xCache is the X-Cache header value to report.
func serveEntry(w http.ResponseWriter, r *http.Request, entry *Entry, xCache string) {
	age := int64(0)
	if !entry.StoredAt.IsZero() {
		age = max(0, int64(time.Since(entry.StoredAt)/time.Second))
	}

	for k, vv := range entry.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("X-Cache", xCache)
	w.Header().Set("Age", strconv.FormatInt(age, 10))

	// Evaluate If-None-Match conditional request
	ifNoneMatch := r.Header.Get("If-None-Match")
	if entry.ETag != "" && ifNoneMatch != "" && etagMatches(ifNoneMatch, entry.ETag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// Fresh entry: serve cached status, headers, and body
	w.WriteHeader(entry.Status)
	if r.Method != http.MethodHead {
		_, _ = w.Write(entry.Body)
	}
}

// fetchAndStore runs the downstream handler with response buffering, stores the response
// if it is cache-eligible, and writes it to w. It returns the stored entry, or nil if the
// response was not stored.
func fetchAndStore(
	w http.ResponseWriter,
	r *http.Request,
	next http.Handler,
	store Store,
	cacheKey string,
	ttl time.Duration,
	maxBodyBytes int,
) *Entry {
	rw := newCachingResponseWriter(w, maxBodyBytes)
	rw.Header().Set("X-Cache", "MISS")

	next.ServeHTTP(rw, r)

	// If the response already streamed due to buffer overflow, do not store.
	if rw.overflow {
		return nil
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

	var entry *Entry
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
		entry = &Entry{
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

	return entry
}

// requestCacheKey returns a stable cache key for a request. Query values are
// encoded in key order so equivalent query strings share a cache entry.
func requestCacheKey(r *http.Request) string {
	return r.Method + " " + r.URL.EscapedPath() + "?" + r.URL.Query().Encode()
}

// CacheMiddleware is an explicit alias for Middleware to support conventional naming.
//
//nolint:revive // name stutter is accepted for API clarity
func CacheMiddleware(store Store, ttl time.Duration, maxBodyBytes int, opts ...Option) middleware.Middleware {
	return Middleware(store, ttl, maxBodyBytes, opts...)
}
