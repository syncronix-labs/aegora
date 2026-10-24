package middleware

import (
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/syncronix-labs/aegora/internal/ratelimit"
)

// KeyFunc extracts a rate-limiting key from an incoming HTTP request.
type KeyFunc func(*http.Request) string

// IPKey extracts the client IP address to use as a rate-limiting key.
// It checks the leftmost IP in the X-Forwarded-For header if present,
// falling back to the host portion of RemoteAddr.
//
// Security Caveat: X-Forwarded-For can be spoofed by untrusted clients if the gateway
// is directly exposed to public traffic without an upstream trusted proxy or load
// balancer stripping untrusted forwarding headers.
func IPKey(req *http.Request) string {
	if req == nil {
		return ""
	}

	if xff := req.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if clientIP := strings.TrimSpace(parts[0]); clientIP != "" {
			return clientIP
		}
	}

	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		return req.RemoteAddr
	}
	return host
}

// APIKey extracts an API key or Bearer token from the incoming request.
// It inspects the X-API-Key header first; if absent, it checks the Authorization
// header for a Bearer token. Returns an empty string if neither is present.
func APIKey(req *http.Request) string {
	if req == nil {
		return ""
	}

	if key := req.Header.Get("X-API-Key"); key != "" {
		return strings.TrimSpace(key)
	}

	auth := req.Header.Get("Authorization")
	if len(auth) > 7 && strings.EqualFold(auth[:7], "bearer ") {
		token := strings.TrimSpace(auth[7:])
		if token != "" {
			return token
		}
	}

	return ""
}

type limitProvider interface {
	Limit() int
}

type remainingProvider interface {
	Remaining() int
}

type retryAfterProvider interface {
	RetryAfter() time.Duration
}

type keyEntry struct {
	limiter  ratelimit.Limiter
	lastSeen time.Time
}

type rateLimiter struct {
	newLimiter func() ratelimit.Limiter
	keyFn      KeyFunc
	skipPaths  map[string]struct{}
	maxKeys    int
	idleSweep  time.Duration
	mu         sync.Mutex
	entries    map[string]*keyEntry
}

func newRateLimiter(newLimiter func() ratelimit.Limiter, keyFn KeyFunc, skipPaths []string) *rateLimiter {
	skipMap := make(map[string]struct{}, len(skipPaths))
	for _, p := range skipPaths {
		skipMap[p] = struct{}{}
	}

	return &rateLimiter{
		newLimiter: newLimiter,
		keyFn:      keyFn,
		skipPaths:  skipMap,
		maxKeys:    10000,
		idleSweep:  5 * time.Minute,
		entries:    make(map[string]*keyEntry),
	}
}

func (rl *rateLimiter) getOrCreate(key string, now time.Time) *keyEntry {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	entry, exists := rl.entries[key]
	if exists {
		entry.lastSeen = now
		return entry
	}

	// Cap exceeded check: sweep idle entries before inserting
	if len(rl.entries) >= rl.maxKeys {
		cutoff := now.Add(-rl.idleSweep)
		for k, v := range rl.entries {
			if v.lastSeen.Before(cutoff) {
				delete(rl.entries, k)
			}
		}

		// If still at or over capacity after idle sweep, evict the oldest key
		if len(rl.entries) >= rl.maxKeys {
			var oldestKey string
			var oldestTime time.Time
			first := true
			for k, v := range rl.entries {
				if first || v.lastSeen.Before(oldestTime) {
					oldestKey = k
					oldestTime = v.lastSeen
					first = false
				}
			}
			if !first {
				delete(rl.entries, oldestKey)
			}
		}
	}

	entry = &keyEntry{
		limiter:  rl.newLimiter(),
		lastSeen: now,
	}
	rl.entries[key] = entry
	return entry
}

func (rl *rateLimiter) serve(w http.ResponseWriter, r *http.Request, next http.Handler) {
	if _, skip := rl.skipPaths[r.URL.Path]; skip {
		next.ServeHTTP(w, r)
		return
	}

	key := rl.keyFn(r)
	now := time.Now()
	entry := rl.getOrCreate(key, now)

	if !entry.limiter.Allow(r.Context()) {
		w.Header().Set("Content-Type", "application/json")
		if lp, ok := entry.limiter.(limitProvider); ok {
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(lp.Limit()))
		}
		w.Header().Set("X-RateLimit-Remaining", "0")

		retryAfterSecs := 60
		if rap, ok := entry.limiter.(retryAfterProvider); ok {
			d := rap.RetryAfter()
			secs := int(math.Ceil(d.Seconds()))
			if secs > 0 {
				retryAfterSecs = secs
			}
		}
		w.Header().Set("Retry-After", strconv.Itoa(retryAfterSecs))

		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"rate limit exceeded"}`))
		return
	}

	if lp, ok := entry.limiter.(limitProvider); ok {
		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(lp.Limit()))
	}
	if rp, ok := entry.limiter.(remainingProvider); ok {
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(rp.Remaining()))
	}

	next.ServeHTTP(w, r)
}

func (rl *rateLimiter) Middleware() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rl.serve(w, r, next)
		})
	}
}

// RateLimit returns a middleware that enforces per-key rate limiting using the
// provided limiter constructor and key extraction function. Requests matching any
// path in skipPaths bypass the rate limiter completely.
func RateLimit(newLimiter func() ratelimit.Limiter, keyFn KeyFunc, skipPaths []string) Middleware {
	rl := newRateLimiter(newLimiter, keyFn, skipPaths)
	return rl.Middleware()
}

// RouteRateLimit returns a middleware that applies a different rate limit middleware
// depending on which route a request matches.
//
// match returns the name of the route the request matches, or "" if none. Requests whose
// route has an entry in byRoute use that entry; a nil entry disables rate limiting for the
// route. All other requests (routes without an entry, or no matching route) use fallback;
// a nil fallback lets them through unlimited.
//
// Every middleware is wrapped around next once, at construction time, so each route keeps
// its own independent per-key buckets for the lifetime of the chain.
func RouteRateLimit(match func(*http.Request) string, byRoute map[string]Middleware, fallback Middleware) Middleware {
	return func(next http.Handler) http.Handler {
		routeHandlers := make(map[string]http.Handler, len(byRoute))
		for name, mw := range byRoute {
			if mw == nil {
				routeHandlers[name] = next
				continue
			}
			routeHandlers[name] = mw(next)
		}

		fallbackHandler := next
		if fallback != nil {
			fallbackHandler = fallback(next)
		}

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if h, ok := routeHandlers[match(r)]; ok {
				h.ServeHTTP(w, r)
				return
			}
			fallbackHandler.ServeHTTP(w, r)
		})
	}
}
