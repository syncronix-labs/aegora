# Architecture and Design Guide

Aegora is a Layer 7 reliability gateway built to protect downstream services from cascading failures, network storms, and uneven load distribution. This document details the request processing pipeline, foundational architectural choices, lessons learned during development, and the concurrency model.

---

## Request Lifecycle

Every client request traverses an ordered sequence of middleware before reaching the dynamic reverse proxy. The order of layers is strictly intentional:

### 1. Recovery
The outermost layer intercepts unexpected panics raised by any downstream handler or third-party dependency. It writes a structured JSON 500 Internal Server Error response, captures full stack traces in debug logs, and prevents the gateway process from crashing. It sits at the top of the chain so that crashes in any subsequent component are safely recovered without leaving connections hanging.

### 2. RequestID
Extracts an existing `X-Request-Id` header from the incoming request or generates a cryptographically random hexadecimal UUID. The identifier is injected into the Go request context and stamped onto the outgoing response headers. It sits immediately below recovery so all downstream telemetry, logs, metrics, and backend calls share a common correlation ID.

### 3. Tracing
Extracts incoming W3C tracecontext headers (`traceparent`, `tracestate`) and starts a new OpenTelemetry server span representing gateway execution. The active span and tracer context are attached to the request context. This layer sits before metrics and logging to ensure that trace and span identifiers are available to all subsequent telemetry collectors.

### 4. Metrics
Instruments the RED method (Rate, Errors, Duration) and in-flight request gauges using Prometheus collectors. It wraps the response writer to capture status codes and measure total processing duration. Positioned above shedding and rate limiting, it records every request attempt, including early 503 load-shed drops and 429 rate-limit rejections.

### 5. AccessLog
Emits a structured JSON log entry via `log/slog` once request processing concludes. It captures the client IP, HTTP method, URL path, matched route name, final status code, request duration, and assigned request ID. It sits after telemetry wrappers so it can inspect the final response state delivered to the client.

### 6. Adaptive Load Shedding
Applies an AIMD (Additive Increase / Multiplicative Decrease) control loop that tracks P99 latency against configured targets. When latency breaches thresholds, it immediately drops lower-priority requests with a 503 Service Unavailable response. Positioned ahead of rate limiting and admission queues, it sheds non-critical load before consuming per-key rate-limit allowances or finite queue buffers.

### 7. Rate Limiting
Enforces client-level request limits using sliding-window timestamp logs or token bucket refill rates, partitioned by remote IP address or API key. Requests exceeding quotas are rejected with 429 Too Many Requests and a `Retry-After` header. It sits before response caching and queueing to enforce quotas uniformly, regardless of whether a response will be served from cache or upstream.

### 8. Response Cache
Evaluates cache eligibility for GET and HEAD requests. On a cache hit, it inspects `If-None-Match` to serve 304 Not Modified when ETags match, or directly returns cached headers and body with `X-Cache: HIT`. Cache hits bypass the admission queue and upstream reverse proxy entirely. On a cache miss, it wraps the writer to capture eligible 200 responses for storage in L1 memory and L2 Redis. Concurrent misses for the same key are coalesced so that only one of them reaches the upstream.

### 9. Admission Queue
Enforces strict concurrency limits (`max_inflight`) and queue depth bounds (`max_queue_depth`) fronting upstream connections. It classifies requests into priority buckets (high, normal, low) and applies CoDel (Controlled Delay) drop logic to discard requests whose queue dwell times exceed latency targets. This protects upstream connection pools from connection stampedes and bufferbloat.

### 10. Reverse Proxy Gateway
Matches requests against configured routes via prefix or regular expression rules. It selects a healthy upstream target using round-robin or least-connections policies, updates passive target health, and forwards traffic via `httputil.ReverseProxy`. Network transports are wrapped with per-route circuit breaking and jittered exponential retry policies.

---

## Key Design Decisions

### Circuit Breaker Outside the Retry Loop
The circuit breaker sits outside the retry transport, wrapping the entire retry execution. This guarantees that the circuit breaker records the final outcome of the retry cycle rather than individual transient network glitches. If a request fails on attempt 1 but succeeds on attempt 2, the breaker records a success. The breaker only registers a failure and trips open if all configured retry attempts fail, preventing premature circuit trips on recoverable hiccups.

### Backoff Signals on Rejection
Every 503 the gateway generates itself carries a `Retry-After` header, but the value is derived from what actually knows when the rejection will end. Load shedding sends a fixed one second because its control loop re-evaluates continuously. The circuit breaker sends the remainder of its current cooldown, rounded up to whole seconds, because that is the exact point at which a half-open probe will be admitted. A client that retries earlier than that is refused again, so the header is the difference between a backoff and a thundering herd. Errors that carry no such knowledge, such as a 502 for an unreachable upstream, deliberately send no `Retry-After` at all rather than inventing one.

### Cache After Rate Limiting
The caching middleware is deliberately positioned after the rate limiting layer. This ensures that client rate limits and quotas apply uniformly across all incoming traffic, preventing malicious or runaway clients from overwhelming gateway memory bandwidth with rapid cache lookups. Once allowed, cache hits return immediately in under 300 nanoseconds, bypassing the admission queue, connection pools, and backend servers.

### Shedding Before Downstream Queues
Adaptive load shedding executes before the admission queue, rate limit token expenditure, and proxy layers. Dropping traffic at the earliest point during a latency crisis is the cheapest operation the gateway can perform. It avoids wasting rate limit budget on requests that would eventually time out, eliminates queue lock contention, and preserves backend capacity for critical endpoints.

### Atomic Handler Swap for Hot Reload
Configuration hot reloading via `POST /admin/reload` creates an entirely new HTTP handler and middleware pipeline in memory. Once validated and constructed, the new pipeline is swapped into active service via `sync/atomic`. In-flight requests continue executing against the old handler and drain naturally, while all incoming requests immediately route through the new handler without dropped connections or mutex bottlenecks on the hot path.

### Multi-Tier Cache with Write-Through and Backfill
The caching subsystem implements a Store interface with a two-tier strategy. Reads first query local L1 memory. On an L1 miss, the store checks L2 Redis. If present in Redis, the entry is returned and backfilled into local L1 memory for future requests. Writes execute as a write-through operation, updating both L1 memory and L2 Redis simultaneously. If Redis is temporarily unreachable, the gateway degrades gracefully to standalone L1 operation.

### LRU Eviction at Capacity
The L1 store is bounded by `max_entries`. When it is full, the entry evicted is the least recently used one, not an arbitrary one. Recency lives in a doubly linked list ordered from least to most recently used, and the key index maps each key to its list node, so promotion and eviction are both O(1) pointer updates rather than a scan.

Both reads and writes count as a use. A `Get` hit moves its entry to the most recently used end, and a `Put` on an existing key does the same, so a key that keeps being requested survives capacity pressure while keys that were stored once and never read age out first. This matters for a gateway specifically: request traffic is heavy-tailed, and arbitrary eviction reliably discards the small hot set that produces most cache hits, collapsing the hit ratio exactly when the gateway is under enough load to fill the cache. Least-frequently-used was rejected because its per-entry counters cost more bookkeeping than the gain recency already delivers on this traffic shape.

The hot path stays cheap. A hit on the most recently used entry returns under the read lock without touching the list, since it is already in the promoted position. Every other hit briefly upgrades to the write lock to relink one node. The upgrade re-checks the entry, so an entry that expired or was evicted in between is reported as a miss instead of being resurrected by the promotion.

### Request Coalescing on Cache Miss
When many requests for the same uncached key arrive at once (a cache stampede, or "thundering herd"), every one of them would otherwise miss and hit the upstream, multiplying load on a backend that may already be recovering. The cache middleware collapses concurrent misses per key with `golang.org/x/sync/singleflight`: the first request (the leader) calls the upstream, and requests arriving while it is in flight (followers) wait and are served the leader's response from the resulting cache entry, including `If-None-Match` evaluation.

Only cache-eligible responses are shared. If the leader's response is not stored (non-200, `Set-Cookie`, `no-store`, or larger than `max_body_bytes`), or the leader panics, each follower calls the upstream itself and observes its own outcome, so failures are never fanned out to other clients. The trade-off is that coalescing protects the upstream while it returns successful responses; during an error burst, followers retry independently. Requests carrying `Cache-Control: no-store` skip coalescing because they are never cached.

### W3C Trace Context Propagation
Aegora wraps its shared HTTP base transport with `otelhttp.NewTransport`. As requests pass through Go's `httputil.ReverseProxy`, the active trace context stored within the request's `context.Context` is automatically injected into outgoing HTTP request headers as `traceparent` and `tracestate`. This enables end-to-end distributed tracing across microservices with zero manual header injection in the proxy loop.

---

## Lessons Learned: Response Writer Buffering

During the implementation of Phase 5 (in-memory response caching), an elusive bug caused repeated cache lookups to return `X-Cache: MISS` even on identical, cacheable 200 OK responses.

### The Problem
The response-buffering wrapper `cachingResponseWriter` initially implemented the `http.Flusher` interface to support streaming responses. However, Go's standard library `httputil.ReverseProxy` checks whether the downstream `http.ResponseWriter` implements `http.Flusher`. When it does, `ReverseProxy` invokes `Flush()` as soon as the first chunk of the response body arrives from the upstream backend.

In our caching writer, `Flush()` signaled that the response had exceeded buffering capacity or required immediate delivery, which set an internal `overflow` flag and flushed captured bytes directly to the client. Consequently, on every proxied request, the first response chunk triggered an immediate flush, flagged the response as overflowing, and aborted caching before the response completed.

### The Fix
Middleware response wrappers intended to buffer responses for caching or validation must implement strictly `http.ResponseWriter` and avoid advertising `http.Flusher`. Stripping the `Flush()` method ensures that `ReverseProxy` buffers standard responses normally through `Write()`, allowing the cache middleware to capture the full body up to `max_body_bytes` and store eligible responses reliably.

---

## Concurrency Model

Aegora is designed to handle high concurrency with minimal lock contention on critical paths.

### Mutex-Guarded State
- **Rate Limiters (`internal/ratelimit`)**: `SlidingWindow` uses a `sync.Mutex` to guard its slice of timestamps during prune and append operations. The in-memory limiter registry uses a `sync.RWMutex` to guard the map of active per-key limiters.
- **Cache Store (`internal/cache`)**: `Cache` uses a `sync.RWMutex` protecting both its key index and its recency list. A hit on the most recently used entry releases the read lock (`RLock`) and returns immediately, so the hottest key never contends for the write lock. Any other hit upgrades to an exclusive lock (`Lock`) to promote the entry to the back of the list, because recency tracking is a mutation. Write-through puts, invalidations, and capacity evictions also take the write lock. The upgrade re-validates freshness, so an entry that expired or was evicted between the two phases is reported as a miss rather than resurrected.
- **Circuit Breakers (`internal/reliability`)**: Each circuit breaker instance protects its state transitions (closed, open, half-open), probe counts, and failure counters with a `sync.Mutex`.
- **Admission Queue (`internal/queue`)**: The priority queue uses a `sync.Mutex` to synchronize heap operations across priority buckets, inflight request counters, and CoDel tracking timestamps.
- **Target Pools (`internal/proxy`)**: Target selection pools use a `sync.Mutex` to update target health state and increment round-robin or least-connection counters.
- **Configuration Reloader (`cmd/aegora`)**: The `gatewayReloader` uses a `sync.Mutex` to serialize concurrent calls to `Reload()`, ensuring only one reload executes at a time.

### Lock-Free Execution
- **Active Handler (`cmd/aegora`)**: The main data plane reads the active gateway handler pointer via `atomic.Pointer` / `atomic.Value`. Requests execute completely lock-free without contending on a global reloader lock.
- **Configuration Snapshot (`cmd/aegora`)**: The active `config.Config` reference is stored in an `atomic.Pointer`, allowing lock-free reads for admin inspection and logging.
- **Dynamic Log Level (`cmd/aegora`)**: Log level adjustments use an atomic `slog.LevelVar`, allowing runtime changes without locking or process restarts.
