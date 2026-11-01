# Configuration Reference

Aegora is configured through a YAML file, specified via the `--config` command-line flag (default: `configs/aegora.yaml`). Select settings can also be overridden using environment variables prefixed with `AEGORA_`.

This document details every section, field, type, default value, validation rule, and runtime reload behavior.

---

## Validating a Configuration

Use `--check-config` to validate a configuration file without starting the gateway. It runs the same loading path as startup (strict YAML decoding that rejects unknown fields, `AEGORA_*` environment overrides, and all validation rules below), then exits without binding any listeners.

```bash
aegora --check-config --config /etc/aegora/aegora.yaml
```

- **Valid**: prints `config OK` to stdout and exits `0`.
- **Invalid**: prints the error, naming the offending field (for example, `config: routes[0].lb: must be one of round_robin, least_connections; got "random"`), to stderr and exits `1`.

This makes it suitable as a CI or pre-deploy gate, for example before `docker compose up` or before triggering `POST /admin/reload`.

---

## Reload Behavior Summary

Aegora supports runtime configuration reloads via `POST /admin/reload` on the admin server (port `9091`). When triggered, Aegora parses and validates the file from disk, builds a replacement middleware chain, and atomically swaps the active handler.

- **Reloadable (Hot Reload)**: Changes take effect immediately without process restart. In-flight requests complete on the previous chain while new requests route to the updated pipeline.
- **Restart Required**: Network listener bindings (IP/port) and one-time process initializations cannot be changed dynamically. Changes to these fields are reported as skipped during reload.

---

## 1. Server (`server`)

Defines data-plane network binding and connection timeouts for the primary gateway listener.

| Field | Type | Default | Reloadable | Description |
|---|---|---|---|---|
| `addr` | string | none (required) | No (Restart) | TCP address and port for client traffic (for example, `"0.0.0.0:8080"` or `"127.0.0.1:8080"`). Must resolve to a valid TCP address. |
| `read_timeout` | Duration | none (required) | No (Restart) | Maximum duration for reading the entire request, including headers and body (for example, `"30s"`). Must be positive. |
| `write_timeout` | Duration | none (required) | No (Restart) | Maximum duration before timing out writes of the response (for example, `"30s"`). Must be positive. |
| `idle_timeout` | Duration | none (required) | No (Restart) | Maximum amount of time to wait for the next request when keep-alives are enabled (for example, `"60s"`). Must be positive. |
| `max_request_body_bytes` | int64 | `0` (unlimited) | Yes | Maximum request body size in bytes (for example, `10485760` for 10 MB). Requests exceeding it are rejected with `413 {"error":"request body too large"}` and never reach the queue or upstream. Must be non-negative. |

Request body limit behavior:
- A declared `Content-Length` above the limit is rejected immediately, before admission queueing or proxying.
- Bodies without a declared length (chunked transfer encoding) are cut off once they exceed the limit, and the gateway responds with `413`.
- Rejections are still recorded by metrics and the access log.

Environment variable overrides:
- `AEGORA_SERVER_ADDR`
- `AEGORA_SERVER_READ_TIMEOUT`
- `AEGORA_SERVER_WRITE_TIMEOUT`
- `AEGORA_SERVER_IDLE_TIMEOUT`

---

## 2. Routes (`routes`)

An array of route definitions. At least one route is required.

| Field | Type | Default | Reloadable | Description |
|---|---|---|---|---|
| `name` | string | none (required) | Yes | Unique identifier for the route (for example, `"api-service"`). Used in metrics labels and log attributes. |
| `path_prefix` | string | none (required) | Yes | URL path prefix to match (for example, `"/api"`). Requests starting with this prefix match the route. |
| `path_regex` | string | `""` | Yes | Optional regular expression pattern to match URL paths. Must compile to a valid regex. Evaluated if path prefix matches. |
| `methods` | []string | `[]` (all) | Yes | Optional list of permitted HTTP methods (for example, `["GET", "POST"]`). Unmatched methods return 405 Method Not Allowed. |
| `headers` | map[string]string | `{}` | Yes | Optional key-value header requirements. Incoming requests must contain all specified headers with matching values. |
| `targets` | []string | none (required) | Yes | List of upstream target base URLs (for example, `["http://backend-1:8081", "http://backend-2:8082"]`). Must be valid URLs with scheme and host. |
| `strip_prefix` | string | `""` | Yes | Optional path prefix to remove before forwarding to upstreams. If specified, must start with `"/"`. |
| `lb` | string | `"round_robin"` | Yes | Load balancing policy across targets: `"round_robin"` or `"least_connections"`. |
| `timeout` | Duration | none (no timeout) | Yes | Optional per-route timeout for the whole outbound exchange (for example, `"5s"`). Unset or zero keeps the current behavior; when set, an upstream that exceeds it aborts and the client receives the gateway's 502 response. Must be non-negative. |
| `rate_limit` | object | none (inherits global) | Yes | Optional per-route override of the global `rate_limit` block. See [Per-Route Overrides](#per-route-overrides). |

**Upstream timeout**: `timeout` bounds the entire outbound exchange, including the response body transfer, not just the time to first byte. Set it above the slowest healthy response time for that route, because a legitimately long download is also cut when the deadline fires. A timed-out attempt is a transport error, so the configured retry policy decides whether another attempt happens.

---

## 3. Rate Limiting (`rate_limit`)

Protects backends against abusive clients and sudden volume spikes.

| Field | Type | Default | Reloadable | Description |
|---|---|---|---|---|
| `enabled` | bool | `false` | Yes | Enables request rate limiting middleware. |
| `limiter` | string | `"sliding_window"` | Yes | Limiter algorithm: `"sliding_window"` or `"token_bucket"`. |
| `max_requests` | int | none (required if enabled) | Yes | Maximum request capacity within the window or bucket. Must be positive. |
| `window` | Duration | none (required for sliding_window) | Yes | Moving time window duration (for example, `"1m"`). Must be positive. |
| `refill_per_second` | float64 | none (required for token_bucket) | Yes | Token refill rate per second for token bucket limiters. Must be positive. |
| `key` | string | `"ip"` | Yes | Partition key for rate limit buckets: `"ip"` (client remote IP) or `"api_key"` (`X-API-Key` or `Authorization` header). |

### Per-Route Overrides

Any route can define its own `rate_limit` block with the same fields as the global block. Only the fields you set override the global values; every other field is inherited.

```yaml
rate_limit:              # global: generous default for every route
  enabled: true
  limiter: "sliding_window"
  max_requests: 600
  window: "1m"
  key: "ip"

routes:
  - name: "login"
    path_prefix: "/login"
    targets: ["http://auth:8080"]
    rate_limit:          # strict: inherits limiter and key, overrides the rest
      max_requests: 5
      window: "1m"
  - name: "internal"
    path_prefix: "/internal"
    targets: ["http://ops:8080"]
    rate_limit:
      enabled: false     # opt this route out of rate limiting
  - name: "api"
    path_prefix: "/"
    targets: ["http://api:8080"]   # no override: uses the global limit
```

Override semantics:
- **Merge**: fields set in the route block replace the global value; omitted fields are inherited. An empty block (`rate_limit: {}`) is treated as no override.
- **Validation**: the merged result is validated with the same rules as the global block, and errors name the route (for example, `config: routes[0].rate_limit.max_requests: must be positive, got 0`).
- **Enable and disable**: `enabled: false` turns limiting off for that route even if the global block is enabled. `enabled: true` turns it on for that route even if the global block is disabled, in which case the remaining required fields must come from the override or the global block.
- **Separate buckets**: each route with an override counts requests in its own buckets, so requests to it do not consume the global allowance (and vice versa). A client can therefore use the global limit and each overriding route's limit independently. Routes without an override, and requests that match no route, share the global buckets.
- **Route matching**: the override applies to the route the gateway actually matches. Matching is done on the percent-encoded path, so a request such as `/%6Cogin` does not match a `/login` route and falls through to the next matching route (for example, a catch-all `/`) and its limit. If a catch-all route forwards to the same backend as a strictly limited route, give the catch-all a limit you are comfortable with for that backend.
- **Client identity**: with `key: "ip"`, the client IP is taken from the leftmost `X-Forwarded-For` entry when present. Clients can set this header themselves, so strict per-route limits (for example, on login) are only as strong as the trust in that header. Deploy Aegora behind a proxy that overwrites `X-Forwarded-For`, or use `key: "api_key"` where possible.

---

## 4. Reliability (`reliability`)

Controls per-route circuit breaking and upstream retry behaviors.

| Field | Type | Default | Reloadable | Description |
|---|---|---|---|---|
| `breaker_failure_threshold` | int | `5` | Yes | Consecutive transport-level failures required to trip the circuit from closed to open. Must be at least 1. |
| `breaker_cooldown` | Duration | `"30s"` | Yes | Duration the circuit remains open before transitioning to half-open for probing. Must be positive. |
| `retry_enabled` | bool | `false` | Yes | Enables automatic retry attempts on network or 5xx failures for idempotent HTTP requests. |
| `retry_max_attempts` | int | `3` | Yes | Maximum total attempts (initial request + retries). Must be at least 1. |
| `retry_initial_delay` | Duration | `"100ms"` | Yes | Base delay before the first retry. Subsequent retries use exponential backoff and full jitter up to 2 seconds. |

---

## 5. Admission Queue (`queue`)

Provides concurrency regulation and active queue management for saturated systems.

| Field | Type | Default | Reloadable | Description |
|---|---|---|---|---|
| `enabled` | bool | `false` | Yes | Enables priority queueing and CoDel admission control. |
| `max_inflight` | int | none (required if enabled) | Yes | Maximum concurrent requests admitted to downstream handlers. Must be positive. |
| `max_queue_depth` | int | none (required if enabled) | Yes | Maximum queued requests waiting for admission. Overflow returns 503. Must be positive. |
| `codel_target` | Duration | none (required if enabled) | Yes | Target queue dwell time (for example, `"100ms"`). If minimum dwell exceeds this target over an interval, drops begin. |
| `codel_interval` | Duration | none (required if enabled) | Yes | Evaluation sliding window for CoDel queue control (for example, `"1s"`). |
| `priority_paths.high` | []string | `[]` | Yes | Path prefixes classified as high priority (for example, `["/api/payments"]`). |
| `priority_paths.low` | []string | `[]` | Yes | Path prefixes classified as low priority (for example, `["/api/analytics"]`). |

Requests without matching path rules default to normal priority, or use `X-Priority: high` / `X-Priority: low` headers if present.

---

## 6. Adaptive Load Shedding (`shedding`)

Dynamically drops lower-priority requests when downstream latency breaches P99 targets.

| Field | Type | Default | Reloadable | Description |
|---|---|---|---|---|
| `enabled` | bool | `false` | Yes | Enables adaptive load shedding. |
| `target_p99` | Duration | none (required if enabled) | Yes | Desired maximum P99 latency threshold (for example, `"500ms"`). Must be positive. |
| `min_admit_rate` | float64 | none (required if enabled) | Yes | Minimum admission floor for shedding controller. Must be between 0.0 and 1.0 exclusive (for example, `0.05`). |
| `adjust_interval` | Duration | none (required if enabled) | Yes | Evaluation frequency for recalculating admission probabilities (for example, `"1s"`). |
| `priority_paths.high` | []string | `[]` | Yes | Path prefixes protected from load shedding. |
| `priority_paths.low` | []string | `[]` | Yes | Path prefixes shed first when P99 exceeds target. |

---

## 7. Caching (`cache`)

Two-tier HTTP response caching for GET and HEAD requests.

| Field | Type | Default | Reloadable | Description |
|---|---|---|---|---|
| `enabled` | bool | `false` | Yes | Enables HTTP response caching. |
| `ttl` | Duration | none (required if enabled) | Yes | Time-to-live for in-memory L1 cache entries (for example, `"60s"`). |
| `l2_enabled` | bool | `false` | Yes | Enables distributed L2 cache backed by Redis. |
| `l2_ttl` | Duration | none (required if l2_enabled) | Yes | Time-to-live for Redis L2 cache entries (for example, `"5m"`). |
| `redis_addr` | string | `""` | Yes | Host and port for Redis instance (for example, `"redis:6379"`). Required if `l2_enabled` is true. |
| `max_entries` | int | none (required if enabled) | Yes | Maximum number of entries stored in local L1 memory. When the limit is reached, the least recently used entry is evicted. |
| `max_body_bytes` | int | none (required if enabled) | Yes | Maximum response body size in bytes eligible for caching (for example, `1048576` for 1 MB). |

Cache eligibility rules:
- Only HTTP 200 responses are cached.
- Requests with `Authorization` headers are never cached or served from cache.
- Requests or responses with `Cache-Control: no-store` are never cached.
- Responses with `Set-Cookie` headers are never cached.

---

## 8. Observability (`observability`)

Logging, metrics, and distributed tracing settings.

| Field | Type | Default | Reloadable | Description |
|---|---|---|---|---|
| `log_level` | string | none (required) | Yes | Minimum log severity: `"debug"`, `"info"`, `"warn"`, or `"error"`. Updated dynamically via `slog.LevelVar`. |
| `metrics_addr` | string | `""` | No (Restart) | TCP address for Prometheus `/metrics` scraping server (for example, `"0.0.0.0:9090"`). |
| `tracing.enabled` | bool | `false` | No (Restart) | Enables OpenTelemetry distributed tracing. |
| `tracing.exporter` | string | `"stdout"` | No (Restart) | Trace exporter destination: `"stdout"` or `"otlp"`. |
| `tracing.otlp_endpoint` | string | `""` | No (Restart) | OTLP HTTP collector host and port (for example, `"localhost:4318"`). Required if exporter is `"otlp"`. |
| `tracing.sample_ratio` | float64 | `1.0` | No (Restart) | Head-based trace sampling ratio between 0.0 (exclusive) and 1.0 (inclusive). |

Environment variable overrides:
- `AEGORA_OBSERVABILITY_LOG_LEVEL`
- `AEGORA_OBSERVABILITY_METRICS_ADDR`

---

## 9. Admin API (`admin`)

Configures the administrative management HTTP server.

| Field | Type | Default | Reloadable | Description |
|---|---|---|---|---|
| `enabled` | bool | `false` | No (Restart) | Enables the administrative server. |
| `addr` | string | `""` | No (Restart) | TCP address for admin listener (for example, `"0.0.0.0:9091"`). Required when admin is enabled. |

Environment variable overrides:
- `AEGORA_ADMIN_ENABLED`
- `AEGORA_ADMIN_ADDR`
