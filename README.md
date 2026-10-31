# Aegora

[![CI](https://github.com/syncronix-labs/aegora/actions/workflows/ci.yml/badge.svg)](https://github.com/syncronix-labs/aegora/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/syncronix-labs/aegora)](https://github.com/syncronix-labs/aegora/blob/main/go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Go Layer 7 reliability gateway: adaptive rate limiting, circuit breaking,
multi-tier caching, and AIMD load shedding for upstream services.

Aegora is an HTTP/1.1 reverse proxy and reliability gateway built in Go. It protects upstream services from overload, cascading failures, latency spikes, and traffic surges through defensive middleware, multi-tier caching, dynamic queueing, adaptive load shedding, and observability.

![Config hot reload demo](assets/demo-hot-reload.gif)
_Live config reload: the rate limit tightens from 60 to 3 requests per
minute on a running gateway. 200 200 200 429 429, zero restarts._

## Table of Contents

- [Why Aegora?](#why-aegora)
- [Install](#install)
- [Architecture](#architecture)
  - [Request Walkthrough](#request-walkthrough)
  - [Ports](#ports)
  - [Design Choices Worth Knowing](#design-choices-worth-knowing)
- [Features](#features)
- [Quickstart](#quickstart)
  - [Building a Versioned Image](#building-a-versioned-image)
  - [Verify Operations](#verify-operations)
- [Configuration](#configuration)
- [Admin API](#admin-api)
- [Benchmarks](#benchmarks)
- [Documentation](#documentation)
- [Project Status](#project-status)
- [Contributing](#contributing)
- [License](#license)

## Why Aegora?

Every backend dies the same way: a traffic surge or one slow
dependency turns into a retry storm, and static limits tuned last
quarter don't save you. Envoy has answers, but you have to operate
Envoy. Aegora packs the overload story into a single Go binary.

- Load shedding that watches latency, not a config file. An AIMD
  controller tracks P99 and throttles lower-priority traffic before
  the backend tips over. No hand-tuned thresholds to revisit after
  every incident.
- Priorities are first-class citizens. Multi-level admission
  queues with CoDel active queue management, so one chatty endpoint
  can't starve the rest. Most lightweight gateways are still plain
  FIFO.
- One pipeline, one YAML, one binary. Shedding, rate limiting,
  multi-tier caching, queueing, circuit breaking, retries, Prometheus
  metrics, OpenTelemetry tracing: a single ordered request chain, no
  sidecars, no control plane.
- Hot reload with zero dropped connections. Config applies
  atomically through the admin API. The GIF above is the proof, not
  a claim.
- Benchmarked, not hand-waved. Cache hits in ~270ns, the
  sliding-window limiter at 0 allocs/op under contention. Numbers
  are in the Benchmarks section below.

---

## Install

Download the release binary for your system and run it. First, find your platform keyword:

| Your system                          | Platform keyword |
| ------------------------------------ | ---------------- |
| macOS on Apple Silicon (M1 or later) | darwin_arm64     |
| macOS on Intel                       | darwin_amd64     |
| Linux on x86_64                      | linux_amd64      |
| Linux on ARM64                       | linux_arm64      |

Then replace `<platform>` in the commands below with your keyword, including the angle brackets. The example uses the current release, v1.1.0; for other versions, see the [releases page](https://github.com/syncronix-labs/aegora/releases).

```sh
curl -fsSLO https://github.com/syncronix-labs/aegora/releases/download/v1.1.0/aegora_1.1.0_<platform>.tar.gz
tar -xzf aegora_1.1.0_<platform>.tar.gz
./aegora --version
```

On macOS, if Gatekeeper refuses the unsigned binary, run `xattr -d com.apple.quarantine ./aegora` once and retry.

To start the gateway, take the sample config, point its routes at your own services, and run:

```sh
curl -fsSLO https://raw.githubusercontent.com/syncronix-labs/aegora/main/configs/aegora.yaml
./aegora --config aegora.yaml
```

The gateway listens on port 8080, metrics on 9090, and the admin API on 9091. Go users can also install from source with `go install github.com/syncronix-labs/aegora/cmd/aegora@latest`.

---

## Architecture

![Architecture](assets/architecture.svg)

Incoming requests traverse the middleware pipeline in order:

```text
Client -> Recovery -> RequestID -> Tracing -> Metrics -> AccessLog -> BodyLimit -> Shedding -> RateLimit -> Cache -> Queue -> Gateway -> Upstreams
```

### Request Walkthrough

- Request lands on `:8080`. Recovery, RequestID, Tracing, Metrics and AccessLog wrap every request, including rejections.
- Body Limit rejects oversized bodies with 413 before they can spend shedding, rate-limit or queue budget.
- Shedding (AIMD) watches p99 latency and trims the admit rate under overload; high-priority paths keep a floor.
- Rate Limit runs before Cache, so limits apply per key (IP or API key) even for cached responses.
- Cache hits return here and skip the queue and the upstream entirely. Misses fall through.
- Queue admits by priority with CoDel, then the Router matches and the Pool picks a healthy target.
- The breaker wraps the retry loop. The final outcome is what gets recorded against route health.

### Ports

| Port   | Surface                                                   |
| ------ | --------------------------------------------------------- |
| `8080` | Gateway data plane (proxied traffic, `/livez`, `/readyz`) |
| `9090` | Prometheus metrics endpoint                               |
| `9091` | Admin API: health, routes, config, reload                 |
| `6379` | Redis, only when rate limiting or cache L2 uses it        |

Config loads once at startup from `aegora.yaml`. Edits take effect only after `POST /admin/reload`, which rebuilds the handler and swaps it atomically. Reload reports which sections applied and which were skipped (restart-only).

### Design Choices Worth Knowing

- Chain is built once at startup and shared, never rebuilt per request. Reload builds a new one and swaps the pointer.
- Client faults are not upstream faults. Oversized bodies return 413 without touching breaker or pool health.
- Redis fails open. If Redis is down, rate limiting and cache L2 degrade quietly instead of taking the gateway down.
- Cache write order is L1 then L2, and L2 TTL must be at least the L1 TTL, or entries vanish between the tiers.
- Response writers stay minimal. Wrappers implement only `http.ResponseWriter`; an extra `Flush()` once made the proxy treat responses as streaming and silently broke cache storage.

---

## Features

### Routing

- **Path Matching**: Prefix and regex route matching with HTTP method and header constraints.
- **Load Balancing**: Round-robin and least-connections upstream selection.
- **Passive Health Checking**: Target failure detection with automatic recovery.
- **Path Rewriting**: Optional prefix stripping before forwarding to upstreams.

### Resilience

- **Rate Limiting**: Sliding window and token bucket limiters by client IP or API key, with in-memory or Redis backends.
- **Circuit Breaker**: Closed, open, and half-open state machine per route.
- **Retry Policies**: Idempotent request retries with exponential backoff and full jitter.
- **Priority Queues**: Multi-level admission queues (high, normal, low) via path rules or `X-Priority` headers.
- **CoDel Active Queue Management**: Controlled Delay drop logic to mitigate bufferbloat.
- **Adaptive Load Shedding**: AIMD controller throttling lower-priority traffic when P99 latency exceeds target.

### Performance

- **Multi-Tier Caching**: In-memory L1 cache with distributed Redis L2 storage.
- **Conditional Validation**: `ETag` generation and `If-None-Match` 304 evaluation.
- **Safe Cache Semantics**: Automatic bypass on `Authorization`, `no-store`, or `Set-Cookie`.

### Observability

- **Prometheus Metrics**: `/metrics` endpoint with RED method counters, duration histograms, and inflight gauges.
- **OpenTelemetry Tracing**: W3C tracecontext propagation with stdout or OTLP exporters and configurable sampling.
- **Structured Logging**: `log/slog` output with request IDs, status codes, latency, and client details.

### Operations

- **Admin API**: Dedicated HTTP listener on port `9091` for health checks, route queries, and configuration snapshots.
- **Hot Reload**: Atomic configuration reload via `POST /admin/reload` without dropping connections.
- **Container Packaging**: Multi-stage Dockerfile and Docker Compose environment.

---

## Quickstart

Start the gateway along with two mock backend services and a Redis instance using Docker Compose:

```bash
make compose-up
```

Alternatively, run Docker Compose directly:

```bash
docker compose -f deployments/docker-compose.yml up --build -d
```

### Building a Versioned Image

The Dockerfile accepts a `VERSION` build argument (default `dev`) that is stamped into the binary and reported by `aegora --version` and `GET /admin/health`:

```bash
docker build -f deployments/Dockerfile --build-arg VERSION=v0.1.0 -t aegora:v0.1.0 .
```

### Verify Operations

Send a request through the gateway to verify routing and load balancing:

```bash
curl -i http://localhost:8080/api/users
```

Expected response:

```http
HTTP/1.1 200 OK
Content-Type: text/plain; charset=utf-8
X-Cache: MISS
X-Request-Id: 52d6beb3dde73b4696b056c8961b981c

backend-1
```

Send the same request again to test the response cache:

```bash
curl -i http://localhost:8080/api/users
```

Expected response:

```http
HTTP/1.1 200 OK
Content-Type: text/plain; charset=utf-8
X-Cache: HIT
...
```

Scrape Prometheus RED telemetry from the metrics listener:

```bash
curl -s http://localhost:9090/metrics | grep aegora
```

Inspect administrative health and build version:

```bash
curl -i http://localhost:9091/admin/health
```

When finished, tear down the demo environment:

```bash
make compose-down
```

---

## Configuration

Aegora is configured through YAML files, with optional overrides via environment variables (`AEGORA_*`). The full configuration specification is located in [configs/aegora.yaml](configs/aegora.yaml).

Validate a config file without starting the gateway (useful in CI):

```bash
aegora --check-config --config configs/aegora.yaml   # prints "config OK" or the invalid field, exits non-zero on error
```

Here is a minimal annotated example configuring a listener, an upstream route, and client rate limiting:

```yaml
server:
  addr: "0.0.0.0:8080" # Ingress port for incoming data-plane traffic
  read_timeout: "30s"
  write_timeout: "30s"
  idle_timeout: "60s"

routes:
  - name: "api-service"
    path_prefix: "/api" # Match requests starting with /api
    methods:
      - "GET"
      - "POST"
    targets:
      - "http://127.0.0.1:8081"
      - "http://127.0.0.1:8082"
    strip_prefix: "/api" # Forward request without /api prefix to backend
    lb: "round_robin" # Selection policy: round_robin or least_connections

rate_limit:
  enabled: true
  limiter: "sliding_window" # sliding_window or token_bucket
  max_requests: 120 # Maximum requests allowed within window
  window: "1m"
  key: "ip" # Rate limit partition: ip or api_key

observability:
  log_level: "info"
  metrics_addr: "0.0.0.0:9090" # Prometheus scrapers connect here

admin:
  enabled: true
  addr: "0.0.0.0:9091" # Dedicated management interface
```

---

## Admin API

Aegora provides a separate administrative server listening on port `9091` by default. Keeping administration isolated from the data plane prevents management commands from contending with customer traffic.

| Method | Path            | Purpose                                                                      |
| ------ | --------------- | ---------------------------------------------------------------------------- |
| `GET`  | `/admin/health` | Liveness verification and binary build version                               |
| `GET`  | `/admin/routes` | JSON array of active route rules, match prefixes, target pools, and policies |
| `GET`  | `/admin/config` | Runtime snapshot of the active server configuration                          |
| `POST` | `/admin/reload` | Trigger hot reload of configuration from disk without downtime               |

Example configuration reload trigger:

```bash
curl -i -X POST http://localhost:9091/admin/reload
```

---

## Benchmarks

Aegora includes benchmarks that measure hot-path performance under realistic conditions.

Run the test suite benchmarks:

```bash
go test -bench=. -benchtime=2s -benchmem ./internal/...
```

### Measured Performance

The following benchmarks were conducted on macOS (Apple M1, 8 cores) running Go 1.26.4:

| Benchmark                       | Iterations (2s) | Latency      | Memory      | Allocs        | Description                                                    |
| ------------------------------- | --------------- | ------------ | ----------- | ------------- | -------------------------------------------------------------- |
| `BenchmarkGatewayChain-8`       | 44,428          | 48,402 ns/op | 41,766 B/op | 124 allocs/op | Full chain cache-miss GET through ReverseProxy to live backend |
| `BenchmarkCacheHit-8`           | 8,467,442       | 269.5 ns/op  | 48 B/op     | 3 allocs/op   | Response cache hit path serving directly from memory           |
| `BenchmarkSlidingWindowAllow-8` | 14,011,202      | 167.6 ns/op  | 0 B/op      | 0 allocs/op   | Sliding window Allow() throughput under parallel contention    |

### Analysis of Results

- **Cache Hit Acceleration**: `BenchmarkCacheHit` serves cached responses in ~270 nanoseconds and only 3 allocations. By comparison, `BenchmarkGatewayChain` takes ~48.4 microseconds and 124 allocations to traverse RequestID generation, Prometheus histogram recordings, rate limit checks, routing lookups, and a loopback HTTP network roundtrip. Serving hits from cache is ~180x faster and eliminates 98% of allocations.
- **Rate Limiter Allocation Profile**: `BenchmarkSlidingWindowAllow` registers 0 allocs/op under high contention. This is expected behavior: `NewSlidingWindow` pre-allocates timestamp storage capacity, so `Allow()` operations lock the mutex, prune expired entries, and append current timestamps without heap escapes or buffer reallocations.

---

## Documentation

- [Configuration Reference](docs/configuration.md): Complete guide to every configuration section, field, default value, validation constraint, and reload behavior.
- [Architecture Guide](docs/architecture.md): Request processing lifecycle, design rationale, response buffering lessons learned, and concurrency model.

---

## Project Status

Aegora is built and maintained by Shubham Kashyap and Meeth Davda. Track ongoing development and planned milestones on [GitHub Issues](https://github.com/syncronix-labs/aegora/issues).

---

## Contributing

Pull requests are welcome.

New here? Start with an issue labeled [good first issue](https://github.com/syncronix-labs/aegora/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22). They are small, self-contained, and each one comes with acceptance criteria. For something larger, see the [help wanted](https://github.com/syncronix-labs/aegora/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22) issues. If you want to build something that is not listed, please open an issue first so the scope is agreed before any code is written.

Contribution guidelines:

- Run `make verify` (formatting, tidy, lint, tests, build) before submitting changes.
- Keep pull requests focused: one feature or bugfix per PR.
- Do not squash-merge; maintain individual commit history with conventional commit messages.
- Any PR that adds or changes a config field must update [docs/configuration.md](docs/configuration.md) in the same PR.

---

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.
