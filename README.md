# Aegora

Aegora is a high-performance Layer 7 reliability gateway designed for modern distributed systems.

The project aims to provide intelligent traffic management, adaptive load shedding, priority-aware request handling, caching, observability, and resilience mechanisms for large-scale services operating under unpredictable workloads.

## Vision

Traditional API gateways and reverse proxies primarily focus on routing and static rate limiting.

Aegora extends this model by introducing reliability-first capabilities such as:

- Dynamic load shedding
- Priority-aware request scheduling
- Intelligent queue management
- Multi-tier caching
- Distributed event durability
- Advanced observability
- Adaptive traffic control

The long-term goal is to build a reliability platform capable of protecting backend services during traffic spikes, cascading failures, latency storms, and partial infrastructure outages.

---

## Core Objectives

### Reliability

Maintain service availability during abnormal traffic conditions and infrastructure failures.

### Performance

Minimize latency and maximize throughput through efficient request processing pipelines.

### Scalability

Support horizontal scaling across distributed deployments.

### Observability

Provide detailed metrics, traces, logs, and operational insights.

### Extensibility

Offer modular components that can evolve independently.

---

## Planned Features

### Traffic Management

- Reverse proxy
- Request routing
- Traffic shaping
- Request prioritization

### Reliability Controls

- Adaptive load shedding
- Circuit breakers
- Retry policies
- Backpressure management

### Rate Limiting

- Token bucket limiting
- Sliding window limiting
- Distributed rate limiting

### Queue Management

- CoDel-based queue control
- Priority queues
- Queue monitoring

### Caching

- In-memory caching
- Multi-tier cache architecture
- Cache invalidation strategies

### Observability

- Prometheus metrics
- Distributed tracing
- Structured logging
- Operational dashboards

### Distributed Systems

- Event durability
- Distributed coordination
- Replication support
- High availability mechanisms

---

## Repository Structure

```text
aegora/
├── cmd/
│   └── aegora/
├── internal/
│   ├── cache/
│   ├── config/
│   ├── proxy/
│   └── ratelimit/
├── pkg/
├── docs/
├── deployments/
├── scripts/
└── test/
```

---

## Getting Started

### Prerequisites

- Go 1.25+
- Git
- Make

### Clone Repository

```bash
git clone https://github.com/syncronix-labs/aegora.git
cd aegora
```

### Install Dependencies

```bash
go mod tidy
```

### Run Application

```bash
make run
```

### Run Tests

```bash
make test
```

### Run Linting

```bash
make lint
```

### Build

```bash
make build
```

---

## Development Workflow

Create a feature branch:

```bash
git checkout -b feature/<feature-name>
```

Validate changes:

```bash
make verify
```

Commit using Conventional Commits:

```text
feat(proxy): implement request routing
fix(cache): resolve eviction bug
refactor(metrics): simplify collector registration
docs(architecture): add gateway design
```

---

## Current Status

The project is currently in the foundation phase.

Initial milestones include:

- Configuration management
- Structured logging
- HTTP server bootstrap
- Health endpoints
- Metrics collection
- Reverse proxy core

---

## License

License details will be added before the first public release.
