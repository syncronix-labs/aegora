package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// statusConfig is the reliability configuration the status tests build gateways with: a low
// failure threshold so a breaker can be driven open in a couple of calls, and a cooldown short
// enough that a test can wait it out.
func statusConfig() ReliabilityConfig {
	return ReliabilityConfig{
		BreakerFailureThreshold: 2,
		BreakerCooldown:         time.Minute,
		BreakerHalfOpenProbes:   1,
		BreakerSuccessThreshold: 1,
		RetryEnabled:            false,
	}
}

func newStatusBackend(t *testing.T) *httptest.Server {
	t.Helper()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(backend.Close)

	return backend
}

func TestGatewayStatus_ReportsClosedBreakersAndHealthyTargets(t *testing.T) {
	backend := newStatusBackend(t)

	gw, err := NewGateway([]RouteConfig{{
		Name:       "alpha",
		PathPrefix: "/alpha",
		Targets:    []string{backend.URL},
	}}, statusConfig())
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}

	statuses := gw.Status()
	if len(statuses) != 1 {
		t.Fatalf("expected 1 route status, got %d", len(statuses))
	}

	status := statuses[0]
	if status.Name != "alpha" {
		t.Fatalf("expected route name alpha, got %q", status.Name)
	}
	if status.BreakerState != "closed" {
		t.Fatalf("expected breaker_state closed before any traffic, got %q", status.BreakerState)
	}
	if status.ConsecutiveFailures != 0 {
		t.Fatalf("expected 0 consecutive failures before any traffic, got %d", status.ConsecutiveFailures)
	}
	if len(status.Targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(status.Targets))
	}
	if status.Targets[0].URL != backend.URL {
		t.Fatalf("expected target %q, got %q", backend.URL, status.Targets[0].URL)
	}
	if !status.Targets[0].Healthy {
		t.Fatalf("expected a fresh target to be healthy")
	}
}

func TestGatewayStatus_FollowsTheBreakerAndThePool(t *testing.T) {
	backend := newStatusBackend(t)

	gw, err := NewGateway([]RouteConfig{{
		Name:       "alpha",
		PathPrefix: "/alpha",
		Targets:    []string{backend.URL},
	}}, statusConfig())
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}

	entry := gw.routes[&gw.router.routes[0]]
	if entry == nil {
		t.Fatal("expected a route entry for the configured route")
	}

	entry.breaker.RecordFailure()
	entry.breaker.RecordFailure()
	entry.pool.ReportFailure(0)

	status := gw.Status()[0]
	if status.BreakerState != "open" {
		t.Fatalf("expected breaker_state open after tripping the threshold, got %q", status.BreakerState)
	}
	if status.ConsecutiveFailures != 2 {
		t.Fatalf("expected 2 consecutive failures, got %d", status.ConsecutiveFailures)
	}
	if status.Targets[0].Healthy {
		t.Fatal("expected the target reported through ReportFailure to be unhealthy")
	}

	entry.pool.ReportSuccess(0)

	if !gw.Status()[0].Targets[0].Healthy {
		t.Fatal("expected ReportSuccess to bring the target back up")
	}
}

func TestGatewayStatus_NamesHalfOpenWithAnUnderscore(t *testing.T) {
	backend := newStatusBackend(t)

	rc := statusConfig()
	rc.BreakerCooldown = time.Millisecond

	gw, err := NewGateway([]RouteConfig{{
		Name:       "alpha",
		PathPrefix: "/alpha",
		Targets:    []string{backend.URL},
	}}, rc)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}

	entry := gw.routes[&gw.router.routes[0]]
	entry.breaker.RecordFailure()
	entry.breaker.RecordFailure()

	time.Sleep(5 * time.Millisecond)

	// Allow moves an expired open breaker into half-open and admits the probe.
	if !entry.breaker.Allow() {
		t.Fatal("expected the breaker to admit a probe after the cooldown")
	}

	if got := gw.Status()[0].BreakerState; got != "half_open" {
		t.Fatalf("expected breaker_state half_open, got %q", got)
	}
}

func TestGatewayStatus_OrdersRoutesByName(t *testing.T) {
	backend := newStatusBackend(t)

	gw, err := NewGateway([]RouteConfig{
		{Name: "zeta", PathPrefix: "/zeta", Targets: []string{backend.URL}},
		{Name: "alpha", PathPrefix: "/alpha", Targets: []string{backend.URL}},
	}, statusConfig())
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}

	statuses := gw.Status()
	if len(statuses) != 2 {
		t.Fatalf("expected 2 route statuses, got %d", len(statuses))
	}
	if statuses[0].Name != "alpha" || statuses[1].Name != "zeta" {
		t.Fatalf("expected alphabetical order, got %q then %q", statuses[0].Name, statuses[1].Name)
	}
}

func TestGatewayStatus_NilGatewayIsEmpty(t *testing.T) {
	var gw *Gateway

	if got := gw.Status(); got != nil {
		t.Fatalf("expected nil status for a nil gateway, got %+v", got)
	}
}

func TestPoolHealth_NilPoolIsEmpty(t *testing.T) {
	var p *Pool

	if got := p.Health(); got != nil {
		t.Fatalf("expected nil health for a nil pool, got %+v", got)
	}
}
