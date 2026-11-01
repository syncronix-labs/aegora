package proxy

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func healthTestReliability() ReliabilityConfig {
	return ReliabilityConfig{
		BreakerFailureThreshold: 5,
		BreakerCooldown:         time.Minute,
		BreakerHalfOpenProbes:   2,
		BreakerSuccessThreshold: 2,
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestNewGateway_HealthCheckValidation(t *testing.T) {
	valid := HealthCheckConfig{Path: "/livez", Interval: 10 * time.Millisecond, Timeout: 5 * time.Millisecond}

	tests := []struct {
		name   string
		mutate func(hc *HealthCheckConfig)
	}{
		{name: "empty path", mutate: func(hc *HealthCheckConfig) { hc.Path = "" }},
		{name: "relative path", mutate: func(hc *HealthCheckConfig) { hc.Path = "livez" }},
		{name: "zero interval", mutate: func(hc *HealthCheckConfig) { hc.Interval = 0 }},
		{name: "zero timeout", mutate: func(hc *HealthCheckConfig) { hc.Timeout = 0 }},
		{name: "timeout equal to interval", mutate: func(hc *HealthCheckConfig) { hc.Timeout = hc.Interval }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hc := valid
			tt.mutate(&hc)

			_, err := NewGateway([]RouteConfig{{
				Name:        "api",
				PathPrefix:  "/",
				Targets:     []string{"http://127.0.0.1:1"},
				HealthCheck: &hc,
			}}, healthTestReliability())
			if err == nil {
				t.Fatal("expected an error for invalid health check settings")
			}
		})
	}
}

func TestGateway_ActiveHealthCheckRemovesAndRestoresTargets(t *testing.T) {
	var aHealthy atomic.Bool

	backendA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			if !aHealthy.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = w.Write([]byte("A"))
	}))
	defer backendA.Close()

	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if r.URL.Path != "/healthz" {
			_, _ = w.Write([]byte("B"))
		}
	}))
	defer backendB.Close()

	gw, err := NewGateway([]RouteConfig{{
		Name:       "api",
		PathPrefix: "/",
		Targets:    []string{backendA.URL, backendB.URL},
		HealthCheck: &HealthCheckConfig{
			Path:     "/healthz",
			Interval: 30 * time.Millisecond,
			Timeout:  10 * time.Millisecond,
		},
	}}, healthTestReliability())
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer gw.Close()

	entry := gw.routes[&gw.router.routes[0]]
	waitForCondition(t, 2*time.Second, func() bool { return !entry.pool.isHealthy(0) },
		"the failing target to be marked down")

	for i := 0; i < 6; i++ {
		rec := httptest.NewRecorder()
		gw.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusOK || rec.Body.String() != "B" {
			t.Fatalf("request %d: expected 200 from B while A is down, got %d %q", i, rec.Code, rec.Body.String())
		}
	}

	aHealthy.Store(true)
	waitForCondition(t, 2*time.Second, func() bool { return entry.pool.isHealthy(0) },
		"the recovered target to be marked up")

	seenA := false
	for i := 0; i < 6 && !seenA; i++ {
		rec := httptest.NewRecorder()
		gw.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Body.String() == "A" {
			seenA = true
		}
	}
	if !seenA {
		t.Fatal("expected the recovered target to serve traffic again")
	}
}

func TestGateway_CloseStopsHealthChecks(t *testing.T) {
	var probes atomic.Int64

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		probes.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	gw, err := NewGateway([]RouteConfig{{
		Name:       "api",
		PathPrefix: "/",
		Targets:    []string{backend.URL},
		HealthCheck: &HealthCheckConfig{
			Path:     "/healthz",
			Interval: 20 * time.Millisecond,
			Timeout:  5 * time.Millisecond,
		},
	}}, healthTestReliability())
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool { return probes.Load() > 1 },
		"repeated health checks to run")

	gw.Close()
	stopped := probes.Load()
	time.Sleep(80 * time.Millisecond)

	if got := probes.Load(); got != stopped {
		t.Fatalf("health checks continued after Close: %d probes before, %d after", stopped, got)
	}

	gw.Close() // Close is idempotent.
}

func TestPool_ActiveHealthOverridesPassiveState(t *testing.T) {
	pool, err := NewPool([]string{"http://10.0.0.1:8080", "http://10.0.0.2:8080"}, "round_robin")
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	pool.SetActiveHealth(0, false)

	_, idx, ok := pool.Next()
	if !ok || idx != 1 {
		t.Fatalf("expected target 1 while target 0 is actively down, got idx=%d ok=%v", idx, ok)
	}
	pool.Done(idx)

	pool.SetActiveHealth(0, true)
	sawZero := false
	for i := 0; i < 2; i++ {
		_, idx, ok := pool.Next()
		if !ok {
			t.Fatalf("call %d after recovery: expected a healthy target", i)
		}
		if idx == 0 {
			sawZero = true
		}
		pool.Done(idx)
	}
	if !sawZero {
		t.Fatal("expected target 0 back in rotation after a successful probe")
	}

	// A successful probe clears the passive cooldown, so a recovered target serves
	// traffic without waiting for the cooldown to elapse.
	pool.ReportFailure(1)
	pool.SetActiveHealth(1, true)

	seen := map[int]bool{}
	for i := 0; i < 4; i++ {
		_, idx, ok := pool.Next()
		if !ok {
			t.Fatalf("call %d: expected a healthy target", i)
		}
		seen[idx] = true
		pool.Done(idx)
	}
	if !seen[0] || !seen[1] {
		t.Fatalf("expected both targets in rotation, saw %v", seen)
	}
}
