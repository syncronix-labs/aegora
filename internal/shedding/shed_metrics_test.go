package shedding

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/syncronix-labs/aegora/internal/metrics"
)

// TestShedding_ShedCounterEndToEnd runs the shedding middleware under the
// metrics middleware and asserts a shed request increments aegora_shed_total.
func TestShedding_ShedCounterEndToEnd(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	c := NewController(100*time.Millisecond, time.Second, 0.05, nil, nil)
	// Force the shed path deterministically, as TestController_RejectsOverload does.
	c.mu.Lock()
	c.admitRate = 0.0000000001
	c.priorityFloor = 0.9
	c.mu.Unlock()

	handler := m.Middleware(nil)(c.Admit(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("shed request should not reach the downstream handler")
	})))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := testutil.ToFloat64(m.ShedTotal.WithLabelValues("unmatched")); got != 1.0 {
		t.Fatalf("aegora_shed_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.QueueDropped.WithLabelValues("unmatched")); got != 0.0 {
		t.Fatalf("aegora_queue_dropped_total = %v, want 0", got)
	}
}
