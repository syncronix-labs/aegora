package queue

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/syncronix-labs/aegora/internal/metrics"
)

// TestQueue_DroppedCounterEndToEnd runs the queue middleware under the metrics
// middleware and asserts a queue-dropped request increments
// aegora_queue_dropped_total.
func TestQueue_DroppedCounterEndToEnd(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	cd := NewCodel(100*time.Millisecond, time.Second)
	q := NewQueue(1, 2, cd, nil, nil)
	q.running = true // tickets stay queued without the background dispatcher

	handler := m.Middleware(nil)(q.Admit(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("dropped request should not reach the downstream handler")
	})))

	// Fill the normal-priority queue to maxDepth so the next request is rejected.
	q.mu.Lock()
	q.normalQueue = append(q.normalQueue, &ticket{priority: PriorityNormal, done: make(chan ticketResult, 1)})
	q.normalQueue = append(q.normalQueue, &ticket{priority: PriorityNormal, done: make(chan ticketResult, 1)})
	q.mu.Unlock()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/test", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := testutil.ToFloat64(m.QueueDropped.WithLabelValues("unmatched")); got != 1.0 {
		t.Fatalf("aegora_queue_dropped_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.ShedTotal.WithLabelValues("unmatched")); got != 0.0 {
		t.Fatalf("aegora_shed_total = %v, want 0", got)
	}
}
