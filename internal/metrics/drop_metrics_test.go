package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/syncronix-labs/aegora/internal/proxy"
)

func TestMiddleware_CountsShedDrops(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		MarkShed(w.Header())
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"server overloaded"}`))
	})

	rec := httptest.NewRecorder()
	m.Middleware(nil)(inner).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api", nil))

	if got := testutil.ToFloat64(m.ShedTotal.WithLabelValues("unmatched")); got != 1.0 {
		t.Fatalf("aegora_shed_total{route=unmatched} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.QueueDropped.WithLabelValues("unmatched")); got != 0.0 {
		t.Fatalf("aegora_queue_dropped_total{route=unmatched} = %v, want 0", got)
	}
}

func TestMiddleware_CountsQueueDrops(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	handler := m.Middleware(nil)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		MarkQueueDropped(w.Header())
		w.WriteHeader(http.StatusServiceUnavailable)
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := testutil.ToFloat64(m.QueueDropped.WithLabelValues("unmatched")); got != 1.0 {
		t.Fatalf("aegora_queue_dropped_total{route=unmatched} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.ShedTotal.WithLabelValues("unmatched")); got != 0.0 {
		t.Fatalf("aegora_shed_total{route=unmatched} = %v, want 0", got)
	}
}

func TestMiddleware_UsesRouteLabelForDropCounters(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	router := &stubRouter{
		matchFn: func(_ *http.Request) *proxy.RouteConfig {
			return &proxy.RouteConfig{Name: "orders"}
		},
	}

	handler := m.Middleware(router)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		MarkShed(w.Header())
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/orders", nil))

	if got := testutil.ToFloat64(m.ShedTotal.WithLabelValues("orders")); got != 1.0 {
		t.Fatalf("aegora_shed_total{route=orders} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.ShedTotal.WithLabelValues("unmatched")); got != 0.0 {
		t.Fatalf("aegora_shed_total{route=unmatched} = %v, want 0", got)
	}
}

func TestMiddleware_NoMarkerDoesNotTouchDropCounters(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	handler := m.Middleware(nil)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api", nil))

	if got := testutil.ToFloat64(m.ShedTotal.WithLabelValues("unmatched")); got != 0.0 {
		t.Fatalf("aegora_shed_total{route=unmatched} = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.QueueDropped.WithLabelValues("unmatched")); got != 0.0 {
		t.Fatalf("aegora_queue_dropped_total{route=unmatched} = %v, want 0", got)
	}
}
