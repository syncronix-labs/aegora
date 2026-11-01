package readiness

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func probe(set *Set, timeout time.Duration) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	set.Handler(timeout).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	return rec
}

func TestAConfigWithNoDependenciesIsAlwaysReady(t *testing.T) {
	set := NewSet()
	if got := set.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0", got)
	}

	rec := probe(set, time.Second)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if body := rec.Body.String(); body != `{"status":"ok"}` {
		t.Fatalf("body = %q, want the unchanged ok payload", body)
	}
}

func TestEveryCheckPassingReportsReady(t *testing.T) {
	set := NewSet()
	set.Add("redis", func(context.Context) error { return nil })
	set.Add("postgres", func(context.Context) error { return nil })

	rec := probe(set, time.Second)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestAFailingCheckNamesTheDependency(t *testing.T) {
	set := NewSet()
	set.Add("redis", func(context.Context) error {
		return errors.New("dial tcp 127.0.0.1:6379: connect: connection refused")
	})

	rec := probe(set, time.Second)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}

	var body struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	if body.Status != "not ready" {
		t.Fatalf("status field = %q, want %q", body.Status, "not ready")
	}
	if len(body.Checks) != 1 {
		t.Fatalf("checks = %v, want exactly one entry", body.Checks)
	}
	detail, ok := body.Checks["redis"]
	if !ok {
		t.Fatalf("checks = %v, want the failing dependency keyed as redis", body.Checks)
	}
	if !strings.HasPrefix(detail, "error: ") {
		t.Fatalf("detail = %q, want it to start with %q", detail, "error: ")
	}
	if !strings.Contains(detail, "connection refused") {
		t.Fatalf("detail = %q, want the underlying error preserved", detail)
	}
}

func TestOnlyTheFailingDependencyIsReported(t *testing.T) {
	set := NewSet()
	set.Add("redis", func(context.Context) error { return nil })
	set.Add("postgres", func(context.Context) error { return errors.New("no route to host") })

	rec := probe(set, time.Second)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}

	var body struct {
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if _, reported := body.Checks["redis"]; reported {
		t.Fatalf("checks = %v, want the healthy dependency left out", body.Checks)
	}
	if _, reported := body.Checks["postgres"]; !reported {
		t.Fatalf("checks = %v, want the failing dependency included", body.Checks)
	}
}

// A dependency that accepts a connection and then never answers must fail the
// probe instead of holding it open until the orchestrator gives up.
func TestASlowCheckFailsInsteadOfHanging(t *testing.T) {
	set := NewSet()
	set.Add("redis", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	started := time.Now()
	rec := probe(set, 50*time.Millisecond)
	elapsed := time.Since(started)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if elapsed > time.Second {
		t.Fatalf("handler took %v, want it bounded by the probe timeout", elapsed)
	}
	if !strings.Contains(rec.Body.String(), "redis") {
		t.Fatalf("body = %q, want the slow dependency named", rec.Body.String())
	}
}

// A probe that ignores its context cannot be waited on, so the handler has to
// return on the deadline regardless.
func TestACheckThatIgnoresItsContextStillFailsTheProbe(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	set := NewSet()
	set.Add("redis", func(context.Context) error {
		<-release
		return nil
	})

	started := time.Now()
	rec := probe(set, 50*time.Millisecond)
	elapsed := time.Since(started)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if elapsed > time.Second {
		t.Fatalf("handler took %v, want it bounded by the probe timeout", elapsed)
	}
}

func TestChecksRunConcurrently(t *testing.T) {
	// Four checks that each take 100ms would need 400ms serially.
	set := NewSet()
	for _, name := range []string{"a", "b", "c", "d"} {
		set.Add(name, func(ctx context.Context) error {
			select {
			case <-time.After(100 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}

	started := time.Now()
	rec := probe(set, 2*time.Second)
	elapsed := time.Since(started)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if elapsed > 300*time.Millisecond {
		t.Fatalf("handler took %v, want the checks run in parallel", elapsed)
	}
}

func TestTheProbeResultIsNotCached(t *testing.T) {
	set := NewSet()

	rec := probe(set, time.Second)

	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
}

func TestAReadinessCheckFlipsBackWhenTheDependencyReturns(t *testing.T) {
	var up atomic.Bool
	set := NewSet()
	set.Add("redis", func(context.Context) error {
		if up.Load() {
			return nil
		}
		return errors.New("connection refused")
	})

	if rec := probe(set, time.Second); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status while down = %d, want 503", rec.Code)
	}

	up.Store(true)

	if rec := probe(set, time.Second); rec.Code != http.StatusOK {
		t.Fatalf("status after recovery = %d, want 200", rec.Code)
	}
}
