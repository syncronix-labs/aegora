package reliability

import (
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestState_String(t *testing.T) {
	tests := []struct {
		state State
		want  string
	}{
		{StateClosed, "closed"},
		{StateOpen, "open"},
		{StateHalfOpen, "half-open"},
		{State(99), "State(99)"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("State(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}

func TestCircuitBreaker_TripsAfterNConsecutiveFailures(t *testing.T) {
	cb := NewCircuitBreaker(3, time.Minute, 1, 1)

	if !cb.Allow() {
		t.Fatal("expected closed breaker to allow requests")
	}

	cb.RecordFailure()
	if cb.State() != StateClosed {
		t.Fatalf("expected state closed after 1 failure, got %v", cb.State())
	}

	cb.RecordFailure()
	if cb.State() != StateClosed {
		t.Fatalf("expected state closed after 2 failures, got %v", cb.State())
	}

	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatalf("expected state open after 3 failures, got %v", cb.State())
	}

	if cb.Allow() {
		t.Fatal("expected open breaker to reject requests")
	}
}

func TestCircuitBreaker_CooldownRemaining(t *testing.T) {
	cooldown := 400 * time.Millisecond
	cb := NewCircuitBreaker(1, cooldown, 1, 1)

	if got := cb.CooldownRemaining(); got != 0 {
		t.Fatalf("expected no cooldown while closed, got %v", got)
	}

	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatalf("expected state open, got %v", cb.State())
	}

	remaining := cb.CooldownRemaining()
	if remaining <= 0 {
		t.Fatalf("expected a positive cooldown while open, got %v", remaining)
	}
	if remaining > cooldown {
		t.Fatalf("expected the cooldown to be at most %v, got %v", cooldown, remaining)
	}

	// The remainder has to shrink as the cooldown elapses, otherwise it cannot
	// tell a client anything the configured cooldown would not.
	time.Sleep(150 * time.Millisecond)
	shrunk := cb.CooldownRemaining()
	if shrunk >= remaining {
		t.Fatalf("expected the cooldown to shrink from %v, got %v", remaining, shrunk)
	}

	// Once the cooldown has elapsed the breaker admits a probe on the next
	// Allow, and a breaker that is no longer holding traffic back has no
	// backoff to advertise.
	time.Sleep(cooldown)
	if !cb.Allow() {
		t.Fatal("expected the breaker to admit a probe after the cooldown")
	}
	if got := cb.CooldownRemaining(); got != 0 {
		t.Fatalf("expected no cooldown once half-open, got %v", got)
	}
}

func TestCircuitBreaker_SuccessResetsConsecutiveFailures(t *testing.T) {
	cb := NewCircuitBreaker(3, time.Minute, 1, 1)

	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != StateClosed {
		t.Fatalf("expected closed state, got %v", cb.State())
	}

	// Success resets consecutive failures to 0
	cb.RecordSuccess()

	// 2 more failures should not trip breaker because count was reset
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != StateClosed {
		t.Fatalf("expected closed state after reset and 2 failures, got %v", cb.State())
	}

	// 3rd failure trips the breaker
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatalf("expected open state after 3rd failure, got %v", cb.State())
	}
}

func TestCircuitBreaker_CooldownTransitionsToHalfOpenAndSuccessCloses(t *testing.T) {
	const cooldown = 30 * time.Millisecond
	cb := NewCircuitBreaker(2, cooldown, 1, 1)

	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatalf("expected open state, got %v", cb.State())
	}

	// Immediately, should reject
	if cb.Allow() {
		t.Fatal("expected rejection while in cooldown")
	}

	// Wait past cooldown
	time.Sleep(cooldown + 10*time.Millisecond)

	// Next Allow should transition to half-open
	if !cb.Allow() {
		t.Fatal("expected probe request to be allowed after cooldown")
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected state half-open, got %v", cb.State())
	}

	// Probe success closes breaker
	cb.RecordSuccess()
	if cb.State() != StateClosed {
		t.Fatalf("expected state closed after probe success, got %v", cb.State())
	}

	if !cb.Allow() {
		t.Fatal("expected closed breaker to allow requests")
	}
}

func TestCircuitBreaker_ProbeFailureReOpensImmediately(t *testing.T) {
	const cooldown = 20 * time.Millisecond
	cb := NewCircuitBreaker(2, cooldown, 1, 1)

	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatalf("expected open state, got %v", cb.State())
	}

	time.Sleep(cooldown + 10*time.Millisecond)

	if !cb.Allow() {
		t.Fatal("expected probe request to be allowed")
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected state half-open, got %v", cb.State())
	}

	// Probe fails -> should immediately re-open
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatalf("expected open state after probe failure, got %v", cb.State())
	}

	if cb.Allow() {
		t.Fatal("expected open breaker to reject requests")
	}
}

func TestCircuitBreaker_HalfOpenMaxProbesAndSuccessThreshold(t *testing.T) {
	const cooldown = 20 * time.Millisecond
	cb := NewCircuitBreaker(1, cooldown, 2, 2)

	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatalf("expected open state, got %v", cb.State())
	}

	time.Sleep(cooldown + 10*time.Millisecond)

	// First probe allowed
	if !cb.Allow() {
		t.Fatal("expected probe 1 to be allowed")
	}
	// Second probe allowed (max probes = 2)
	if !cb.Allow() {
		t.Fatal("expected probe 2 to be allowed")
	}
	// Third probe denied because halfOpenInFlight == 2 == halfOpenMaxProbes
	if cb.Allow() {
		t.Fatal("expected probe 3 to be rejected when in-flight probes reach max")
	}

	// Complete probe 1 successfully -> success count 1/2, in-flight 1
	cb.RecordSuccess()
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected half-open state before reaching success threshold, got %v", cb.State())
	}

	// Complete probe 2 successfully -> success count 2/2 -> state closed!
	cb.RecordSuccess()
	if cb.State() != StateClosed {
		t.Fatalf("expected closed state after reaching success threshold, got %v", cb.State())
	}
}

func TestCircuitBreaker_PanicsOnNonPositive(t *testing.T) {
	tests := []struct {
		name              string
		failureThreshold  int
		cooldown          time.Duration
		halfOpenMaxProbes int
		successThreshold  int
	}{
		{"zero failureThreshold", 0, time.Second, 1, 1},
		{"negative failureThreshold", -1, time.Second, 1, 1},
		{"zero cooldown", 1, 0, 1, 1},
		{"negative cooldown", 1, -time.Second, 1, 1},
		{"zero halfOpenMaxProbes", 1, time.Second, 0, 1},
		{"negative halfOpenMaxProbes", 1, time.Second, -1, 1},
		{"zero successThreshold", 1, time.Second, 1, 0},
		{"negative successThreshold", 1, time.Second, 1, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected NewCircuitBreaker to panic on non-positive arguments")
				}
			}()

			_ = NewCircuitBreaker(tt.failureThreshold, tt.cooldown, tt.halfOpenMaxProbes, tt.successThreshold)
		})
	}
}

func TestCircuitBreaker_ConcurrentRace(t *testing.T) {
	_ = t
	cb := NewCircuitBreaker(5, 5*time.Millisecond, 2, 2)

	const workers = 20
	const iterations = 200

	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if cb.Allow() {
					if (id+i)%3 == 0 {
						cb.RecordFailure()
					} else {
						cb.RecordSuccess()
					}
				}
			}
		}(w)
	}

	wg.Wait()
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestBreakerTransport(t *testing.T) {
	t.Run("open breaker returns ErrCircuitOpen without calling base", func(t *testing.T) {
		cb := NewCircuitBreaker(1, time.Minute, 1, 1)
		cb.RecordFailure() // open breaker

		var called atomic.Bool
		stub := roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			called.Store(true)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(http.NoBody),
			}, nil
		})

		bt := &BreakerTransport{
			Base:    stub,
			Breaker: cb,
		}

		req, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
		resp, err := bt.RoundTrip(req)
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("expected ErrCircuitOpen, got %v", err)
		}
		if resp != nil {
			t.Fatalf("expected nil response, got %v", resp)
		}
		if called.Load() {
			t.Fatal("expected base transport NOT to be called when breaker is open")
		}
	})

	t.Run("5xx records failure", func(t *testing.T) {
		cb := NewCircuitBreaker(2, time.Minute, 1, 1)

		stub := roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(http.NoBody),
			}, nil
		})

		bt := &BreakerTransport{Base: stub, Breaker: cb}
		req, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)

		// 1st 500 response
		resp1, _ := bt.RoundTrip(req)
		if resp1 != nil && resp1.Body != nil {
			_ = resp1.Body.Close()
		}
		if cb.State() != StateClosed {
			t.Fatalf("expected closed state after 1st 500, got %v", cb.State())
		}

		// 2nd 500 response -> trips breaker
		resp2, _ := bt.RoundTrip(req)
		if resp2 != nil && resp2.Body != nil {
			_ = resp2.Body.Close()
		}
		if cb.State() != StateOpen {
			t.Fatalf("expected open state after 2nd 500, got %v", cb.State())
		}
	})

	t.Run("200 records success", func(t *testing.T) {
		cb := NewCircuitBreaker(2, time.Minute, 1, 1)
		cb.RecordFailure() // 1 failure

		stub := roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(http.NoBody),
			}, nil
		})

		bt := &BreakerTransport{Base: stub, Breaker: cb}
		req, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)

		resp, _ := bt.RoundTrip(req)
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		// Success should have reset failure count to 0
		cb.RecordFailure()
		if cb.State() != StateClosed {
			t.Fatalf("expected state to remain closed because previous 200 reset failure count, got %v", cb.State())
		}
	})

	t.Run("transport error records failure", func(t *testing.T) {
		cb := NewCircuitBreaker(1, time.Minute, 1, 1)

		stub := roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			return nil, errors.New("connection reset by peer")
		})

		bt := &BreakerTransport{Base: stub, Breaker: cb}
		req, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)

		resp, err := bt.RoundTrip(req)
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if err == nil {
			t.Fatal("expected transport error, got nil")
		}
		if cb.State() != StateOpen {
			t.Fatalf("expected open breaker after transport error, got %v", cb.State())
		}
	})

	t.Run("oversized chunked body error does not trip closed breaker", func(t *testing.T) {
		cb := NewCircuitBreaker(1, time.Minute, 1, 1)

		stub := roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			return nil, &http.MaxBytesError{Limit: 10}
		})

		bt := &BreakerTransport{Base: stub, Breaker: cb}
		req, _ := http.NewRequest(http.MethodPost, "http://example.com", nil)

		resp, err := bt.RoundTrip(req)
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		var maxBytesErr *http.MaxBytesError
		if !errors.As(err, &maxBytesErr) {
			t.Fatalf("expected *http.MaxBytesError, got %v", err)
		}
		if cb.State() != StateClosed {
			t.Fatalf("expected breaker to remain closed after client-fault oversized body error, got %v", cb.State())
		}
	})

	t.Run("oversized chunked body error in half-open does not consume the probe outcome", func(t *testing.T) {
		cb := NewCircuitBreaker(1, 10*time.Millisecond, 1, 1)
		cb.RecordFailure() // open breaker
		if cb.State() != StateOpen {
			t.Fatalf("expected open state, got %v", cb.State())
		}

		time.Sleep(20 * time.Millisecond)

		stub := roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			return nil, &http.MaxBytesError{Limit: 10}
		})
		bt := &BreakerTransport{Base: stub, Breaker: cb}
		req, _ := http.NewRequest(http.MethodPost, "http://example.com", nil)

		resp, err := bt.RoundTrip(req) // consumes the half-open probe slot
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		var maxBytesErr *http.MaxBytesError
		if !errors.As(err, &maxBytesErr) {
			t.Fatalf("expected *http.MaxBytesError, got %v", err)
		}
		if cb.State() != StateHalfOpen {
			t.Fatalf("expected breaker to remain half-open after client-fault oversized body error, got %v", cb.State())
		}

		// A following healthy probe must still be allowed and should close the breaker.
		if !cb.Allow() {
			t.Fatal("expected healthy probe to be allowed after oversized-body error released its probe slot")
		}
		cb.RecordSuccess()
		if cb.State() != StateClosed {
			t.Fatalf("expected breaker to close after healthy probe following oversized-body error, got %v", cb.State())
		}
	})
}
