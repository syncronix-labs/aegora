// Package reliability provides Layer 7 fault-tolerance primitives for Aegora,
// including circuit breaking and automatic retry with exponential backoff.
package reliability

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// State represents the current operational mode of a circuit breaker.
type State int

const (
	// StateClosed allows requests to proceed normally, tracking consecutive failures.
	StateClosed State = iota
	// StateOpen fails fast without sending traffic to the upstream service.
	StateOpen
	// StateHalfOpen permits limited probe requests to test whether the upstream has recovered.
	StateHalfOpen
)

// String returns the human-readable name of the circuit breaker state.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return fmt.Sprintf("State(%d)", s)
	}
}

// ErrCircuitOpen is returned by BreakerTransport when the circuit breaker is open
// and rejects requests without calling the underlying transport.
var ErrCircuitOpen = errors.New("circuit breaker open")

// CircuitBreaker tracks upstream service health and trips open when failures
// exceed a configured threshold, preventing cascading failures across the gateway.
type CircuitBreaker struct {
	mu                  sync.Mutex
	state               State
	consecutiveFailures int
	halfOpenInFlight    int
	halfOpenSuccesses   int
	lastTransition      time.Time
	failureThreshold    int
	cooldown            time.Duration
	halfOpenMaxProbes   int
	successThreshold    int
}

// NewCircuitBreaker constructs a circuit breaker with the given thresholds and cooldown.
// It panics if any argument is non-positive.
func NewCircuitBreaker(
	failureThreshold int,
	cooldown time.Duration,
	halfOpenMaxProbes int,
	successThreshold int,
) *CircuitBreaker {
	if failureThreshold <= 0 {
		panic("reliability: failureThreshold must be positive")
	}
	if cooldown <= 0 {
		panic("reliability: cooldown must be positive")
	}
	if halfOpenMaxProbes <= 0 {
		panic("reliability: halfOpenMaxProbes must be positive")
	}
	if successThreshold <= 0 {
		panic("reliability: successThreshold must be positive")
	}

	return &CircuitBreaker{
		state:             StateClosed,
		lastTransition:    time.Now(),
		failureThreshold:  failureThreshold,
		cooldown:          cooldown,
		halfOpenMaxProbes: halfOpenMaxProbes,
		successThreshold:  successThreshold,
	}
}

// Allow reports whether a request is permitted to proceed.
// In Closed state, all requests are allowed.
// In Open state, requests are rejected until cooldown elapses, transitioning to Half-Open.
// In Half-Open state, requests are allowed only while in-flight probes < halfOpenMaxProbes.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		if now.Sub(cb.lastTransition) >= cb.cooldown {
			cb.state = StateHalfOpen
			cb.lastTransition = now
			cb.halfOpenInFlight = 1
			cb.halfOpenSuccesses = 0
			return true
		}
		return false
	case StateHalfOpen:
		if cb.halfOpenInFlight < cb.halfOpenMaxProbes {
			cb.halfOpenInFlight++
			return true
		}
		return false
	default:
		return false
	}
}

// CooldownRemaining reports how much of the open-state cooldown is left before a
// half-open probe will be allowed. It returns zero whenever the breaker is not
// open: a closed breaker is not holding traffic back, and a half-open one has
// already admitted its probes, so neither has a point in time to advertise.
func (cb *CircuitBreaker) CooldownRemaining() time.Duration {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state != StateOpen {
		return 0
	}
	remaining := cb.cooldown - time.Since(cb.lastTransition)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// RecordSuccess records a successful request.
// In Closed state, it resets the consecutive failure counter.
// In Half-Open state, it tracks successful probes and closes the breaker when reaching successThreshold.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		cb.consecutiveFailures = 0
	case StateHalfOpen:
		if cb.halfOpenInFlight > 0 {
			cb.halfOpenInFlight--
		}
		cb.halfOpenSuccesses++
		if cb.halfOpenSuccesses >= cb.successThreshold {
			cb.state = StateClosed
			cb.consecutiveFailures = 0
			cb.halfOpenInFlight = 0
			cb.halfOpenSuccesses = 0
			cb.lastTransition = time.Now()
		}
	}
}

// RecordFailure records a failed request (transport error or 5xx HTTP response).
// In Closed state, it increments consecutive failures and trips open upon reaching failureThreshold.
// In Half-Open state, any failure immediately trips the circuit open again.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case StateClosed:
		cb.consecutiveFailures++
		if cb.consecutiveFailures >= cb.failureThreshold {
			cb.state = StateOpen
			cb.lastTransition = now
			cb.consecutiveFailures = 0
			cb.halfOpenInFlight = 0
			cb.halfOpenSuccesses = 0
		}
	case StateHalfOpen:
		cb.state = StateOpen
		cb.lastTransition = now
		cb.consecutiveFailures = 0
		cb.halfOpenInFlight = 0
		cb.halfOpenSuccesses = 0
	}
}

// releaseProbe frees a Half-Open in-flight probe slot without recording a
// success or failure outcome. It is used for errors that are the client's
// fault rather than the upstream's, so they must not influence the breaker's
// state while still not leaking the probe slot reserved by Allow().
func (cb *CircuitBreaker) releaseProbe() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == StateHalfOpen && cb.halfOpenInFlight > 0 {
		cb.halfOpenInFlight--
	}
}

// State returns the current state of the circuit breaker.
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// BreakerTransport wraps an http.RoundTripper with circuit breaker enforcement.
// If the breaker is open, it returns ErrCircuitOpen without executing the base transport.
type BreakerTransport struct {
	Base    http.RoundTripper
	Breaker *CircuitBreaker
}

// RoundTrip executes the HTTP request through the circuit breaker.
// Transport errors and responses with HTTP status >= 500 are recorded as failures;
// all other responses are recorded as successes. *http.MaxBytesError (the client
// exceeded server.max_request_body_bytes) is the client's fault, not the
// upstream's, so it releases the probe slot instead of recording a failure.
func (b *BreakerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if b.Breaker != nil && !b.Breaker.Allow() {
		return nil, ErrCircuitOpen
	}

	base := b.Base
	if base == nil {
		base = http.DefaultTransport
	}

	resp, err := base.RoundTrip(req)
	if err != nil {
		if b.Breaker != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				b.Breaker.releaseProbe()
			} else {
				b.Breaker.RecordFailure()
			}
		}
		return nil, err
	}

	if resp.StatusCode >= http.StatusInternalServerError {
		if b.Breaker != nil {
			b.Breaker.RecordFailure()
		}
		return resp, nil
	}

	if b.Breaker != nil {
		b.Breaker.RecordSuccess()
	}
	return resp, nil
}
