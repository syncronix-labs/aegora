// Package shedding provides adaptive load shedding with an Additive Increase /
// Multiplicative Decrease (AIMD) controller for Layer 7 gateway protection.
package shedding

import (
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syncronix-labs/aegora/internal/httperr"
	"github.com/syncronix-labs/aegora/internal/metrics"
	"github.com/syncronix-labs/aegora/internal/middleware"
	"github.com/syncronix-labs/aegora/internal/queue"
)

// ringBufferSize defines the fixed sample window for latency percentile calculations.
const ringBufferSize = 1024

// defaultPriorityFloor defines the minimum admission rate above which high-priority
// traffic is guaranteed admission without probabilistic load shedding.
const defaultPriorityFloor = 0.1

// Signals captures real-time telemetry used by the load shedding controller.
//
// Future extensions:
//  1. Error rate (e.g. upstream 5xx response ratio): can trigger proactive load shedding
//     when backends fail fast or crash under load without elevating latency.
//  2. Queue depth: waiting ticket count from the admission queue can detect standing
//     backlog before p99 latency fully reflects downstream queueing delays.
type Signals struct {
	P99Latency time.Duration
	InFlight   int
}

// Priority aliases queue.Priority for classification in shedding decisions.
type Priority = queue.Priority

const (
	// PriorityHigh indicates critical traffic (e.g. payment endpoints).
	PriorityHigh = queue.PriorityHigh
	// PriorityNormal indicates standard traffic (default).
	PriorityNormal = queue.PriorityNormal
	// PriorityLow indicates bulk or non-urgent traffic (e.g. analytics).
	PriorityLow = queue.PriorityLow
)

// Controller implements an adaptive load shedding controller governed by an AIMD
// (Additive Increase / Multiplicative Decrease) control law.
//
// Algorithm & Control Dynamics:
// Inspired by TCP congestion control (Jacobson 1988), the controller continuously
// samples end-to-end request latency into a fixed-capacity ring buffer to estimate
// the 99th-percentile (p99) latency of downstream services.
//
// Periodically (throttled to Interval):
//  1. Multiplicative Decrease: If p99 latency breaches targetLatency, the admission rate
//     is reduced by 10% (admitRate = max(minRate, admitRate * 0.9)). This rapid, multiplicative
//     throttling sheds excess load exponentially to pull saturated backends out of congestion collapse.
//  2. Additive Increase: When p99 latency remains within acceptable bounds (<= targetLatency),
//     the admission rate is incremented linearly by 5% (admitRate = min(1.0, admitRate + 0.05)).
//     This conservative increase gently probes available backend headroom without inducing latency spikes.
//  3. Priority Differentiation: Requests are classified into priority tiers (High, Normal, Low).
//     While admitRate >= priorityFloor (0.1), High-priority requests bypass probabilistic drops,
//     ensuring critical traffic is protected until the gateway enters catastrophic distress.
type Controller struct {
	mu            sync.Mutex
	admitRate     float64
	targetLatency time.Duration
	minRate       float64
	interval      time.Duration
	lastAdjust    time.Time
	priorityFloor float64

	latencies [ringBufferSize]time.Duration
	head      int
	fillCount int

	inflight  atomic.Int64
	highPaths []string
	lowPaths  []string
}

// NewController constructs a new adaptive load shedding Controller.
// It initializes admitRate to 1.0 (100% admitted).
// It panics if targetLatency <= 0, interval <= 0, or minRate is not strictly between 0.0 and 1.0.
func NewController(targetLatency, interval time.Duration, minRate float64, highPaths, lowPaths []string) *Controller {
	if targetLatency <= 0 {
		panic("shedding: targetLatency must be positive")
	}
	if interval <= 0 {
		panic("shedding: interval must be positive")
	}
	if minRate <= 0.0 || minRate >= 1.0 {
		panic("shedding: minRate must be between 0.0 and 1.0 (exclusive)")
	}

	return &Controller{
		admitRate:     1.0,
		targetLatency: targetLatency,
		minRate:       minRate,
		interval:      interval,
		lastAdjust:    time.Now(),
		priorityFloor: defaultPriorityFloor,
		highPaths:     highPaths,
		lowPaths:      lowPaths,
	}
}

// AdmitRate returns the current admission probability in [0.0, 1.0].
func (c *Controller) AdmitRate() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.admitRate
}

// Signals returns a snapshot of the controller's current telemetry signals.
func (c *Controller) Signals() Signals {
	c.mu.Lock()
	p99 := c.p99Locked()
	c.mu.Unlock()

	return Signals{
		P99Latency: p99,
		InFlight:   int(c.inflight.Load()),
	}
}

// recordLatency inserts a completed request duration into the ring buffer.
func (c *Controller) recordLatency(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.latencies[c.head] = d
	c.head = (c.head + 1) % ringBufferSize
	if c.fillCount < ringBufferSize {
		c.fillCount++
	}
}

// p99Locked computes the 99th-percentile latency from the ring buffer.
// Must be called with c.mu held.
func (c *Controller) p99Locked() time.Duration {
	if c.fillCount == 0 {
		return 0
	}

	copied := make([]time.Duration, c.fillCount)
	copy(copied, c.latencies[:c.fillCount])

	slices.Sort(copied)

	idx := int(float64(len(copied)-1) * 0.99)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(copied) {
		idx = len(copied) - 1
	}
	return copied[idx]
}

// maybeAdjust evaluates the AIMD control law if interval has elapsed since lastAdjust.
func (c *Controller) maybeAdjust(p99 time.Duration) {
	c.maybeAdjustAt(time.Now(), p99)
}

// maybeAdjustAt evaluates the AIMD control law for a specific point in time.
func (c *Controller) maybeAdjustAt(now time.Time, p99 time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.lastAdjust.IsZero() && now.Sub(c.lastAdjust) < c.interval {
		return
	}
	c.lastAdjust = now

	if p99 > c.targetLatency {
		// Multiplicative decrease: quickly shed load when congested
		c.admitRate *= 0.9
		if c.admitRate < c.minRate {
			c.admitRate = c.minRate
		}
	} else {
		// Additive increase: cautiously probe available capacity
		c.admitRate += 0.05
		if c.admitRate > 1.0 {
			c.admitRate = 1.0
		}
	}
}

// decide reports whether a request of the given priority should be admitted
// based on a pseudo-random roll in [0.0, 1.0).
//
// Decision Rule:
//   - General admission: admitted if roll < admitRate.
//   - Priority floor: High-priority requests are additionally admitted while
//     admitRate >= priorityFloor (0.1, or 0.2 in extended research baselines),
//     shielding critical paths during moderate overload.
func (c *Controller) decide(p Priority, roll float64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if roll < c.admitRate {
		return true
	}

	if p == PriorityHigh && c.admitRate >= c.priorityFloor {
		return true
	}

	return false
}

// Admit wraps downstream HTTP handlers with adaptive load shedding.
func (c *Controller) Admit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.inflight.Add(1)
		defer c.inflight.Add(-1)

		p := queue.Classify(r, c.highPaths, c.lowPaths)

		c.mu.Lock()
		p99 := c.p99Locked()
		c.mu.Unlock()

		c.maybeAdjust(p99)

		roll := rand.Float64()
		if !c.decide(p, roll) {
			metrics.MarkShed(w.Header())
			w.Header().Set("Retry-After", "1")
			httperr.Write(w, http.StatusServiceUnavailable, "server overloaded", middleware.GetRequestID(r))
			return
		}

		start := time.Now()
		defer func() {
			c.recordLatency(time.Since(start))
		}()

		next.ServeHTTP(w, r)
	})
}
