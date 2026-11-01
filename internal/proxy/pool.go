package proxy

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// Pool manages a collection of upstream target servers, balancing requests across
// healthy instances using round-robin or least-connections policies and tracking
// target health and in-flight requests.
type Pool struct {
	targets        []*url.URL
	policy         string
	counter        atomic.Uint64
	inFlight       []atomic.Int64
	mu             sync.RWMutex
	unhealthyUntil []time.Time
	// activeUp is the active health-check verdict per target. Targets start up and are
	// only changed by an active checker; routes without health checks never touch it.
	activeUp []bool
	cooldown time.Duration
}

// NewPool parses target URLs, validates the load balancing policy, and initializes
// an upstream pool with a default 30-second failure cooldown.
func NewPool(targets []string, policy string) (*Pool, error) {
	if len(targets) == 0 {
		return nil, errors.New("pool: must have at least one target")
	}

	parsedTargets := make([]*url.URL, len(targets))
	for i, target := range targets {
		u, err := url.Parse(target)
		if err != nil {
			return nil, fmt.Errorf("pool: invalid target %q: %w", target, err)
		}
		if u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("pool: target %q must have scheme and host", target)
		}
		parsedTargets[i] = u
	}

	if policy == "" {
		policy = "round_robin"
	}
	if policy != "round_robin" && policy != "least_connections" {
		return nil, fmt.Errorf("pool: unsupported load balancing policy %q", policy)
	}

	activeUp := make([]bool, len(parsedTargets))
	for i := range activeUp {
		activeUp[i] = true
	}

	return &Pool{
		targets:        parsedTargets,
		policy:         policy,
		inFlight:       make([]atomic.Int64, len(parsedTargets)),
		unhealthyUntil: make([]time.Time, len(parsedTargets)),
		activeUp:       activeUp,
		cooldown:       30 * time.Second,
	}, nil
}

// Next selects the next healthy upstream target using the configured policy,
// increments the selected target's in-flight request counter, and returns the target URL
// and its index. If all targets are unhealthy, ok is false.
func (p *Pool) Next() (target *url.URL, index int, ok bool) {
	if p == nil || len(p.targets) == 0 {
		return nil, -1, false
	}

	switch p.policy {
	case "least_connections":
		return p.nextLeastConnections()
	default:
		return p.nextRoundRobin()
	}
}

func (p *Pool) nextRoundRobin() (*url.URL, int, bool) {
	n := uint64(len(p.targets))
	start := p.counter.Add(1) - 1

	for i := uint64(0); i < n; i++ {
		candidate := int((start + i) % n)
		if p.isHealthy(candidate) {
			p.inFlight[candidate].Add(1)
			return p.targets[candidate], candidate, true
		}
	}

	return nil, -1, false
}

func (p *Pool) nextLeastConnections() (*url.URL, int, bool) {
	bestIndex := -1
	var minCount int64 = math.MaxInt64

	offset := int(p.counter.Add(1) - 1)
	n := len(p.targets)

	for i := 0; i < n; i++ {
		candidate := (offset + i) % n
		if !p.isHealthy(candidate) {
			continue
		}
		count := p.inFlight[candidate].Load()
		if count < minCount {
			minCount = count
			bestIndex = candidate
		}
	}

	if bestIndex == -1 {
		return nil, -1, false
	}

	p.inFlight[bestIndex].Add(1)
	return p.targets[bestIndex], bestIndex, true
}

// Done decrements the in-flight request count for the specified target index.
func (p *Pool) Done(index int) {
	if p == nil || index < 0 || index >= len(p.inFlight) {
		return
	}
	p.inFlight[index].Add(-1)
}

// ReportFailure marks the target at the given index as unhealthy until now + cooldown.
func (p *Pool) ReportFailure(index int) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if index >= 0 && index < len(p.unhealthyUntil) {
		p.unhealthyUntil[index] = time.Now().Add(p.cooldown)
	}
}

// ReportSuccess clears any unhealthy state for the target at the given index.
func (p *Pool) ReportSuccess(index int) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if index >= 0 && index < len(p.unhealthyUntil) {
		p.unhealthyUntil[index] = time.Time{}
	}
}

// SetActiveHealth records the active health-check verdict for a target. Reporting a target
// up also clears its passive failure cooldown: the successful probe is the recovery signal,
// so a recovered backend serves traffic again without waiting for the cooldown to expire.
// Reporting a target down keeps it out of rotation until a later probe marks it up.
func (p *Pool) SetActiveHealth(index int, up bool) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if index < 0 || index >= len(p.activeUp) {
		return
	}
	p.activeUp[index] = up
	if up {
		p.unhealthyUntil[index] = time.Time{}
	}
}

func (p *Pool) isHealthy(index int) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if index < 0 || index >= len(p.unhealthyUntil) {
		return false
	}

	if !p.activeUp[index] {
		return false
	}

	until := p.unhealthyUntil[index]
	if until.IsZero() {
		return true
	}
	return !time.Now().Before(until)
}
