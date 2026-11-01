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
	weights        []int
	weighted       bool
	wrrMu          sync.Mutex
	currentWeights []int
	counter        atomic.Uint64
	inFlight       []atomic.Int64
	mu             sync.RWMutex
	unhealthyUntil []time.Time
	cooldown       time.Duration
}

// NewPool parses target URLs, validates the load balancing policy, and initializes
// an upstream pool with a default 30-second failure cooldown.
func NewPool(targets []string, policy string) (*Pool, error) {
	return NewPoolWithWeights(targets, policy, nil)
}

// NewPoolWithWeights initializes a pool with optional per-target weights keyed by
// target URL. Weights must be positive, are only supported with the round_robin
// policy, and default to 1 for targets without an entry.
func NewPoolWithWeights(targets []string, policy string, weights map[string]int) (*Pool, error) {
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

	parsedWeights := make([]int, len(parsedTargets))
	for i := range parsedWeights {
		parsedWeights[i] = 1
	}
	weighted := false
	if len(weights) > 0 {
		if policy != "round_robin" {
			return nil, fmt.Errorf("pool: weights are only supported with policy %q, got %q", "round_robin", policy)
		}
		indexByTarget := make(map[string]int, len(targets))
		for i, target := range targets {
			indexByTarget[target] = i
		}
		for target, weight := range weights {
			i, found := indexByTarget[target]
			if !found {
				return nil, fmt.Errorf("pool: weight configured for unknown target %q", target)
			}
			if weight <= 0 {
				return nil, fmt.Errorf("pool: weight for target %q must be positive, got %d", target, weight)
			}
			parsedWeights[i] = weight
			if weight != 1 {
				weighted = true
			}
		}
	}

	return &Pool{
		targets:        parsedTargets,
		policy:         policy,
		weights:        parsedWeights,
		weighted:       weighted,
		currentWeights: make([]int, len(parsedTargets)),
		inFlight:       make([]atomic.Int64, len(parsedTargets)),
		unhealthyUntil: make([]time.Time, len(parsedTargets)),
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
		if p.weighted {
			return p.nextWeightedRoundRobin()
		}
		return p.nextRoundRobin()
	}
}

// nextWeightedRoundRobin implements the smooth weighted round-robin algorithm
// (as used by nginx): each healthy target's current weight grows by its
// configured weight, the highest current weight is selected, and the total
// weight of the healthy targets is subtracted from it. This yields an even
// interleaving of targets proportional to their weights.
func (p *Pool) nextWeightedRoundRobin() (*url.URL, int, bool) {
	p.wrrMu.Lock()
	defer p.wrrMu.Unlock()

	bestIndex := -1
	total := 0

	for i := range p.targets {
		if !p.isHealthy(i) {
			continue
		}
		p.currentWeights[i] += p.weights[i]
		total += p.weights[i]
		if bestIndex == -1 || p.currentWeights[i] > p.currentWeights[bestIndex] {
			bestIndex = i
		}
	}

	if bestIndex == -1 {
		return nil, -1, false
	}

	p.currentWeights[bestIndex] -= total
	p.inFlight[bestIndex].Add(1)
	return p.targets[bestIndex], bestIndex, true
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

func (p *Pool) isHealthy(index int) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if index < 0 || index >= len(p.unhealthyUntil) {
		return false
	}

	until := p.unhealthyUntil[index]
	if until.IsZero() {
		return true
	}
	return !time.Now().Before(until)
}
