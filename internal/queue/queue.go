package queue

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/syncronix-labs/aegora/internal/httperr"
	"github.com/syncronix-labs/aegora/internal/metrics"
	"github.com/syncronix-labs/aegora/internal/middleware"
)

// Priority represents the traffic classification tier for incoming requests.
type Priority int

const (
	// PriorityHigh indicates critical, latency-sensitive traffic (e.g. checkout, payments).
	PriorityHigh Priority = iota
	// PriorityNormal represents standard gateway traffic.
	PriorityNormal
	// PriorityLow represents bulk, background, or non-critical traffic (e.g. analytics).
	PriorityLow
)

// String returns the string representation of the priority level.
func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	default:
		return fmt.Sprintf("Priority(%d)", p)
	}
}

// Classify determines the request priority based on headers and path rules.
//
// Classification Precedence:
//  1. Header override: An explicit `X-Priority` header ("high", "low", or "normal",
//     case-insensitive) takes precedence over all path-based routing rules.
//  2. High-priority path prefix: If the request URL path begins with any prefix in highPaths,
//     the request is assigned PriorityHigh.
//  3. Low-priority path prefix: If the request URL path begins with any prefix in lowPaths,
//     the request is assigned PriorityLow.
//  4. Default tier: All remaining requests are assigned PriorityNormal.
func Classify(r *http.Request, highPaths, lowPaths []string) Priority {
	if r == nil {
		return PriorityNormal
	}

	if hdr := r.Header.Get("X-Priority"); hdr != "" {
		switch strings.ToLower(strings.TrimSpace(hdr)) {
		case "high":
			return PriorityHigh
		case "low":
			return PriorityLow
		case "normal":
			return PriorityNormal
		}
	}

	if r.URL != nil {
		path := r.URL.Path
		for _, prefix := range highPaths {
			if strings.HasPrefix(path, prefix) {
				return PriorityHigh
			}
		}
		for _, prefix := range lowPaths {
			if strings.HasPrefix(path, prefix) {
				return PriorityLow
			}
		}
	}

	return PriorityNormal
}

type ticketResult int

const (
	resultAdmitted ticketResult = iota
	resultDropped
)

type ticket struct {
	priority    Priority
	enqueueTime time.Time
	done        chan ticketResult
}

// Queue provides priority-based admission control with concurrency bounding and CoDel drop logic.
type Queue struct {
	mu              sync.Mutex
	highQueue       []*ticket
	normalQueue     []*ticket
	lowQueue        []*ticket
	maxInflight     int
	inflight        int
	maxDepth        int
	codel           *Codel
	stopCh          chan struct{}
	wakeCh          chan struct{}
	running         bool
	highServesCount int
	highPaths       []string
	lowPaths        []string
}

// NewQueue constructs a new admission control Queue.
// It panics if maxInflight or maxDepth are non-positive, or if c is nil.
func NewQueue(maxInflight, maxDepth int, c *Codel, highPaths, lowPaths []string) *Queue {
	if maxInflight <= 0 {
		panic("queue: maxInflight must be positive")
	}
	if maxDepth <= 0 {
		panic("queue: maxDepth must be positive")
	}
	if c == nil {
		panic("queue: codel must not be nil")
	}

	return &Queue{
		maxInflight: maxInflight,
		maxDepth:    maxDepth,
		codel:       c,
		stopCh:      make(chan struct{}),
		wakeCh:      make(chan struct{}, 1),
		highPaths:   highPaths,
		lowPaths:    lowPaths,
	}
}

// Start launches the background dispatcher loop. It is idempotent and a no-op if already running.
func (q *Queue) Start() {
	q.mu.Lock()
	if q.running {
		q.mu.Unlock()
		return
	}
	q.running = true
	q.mu.Unlock()

	go q.dispatchLoop()
}

// Stop terminates the dispatcher loop and unblocks all queued requests with resultDropped.
func (q *Queue) Stop() {
	q.mu.Lock()
	if !q.running {
		q.mu.Unlock()
		return
	}
	q.running = false
	close(q.stopCh)

	drain := func(slice []*ticket) {
		for _, t := range slice {
			select {
			case t.done <- resultDropped:
			default:
			}
		}
	}

	drain(q.highQueue)
	drain(q.normalQueue)
	drain(q.lowQueue)

	q.highQueue = nil
	q.normalQueue = nil
	q.lowQueue = nil
	q.mu.Unlock()

	q.notify()
}

// Admit returns an http.Handler middleware that enqueues the request and enforces admission control.
func (q *Queue) Admit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		priority := Classify(r, q.highPaths, q.lowPaths)

		q.mu.Lock()
		if !q.running {
			q.mu.Unlock()
			writeOverloadedJSON(w, r)
			return
		}

		// Enforce maximum queue depth per priority tier.
		var queueLen int
		switch priority {
		case PriorityHigh:
			queueLen = len(q.highQueue)
		case PriorityNormal:
			queueLen = len(q.normalQueue)
		case PriorityLow:
			queueLen = len(q.lowQueue)
		}

		if queueLen >= q.maxDepth {
			q.mu.Unlock()
			writeOverloadedJSON(w, r)
			return
		}

		t := &ticket{
			priority:    priority,
			enqueueTime: time.Now(),
			done:        make(chan ticketResult, 1),
		}

		switch priority {
		case PriorityHigh:
			q.highQueue = append(q.highQueue, t)
		case PriorityNormal:
			q.normalQueue = append(q.normalQueue, t)
		case PriorityLow:
			q.lowQueue = append(q.lowQueue, t)
		}
		q.mu.Unlock()

		q.notify()

		select {
		case res := <-t.done:
			if res == resultDropped {
				writeOverloadedJSON(w, r)
				return
			}

			// Request admitted: run the downstream handler.
			defer func() {
				q.mu.Lock()
				q.inflight--
				q.mu.Unlock()
				q.notify()
			}()

			next.ServeHTTP(w, r)

		case <-r.Context().Done():
			// Client cancelled while waiting: remove ticket from queue to avoid memory leaks.
			q.removeTicket(t)
		}
	})
}

// removeTicket removes a waiting ticket upon client context cancellation.
func (q *Queue) removeTicket(t *ticket) {
	q.mu.Lock()
	defer q.mu.Unlock()

	removeFrom := func(slice []*ticket) ([]*ticket, bool) {
		for i, item := range slice {
			if item == t {
				return append(slice[:i], slice[i+1:]...), true
			}
		}
		return slice, false
	}

	var removed bool
	switch t.priority {
	case PriorityHigh:
		q.highQueue, removed = removeFrom(q.highQueue)
	case PriorityNormal:
		q.normalQueue, removed = removeFrom(q.normalQueue)
	case PriorityLow:
		q.lowQueue, removed = removeFrom(q.lowQueue)
	}

	// If the ticket was already popped by the dispatcher concurrently and admitted,
	// decrement the inflight counter so capacity is not leaked.
	if !removed {
		select {
		case res := <-t.done:
			if res == resultAdmitted {
				q.inflight--
				q.notify()
			}
		default:
		}
	}
}

// popLocked pops the next ticket under lock, respecting priority and starvation rules.
func (q *Queue) popLocked() *ticket {
	hasHigh := len(q.highQueue) > 0
	hasNormal := len(q.normalQueue) > 0
	hasLow := len(q.lowQueue) > 0

	if !hasHigh && !hasNormal && !hasLow {
		return nil
	}

	// Starvation guard:
	// Serve a low-priority ticket at least once every 10 consecutive high-priority serves.
	//
	// Tradeoff:
	// Strict priority ordering guarantees minimal latency for high-priority requests but risks
	// indefinite starvation of low-priority queues during sustained traffic peaks.
	// The starvation guard bounds low-priority wait times by guaranteeing at least 1 low-priority
	// dequeue per 10 high-priority dequeues (approx. 9% bandwidth guarantee under dual saturation),
	// introducing minimal jitter to high-priority requests while maintaining overall system liveness.
	if q.highServesCount >= 10 && hasLow {
		q.highServesCount = 0
		t := q.lowQueue[0]
		q.lowQueue = q.lowQueue[1:]
		return t
	}

	if hasHigh {
		q.highServesCount++
		t := q.highQueue[0]
		q.highQueue = q.highQueue[1:]
		return t
	}

	q.highServesCount = 0
	if hasNormal {
		t := q.normalQueue[0]
		q.normalQueue = q.normalQueue[1:]
		return t
	}

	if hasLow {
		t := q.lowQueue[0]
		q.lowQueue = q.lowQueue[1:]
		return t
	}

	return nil
}

// pop retrieves the next ticket according to priority ordering and starvation rules.
// Exposed as an unexported in-package method for unit testing.
func (q *Queue) pop() *ticket {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.popLocked()
}

// notify wakes the dispatcher without blocking.
func (q *Queue) notify() {
	select {
	case q.wakeCh <- struct{}{}:
	default:
	}
}

// dispatchLoop continuously pops tickets when inflight < maxInflight, consults CoDel,
// and notifies waiting requests.
func (q *Queue) dispatchLoop() {
	for {
		q.mu.Lock()
		if !q.running {
			q.mu.Unlock()
			return
		}

		if q.inflight >= q.maxInflight {
			q.mu.Unlock()
			select {
			case <-q.stopCh:
				return
			case <-q.wakeCh:
				continue
			}
		}

		t := q.popLocked()
		if t == nil {
			q.mu.Unlock()
			select {
			case <-q.stopCh:
				return
			case <-q.wakeCh:
				continue
			}
		}

		// Mutex unlocked while evaluating CoDel and notifying ticket
		q.mu.Unlock()

		now := time.Now()
		sojourn := now.Sub(t.enqueueTime)
		if q.codel != nil && q.codel.ShouldDrop(sojourn) {
			t.done <- resultDropped
			continue
		}

		q.mu.Lock()
		q.inflight++
		q.mu.Unlock()

		t.done <- resultAdmitted
	}
}

func writeOverloadedJSON(w http.ResponseWriter, r *http.Request) {
	metrics.MarkQueueDropped(w.Header())
	httperr.Write(w, http.StatusServiceUnavailable, "server overloaded", middleware.GetRequestID(r))
}
