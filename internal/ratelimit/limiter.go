// Package ratelimit provides abstractions and implementations for
// request rate limiting used by Aegora.
package ratelimit

import "context"

// Limiter represents a generic rate limiter implementation.
type Limiter interface {
	Allow(ctx context.Context) bool
}
