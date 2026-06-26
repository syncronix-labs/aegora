package middleware

import "net/http"

// Handler represents the final handler in the middleware chain.
type Handler http.Handler

// Middleware wraps an HTTP handler with additional behavior.
type Middleware func(http.Handler) http.Handler
