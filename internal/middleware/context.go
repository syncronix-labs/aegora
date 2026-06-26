package middleware

import "net/http"

// RequestContext carries request-scoped state through the middleware chain.
type RequestContext struct {
	Request  *http.Request
	Response http.ResponseWriter

	Route string

	RequestID string

	Metadata map[string]any
}
