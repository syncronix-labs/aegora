package middleware

import "net/http"

// Chain represents an ordered middleware pipeline.
type Chain struct {
	middlewares []Middleware
}

// New creates a middleware chain.
func New(mw ...Middleware) *Chain {
	return &Chain{
		middlewares: mw,
	}
}

// Then applies the middleware chain to the provided handler.
func (c *Chain) Then(h http.Handler) http.Handler {
	for i := len(c.middlewares) - 1; i >= 0; i-- {
		h = c.middlewares[i](h)
	}

	return h
}
