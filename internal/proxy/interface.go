package proxy

import "net/http"

// Proxy represents an HTTP reverse proxy.
type Proxy interface {
	ServeHTTP(http.ResponseWriter, *http.Request)
}
