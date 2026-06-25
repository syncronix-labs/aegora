package proxy

import (
	"net/http"
	"net/http/httputil"
	"net/url"
)

// ReverseProxy wraps httputil.ReverseProxy and provides
// Aegora-specific request handling behavior.
type ReverseProxy struct {
	proxy *httputil.ReverseProxy
}

// New creates a reverse proxy for the specified target URL.
func New(target string) (*ReverseProxy, error) {
	targetURL, err := url.Parse(target)
	if err != nil {
		return nil, err
	}

	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(targetURL)

			// Remove any spoofed forwarding headers.
			pr.Out.Header.Del("X-Forwarded-For")
			pr.Out.Header.Del("X-Forwarded-Host")
			pr.Out.Header.Del("X-Forwarded-Proto")

			pr.SetXForwarded()

			pr.Out.Header.Set("X-Aegora", "true")
		},
		Transport: NewTransport(),
	}

	return &ReverseProxy{
		proxy: rp,
	}, nil
}

func (r *ReverseProxy) ServeHTTP(
	w http.ResponseWriter,
	req *http.Request,
) {
	r.proxy.ServeHTTP(w, req)
}
