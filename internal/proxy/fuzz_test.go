package proxy

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// fuzzRouter builds a router with a prefix route, a longer prefix route and a
// regex route, so both matching strategies are exercised by the same input.
func fuzzRouter(t testing.TB) *Router {
	t.Helper()

	router, err := NewRouter([]RouteConfig{
		{Name: "api", PathPrefix: "/api", Targets: []string{"http://127.0.0.1:1"}, Methods: []string{http.MethodGet, http.MethodPost}},
		{Name: "versioned", PathPrefix: "/api/v1", Targets: []string{"http://127.0.0.1:1"}},
		{Name: "files", PathRegexPattern: `^/files/[0-9]+$`, Targets: []string{"http://127.0.0.1:1"}},
		{Name: "root", PathPrefix: "/", Targets: []string{"http://127.0.0.1:1"}},
	})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return router
}

// FuzzRouterMatch asserts that matching an arbitrary path never panics and never
// returns a route from outside the table it was built with.
func FuzzRouterMatch(f *testing.F) {
	seeds := []struct{ method, path, query string }{
		{http.MethodGet, "/", ""},
		{http.MethodPost, "/api/v1/users", "a=1"},
		{http.MethodGet, "/files/42", ""},
		{http.MethodGet, "/files/42/../../etc/passwd", ""},
		{http.MethodGet, "/api/%2e%2e/%2f", "q=%"},
		{http.MethodGet, "/" + strings.Repeat("a", 4096), ""},
		{http.MethodGet, "/héllo/世界", ""},
		{"", "", ""},
	}
	for _, seed := range seeds {
		f.Add(seed.method, seed.path, seed.query)
	}

	router := fuzzRouter(f)
	known := make(map[string]struct{})
	for _, route := range router.Routes() {
		known[route.Name] = struct{}{}
	}

	f.Fuzz(func(t *testing.T, method, path, query string) {
		req := &http.Request{
			Method: method,
			URL:    &url.URL{Path: path, RawQuery: query},
			Header: http.Header{},
		}

		matched := router.Match(req)
		if matched == nil {
			return
		}
		if _, ok := known[matched.Name]; !ok {
			t.Fatalf("Match returned %q, which is not in the route table, for path %q", matched.Name, path)
		}
	})
}
