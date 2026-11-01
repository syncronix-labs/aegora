// Package proxy provides HTTP reverse proxy functionality
// and request routing primitives for Aegora.
package proxy

import (
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"
)

// RouteConfig defines routing rules and destinations for a proxy route.
type RouteConfig struct {
	Name             string
	PathPrefix       string
	PathRegex        *regexp.Regexp
	PathRegexPattern string
	Methods          []string
	Headers          map[string]string
	Targets          []string
	StripPrefix      string
	LBPolicy         string
	// Timeout bounds the whole outbound exchange for the route when positive.
	Timeout time.Duration
}

// Router matches incoming HTTP requests to configured backend routes based on
// path specificity, HTTP methods, and header rules.
type Router struct {
	routes []RouteConfig
}

// NewRouter creates a new Router from a slice of route configurations.
// It compiles any uncompiled regex patterns, validates that each route has at
// least one target, and sorts routes by specificity.
//
// Route Precedence Rules:
//  1. Regex routes take precedence over prefix-only routes because regular
//     expressions specify narrow, explicit pattern constraints.
//  2. Among routes with the same regex status (both regex or both non-regex),
//     routes are sorted by descending PathPrefix length so that longer, more
//     specific path prefixes (e.g. /api/v1/users) are evaluated before shorter,
//     broader prefixes (e.g. /api/v1, /api, or /).
//  3. For routes with identical specificity criteria, sort.SliceStable
//     preserves their original registration order.
func NewRouter(routes []RouteConfig) (*Router, error) {
	compiled := make([]RouteConfig, len(routes))
	for i, r := range routes {
		route := r

		if route.PathRegex == nil && route.PathRegexPattern != "" {
			re, err := regexp.Compile(route.PathRegexPattern)
			if err != nil {
				return nil, fmt.Errorf("router: route %q: invalid regex %q: %w", route.Name, route.PathRegexPattern, err)
			}
			route.PathRegex = re
		}

		if len(route.Targets) == 0 {
			return nil, fmt.Errorf("router: route %q: must have at least one target", route.Name)
		}

		compiled[i] = route
	}

	sort.SliceStable(compiled, func(i, j int) bool {
		iRegex := compiled[i].PathRegex != nil
		jRegex := compiled[j].PathRegex != nil
		if iRegex != jRegex {
			return iRegex
		}

		return len(compiled[i].PathPrefix) > len(compiled[j].PathPrefix)
	})

	return &Router{
		routes: compiled,
	}, nil
}

// Routes returns a copy of the router's sorted routes.
func (r *Router) Routes() []RouteConfig {
	if r == nil {
		return nil
	}
	out := make([]RouteConfig, len(r.routes))
	copy(out, r.routes)
	return out
}

// Match evaluates the incoming HTTP request against the registered routes in
// specificity order and returns the first matching RouteConfig, or nil if no
// route matches.
//
// A route matches if and only if ALL of the following criteria hold:
//  1. Method: route.Methods is empty, or contains req.Method (case-insensitive).
//  2. Headers: route.Headers is empty, or req.Header contains every specified key
//     with an exact matching value.
//  3. Path: if route.PathRegex is set, the path must match the regular expression;
//     otherwise, the path must have route.PathPrefix as a prefix.
//
// Path evaluation is performed against req.URL.EscapedPath() (falling back to
// req.URL.Path if empty). EscapedPath() is chosen because it preserves percent-encoded
// sequences (e.g., %2F for '/', %20 for space), preventing path-traversal bypasses,
// ambiguous segment decoding, and request-smuggling vulnerabilities before routing.
func (r *Router) Match(req *http.Request) *RouteConfig {
	if r == nil || req == nil || req.URL == nil {
		return nil
	}

	path := req.URL.EscapedPath()
	if path == "" {
		path = req.URL.Path
	}
	if path == "" {
		path = "/"
	}

	for i := range r.routes {
		route := &r.routes[i]

		// 1. Method check: empty means all methods allowed.
		if len(route.Methods) > 0 {
			matchedMethod := false
			for _, m := range route.Methods {
				if strings.EqualFold(m, req.Method) {
					matchedMethod = true
					break
				}
			}
			if !matchedMethod {
				continue
			}
		}

		// 2. Headers check: empty means no header constraints.
		if len(route.Headers) > 0 {
			matchedHeaders := true
			for k, v := range route.Headers {
				if req.Header.Get(k) != v {
					matchedHeaders = false
					break
				}
			}
			if !matchedHeaders {
				continue
			}
		}

		// 3. Path check: regex if set, otherwise prefix match.
		if route.PathRegex != nil {
			if !route.PathRegex.MatchString(path) {
				continue
			}
		} else if !strings.HasPrefix(path, route.PathPrefix) {
			continue
		}

		return route
	}

	return nil
}
