package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// CORSConfig configures cross-origin response headers. Origins are matched
// exactly; "*" allows every origin. An empty AllowedMethods uses the common
// browser methods, and an empty AllowedHeaders echoes the requested headers.
type CORSConfig struct {
	Enabled        bool
	AllowedOrigins []string
	AllowedMethods []string
	AllowedHeaders []string
	MaxAge         time.Duration
}

var defaultCORSMethods = []string{
	http.MethodGet,
	http.MethodPost,
	http.MethodPut,
	http.MethodPatch,
	http.MethodDelete,
	http.MethodOptions,
}

// CORS adds Access-Control-* headers to cross-origin responses and answers
// preflight requests with 204 without invoking the rest of the chain. A
// request from an origin that is not allowed passes through untouched, so the
// browser still sees the gateway's normal response and blocks it client-side.
func CORS(cfg CORSConfig) Middleware {
	if !cfg.Enabled {
		return func(next http.Handler) http.Handler { return next }
	}

	origins := make(map[string]struct{}, len(cfg.AllowedOrigins))
	wildcard := false
	for _, origin := range cfg.AllowedOrigins {
		if origin == "*" {
			wildcard = true
			continue
		}
		origins[origin] = struct{}{}
	}

	methods := cfg.AllowedMethods
	if len(methods) == 0 {
		methods = defaultCORSMethods
	}
	allowMethods := strings.Join(methods, ", ")
	allowHeaders := strings.Join(cfg.AllowedHeaders, ", ")

	maxAge := ""
	if cfg.MaxAge >= time.Second {
		maxAge = strconv.Itoa(int(cfg.MaxAge / time.Second))
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin == "" {
				next.ServeHTTP(w, r)
				return
			}

			allowed := wildcard
			if !allowed {
				_, allowed = origins[origin]
			}
			if !allowed {
				next.ServeHTTP(w, r)
				return
			}

			if wildcard {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Add("Vary", "Origin")
			}

			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				w.Header().Set("Access-Control-Allow-Methods", allowMethods)
				if allowHeaders != "" {
					w.Header().Set("Access-Control-Allow-Headers", allowHeaders)
				} else if requested := r.Header.Get("Access-Control-Request-Headers"); requested != "" {
					w.Header().Set("Access-Control-Allow-Headers", requested)
				}
				if maxAge != "" {
					w.Header().Set("Access-Control-Max-Age", maxAge)
				}
				if !wildcard {
					w.Header().Add("Vary", "Access-Control-Request-Method")
					w.Header().Add("Vary", "Access-Control-Request-Headers")
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
