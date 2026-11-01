package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/syncronix-labs/aegora/internal/middleware"
)

func corsHandler(cfg middleware.CORSConfig, reached *bool) http.Handler {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		*reached = true
		w.WriteHeader(http.StatusOK)
	})
	return middleware.CORS(cfg)(next)
}

func TestCORS(t *testing.T) {
	allowed := middleware.CORSConfig{
		Enabled:        true,
		AllowedOrigins: []string{"https://app.example.com"},
		AllowedMethods: []string{http.MethodGet, http.MethodPost},
		AllowedHeaders: []string{"Authorization", "Content-Type"},
		MaxAge:         time.Hour,
	}
	restricted := middleware.CORSConfig{
		Enabled:        true,
		AllowedOrigins: []string{"https://app.example.com"},
	}
	wildcard := middleware.CORSConfig{
		Enabled:        true,
		AllowedOrigins: []string{"*"},
	}

	tests := []struct {
		name       string
		cfg        middleware.CORSConfig
		method     string
		origin     string
		acrm       string
		acrh       string
		wantStatus int
		wantOrigin string
		wantVary   bool
		wantMaxAge string
		wantHeader string
		wantReach  bool
	}{
		{
			name:       "disabled passes through",
			cfg:        middleware.CORSConfig{},
			method:     http.MethodGet,
			origin:     "https://app.example.com",
			wantStatus: http.StatusOK,
			wantReach:  true,
		},
		{
			name:       "wildcard allows any origin",
			cfg:        wildcard,
			method:     http.MethodGet,
			origin:     "https://other.example.com",
			wantStatus: http.StatusOK,
			wantOrigin: "*",
			wantReach:  true,
		},
		{
			name:       "restricted origin is echoed",
			cfg:        restricted,
			method:     http.MethodGet,
			origin:     "https://app.example.com",
			wantStatus: http.StatusOK,
			wantOrigin: "https://app.example.com",
			wantVary:   true,
			wantReach:  true,
		},
		{
			name:       "unknown origin passes without headers",
			cfg:        restricted,
			method:     http.MethodGet,
			origin:     "https://evil.example.com",
			wantStatus: http.StatusOK,
			wantReach:  true,
		},
		{
			name:       "preflight is answered without the chain",
			cfg:        allowed,
			method:     http.MethodOptions,
			origin:     "https://app.example.com",
			acrm:       http.MethodPost,
			acrh:       "X-Requested-With",
			wantStatus: http.StatusNoContent,
			wantOrigin: "https://app.example.com",
			wantVary:   true,
			wantMaxAge: "3600",
			wantHeader: "Authorization, Content-Type",
			wantReach:  false,
		},
		{
			name:       "preflight echoes requested headers when none configured",
			cfg:        restricted,
			method:     http.MethodOptions,
			origin:     "https://app.example.com",
			acrm:       http.MethodPost,
			acrh:       "X-Token, X-Trace",
			wantStatus: http.StatusNoContent,
			wantOrigin: "https://app.example.com",
			wantVary:   true,
			wantHeader: "X-Token, X-Trace",
			wantReach:  false,
		},
		{
			name:       "preflight from unknown origin is not short-circuited",
			cfg:        restricted,
			method:     http.MethodOptions,
			origin:     "https://evil.example.com",
			acrm:       http.MethodPost,
			wantStatus: http.StatusOK,
			wantReach:  true,
		},
		{
			name:       "request without origin is untouched",
			cfg:        restricted,
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
			wantReach:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reached := false
			handler := corsHandler(tt.cfg, &reached)

			req := httptest.NewRequest(tt.method, "/api", nil)
			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}
			if tt.acrm != "" {
				req.Header.Set("Access-Control-Request-Method", tt.acrm)
			}
			if tt.acrh != "" {
				req.Header.Set("Access-Control-Request-Headers", tt.acrh)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if tt.wantStatus == http.StatusNoContent {
				if got := rec.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(got, tt.acrm) {
					t.Fatalf("allow-methods = %q, want it to contain %q", got, tt.acrm)
				}
			}
			if got := rec.Header().Get("Access-Control-Allow-Origin"); got != tt.wantOrigin {
				t.Fatalf("allow-origin = %q, want %q", got, tt.wantOrigin)
			}
			if got := rec.Header().Get("Access-Control-Allow-Headers"); got != tt.wantHeader {
				t.Fatalf("allow-headers = %q, want %q", got, tt.wantHeader)
			}
			if got := rec.Header().Get("Access-Control-Max-Age"); got != tt.wantMaxAge {
				t.Fatalf("max-age = %q, want %q", got, tt.wantMaxAge)
			}
			if tt.wantVary && !varyContains(rec.Header(), "Origin") {
				t.Fatalf("Vary = %q, want it to contain Origin", rec.Header().Values("Vary"))
			}
			if !tt.wantVary && len(rec.Header().Values("Vary")) != 0 {
				t.Fatalf("unexpected Vary = %q", rec.Header().Values("Vary"))
			}
			if reached != tt.wantReach {
				t.Fatalf("downstream reached = %v, want %v", reached, tt.wantReach)
			}
		})
	}
}

func varyContains(header http.Header, want string) bool {
	for _, value := range header.Values("Vary") {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), want) {
				return true
			}
		}
	}
	return false
}
