package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProxyForwardsRequest(t *testing.T) {
	backend := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			w.WriteHeader(http.StatusOK)
		}),
	)

	defer backend.Close()

	proxy, err := New(backend.URL)

	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		"/",
		nil,
	)

	rec := httptest.NewRecorder()

	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf(
			"expected 200 got %d",
			rec.Code,
		)
	}
}

func TestProxyInjectsHeader(t *testing.T) {
	var header string

	backend := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			header = r.Header.Get("X-Aegora")

			w.WriteHeader(http.StatusOK)
		}),
	)

	defer backend.Close()

	proxy, _ := New(backend.URL)

	req := httptest.NewRequest(
		http.MethodGet,
		"/",
		nil,
	)

	rec := httptest.NewRecorder()

	proxy.ServeHTTP(rec, req)

	if header != "true" {
		t.Fatalf(
			"expected X-Aegora=true, got %q",
			header,
		)
	}
}
