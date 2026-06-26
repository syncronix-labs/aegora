package middleware

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestMiddlewareExecutionOrder(t *testing.T) {

	var calls []string

	first := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls = append(calls, "first-before")

			next.ServeHTTP(w, r)

			calls = append(calls, "first-after")
		})
	}

	second := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls = append(calls, "second-before")

			next.ServeHTTP(w, r)

			calls = append(calls, "second-after")
		})
	}

	handler := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		calls = append(calls, "handler")
	})

	chain := New(first, second)

	req := httptest.NewRequest(http.MethodGet, "/", nil)

	rec := httptest.NewRecorder()

	chain.Then(handler).ServeHTTP(rec, req)

	expected := []string{
		"first-before",
		"second-before",
		"handler",
		"second-after",
		"first-after",
	}

	if !reflect.DeepEqual(expected, calls) {
		t.Fatal(calls)
	}
}
