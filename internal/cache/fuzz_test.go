package cache

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func fuzzRequest(rawQuery string) *http.Request {
	return &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Path: "/p", RawQuery: rawQuery},
		Header: http.Header{},
	}
}

// reversedQuery returns the same pairs in the opposite order. It reports false
// when the comparison would be meaningless: a query that does not parse, or one
// that repeats a parameter name, where the order of the values is itself part of
// the key.
func reversedQuery(rawQuery string) (string, bool) {
	if rawQuery == "" {
		return "", true
	}

	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return "", false
	}
	for _, vs := range values {
		if len(vs) > 1 {
			return "", false
		}
	}

	pairs := strings.Split(rawQuery, "&")
	for i, j := 0, len(pairs)-1; i < j; i, j = i+1, j-1 {
		pairs[i], pairs[j] = pairs[j], pairs[i]
	}
	return strings.Join(pairs, "&"), true
}

// FuzzRequestCacheKey asserts that building a cache key from an arbitrary query
// string never panics, is deterministic, and ignores the order of the parameters.
func FuzzRequestCacheKey(f *testing.F) {
	seeds := []string{
		"",
		"a=1",
		"a=1&b=2",
		"b=2&a=1",
		"a=1&a=2",
		"q=%",
		"=&&",
		"a=",
		"a",
		strings.Repeat("k=v&", 500),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, rawQuery string) {
		key := requestCacheKey(fuzzRequest(rawQuery))

		if again := requestCacheKey(fuzzRequest(rawQuery)); again != key {
			t.Fatalf("key is not deterministic for %q: %q then %q", rawQuery, key, again)
		}

		reversed, ok := reversedQuery(rawQuery)
		if !ok {
			return
		}
		if other := requestCacheKey(fuzzRequest(reversed)); other != key {
			t.Fatalf("parameter order changed the key:\n  %q -> %q\n  %q -> %q",
				rawQuery, key, reversed, other)
		}
	})
}
