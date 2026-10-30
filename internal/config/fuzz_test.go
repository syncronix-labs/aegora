package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// FuzzLoad asserts that loading arbitrary bytes as a config document never
// panics: the loader either returns a config or an error.
func FuzzLoad(f *testing.F) {
	seeds := []string{
		"",
		"server:\n  port: 8080\n",
		"routes: []\n",
		"routes:\n  - name: a\n    path_prefix: /a\n    targets: [http://127.0.0.1:1]\n",
		"\x00\xff\xfe",
		"{{{{",
		"- - - -",
		"a: &x [*x]\n",
		strings.Repeat("a: 1\n", 2000),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, document string) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}

		if _, err := Load(path); err != nil && err.Error() == "" {
			t.Fatal("Load returned an error with no message")
		}
	})
}
