package main_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestAegoraCLIStartupMessage(t *testing.T) {
	repoRoot := findRepoRoot(t)

	cmd := exec.Command("go", "run", "./cmd/aegora")
	cmd.Dir = repoRoot

	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stdout

	if err := cmd.Run(); err != nil {
		t.Fatalf("go run ./cmd/aegora failed: %v\noutput:\n%s", err, stdout.String())
	}

	output := strings.TrimSpace(stdout.String())
	if !strings.Contains(output, "Aegora starting...") {
		t.Fatalf("expected startup output to contain %q, got %q", "Aegora starting...", output)
	}
}

func findRepoRoot(t *testing.T) string {
	t.Helper()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd() failed: %v", err)
	}

	return filepath.Clean(filepath.Join(cwd, ".."))
}
