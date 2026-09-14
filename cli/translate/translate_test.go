package translate

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTranslateWriteError(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "BUILD.js")
	if err := os.WriteFile(input, nil, 0644); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "BUILD.bazel")
	if err := os.Mkdir(output, 0755); err != nil {
		t.Fatal(err)
	}
	got, err := Translate(input)
	if err == nil {
		t.Fatal("expected output write error")
	}
	if got != "" {
		t.Fatalf("returned unusable output path %q", got)
	}
	if pathErr, ok := errors.AsType[*os.PathError](err); !ok || pathErr.Path != output {
		t.Fatalf("got %v, want filesystem error for %q", err, output)
	}
}

func TestTranslateWritesOutput(t *testing.T) {
	input := filepath.Join(t.TempDir(), "BUILD.js")
	if err := os.WriteFile(input, nil, 0644); err != nil {
		t.Fatal(err)
	}
	got, err := Translate(input)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(got)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "# Source file: "+input) {
		t.Fatalf("missing generated header: %s", contents)
	}
}
