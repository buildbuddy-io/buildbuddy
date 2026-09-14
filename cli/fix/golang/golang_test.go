package golang

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeTestFile(t *testing.T, filename, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, []byte(contents), 0644); err != nil {
		t.Fatal(err)
	}
}

func readTestFile(t *testing.T, filename string) string {
	t.Helper()
	contents, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}

func makeReadOnly(t *testing.T, filename string) {
	t.Helper()
	if err := os.Chmod(filename, 0444); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filename, 0644) })
	f, err := os.OpenFile(filename, os.O_WRONLY, 0)
	if err == nil {
		f.Close()
		t.Skip("filesystem permissions do not prevent writing (running as root?)")
	}
}

func TestConsolidateDepFilesErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T) map[string][]string
		want  string
	}{
		{
			name: "missing go.mod",
			setup: func(t *testing.T) map[string][]string {
				return map[string][]string{"go.mod": {"missing/go.mod"}}
			},
			want: "read go.mod",
		},
		{
			name: "invalid go.mod",
			setup: func(t *testing.T) map[string][]string {
				writeTestFile(t, "go.mod", "not a valid go.mod\n")
				return map[string][]string{"go.mod": {"go.mod"}}
			},
			want: "parse go.mod",
		},
		{
			name: "missing module directive",
			setup: func(t *testing.T) map[string][]string {
				writeTestFile(t, "go.mod", "go 1.20\n")
				return map[string][]string{"go.mod": {"go.mod"}}
			},
			want: "no module directive",
		},
		{
			name: "BUILD read failure",
			setup: func(t *testing.T) map[string][]string {
				writeTestFile(t, "go.mod", "module example.com/test\n")
				if err := os.Mkdir("BUILD", 0755); err != nil {
					t.Fatal(err)
				}
				return map[string][]string{"go.mod": {"go.mod"}}
			},
			want: "read BUILD",
		},
		{
			name: "BUILD append failure",
			setup: func(t *testing.T) map[string][]string {
				writeTestFile(t, "go.mod", "module example.com/test\n")
				writeTestFile(t, "BUILD.bazel", "# existing file\n")
				makeReadOnly(t, "BUILD.bazel")
				return map[string][]string{"go.mod": {"go.mod"}}
			},
			want: "write module prefix",
		},
		{
			name: "go.work write failure",
			setup: func(t *testing.T) map[string][]string {
				writeTestFile(t, "one/go.mod", "module example.com/one\n")
				writeTestFile(t, "two/go.mod", "module example.com/two\n")
				if err := os.Mkdir("go.work", 0755); err != nil {
					t.Fatal(err)
				}
				return map[string][]string{"go.mod": {"one/go.mod", "two/go.mod"}}
			},
			want: `write "go.work"`,
		},
		{
			name: "multiple go.work files with single module",
			setup: func(t *testing.T) map[string][]string {
				// Validation must happen before reading or modifying any files.
				return map[string][]string{"go.mod": {"go.mod"}, "go.work": {"one/go.work", "two/go.work"}}
			},
			want: "multiple go.work files",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			deps := tc.setup(t)
			_, err := (&Golang{}).ConsolidateDepFiles(deps)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want error containing %q", err, tc.want)
			}
			if _, ok := deps["go.mod"]; !ok {
				t.Fatal("removed dependency files after failure")
			}
		})
	}
}

func TestConsolidateDepFilesSetsPrefixIdempotently(t *testing.T) {
	for _, basename := range []string{"BUILD", "BUILD.bazel"} {
		t.Run(basename, func(t *testing.T) {
			t.Chdir(t.TempDir())
			writeTestFile(t, "go.mod", "module example.com/test\n")
			writeTestFile(t, basename, "# existing file\n")
			deps := map[string][]string{"go.mod": {"go.mod"}, "package-lock.json": {"package-lock.json"}}
			for range 2 {
				got, err := (&Golang{}).ConsolidateDepFiles(deps)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, deps) {
					t.Fatalf("got %v, want %v", got, deps)
				}
			}
			contents := readTestFile(t, basename)
			if strings.Count(contents, "# gazelle:prefix example.com/test") != 1 {
				t.Fatalf("expected exactly one module prefix: %s", contents)
			}
		})
	}
}

func TestConsolidateDepFilesPreservesExistingPrefix(t *testing.T) {
	t.Chdir(t.TempDir())
	writeTestFile(t, "go.mod", "module example.com/test\n")
	const contents = "# gazelle:prefix example.com/custom\n"
	writeTestFile(t, "BUILD.bazel", contents)
	if _, err := (&Golang{}).ConsolidateDepFiles(map[string][]string{"go.mod": {"go.mod"}}); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, "BUILD.bazel"); got != contents {
		t.Fatalf("existing prefix changed: %s", got)
	}
}

func TestConsolidateDepFilesCreatesWorkspace(t *testing.T) {
	t.Chdir(t.TempDir())
	writeTestFile(t, "one/go.mod", "module example.com/one\n")
	writeTestFile(t, "two/go.mod", "module example.com/two\n")
	deps := map[string][]string{"go.mod": {"one/go.mod", "two/go.mod"}}
	if _, err := (&Golang{}).ConsolidateDepFiles(deps); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, "go.work"); got != "go "+defaultGoVersion+"\nuse ./one\nuse ./two\n" {
		t.Fatalf("unexpected go.work contents: %s", got)
	}
	for _, module := range []string{"one", "two"} {
		if got := readTestFile(t, filepath.Join(module, "BUILD.bazel")); !strings.Contains(got, "# gazelle:prefix example.com/"+module) {
			t.Fatalf("missing prefix: %s", got)
		}
	}
}

func TestRegisterDepsErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T)
		want  string
	}{
		{"missing module file", func(t *testing.T) {}, "read module file"},
		{"missing go.mod", func(t *testing.T) {
			writeTestFile(t, "MODULE.bazel", "")
		}, "read go.mod"},
		{"invalid go.mod", func(t *testing.T) {
			writeTestFile(t, "MODULE.bazel", "")
			writeTestFile(t, "go.mod", "not a valid go.mod\n")
		}, "parse go.mod"},
		{"module append failure", func(t *testing.T) {
			writeTestFile(t, "MODULE.bazel", "")
			writeTestFile(t, "go.mod", "module example.com/test\n")
			makeReadOnly(t, "MODULE.bazel")
		}, "register Go dependencies"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			tc.setup(t)
			err := (&Golang{}).RegisterDeps("go.mod", "MODULE.bazel")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestRegisterDepsIdempotent(t *testing.T) {
	t.Chdir(t.TempDir())
	writeTestFile(t, "MODULE.bazel", "")
	writeTestFile(t, "go.mod", "module example.com/test\nrequire example.com/direct v1.0.0\nrequire example.com/indirect v1.0.0 // indirect\n")
	g := &Golang{}
	if err := g.RegisterDeps("go.mod", "MODULE.bazel"); err != nil {
		t.Fatal(err)
	}
	contents := readTestFile(t, "MODULE.bazel")
	if !strings.Contains(contents, `"com_example_direct"`) || strings.Contains(contents, "com_example_indirect") {
		t.Fatalf("unexpected registered dependencies: %s", contents)
	}
	if err := g.RegisterDeps("go.mod", "MODULE.bazel"); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, "MODULE.bazel"); got != contents {
		t.Fatalf("already configured dependencies changed: %s", got)
	}
}

func TestAppendToFileErrors(t *testing.T) {
	if err := appendToFile(t.TempDir(), "contents"); err == nil {
		t.Fatal("expected error appending to directory")
	}
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skip("/dev/full unavailable")
	}
	err := appendToFile("/dev/full", "contents")
	if _, ok := errors.AsType[*os.PathError](err); !ok {
		t.Fatalf("got %v, want a write error", err)
	}
}
