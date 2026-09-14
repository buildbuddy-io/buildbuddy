package add

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testRegistryResponse() *RegistryResponse {
	return &RegistryResponse{
		WorkspaceSnippet: `http_archive(name = "rules_go")`,
		ModuleSnippet:    `bazel_dep(name = "rules_go", version = "1.0")`,
		Repo:             Repo{FullName: "bazelbuild/rules_go"},
	}
}

func TestExistingDependency(t *testing.T) {
	for _, tc := range []struct {
		name     string
		contents string
		add      func(*os.File, string, string, *RegistryResponse) error
	}{
		{"workspace same version", GenerateWorkspaceSnippet("github/bazelbuild/rules_go", "1.0", testRegistryResponse()), addToWorkspace},
		{"workspace different version", GenerateWorkspaceSnippet("github/bazelbuild/rules_go", "0.9", testRegistryResponse()), addToWorkspace},
		{"workspace manual installation", `url = "https://github.com/bazelbuild/rules_go/archive/v1.0.tar.gz"`, addToWorkspace},
		{"module same version", `bazel_dep(name = "rules_go", version = "1.0")`, addToModule},
		{"module different version", `bazel_dep(name = "rules_go", version = "0.9")`, addToModule},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "deps.bazel")
			if err := os.WriteFile(filename, []byte(tc.contents), 0644); err != nil {
				t.Fatal(err)
			}
			f, err := os.OpenFile(filename, os.O_APPEND|os.O_RDWR, 0644)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			err = tc.add(f, "github/bazelbuild/rules_go", "1.0", testRegistryResponse())
			if !errors.Is(err, ErrAlreadyExists) {
				t.Fatalf("got %v, want ErrAlreadyExists", err)
			}
			if !strings.Contains(err.Error(), "already contains") {
				t.Fatalf("missing explanation: %v", err)
			}
			contents, err := os.ReadFile(filename)
			if err != nil {
				t.Fatal(err)
			}
			if string(contents) != tc.contents {
				t.Fatalf("existing dependency file changed: %s", contents)
			}
		})
	}
}

func TestDependencyIOErrors(t *testing.T) {
	for name, add := range map[string]func(*os.File, string, string, *RegistryResponse) error{
		"workspace": addToWorkspace,
		"module":    addToModule,
	} {
		for _, operation := range []string{"read", "write"} {
			t.Run(name+"/"+operation, func(t *testing.T) {
				filename := filepath.Join(t.TempDir(), "deps.bazel")
				if err := os.WriteFile(filename, nil, 0644); err != nil {
					t.Fatal(err)
				}
				f, err := os.Open(filename) // Read-only: appending must fail.
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				if operation == "read" {
					if err := f.Close(); err != nil {
						t.Fatal(err)
					}
				}
				err = add(f, "github/bazelbuild/rules_go", "1.0", testRegistryResponse())
				if err == nil || errors.Is(err, ErrAlreadyExists) {
					t.Fatalf("got %v, want a non-sentinel IO error", err)
				}
				if _, ok := errors.AsType[*os.PathError](err); !ok {
					t.Fatalf("got %v, want a filesystem error", err)
				}
			})
		}
	}
}

func TestModuleMissingRegistrySnippet(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "MODULE.bazel")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	resp := testRegistryResponse()
	resp.ModuleSnippet = ""
	err = addToModule(f, "github/bazelbuild/rules_go", "1.0", resp)
	if err == nil || errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("got %v, want a non-sentinel missing snippet error", err)
	}
}

func TestAddNewDependency(t *testing.T) {
	for name, add := range map[string]func(*os.File, string, string, *RegistryResponse) error{
		"workspace": addToWorkspace,
		"module":    addToModule,
	} {
		t.Run(name, func(t *testing.T) {
			f, err := os.CreateTemp(t.TempDir(), "deps.bazel")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if err := add(f, "github/bazelbuild/rules_go", "1.0", testRegistryResponse()); err != nil {
				t.Fatal(err)
			}
			contents, err := os.ReadFile(f.Name())
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(contents), "rules_go") {
				t.Fatalf("dependency not added: %s", contents)
			}
		})
	}
}
