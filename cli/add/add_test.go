package add_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/add"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

func TestParseModuleInput(t *testing.T) {
	for _, tc := range []struct {
		in               string
		wantMod, wantVer string
	}{
		{"rules_go", "rules_go", ""},
		{"rules_go@0.46.0", "rules_go", "0.46.0"},
		{"github.com/bazelbuild/rules_go", "github/bazelbuild/rules_go", ""},
		{"https://github.com/bazelbuild/rules_go", "github/bazelbuild/rules_go", ""},
		{"https://github.com/bazelbuild/rules_go/", "github/bazelbuild/rules_go", ""},
		{"https://github.com/bazelbuild/rules_go@v0.46.0", "github/bazelbuild/rules_go", "v0.46.0"},
		// Trailing slash before version is preserved as-is (only fully trailing slashes are trimmed).
		{"foo@1.0.0@extra", "foo", "1.0.0@extra"},
	} {
		t.Run(tc.in, func(t *testing.T) {
			gotMod, gotVer := add.ParseModuleInput(tc.in)
			assert.Equal(t, tc.wantMod, gotMod)
			assert.Equal(t, tc.wantVer, gotVer)
		})
	}
}

func TestGenerateWorkspaceSnippet_Markers(t *testing.T) {
	resp := &add.RegistryResponse{WorkspaceSnippet: "http_archive(name = \"rules_go\")"}
	out := add.GenerateWorkspaceSnippet("rules_go", "0.46.0", resp)

	require.Contains(t, out, "###### Begin auto-generated section for [https://registry.build/rules_go@0.46.0]")
	require.Contains(t, out, "###### End auto-generated section for [https://registry.build/rules_go@0.46.0]")
	require.Contains(t, out, resp.WorkspaceSnippet)

	// The header regex should round-trip the module + version.
	m := add.HeaderRegex.FindStringSubmatch(out)
	require.Len(t, m, 3)
	assert.Equal(t, "rules_go", m[1])
	assert.Equal(t, "0.46.0", m[2])
}

func TestGenerateWorkspaceSnippet_PrefersReleaseSpecificSnippet(t *testing.T) {
	resp := &add.RegistryResponse{
		WorkspaceSnippet: "DEFAULT",
		Releases: []add.Release{
			{Name: "v0.46.0", WorkspaceSnippet: "FOR_V_PREFIX"},
			{Name: "0.47.0", WorkspaceSnippet: "FOR_BARE"},
		},
	}

	out := add.GenerateWorkspaceSnippet("rules_go", "0.46.0", resp)
	assert.Contains(t, out, "FOR_V_PREFIX", "should match release named v0.46.0 when version is 0.46.0")
	assert.NotContains(t, out, "DEFAULT")

	out = add.GenerateWorkspaceSnippet("rules_go", "0.47.0", resp)
	assert.Contains(t, out, "FOR_BARE")

	out = add.GenerateWorkspaceSnippet("rules_go", "9.9.9", resp)
	assert.Contains(t, out, "DEFAULT", "should fall back to top-level WorkspaceSnippet")
}

func TestGenerateWorkspaceSnippet_TrimsSnippetWhitespace(t *testing.T) {
	resp := &add.RegistryResponse{WorkspaceSnippet: "\n\n  http_archive(...)\n\n"}
	out := add.GenerateWorkspaceSnippet("foo", "1", resp)
	// strings.TrimSpace strips leading/trailing whitespace (including the
	// indentation we added). The result should not contain any triple
	// newlines, since the snippet is framed with single blank lines.
	assert.Contains(t, out, "http_archive(...)")
	assert.NotContains(t, out, "\n\n\n", "snippet whitespace should be trimmed")
}

func TestGenerateModuleSnippet_RoundTrip(t *testing.T) {
	snippet := `bazel_dep(name = "rules_go", version = "0.46.0")`
	resp := &add.RegistryResponse{ModuleSnippet: snippet}

	out := add.GenerateModuleSnippet("rules_go", "0.46.0", resp)
	assert.True(t, strings.HasSuffix(out, "\n"), "snippet should end with newline")

	m := add.ModuleRegex.FindStringSubmatch(out)
	require.Len(t, m, 3)
	assert.Equal(t, "rules_go", m[1])
	assert.Equal(t, "0.46.0", m[2])
}

// ---------------------------------------------------------------------------
// File-mutation helpers (addToWorkspace, addToModule)
// ---------------------------------------------------------------------------

// openFreshFile mirrors how add.go opens the workspace file in production:
// O_APPEND|O_CREATE|O_RDWR with the read cursor at offset 0.
func openFreshFile(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	return f
}

func openTempFile(t *testing.T, name, contents string) (*os.File, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(contents), 0644))
	return openFreshFile(t, path), path
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(b)
}

func TestAddToWorkspace_HappyPath(t *testing.T) {
	f, path := openTempFile(t, "WORKSPACE", "# header\n")
	resp := &add.RegistryResponse{
		WorkspaceSnippet: "http_archive(name = \"rules_go\")",
		Repo:             add.Repo{FullName: "bazelbuild/rules_go"},
	}
	require.NoError(t, add.AddToWorkspace(f, "rules_go", "0.46.0", resp))

	got := readFile(t, path)
	assert.True(t, strings.HasPrefix(got, "# header\n"), "existing content preserved")
	assert.Contains(t, got, "###### Begin auto-generated section for [https://registry.build/rules_go@0.46.0]")
	assert.Contains(t, got, resp.WorkspaceSnippet)
}

func TestAddToWorkspace_IdempotencySameVersion(t *testing.T) {
	f, path := openTempFile(t, "WORKSPACE", "")
	resp := &add.RegistryResponse{WorkspaceSnippet: "x", Repo: add.Repo{FullName: "bazelbuild/rules_go"}}
	require.NoError(t, add.AddToWorkspace(f, "rules_go", "0.46.0", resp))
	require.NoError(t, f.Close())

	before := readFile(t, path)
	f2 := openFreshFile(t, path)
	err := add.AddToWorkspace(f2, "rules_go", "0.46.0", resp)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already contains")
	assert.Contains(t, err.Error(), "requested version")
	assert.Equal(t, before, readFile(t, path), "file should be unchanged on duplicate add")
}

func TestAddToWorkspace_VersionMismatchReportsBoth(t *testing.T) {
	f, path := openTempFile(t, "WORKSPACE", "")
	resp := &add.RegistryResponse{WorkspaceSnippet: "x", Repo: add.Repo{FullName: "bazelbuild/rules_go"}}
	require.NoError(t, add.AddToWorkspace(f, "rules_go", "0.46.0", resp))
	require.NoError(t, f.Close())

	before := readFile(t, path)
	f2 := openFreshFile(t, path)
	err := add.AddToWorkspace(f2, "rules_go", "0.47.0", resp)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "0.46.0")
	assert.Contains(t, err.Error(), "0.47.0")
	assert.Equal(t, before, readFile(t, path))
}

func TestAddToWorkspace_DetectsManualInstallByRepoFullName(t *testing.T) {
	// Pre-populate WORKSPACE with a hand-written reference to the repo. There
	// are no auto-generated markers, but the Repo.FullName substring is
	// present, so the add should refuse.
	f, path := openTempFile(t, "WORKSPACE", "# manual: github.com/bazelbuild/rules_go\n")
	resp := &add.RegistryResponse{WorkspaceSnippet: "x", Repo: add.Repo{FullName: "bazelbuild/rules_go"}}

	before := readFile(t, path)
	err := add.AddToWorkspace(f, "rules_go", "0.46.0", resp)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "likely")
	assert.Contains(t, err.Error(), "manually installed")
	assert.Equal(t, before, readFile(t, path))
}

func TestAddToWorkspace_MultipleDistinctModules(t *testing.T) {
	f, path := openTempFile(t, "WORKSPACE", "")
	respA := &add.RegistryResponse{WorkspaceSnippet: "a", Repo: add.Repo{FullName: "a/a"}}
	respB := &add.RegistryResponse{WorkspaceSnippet: "b", Repo: add.Repo{FullName: "b/b"}}
	require.NoError(t, add.AddToWorkspace(f, "a", "1", respA))
	require.NoError(t, f.Close())
	f2 := openFreshFile(t, path)
	require.NoError(t, add.AddToWorkspace(f2, "b", "2", respB))

	got := readFile(t, path)
	matches := add.HeaderRegex.FindAllStringSubmatch(got, -1)
	require.Len(t, matches, 2)
	assert.Equal(t, "a", matches[0][1])
	assert.Equal(t, "1", matches[0][2])
	assert.Equal(t, "b", matches[1][1])
	assert.Equal(t, "2", matches[1][2])
}

func TestAddToModule_HappyPath(t *testing.T) {
	f, path := openTempFile(t, "MODULE.bazel", "module(name = \"x\")\n")
	resp := &add.RegistryResponse{ModuleSnippet: `bazel_dep(name = "rules_go", version = "0.46.0")`}

	require.NoError(t, add.AddToModule(f, "rules_go", "0.46.0", resp))

	got := readFile(t, path)
	assert.Contains(t, got, "module(name = \"x\")")
	assert.Contains(t, got, `bazel_dep(name = "rules_go", version = "0.46.0")`)
}

func TestAddToModule_IdempotencySameVersion(t *testing.T) {
	f, path := openTempFile(t, "MODULE.bazel", "")
	resp := &add.RegistryResponse{ModuleSnippet: `bazel_dep(name = "rules_go", version = "0.46.0")`}
	require.NoError(t, add.AddToModule(f, "rules_go", "0.46.0", resp))
	require.NoError(t, f.Close())

	before := readFile(t, path)
	f2 := openFreshFile(t, path)
	err := add.AddToModule(f2, "rules_go", "0.46.0", resp)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already contains")
	assert.Equal(t, before, readFile(t, path))
}

func TestAddToModule_VersionMismatchReportsBoth(t *testing.T) {
	f, path := openTempFile(t, "MODULE.bazel", "")
	respOld := &add.RegistryResponse{ModuleSnippet: `bazel_dep(name = "rules_go", version = "0.46.0")`}
	respNew := &add.RegistryResponse{ModuleSnippet: `bazel_dep(name = "rules_go", version = "0.47.0")`}
	require.NoError(t, add.AddToModule(f, "rules_go", "0.46.0", respOld))
	require.NoError(t, f.Close())

	before := readFile(t, path)
	f2 := openFreshFile(t, path)
	err := add.AddToModule(f2, "rules_go", "0.47.0", respNew)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "0.46.0")
	assert.Contains(t, err.Error(), "0.47.0")
	assert.Equal(t, before, readFile(t, path))
}

// ---------------------------------------------------------------------------
// HandleAdd integration tests (against an httptest registry server)
// ---------------------------------------------------------------------------

// fakeRegistry serves canned RegistryResponses keyed by module path
// (the part after "/" in /<module>/data.json) and records each lookup.
type fakeRegistry struct {
	server   *httptest.Server
	requests []string // captured module paths
}

func newFakeRegistry(t *testing.T, responses map[string]add.RegistryResponse) *fakeRegistry {
	t.Helper()
	fr := &fakeRegistry{}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Path is /<module>/data.json; module may itself contain slashes.
		path := strings.TrimPrefix(r.URL.Path, "/")
		path = strings.TrimSuffix(path, "/data.json")
		fr.requests = append(fr.requests, path)
		resp, ok := responses[path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	fr.server = httptest.NewServer(mux)
	t.Cleanup(fr.server.Close)
	return fr
}

// pointRegistryAt swaps RegistryEndpoint to target the given test server,
// restoring it after the test.
func pointRegistryAt(t *testing.T, baseURL string) {
	t.Helper()
	prev := add.RegistryEndpoint
	add.RegistryEndpoint = baseURL + "/%s/data.json"
	t.Cleanup(func() { add.RegistryEndpoint = prev })
}

// setupWorkspace creates a temp dir and configures the package-level
// findWorkspaceFile seam to resolve into it. If a WORKSPACE or
// MODULE.bazel file exists in the dir, that name is used; otherwise
// MODULE.bazel is assumed (matching production's CreateModuleIfNotExists
// behavior, but without touching the real cwd).
func setupWorkspace(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	prev := add.FindWorkspaceFile
	add.FindWorkspaceFile = func() (string, string, error) {
		for _, name := range []string{"MODULE.bazel", "WORKSPACE", "WORKSPACE.bazel"} {
			if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
				return dir, name, nil
			}
		}
		// Fall back: create an empty MODULE.bazel like production does.
		path := filepath.Join(dir, "MODULE.bazel")
		if err := os.WriteFile(path, nil, 0644); err != nil {
			return "", "", err
		}
		return dir, "MODULE.bazel", nil
	}
	t.Cleanup(func() { add.FindWorkspaceFile = prev })
	return dir
}

func TestHandleAdd_WorkspaceFlow(t *testing.T) {
	dir := setupWorkspace(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "WORKSPACE"), []byte("# top\n"), 0644))

	fr := newFakeRegistry(t, map[string]add.RegistryResponse{
		"rules_go": {
			Name:                              "rules_go",
			WorkspaceSnippet:                  "http_archive(name = \"rules_go\")",
			LatestReleaseWithWorkspaceSnippet: "0.46.0",
			Repo:                              add.Repo{FullName: "bazelbuild/rules_go"},
		},
	})
	pointRegistryAt(t, fr.server.URL)

	add.ResetFlags()
	code, err := add.HandleAdd([]string{"rules_go"})
	require.NoError(t, err)
	assert.Equal(t, 0, code)

	got := readFile(t, filepath.Join(dir, "WORKSPACE"))
	assert.Contains(t, got, "###### Begin auto-generated section for [https://registry.build/rules_go@0.46.0]")
	assert.Contains(t, got, "http_archive(name = \"rules_go\")")

	// Second invocation with the same module + version should refuse (exit 1, error returned).
	add.ResetFlags()
	code, err = add.HandleAdd([]string{"rules_go@0.46.0"})
	require.Error(t, err)
	assert.Equal(t, 1, code)
	assert.Contains(t, err.Error(), "already contains")
}

func TestHandleAdd_ModuleFlow(t *testing.T) {
	dir := setupWorkspace(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "MODULE.bazel"), []byte("module(name = \"x\")\n"), 0644))

	fr := newFakeRegistry(t, map[string]add.RegistryResponse{
		"rules_go": {
			Name:                           "rules_go",
			ModuleSnippet:                  `bazel_dep(name = "rules_go", version = "0.46.0")`,
			LatestReleaseWithModuleSnippet: "0.46.0",
			Repo:                           add.Repo{FullName: "bazelbuild/rules_go"},
		},
	})
	pointRegistryAt(t, fr.server.URL)

	add.ResetFlags()
	code, err := add.HandleAdd([]string{"rules_go@0.46.0"})
	require.NoError(t, err)
	assert.Equal(t, 0, code)

	got := readFile(t, filepath.Join(dir, "MODULE.bazel"))
	assert.Contains(t, got, `bazel_dep(name = "rules_go", version = "0.46.0")`)

	// Re-add should be refused.
	add.ResetFlags()
	code, err = add.HandleAdd([]string{"rules_go@0.46.0"})
	require.Error(t, err)
	assert.Equal(t, 1, code)
}

func TestHandleAdd_TransitiveOnModuleIsNoOp(t *testing.T) {
	dir := setupWorkspace(t)
	original := "module(name = \"x\")\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "MODULE.bazel"), []byte(original), 0644))

	fr := newFakeRegistry(t, map[string]add.RegistryResponse{
		"rules_go": {
			Name:                           "rules_go",
			ModuleSnippet:                  `bazel_dep(name = "rules_go", version = "0.46.0")`,
			LatestReleaseWithModuleSnippet: "0.46.0",
		},
	})
	pointRegistryAt(t, fr.server.URL)

	add.ResetFlags()
	code, err := add.HandleAdd([]string{"~rules_go"})
	require.NoError(t, err)
	assert.Equal(t, 0, code)
	assert.Equal(t, original, readFile(t, filepath.Join(dir, "MODULE.bazel")), "transitive add on MODULE should be a no-op")
}

func TestHandleAdd_GitHubURLNormalization(t *testing.T) {
	dir := setupWorkspace(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "MODULE.bazel"), []byte("module(name = \"x\")\n"), 0644))

	fr := newFakeRegistry(t, map[string]add.RegistryResponse{
		"github/bazelbuild/rules_go": {
			Name:                           "rules_go",
			ModuleSnippet:                  `bazel_dep(name = "rules_go", version = "0.46.0")`,
			LatestReleaseWithModuleSnippet: "0.46.0",
		},
	})
	pointRegistryAt(t, fr.server.URL)

	add.ResetFlags()
	code, err := add.HandleAdd([]string{"https://github.com/bazelbuild/rules_go@0.46.0"})
	require.NoError(t, err)
	assert.Equal(t, 0, code)

	require.NotEmpty(t, fr.requests)
	assert.Equal(t, "github/bazelbuild/rules_go", fr.requests[0],
		"GitHub URL form should be normalized to github/<owner>/<repo>")
}

func TestHandleAdd_NotFound(t *testing.T) {
	setupWorkspace(t)
	fr := newFakeRegistry(t, map[string]add.RegistryResponse{}) // empty -> always 404
	pointRegistryAt(t, fr.server.URL)

	add.ResetFlags()
	code, err := add.HandleAdd([]string{"definitely_not_a_module"})
	require.Error(t, err)
	assert.Equal(t, 1, code)
	assert.Contains(t, err.Error(), "not found")
}

func TestHandleAdd_AmbiguousNonInteractive(t *testing.T) {
	setupWorkspace(t)
	fr := newFakeRegistry(t, map[string]add.RegistryResponse{
		"foo": {
			// No Name set -> treated as a disambiguation response.
			Disambiguation: []add.Disambiguation{
				{Path: "github/a/foo", Stars: 10},
				{Path: "github/b/foo", Stars: 20},
			},
		},
	})
	pointRegistryAt(t, fr.server.URL)

	// stdin/stderr are not TTYs in `go test`, so showPicker should refuse
	// rather than block.
	add.ResetFlags()
	code, err := add.HandleAdd([]string{"foo"})
	require.Error(t, err)
	assert.Equal(t, 1, code)
	assert.Contains(t, err.Error(), "ambiguous")
}

func TestHandleAdd_DisambiguationLengthOneAutoResolves(t *testing.T) {
	dir := setupWorkspace(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "MODULE.bazel"), []byte("module(name = \"x\")\n"), 0644))

	fr := newFakeRegistry(t, map[string]add.RegistryResponse{
		"foo": {
			// Single disambiguation candidate triggers an auto re-fetch.
			Disambiguation: []add.Disambiguation{{Path: "github/some/foo", Stars: 1}},
		},
		"github/some/foo": {
			Name:                           "foo",
			ModuleSnippet:                  `bazel_dep(name = "foo", version = "1.0.0")`,
			LatestReleaseWithModuleSnippet: "1.0.0",
		},
	})
	pointRegistryAt(t, fr.server.URL)

	add.ResetFlags()
	code, err := add.HandleAdd([]string{"foo"})
	require.NoError(t, err)
	assert.Equal(t, 0, code)
	assert.Contains(t, readFile(t, filepath.Join(dir, "MODULE.bazel")),
		`bazel_dep(name = "foo", version = "1.0.0")`)
	assert.Equal(t, []string{"foo", "github/some/foo"}, fr.requests)
}
