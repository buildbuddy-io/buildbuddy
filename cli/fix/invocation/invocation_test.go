package invocation

import (
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// Use a recorded `bazel help` flag database so tests don't shell out.
	parser.SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
	m.Run()
}

func TestParseInvocationID(t *testing.T) {
	const id = "0f8fad5b-d9cb-469f-a165-70867728950e"

	for _, tc := range []struct {
		name string
		in   string
	}{
		{"bare ID", id},
		{"invocation URL", "https://app.buildbuddy.io/invocation/" + id},
		{"self-hosted URL", "https://buildbuddy.mycorp.internal/invocation/" + id},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseInvocationID(tc.in)
			require.NoError(t, err)
			assert.Equal(t, id, got)
		})
	}
}

func TestParseInvocationID_Invalid(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"target label", "//foo:bar_test"},
		{"relative target label", ":bar_test"},
		{"flag", "--diff"},
		{"not a uuid", "not-a-uuid"},
		{"truncated uuid", "0f8fad5b-d9cb-469f-a165"},
		// uuid.Pattern is anchored and lowercase-only, so these forms are
		// rejected here exactly as they are by `bb explain`.
		{"uppercase uuid", "0F8FAD5B-D9CB-469F-A165-70867728950E"},
		{"URL with trailing slash", "https://app.buildbuddy.io/invocation/0f8fad5b-d9cb-469f-a165-70867728950e/"},
		{"URL with query string", "https://app.buildbuddy.io/invocation/0f8fad5b-d9cb-469f-a165-70867728950e?target=x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseInvocationID(tc.in)
			assert.Error(t, err)
		})
	}
}

func TestWithTarget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cmd    []string
		target string
		want   []string
	}{
		{
			name:   "narrows a wildcard pattern",
			cmd:    []string{"test", "//..."},
			target: "//foo:bar_test",
			want:   []string{"test", "//foo:bar_test"},
		},
		{
			name:   "replaces multiple targets",
			cmd:    []string{"test", "//foo/...", "//bar/..."},
			target: "//foo:bar_test",
			want:   []string{"test", "//foo:bar_test"},
		},
		{
			name:   "preserves attached flags",
			cmd:    []string{"test", "--config=ci", "--nocache_test_results", "//..."},
			target: "//foo:bar_test",
			want:   []string{"test", "--config=ci", "--nocache_test_results", "//foo:bar_test"},
		},
		{
			// The recorded command line is un-canonicalized, so a flag and its
			// value can be separate arguments. "errors" must not be mistaken
			// for a target and dropped along with "//...".
			name:   "preserves separated flag values",
			cmd:    []string{"test", "--test_output", "errors", "//..."},
			target: "//foo:bar_test",
			want:   []string{"test", "--test_output", "errors", "//foo:bar_test"},
		},
		{
			name:   "preserves startup options",
			cmd:    []string{"--nosystem_rc", "test", "//..."},
			target: "//foo:bar_test",
			want:   []string{"--nosystem_rc", "test", "//foo:bar_test"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := withTarget(tc.cmd, tc.target)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestTail(t *testing.T) {
	assert.Equal(t, "short", tail("short", 32))
	assert.Equal(t, "abcde", tail("abcde", 5))

	// The end is kept, since bazel reports the failure after the build output.
	got := tail("0123456789", 4)
	assert.True(t, strings.HasSuffix(got, "6789"))
	assert.Contains(t, got, "6 earlier bytes omitted")
}
