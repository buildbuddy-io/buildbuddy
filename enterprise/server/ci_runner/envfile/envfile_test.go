package envfile_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/ci_runner/envfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	// Parse follows GitHub's GITHUB_ENV syntax, so these cases pin down
	// GitHub's semantics: no trimming of names or delimiters, exact delimiter
	// matching, and "=" vs "<<" precedence based on whichever appears first
	// in the line.
	for _, tc := range []struct {
		name    string
		content string
		want    map[string]string
		wantErr string
	}{
		{
			name:    "empty file",
			content: "",
			want:    map[string]string{},
		},
		{
			name:    "single-line var",
			content: "FOO=bar\n",
			want:    map[string]string{"FOO": "bar"},
		},
		{
			name:    "multiple vars with empty lines between",
			content: "FOO=bar\n\nBAZ=qux\n",
			want:    map[string]string{"FOO": "bar", "BAZ": "qux"},
		},
		{
			name:    "value containing '='",
			content: "FOO=a=b\n",
			want:    map[string]string{"FOO": "a=b"},
		},
		{
			// "=" appears before "<<", so this is a single-line var,
			// not a heredoc.
			name:    "value containing '<<' after '='",
			content: "URL=https://example.com<<path\n",
			want:    map[string]string{"URL": "https://example.com<<path"},
		},
		{
			// Quotes are not interpreted; they are part of the value.
			name:    "quotes are preserved literally",
			content: "FOO=\"double quoted\"\nBAR='single quoted'\n",
			want:    map[string]string{"FOO": `"double quoted"`, "BAR": `'single quoted'`},
		},
		{
			// Like GITHUB_ENV, names are not trimmed.
			name:    "name is not trimmed",
			content: " FOO=bar\n",
			want:    map[string]string{" FOO": "bar"},
		},
		{
			name:    "last assignment wins for duplicate names",
			content: "FOO=a\nFOO=b\n",
			want:    map[string]string{"FOO": "b"},
		},
		{
			name:    "multiline var",
			content: "FOO<<EOF\nline1\nline2\nEOF\n",
			want:    map[string]string{"FOO": "line1\nline2"},
		},
		{
			name:    "multiline var with empty value",
			content: "FOO<<EOF\nEOF\n",
			want:    map[string]string{"FOO": ""},
		},
		{
			// Empty and whitespace-only lines within a multiline value are
			// preserved verbatim.
			name:    "multiline var preserves whitespace",
			content: "FOO<<EOF\nline1\n\n  indented\n \nEOF\n",
			want:    map[string]string{"FOO": "line1\n\n  indented\n "},
		},
		{
			// A line containing "=" within a multiline value is part of the
			// value, not a new assignment.
			name:    "multiline var containing '='",
			content: "FOO<<EOF\nBAR=baz\nEOF\n",
			want:    map[string]string{"FOO": "BAR=baz"},
		},
		{
			// A line that contains the delimiter plus trailing whitespace
			// does not close the heredoc; only an exact match does.
			name:    "closing delimiter must match exactly",
			content: "FOO<<EOF\nEOF \nEOF\n",
			want:    map[string]string{"FOO": "EOF "},
		},
		{
			name:    "line without '=' or '<<'",
			content: "FOO\n",
			wantErr: `line 1: invalid format "FOO": expected NAME=value or NAME<<DELIMITER`,
		},
		{
			// Whitespace-only lines are not skipped; like GITHUB_ENV, only
			// exactly empty lines are.
			name:    "whitespace-only line",
			content: "   \n",
			wantErr: "line 1: invalid format",
		},
		{
			name:    "empty name",
			content: "=bar\n",
			wantErr: `line 1: invalid format "=bar": variable name must not be empty`,
		},
		{
			name:    "empty delimiter",
			content: "FOO<<\n",
			wantErr: `line 1: invalid format "FOO<<": variable name and delimiter must not be empty`,
		},
		{
			name:    "missing closing delimiter",
			content: "FOO<<EOF\nline1\nline2\n",
			wantErr: `line 1: matching delimiter "EOF" not found for variable "FOO"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			envVars, err := envfile.Parse(tc.content)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, envVars)
		})
	}
}

func TestProvision(t *testing.T) {
	// Write stale content to the env file path, simulating a leftover file
	// from a previous run.
	path := filepath.Join(t.TempDir(), envfile.FileName)
	err := os.WriteFile(path, []byte("STALE_VAR=1\n"), 0644)
	require.NoError(t, err)
	// Use t.Setenv to restore the BUILDBUDDY_ENV var after the test;
	// Provision overwrites it below.
	t.Setenv(envfile.EnvVarName, "")

	err = envfile.Provision(path)
	require.NoError(t, err)

	// The env file should be empty (stale content discarded), and
	// BUILDBUDDY_ENV should point to it.
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Empty(t, string(content))
	assert.Equal(t, path, os.Getenv(envfile.EnvVarName))
}

func TestParseAndReset(t *testing.T) {
	// Write an env var to the env file, as a step would.
	path := filepath.Join(t.TempDir(), envfile.FileName)
	err := os.WriteFile(path, []byte("FOO=bar\n"), 0644)
	require.NoError(t, err)

	// Parsing should return the env var and truncate the file so that the
	// var is not applied again after the next step.
	envVars, err := envfile.ParseAndReset(path)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"FOO": "bar"}, envVars)
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Empty(t, string(content))
}
