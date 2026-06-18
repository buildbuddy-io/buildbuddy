package registry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseGithubRequest(t *testing.T) {
	for name, tc := range map[string]struct {
		input     string
		repo      string
		owner     string
		tag       string
		version   string
		errString string
	}{
		"empty": {
			input:   "",
			repo:    "",
			owner:   "",
			version: "",
			tag:     "",
		},
		"basic": {
			input:   "/modules/repo/v1.2.3-github.owner",
			repo:    "repo",
			owner:   "owner",
			version: "v1.2.3-github.owner",
			tag:     "v1.2.3",
		},
		"ignorable noise": {
			input:   "/modules/repo/v1.2.3-github.owner//abc/ABC/~!@#$%^&*()_+{}|:\"<>?`1234567890-=[]\\;',./",
			repo:    "repo",
			owner:   "owner",
			version: "v1.2.3-github.owner",
			tag:     "v1.2.3",
		},
		"complicated repo": {
			input:   "/modules/rEp0._!'\")(>%<-sitory/v1.2.3-github.owner//abc/ABC/~!@#$%^&*()_+{}|:\"<>?`1234567890-=[]\\;',./",
			repo:    "rEp0._!'\")(>%<-sitory",
			owner:   "owner",
			version: "v1.2.3-github.owner",
			tag:     "v1.2.3",
		},
		"empty repo": {
			input:   "/modules//v1.2.3-github.owner//abc/ABC/~!@#$%^&*()_+{}|:\"<>?`1234567890-=[]\\;',./",
			repo:    "",
			owner:   "owner",
			version: "v1.2.3-github.owner",
			tag:     "v1.2.3",
		},
		"invalid repo": {
			input:     "/modules/re@po/v1.2.3-github.owner",
			errString: "Invalid path: /modules/re@po/v1.2.3-github.owner",
		},
		"complicated owner": {
			input:   "/modules/repo/v1.2.3-github.-github.-github.0wN._!'\")(>%<-er//abc/ABC/~!@#$%^&*()_+{}|:\"<>?`1234567890-=[]\\;',./",
			repo:    "repo",
			owner:   "-github.-github.0wN._!'\")(>%<-er",
			version: "v1.2.3-github.-github.-github.0wN._!'\")(>%<-er",
			tag:     "v1.2.3",
		},
		"empty owner": {
			input:   "/modules/repo/v1.2.3-github.//abc/ABC/~!@#$%^&*()_+{}|:\"<>?`1234567890-=[]\\;',./",
			repo:    "repo",
			owner:   "",
			version: "v1.2.3-github.",
			tag:     "v1.2.3",
		},
		"invalid owner": {
			input:     "/modules/repo/v1.2.3-github.@owner",
			errString: "Invalid path: /modules/repo/v1.2.3-github.@owner",
		},
		"missing owner separator": {
			input:   "/modules/repo/v1.2.3//abc/ABC/~!@#$%^&*()_+{}|:\"<>?`1234567890-=[]\\;',./",
			repo:    "repo",
			owner:   "",
			version: "v1.2.3",
			tag:     "v1.2.3",
		},
		"complicated version": {
			input:   "/modules/repo/v._!'\")(>%<-1.2.3-github.owner//abc/ABC/~!@#$%^&*()_+{}|:\"<>?`1234567890-=[]\\;',./",
			repo:    "repo",
			owner:   "owner",
			version: "v._!'\")(>%<-1.2.3-github.owner",
			tag:     "v._!'\")(>%<-1.2.3",
		},
		"empty version": {
			input:   "/modules/repo/-github.owner//abc/ABC/~!@#$%^&*()_+{}|:\"<>?`1234567890-=[]\\;',./",
			repo:    "repo",
			owner:   "owner",
			version: "-github.owner",
			tag:     "",
		},
		"invalid version": {
			input:     "/modules/repo/v@1.2.3-github.owner",
			errString: "Invalid path: /modules/repo/v@1.2.3-github.owner",
		},
		"empty tag": {
			input:   "/modules/repo///abc/ABC/~!@#$%^&*()_+{}|:\"<>?`1234567890-=[]\\;',./",
			repo:    "repo",
			owner:   "",
			version: "",
			tag:     "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			repo, owner, version, tag, err := parseGithubRequest(tc.input)
			assert.Equalf(t, tc.repo, repo, "repo does not match.")
			assert.Equal(t, tc.owner, owner, "owner does not match.")
			assert.Equal(t, tc.version, version, "version does not match.")
			assert.Equal(t, tc.tag, tag, "tag does not match.")
			if tc.errString == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualErrorf(t, err, tc.errString, "error does not match.")
			}
		})
	}
}

func TestModuleSnippet(t *testing.T) {
	for name, tc := range map[string]struct {
		repo    string
		version string
		body    []byte
		snippet []byte
	}{
		"replace": {
			repo:    "repo",
			version: "v1.2.3-github.owner",
			body: []byte(
				`module(
	name = "foo",
	repo_name = "bar",
)

include("//some:stuff.MODULE.bazel")
include("//other:things.MODULE.bazel")

`,
			),
			snippet: []byte(
				`module(name="repo", version="v1.2.3-github.owner")

include("//some:stuff.MODULE.bazel")
include("//other:things.MODULE.bazel")

`,
			),
		},
		"prepend": {
			repo:    "repo",
			version: "v1.2.3-github.owner",
			body: []byte(
				`include("//some:stuff.MODULE.bazel")
include("//other:things.MODULE.bazel")

`,
			),
			snippet: []byte(
				`module(name="repo", version="v1.2.3-github.owner")

include("//some:stuff.MODULE.bazel")
include("//other:things.MODULE.bazel")

`,
			),
		},
		"empty": {
			repo:    "repo",
			version: "v1.2.3-github.owner",
			body:    []byte{},
			snippet: []byte(
				`module(name="repo", version="v1.2.3-github.owner")

`,
			),
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, string(tc.snippet), string(moduleSnippet(tc.repo, tc.version, tc.body)))
		})
	}
}

func TestEscapeStringForStarlark(t *testing.T) {
	for name, tc := range map[string]struct {
		input  string
		output string
	}{
		"empty": {
			input:  "",
			output: "",
		},
		"normal": {
			input:  "repo",
			output: "repo",
		},
		"quotes": {
			input:  "re\"\"p\"o\"",
			output: "re\\\"\\\"p\\\"o\\\"",
		},
		"backslash": {
			input:  "re\\\\p\\o\\",
			output: "re\\\\\\\\p\\\\o\\\\",
		},
		"quotes and backslash": {
			input:  "re\"p\\o\\\"",
			output: "re\\\"p\\\\o\\\\\\\"",
		},
		"comprehensive": {
			input:  "`1234567890-=abcdefghijklmnopqrstuvwxyz[]\\;',./~!@#$%^&*()_+ABCDEFGHIJKLMNOPQRSTUVWXYZ{}|:\"<>?",
			output: "`1234567890-=abcdefghijklmnopqrstuvwxyz[]\\\\;',./~!@#$%^&*()_+ABCDEFGHIJKLMNOPQRSTUVWXYZ{}|:\\\"<>?",
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.output, escapeStringForStarlark(tc.input))
		})
	}
}
