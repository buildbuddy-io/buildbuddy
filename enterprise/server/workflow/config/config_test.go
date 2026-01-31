package config_test

import (
	"bytes"
	"net/url"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config/test_data"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
)

func TestWorkflowConf_Parse_BasicConfig_Valid(t *testing.T) {
	conf, err := config.NewConfig(bytes.NewReader(test_data.BasicYaml))

	assert.NoError(t, err)
	assert.Equal(t, &config.BuildBuddyConfig{
		Actions: []*config.Action{
			{
				Name: "Build and test",
				Triggers: &config.Triggers{
					Push: &config.PushTrigger{
						Branches: []string{"main"},
					},
					PullRequest: &config.PullRequestTrigger{
						Branches: []string{"main"},
					},
				},
				Steps: []*rnpb.Step{
					{
						Run: "bazel build //...",
					},
					{
						Run: "bazel test //...",
					},
				},
			},
		},
	}, conf)
}

func TestWorkflowConf_Parse_YamlWithRunBlock(t *testing.T) {
	conf, err := config.NewConfig(bytes.NewReader(test_data.YamlWithRunBlock))

	assert.NoError(t, err)
	assert.Equal(t, &config.BuildBuddyConfig{
		Actions: []*config.Action{
			{
				Name: "Build and test",
				Triggers: &config.Triggers{
					Push: &config.PushTrigger{
						Branches: []string{"main"},
					},
				},
				Steps: []*rnpb.Step{
					{
						Run: `echo "This is a multi-line run block"
echo "Should still parse!"
`,
					},
				},
			},
		},
	}, conf)
}

func TestWorkflowConf_Parse_InvalidConfig_Error(t *testing.T) {
	// Unquoted bazel command
	s := `
actions:
  - name: Test
    steps:
      - "bazel test foo
`
	conf, err := config.NewConfig(strings.NewReader(s))
	assert.Nil(t, conf)
	assert.Error(t, err)
}

func TestMatchesAnyTrigger_SupportsBasicWildcard(t *testing.T) {
	for _, testCase := range []struct {
		pattern, branchName string
		shouldMatch         bool
	}{
		{"main", "main", true},
		{"main", "other", false},
		{"main", "gh-readonly-queue/main/pr-1-1111111111111111111111111111111111111111", false},
		{"*", "main", true},
		{"*", "other", true},
		{"*", "gh-readonly-queue/main/pr-1-1111111111111111111111111111111111111111", true},
		{"gh-readonly-queue/*", "main", false},
		{"gh-readonly-queue/*", "other", false},
		{"gh-readonly-queue/*", "gh-readonly-queue", false},
		{"gh-readonly-queue/*", "gh-readonly-queue/", true},
		{"gh-readonly-queue/*", "gh-readonly-queue/main/pr-1-1111111111111111111111111111111111111111", true},
		{"gh-readonly-queue/*", "gh-READONLY-queue/main/pr-1-1111111111111111111111111111111111111111", false},
		{"release-*", "release-20240101", true},
		{"release-*", "releasefoo", false},
		{"*-release", "20240101-release", true},
		{"*-release", "foorelease", false},
		{"prefix*suffix", "prefix_suffix", true},
		{"prefix*suffix", "_prefix_suffix", false},
		{"prefix*suffix", "prefix_suffix_", false},
		{"prefix*suffix", "prefixsuffix", true},
	} {
		action := &config.Action{
			Triggers: &config.Triggers{
				Push: &config.PushTrigger{Branches: []string{testCase.pattern}},
			},
		}
		event := "push"

		match := config.MatchesAnyTrigger(action, event, testCase.branchName, "" /*=tag*/)

		assert.Equal(t, testCase.shouldMatch, match, "expected match(%q, %q) => %v", testCase.branchName, testCase.pattern, testCase.shouldMatch)
	}
}

func TestMatchesAndTrigger_NegationPatterns(t *testing.T) {
	for _, tc := range []struct {
		name           string
		patterns       []string
		branch         string
		shouldMatch    []string
		shouldNotMatch []string
	}{
		{
			name:           "negation pattern with exact branch",
			patterns:       []string{"*", "!main"},
			shouldMatch:    []string{"feature", "main1"},
			shouldNotMatch: []string{"main"},
		},
		{
			name:           "negation pattern with wildcard",
			patterns:       []string{"*", "!release-*"},
			shouldMatch:    []string{"main", "feature", "releasefoo"},
			shouldNotMatch: []string{"release-20240101"},
		},
		{
			name:           "negation pattern with exception - last match wins",
			patterns:       []string{"*", "!release-*", "release-exception", "some-other-branch"},
			shouldMatch:    []string{"main", "feature", "release-exception", "some-other-branch"},
			shouldNotMatch: []string{"release-20240101"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			action := &config.Action{
				Triggers: &config.Triggers{
					Push: &config.PushTrigger{Branches: tc.patterns},
				},
			}
			event := "push"
			for _, branch := range tc.shouldMatch {
				m := config.MatchesAnyTrigger(action, event, branch, "")
				assert.True(t, m, "MatchesAnyTrigger(%v, %q) should be true", branch, tc.patterns)
			}
			for _, branch := range tc.shouldNotMatch {
				m := config.MatchesAnyTrigger(action, event, branch, "")
				assert.False(t, m, "MatchesAnyTrigger(%v, %q) should be false", branch, tc.patterns)
			}
		})
	}
}

func TestMatchesAnyTrigger_TagPushMatchesTagPattern(t *testing.T) {
	for _, tc := range []struct {
		pattern, tag string
		shouldMatch  bool
	}{
		{"v*", "v1.0.0", true},
		{"v*", "v2.0", true},
		{"v*", "release-1.0", false},
		{"*", "v1.0.0", true},
		{"*", "anything", true},
		{"v1.0.0", "v1.0.0", true},
		{"v1.0.0", "v1.0.1", false},
		{"release-*", "release-2024", true},
		{"release-*", "v1.0.0", false},
	} {
		action := &config.Action{
			Triggers: &config.Triggers{
				Push: &config.PushTrigger{Tags: []string{tc.pattern}},
			},
		}
		match := config.MatchesAnyTrigger(action, "push", "", tc.tag)
		assert.Equal(t, tc.shouldMatch, match, "expected match(tag=%q, pattern=%q) => %v", tc.tag, tc.pattern, tc.shouldMatch)
	}
}

func TestMatchesAnyTrigger_TagPushDoesNotMatchBranchOnlyTrigger(t *testing.T) {
	action := &config.Action{
		Triggers: &config.Triggers{
			Push: &config.PushTrigger{Branches: []string{"*"}},
		},
	}
	match := config.MatchesAnyTrigger(action, "push", "", "v1.0.0")
	assert.False(t, match, "tag push should not match branch-only trigger")
}

func TestMatchesAnyTrigger_BranchPushDoesNotMatchTagOnlyTrigger(t *testing.T) {
	action := &config.Action{
		Triggers: &config.Triggers{
			Push: &config.PushTrigger{Tags: []string{"v*"}},
		},
	}
	match := config.MatchesAnyTrigger(action, "push", "main", "")
	assert.False(t, match, "branch push should not match tag-only trigger")
}

func TestMatchesAnyTrigger_TagNegationPatterns(t *testing.T) {
	action := &config.Action{
		Triggers: &config.Triggers{
			Push: &config.PushTrigger{Tags: []string{"v*", "!v0.9.0"}},
		},
	}
	assert.True(t, config.MatchesAnyTrigger(action, "push", "", "v1.0.0"))
	assert.False(t, config.MatchesAnyTrigger(action, "push", "", "v0.9.0"))
}

func TestMatchesAnyTrigger_BothBranchAndTagTriggers(t *testing.T) {
	action := &config.Action{
		Triggers: &config.Triggers{
			Push: &config.PushTrigger{
				Branches: []string{"main", "release-*"},
				Tags:     []string{"v*"},
			},
		},
	}

	// Tag push matches tag pattern, not branch pattern.
	assert.True(t, config.MatchesAnyTrigger(action, "push", "", "v1.0.0"))
	assert.False(t, config.MatchesAnyTrigger(action, "push", "", "nightly-2024"))

	// Branch push matches branch pattern, not tag pattern.
	assert.True(t, config.MatchesAnyTrigger(action, "push", "main", ""))
	assert.True(t, config.MatchesAnyTrigger(action, "push", "release-2024", ""))
	assert.False(t, config.MatchesAnyTrigger(action, "push", "feature-x", ""))

	// Tag named "main" matches tag patterns (not branch patterns),
	// so it should not match since "main" doesn't match "v*".
	assert.False(t, config.MatchesAnyTrigger(action, "push", "", "main"))
}

func TestGetGitFetchFilters(t *testing.T) {
	for _, test := range []struct {
		Name    string
		YAML    string
		Filters []string
	}{
		{
			Name:    "Default",
			YAML:    "",
			Filters: []string{"blob:none"},
		},
		{
			Name:    "EmptyList",
			YAML:    "git_fetch_filters: []",
			Filters: []string{},
		},
		{
			Name:    "NonEmptyList",
			YAML:    `git_fetch_filters: ["tree:0"]`,
			Filters: []string{"tree:0"},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			s := `actions: [ { name: Test, ` + test.YAML + ` } ]`
			cfg, err := config.NewConfig(strings.NewReader(s))
			require.NoError(t, err)
			require.Equal(t, test.Filters, cfg.Actions[0].GetGitFetchFilters())
		})
	}
}

func TestCodeSearchAction(t *testing.T) {
	apiURL, err := url.Parse("grpcs://example.com")
	require.NoError(t, err)
	ghURL := "https://github.com/buildbuddy-io/buildbuddy"

	action := config.CodesearchIncrementalUpdateAction(apiURL, ghURL, "master")

	require.NotNil(t, action)
	assert.Equal(t, config.CSIncrementalUpdateName, action.Name)
	require.NotNil(t, action.Triggers)
	require.NotNil(t, action.Triggers.Push)
	assert.Equal(t, []string{"master"}, action.Triggers.Push.Branches)
	require.NotNil(t, action.Steps)
	assert.Len(t, action.Steps, 1)
	assert.Contains(t, action.Steps[0].Run, apiURL.String())
	assert.Contains(t, action.Steps[0].Run, ghURL)
}
