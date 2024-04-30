package config_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config/test_data"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				BazelCommands: []string{
					"build //...",
					"test //...",
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
    bazel_commands:
      - "test foo
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
	} {
		action := &config.Action{
			Triggers: &config.Triggers{
				Push: &config.PushTrigger{Branches: []string{testCase.pattern}},
			},
		}
		event := "push"

		match := config.MatchesAnyTrigger(action, event, testCase.branchName)

		assert.Equal(t, testCase.shouldMatch, match, "expected match(%q, %q) => %v", testCase.branchName, testCase.pattern, testCase.shouldMatch)
	}
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
