package config_test

import (
	"bytes"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config/test_data"

	"github.com/stretchr/testify/assert"
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
