package config_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config"

	"github.com/stretchr/testify/assert"
)

func testDataReader(t *testing.T, relPath string) io.Reader {
	data, ok := Data["enterprise/server/workflow/config/"+relPath]
	if !ok {
		t.Fatalf("no such file: %q", relPath)
	}
	return bytes.NewReader(data)
}

func TestWorkflowConf_Parse_BasicConfig_Valid(t *testing.T) {
	conf, err := config.NewConfig(testDataReader(t, "test_data/basic.yaml"))

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
