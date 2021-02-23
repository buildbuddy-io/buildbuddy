package workflowconf_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflowconf"

	"github.com/stretchr/testify/assert"
)

func testDataReader(t *testing.T, relPath string) io.Reader {
	data, ok := Data["enterprise/server/workflowconf/"+relPath]
	if !ok {
		t.Fatalf("no such file: %q", relPath)
	}
	return bytes.NewReader(data)
}

func TestWorkflowConf_Parse_BasicConfig_Valid(t *testing.T) {
	conf, err := workflowconf.NewConfig(testDataReader(t, "test_data/basic.yaml"))

	assert.NoError(t, err)
	assert.Equal(t, &workflowconf.BuildBuddyConfig{
		Actions: []*workflowconf.Action{
			{
				Name: "Build and test",
				Triggers: &workflowconf.Triggers{
					Push: &workflowconf.PushTrigger{
						Branches: []string{"main"},
					},
					PullRequest: &workflowconf.PullRequestTrigger{
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
