package workflowconf

import (
	"bytes"
	"io"
	"testing"

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
	conf, err := NewConfig(testDataReader(t, "test_data/basic.yaml"))

	assert.NoError(t, err)
	assert.Equal(t, &BuildBuddyConfig{
		Actions: []*Action{
			{
				Name: "CI",
				Triggers: &Triggers{
					Push: &PushTrigger{
						Branches: []string{"main"},
					},
					PullRequest: &PullRequestTrigger{
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
