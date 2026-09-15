package slashcommand

import (
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config"
	"github.com/buildbuddy-io/buildbuddy/proto/runner"
)

// command is a GitHub pull request slash command with backend-defined workflow
// behavior.
type command struct {
	name           string
	workflowAction func() *config.Action
}

var supportedCommands = []*command{
	{
		name: "/bb-review",
		workflowAction: func() *config.Action {
			return &config.Action{
				Name: "BB Code Review",
				Steps: []*runner.Step{
					{
						Run: `
bb agent review --force
						`,
					},
				},
			}
		},
	},
}

// IsCommand returns whether a pull request comment body is a supported command.
func IsCommand(body string) bool {
	_, ok := getSlashCommand(body)
	return ok
}

func WorkflowAction(commentBody string) (*config.Action, bool) {
	c, ok := getSlashCommand(commentBody)
	if !ok || c.workflowAction == nil {
		return nil, false
	}
	return c.workflowAction(), true
}

func getSlashCommand(body string) (*command, bool) {
	body = strings.TrimSpace(body)
	for _, c := range supportedCommands {
		if c.name == body {
			return c, true
		}
	}
	return nil, false
}
