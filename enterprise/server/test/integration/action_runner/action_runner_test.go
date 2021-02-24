package action_runner_test

import (
	"testing"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
)

type result struct {
	output   string
	exitCode int
	err      error
}

func invokeActionRunner(t *testing.T, args []string, env []string) (*result, error) {

	return &result{}
}

func TestActionRunner_CIPlaygroundRepo_InvocationUpdatesGitHubStatus(t *testing.T) {
	binPath := bazelgo.Runfile("enterprise/server/cmd/action_runner/_action_runner/action_runner")
}
