package workflowcmd_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflowcmd"
	"github.com/stretchr/testify/assert"
)

func TestGenerateShellScript(t *testing.T) {
	repoURL := "git@github.com:buildbuddy-io/buildbuddy.git"
	commitSHA := "ABCD123"

	script, err := workflowcmd.GenerateShellScript(repoURL, commitSHA)
	assert.Nil(t, err)
	assert.Regexp(t, "git clone -q git@github.com:buildbuddy-io/buildbuddy.git", script, "script should clone repo")
	assert.Regexp(t, "git checkout -q ABCD123", script, "script should checkout SHA")
	assert.Regexp(t, "bazelisk test //...", script, "script should run bazelisk test")
}
