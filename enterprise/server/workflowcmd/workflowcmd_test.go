package workflowcmd_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflowcmd"
	"github.com/stretchr/testify/assert"
)

func TestGenerateShellScript(t *testing.T) {
	ci := &workflowcmd.CommandInfo{
		RepoURL:   "https://github.com/buildbuddy-io/buildbuddy",
		CommitSHA: "59ed6f6a4b185a7d8bcf25641bd3c23d2c0ca259",
	}

	scriptBytes, err := workflowcmd.GenerateShellScript(ci)

	script := string(scriptBytes)
	assert.NoError(t, err)
	assert.Equal(t, script, `#!/bin/bash
set -o errexit
set -o pipefail
mkdir buildbuddy
cd buildbuddy
git init
git remote add origin https://github.com/buildbuddy-io/buildbuddy
git fetch origin 59ed6f6a4b185a7d8bcf25641bd3c23d2c0ca259
git checkout 59ed6f6a4b185a7d8bcf25641bd3c23d2c0ca259
bazelisk test //...
`)
}
