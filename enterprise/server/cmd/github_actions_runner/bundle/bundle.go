package bundle

import (
	_ "embed"
)

//go:embed buildbuddy_github_actions_runner
var RunnerBytes []byte
