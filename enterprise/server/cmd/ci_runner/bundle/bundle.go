package bundle

import (
	_ "embed"
)

const RunnerName = "buildbuddy_ci_runner"

//go:embed buildbuddy_ci_runner
var CiRunnerBytes []byte
