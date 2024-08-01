package bundle

import (
	_ "embed"
)

//go:embed buildbuddy_ci_runner
var CiRunnerBytes []byte
