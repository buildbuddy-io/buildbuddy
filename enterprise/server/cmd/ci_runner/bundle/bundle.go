package bundle

import (
	_ "embed"
)

const RunnerName = "buildbuddy_ci_runner"
const BazelWrapperName = "buildbuddy_bazel_wrapper"

//go:embed buildbuddy_ci_runner
var CiRunnerBytes []byte

//go:embed buildbuddy_bazel_wrapper
var BazelWrapperBytes []byte
