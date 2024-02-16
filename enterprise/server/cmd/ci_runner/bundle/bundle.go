package bundle

import (
	"embed"
	"io/fs"
)

const RunnerName = "buildbuddy_ci_runner"

// NB: Include everything in bazel `embedsrcs` with `*`.
//
//go:embed buildbuddy_ci_runner
var all embed.FS

func OpenRunner() (fs.File, error) {
	return all.Open(RunnerName)
}

func ReadRunner() ([]byte, error) {
	return fs.ReadFile(all, RunnerName)
}
