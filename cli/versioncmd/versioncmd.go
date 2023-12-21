package versioncmd

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/plugin"
	"github.com/buildbuddy-io/buildbuddy/cli/setup"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func HandleVersion(args []string) (exitCode int, err error) {
	if arg.ContainsExact(args, "--cli") {
		fmt.Println(version.String())
		return 0, nil
	}

	fmt.Printf("bb %s\n", version.String())

	return runBazelVersion(args)
}

// runBazelVersion forwards the `version` command to bazel (i.e. `bazel version`)
func runBazelVersion(args []string) (int, error) {
	tempDir, err := os.MkdirTemp("", "buildbuddy-cli-*")
	if err != nil {
		return 1, err
	}
	defer func() {
		os.RemoveAll(tempDir)
	}()

	// Add the `version` command back to args before forwarding
	// the command to bazel
	args = append([]string{"version"}, args...)

	plugins, bazelArgs, execArgs, err := setup.Setup(args, tempDir)
	if err != nil {
		return 1, status.WrapError(err, "bazel setup")
	}

	outputPath := filepath.Join(tempDir, "bazel.log")
	return plugin.RunBazeliskWithPlugins(
		arg.JoinExecutableArgs(bazelArgs, execArgs),
		outputPath, plugins)
}
