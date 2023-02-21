package fix

import (
	"os"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"

	gazelle "github.com/bazelbuild/bazel-gazelle/cmd/gazelle"
)

func HandleFix(args []string) (exitCode int, err error) {
	command, idx := arg.GetCommandAndIndex(args)
	if command != "fix" {
		return -1, nil
	}

	if idx != 0 {
		log.Debugf("Unexpected flag: %s", args[0])
		return 1, nil
	}

	// Run gazelle with the transformed args so far (e.g. if we ran bb with
	// `--verbose=1`, this will make sure we don't pass `--verbose=1` to
	// gazelle, which doesn't understand that flag).
	originalArgs := os.Args
	defer func() {
		os.Args = originalArgs
	}()
	os.Args = args
	gazelle.Run()

	return 0, nil
}
