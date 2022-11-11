package metadata

import (
	"encoding/json"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
)

func getCommandLineToolName() string {
	if bazelisk.IsInvokedByBazelisk() {
		return "bazel"
	}
	return "bb"
}

func AppendBuildMetadata(args, originalArgs []string) ([]string, error) {
	originalArgs = append(
		[]string{getCommandLineToolName()},
		originalArgs[1:]...)

	originalArgsJSON, err := json.Marshal(originalArgs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal original command arguments: %s", err)
	}
	// TODO: Add metadata to help understand how config files were expanded,
	// and how plugins modified args (maybe not as build_metadata, but
	// rather as some sort of artifact we attach to the invocation ID).
	args = append(args, "--build_metadata=EXPLICIT_COMMAND_LINE="+string(originalArgsJSON))
	return args, nil
}
