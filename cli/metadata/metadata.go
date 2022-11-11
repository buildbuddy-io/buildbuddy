package metadata

import (
	"encoding/json"
	"fmt"
)

func AppendBuildMetadata(args, originalArgs []string) ([]string, error) {
	// Ensure arg0 is "bb" (by default, it is an abs path)
	originalArgs = append([]string{"bb"}, originalArgs[1:]...)

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
