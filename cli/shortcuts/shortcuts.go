package shortcuts

import "github.com/buildbuddy-io/buildbuddy/cli/arg"

var (
	shortcuts = map[string]string{
		"b": "build",
		"t": "test",
		"q": "query",
		"r": "run",
		"f": "fix",
	}
)

// HandleShorcuts finds the first non-flag command and tries to expand it
func HandleShortcuts(args []string) []string {
	command, idx := arg.GetCommandAndIndex(args)
	if expanded, ok := shortcuts[command]; ok {
		args[idx] = expanded
	}
	return args
}
