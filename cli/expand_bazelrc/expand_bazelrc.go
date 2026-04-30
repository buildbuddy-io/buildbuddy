package expand_bazelrc

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
)

const usage = `
usage: bb expand-bazelrc --args='["--bazelrc=custom.bazelrc", "build","//...", "--config=ci"]' [--workspace=/path/to/workspace]

Expands --bazelrc and --config options from the given Bazel argument array.

The original --bazelrc and --config options are removed from the output, after they've been expanded.
--ignore_all_rc_files is appended to the output, so Bazel does not re-load rc files.

If --ignore_all_rc_files is already present in input args, rc parsing is skipped.

Arguments:
  --args: Required JSON array of Bazel args.
  --workspace: Optional workspace root. If set, this is used for resolving
               workspace-relative bazelrc imports such as %workspace%/...

Output:
  Prints a JSON array of expanded args to stdout.
`

func HandleExpandBazelrc(args []string) (int, error) {
	flags := flag.NewFlagSet("expand-bazelrc", flag.ContinueOnError)
	argsJSON := flags.String("args", "", "Required JSON array of bazel args.")
	workspaceDir := flags.String("workspace", "", "Optional workspace root used to resolve workspace-relative bazelrc imports.")
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return -1, err
	}

	if flags.NArg() > 0 {
		return -1, fmt.Errorf("unexpected positional args: %s", strings.Join(flags.Args(), " "))
	}
	if strings.TrimSpace(*argsJSON) == "" {
		return -1, fmt.Errorf("--args is required")
	}

	expandedArgs, err := expandArgs(*argsJSON, *workspaceDir)
	if err != nil {
		return -1, err
	}

	b, err := json.Marshal(expandedArgs)
	if err != nil {
		return -1, err
	}
	if _, err := fmt.Fprintln(os.Stdout, string(b)); err != nil {
		return -1, err
	}
	return 0, nil
}

func expandArgs(argsJSON, workspaceDir string) ([]string, error) {
	var bazelArgs []string
	if err := json.Unmarshal([]byte(argsJSON), &bazelArgs); err != nil {
		return nil, fmt.Errorf("--args_json must be a JSON array of strings: %s", err)
	}

	parsedArgs, err := parser.ParseArgs(bazelArgs)
	if err != nil {
		return nil, err
	}
	if workspaceDir != "" {
		parsedArgs, err = parser.ResolveArgsWithWorkspace(parsedArgs, workspaceDir)
	} else {
		parsedArgs, err = parser.ResolveArgs(parsedArgs)
	}
	if err != nil {
		return nil, err
	}
	return parsedArgs.Format(), nil
}
