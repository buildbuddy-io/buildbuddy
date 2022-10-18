package help

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
)

const (
	cliName = "bb"
)

var (
	// helpModifiers are flags that affect how the help output is displayed.
	helpModifiers = map[string]struct{}{
		"--long":  {},
		"--short": {},
	}
)

func HandleHelp(args []string) (exitCode int, err error) {
	args, _ = arg.SplitPassthroughArgs(args)

	cmd, idx := arg.GetCommandAndIndex(args)
	// If no command is specified, show general help.
	// TODO: Allow configuring a "default command" that is run when
	// no args are passed? Like `build //...`
	if idx == -1 {
		return showHelp("", getHelpModifiers(args))
	}
	if cmd == "help" {
		// Get the subcommand (the string "build" in "bazel help build")
		subcommand, _ := arg.GetCommandAndIndex(args[idx+1:])
		return showHelp(subcommand, getHelpModifiers(args))
	}
	if arg.ContainsExact(args, "-h") || arg.ContainsExact(args, "--help") {
		// Assume cmd is the bazel subcommand if set.
		// Bazel will show "ERROR: 'foo' is not a known command" if the
		// subcommand is invalid.
		return showHelp(cmd, getHelpModifiers(args))
	}
	return -1, nil
}

func showHelp(subcommand string, modifiers []string) (exitCode int, err error) {
	bazelArgs := []string{"help"}
	if subcommand != "" {
		bazelArgs = append(bazelArgs, subcommand)
	}
	bazelArgs = append(bazelArgs, modifiers...)
	buf := bytes.NewBuffer(nil)
	exitCode, err = bazelisk.Run(bazelArgs, "/dev/null" /*=outputPath*/, buf)
	if err != nil {
		io.Copy(os.Stdout, buf)
		return exitCode, err
	}
	if exitCode != 0 {
		io.Copy(os.Stdout, buf)
		return exitCode, nil
	}
	// Match "Usage: bazel <command> <options> ..."
	usagePattern := regexp.MustCompile(`^(.*?Usage:\s+)bazel(\s+.*)$`)
	// Match example "bazel help ..." commands in "Getting more help" section
	moreHelpPattern := regexp.MustCompile(`^(\s*)bazel( help\s+.*)$`)
	// Get help output lines with trailing newlines removed
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	for _, line := range lines {
		// Bazel shows its release version at the top of the help output;
		// show ours too.
		if strings.Contains(line, "[bazel release") {
			releaseTag := fmt.Sprintf("[%s release %v]", cliName, version.String())
			fmt.Println(padStart(releaseTag, len(line)))
			fmt.Println(line)
			continue
		}
		if m := usagePattern.FindStringSubmatch(line); m != nil {
			fmt.Println(m[1] + cliName + m[2])
			continue
		}
		if m := moreHelpPattern.FindStringSubmatch(line); m != nil {
			fmt.Println(m[1] + cliName + m[2])
			continue
		}
		fmt.Println(line)
	}
	return exitCode, nil
}

func getHelpModifiers(args []string) []string {
	var out []string
	for _, arg := range args {
		if _, ok := helpModifiers[arg]; ok {
			out = append(out, arg)
		}
	}
	return out
}

func padStart(value string, targetLength int) string {
	for len(value) < targetLength {
		value = " " + value
	}
	return value
}
