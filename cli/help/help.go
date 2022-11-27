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
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
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
		bazelCommand, _ := parser.GetBazelCommandAndIndex(args[idx+1:])
		return showHelp(bazelCommand, getHelpModifiers(args))
	}
	if arg.ContainsExact(args, "-h") || arg.ContainsExact(args, "--help") {
		bazelCommand, _ := parser.GetBazelCommandAndIndex(args)
		// Sanity check to work around potential issues with
		// GetBazelCommandAndIndex (see TODOs on that func).
		if cmd != bazelCommand {
			return -1, nil
		}
		return showHelp(bazelCommand, getHelpModifiers(args))
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
	exitCode, err = bazelisk.Run(bazelArgs, &bazelisk.RunOpts{Stdout: buf, Stderr: buf})
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
		line = strings.TrimRight(line, "\r")

		if line == "Available commands:" {
			fmt.Println("bazel commands:")
			continue
		}
		if line == "Getting more help:" {
			// Before the "Getting more help" section, print bb commands.
			printBBCommands()
			fmt.Println(line)
			continue
		}
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

func printBBCommands() {
	// TODO: Have commands add themselves to a registry and get the command
	// names / descriptions from there.
	columns := [][]string{
		{"analyze", "Analyzes the dependency graph."},
		{"install", "Installs a bb plugin (https://buildbuddy.io/plugins)."},
		{"login", "Configures bb commands to use your BuildBuddy API key."},
		{"print", "Displays various log file types written by bazel."},
		{"remote", "Runs a bazel command in the cloud with BuildBuddy's hosted bazel service."},
		{"update", "Updates the bb CLI to the latest version."},
	}
	fmt.Println("bb commands:")
	for _, row := range columns {
		fmt.Printf("  %s  %s\n", padEnd(row[0], 18), row[1])
	}
	fmt.Println()
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

func padEnd(value string, targetLength int) string {
	for len(value) < targetLength {
		value += " "
	}
	return value
}
