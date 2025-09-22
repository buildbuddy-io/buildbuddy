package help

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/cli_command"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/arguments"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/parsed"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
)

const (
	cliName = "bb"
)

// findTargetCommandFromHelpArgs extracts the target command from "bb help <command>" arguments.
// Returns the command name if found, empty string otherwise.
func findTargetCommandFromHelpArgs(orderedArgs *parsed.OrderedArgs) string {
	_, command := parsed.Find[*parsed.Command](orderedArgs.Args)
	if command == nil {
		return ""
	}

	// If the command is "help", look for the next positional argument
	if command.Value == "help" {
		for i, arg := range orderedArgs.Args {
			if pos, ok := arg.(*arguments.PositionalArgument); ok && pos.Value == "help" {
				// Check the next argument
				if i+1 < len(orderedArgs.Args) {
					if nextPos, ok := orderedArgs.Args[i+1].(*arguments.PositionalArgument); ok {
						return nextPos.Value
					}
				}
				break
			}
		}
	}

	// Otherwise return the command itself
	return command.Value
}

// tryShowBBCommandHelp checks if the target command is a BB CLI command and shows its help.
// Returns true if help was shown, false if not a BB CLI command.
func tryShowBBCommandHelp(targetCommand string) bool {
	if targetCommand == "" {
		return false
	}

	if bbCommand := cli_command.GetCommand(targetCommand); bbCommand != nil {
		fmt.Printf("Usage: bb %s\n\n%s\n", bbCommand.Name, bbCommand.Help)
		return true
	}

	return false
}

// HandleHelp Valid cases to trigger help:
// * bb (no additional command passed)
// * bb help
// * bb help `command name`
// * bb -h `command name`
// * bb `command name` -h
// * bb --help `command name`
// * bb `command name` --help
func HandleHelp(args parsed.Args) (exitCode int, err error) {
	// Check if the help request is for a BB CLI command
	if orderedArgs, ok := args.(*parsed.OrderedArgs); ok {
		targetCommand := findTargetCommandFromHelpArgs(orderedArgs)
		if tryShowBBCommandHelp(targetCommand) {
			return 0, nil
		}
	}

	// Not a BB CLI command, forward to Bazel as usual
	buf := &bytes.Buffer{}
	exitCode, err = bazelisk.Run(args.Format(), &bazelisk.RunOpts{Stdout: buf, Stderr: buf})
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
	fmt.Println("bb commands:")
	for _, c := range cli_command.Commands {
		fmt.Printf("  %s  %s\n", padEnd(c.Name, 18), c.Help)
	}
	fmt.Println()
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
