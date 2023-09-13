package picker

import (
	"bytes"
	"os"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/terminal"
	"github.com/manifoldco/promptui"
)

func HandlePicker(args []string) []string {
	// If targets are already specified, don't do anything.
	if len(arg.GetTargets(args)) > 0 {
		return args
	}

	// If the command is build, test, or query without a specified target - apply to all targets.
	command := arg.GetCommand(args)

	// Skip using the picker if the user has specified a query file.
	if strings.Contains(command, "query") && arg.Has(args, "query_file") {
		return args
	}

	// Skip using the picker if the user has specified a target pattern file.
	if (command == "build" || command == "test") && arg.Has(args, "target_pattern_file") {
		return args
	}

	// If it's a build, test, or query - apply to all targets.
	if command == "build" || command == "test" || command == "query" {
		args = append(args, "//...")
		return args
	}

	// If it's not a run command, we're done here.
	if command != "run" {
		return args
	}

	// If it's a run, query executable targets.
	queryArgs := []string{"query", "--keep_going", `kind(".*_(binary|application)", ...) + attr(executable, 1, ...)`}
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	opts := &bazelisk.RunOpts{Stdout: stdout, Stderr: stderr}
	bazelisk.Run(queryArgs, opts)
	targetString := strings.TrimSpace(stdout.String())
	targets := strings.Split(targetString, "\n")

	// We didn't find any executable targets to run.
	if len(targets) == 0 || targets[0] == "" {
		log.Printf("No runnable targets found!")
		return args
	}

	// If there is only one executable target, run it.
	if len(targets) == 1 {
		args = append(args, targets[0])
		return args
	}

	// If not running interactively, we can't show a prompt.
	if !terminal.IsTTY(os.Stdin) || !terminal.IsTTY(os.Stderr) {
		return args
	}

	// If there are more than one executable targets, show a picker.
	prompt := promptui.Select{
		Label:             "Select target to run",
		Items:             targets,
		Stdout:            &bellSkipper{},
		Size:              10,
		Searcher:          searcher(targets),
		StartInSearchMode: true,
		Keys: &promptui.SelectKeys{
			Prev:     promptui.Key{Code: promptui.KeyPrev, Display: promptui.KeyPrevDisplay},
			Next:     promptui.Key{Code: promptui.KeyNext, Display: promptui.KeyNextDisplay},
			PageUp:   promptui.Key{Code: promptui.KeyBackward, Display: promptui.KeyBackwardDisplay},
			PageDown: promptui.Key{Code: promptui.KeyForward, Display: promptui.KeyForwardDisplay},
			Search:   promptui.Key{Code: '?', Display: "?"},
		},
	}
	_, result, err := prompt.Run()
	if err != nil {
		log.Printf("Failed to select target: %v", err)
		return args
	}
	args = append(args, result)

	return args
}

func searcher(targets []string) func(input string, index int) bool {
	return func(input string, index int) bool {
		return strings.Contains(targets[index], input)
	}
}

// This is a workaround for the bell issue documented in
// https://github.com/manifoldco/promptui/issues/49.
type bellSkipper struct{}

func (bs *bellSkipper) Write(b []byte) (int, error) {
	const charBell = 7 // c.f. readline.CharBell
	if len(b) == 1 && b[0] == charBell {
		return 0, nil
	}
	return os.Stderr.Write(b)
}

func (bs *bellSkipper) Close() error {
	return os.Stderr.Close()
}
