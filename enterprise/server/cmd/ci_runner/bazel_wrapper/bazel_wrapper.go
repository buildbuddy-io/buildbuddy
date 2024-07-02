package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/creack/pty"
	"github.com/google/shlex"
)

// This is a wrapper to Bazel that appends Bazel flags.
var (
	BazelWrapperFlagset = flag.NewFlagSet("bazel_wrapper", flag.ContinueOnError)

	rootPath = BazelWrapperFlagset.String("root_path", "", "The absolute path of the workspace root dir. See ci_runner/main.go:workspace for more details.")

	bazelBinary       = BazelWrapperFlagset.String("bazel_bin", "", "The bazel binary to invoke.")
	bazelStartupFlags = BazelWrapperFlagset.String("bazel_startup_flags", "", "Startup flags to pass to bazel. The value can include spaces and will be properly tokenized.")
	extraBazelArgs    = BazelWrapperFlagset.String("extra_bazel_args", "", "Extra flags to pass to the bazel command. The value can include spaces and will be properly tokenized.")
)

const (
	// buildbuddyBazelrcPath is the path where we write a bazelrc file to be
	// applied to all bazel commands. The path is relative to the runner workspace
	// root, which notably is not the same as the bazel workspace / git repo root.
	//
	// This bazelrc takes lower precedence than the workspace .bazelrc.
	buildbuddyBazelrcPath = "buildbuddy.bazelrc"
	// Name of the bazel output base dir. This is written under the workspace
	// so that it can be cleaned up when the workspace is cleaned up.
	outputBaseDirName = "output-base"

	// ANSI codes for cases where the aurora equivalent is not supported by our UI
	// (ex: aurora's "grayscale" mode results in some ANSI codes that we don't currently
	// parse correctly).
	ansiGray  = "\033[90m"
	ansiReset = "\033[0m"
)

func main() {
	args := os.Args[1:]
	err := BazelWrapperFlagset.Parse(args)
	if err != nil {
		log.Fatalf("%s", err.Error())
	}

	if *bazelBinary == "" {
		log.Fatalf("--bazel-bin is required.")
	}
	args = removeWrapperFlags(args)

	args, err = appendStartupArgs(args)
	if err != nil {
		log.Fatalf("%s", err.Error())
	}
	args, err = appendBazelArgs(args)
	if err != nil {
		log.Fatalf("%s", err.Error())
	}

	io.WriteString(os.Stderr, ansiGray+fmt.Sprintf("Running command: %s %s\n\n", *bazelBinary, strings.Join(args, " "))+ansiReset+" ")
	if _, err := runCommand(*bazelBinary, args, os.Stderr); err != nil {
		log.Fatalf("%s", err.Error())
	}
}

func runCommand(executable string, args []string, out io.Writer) (string, error) {
	var buf bytes.Buffer
	w := io.MultiWriter(out, &buf)

	cmd := exec.Command(executable, args...)
	cmd.Env = os.Environ()
	size := &pty.Winsize{Rows: uint16(20), Cols: uint16(114)}
	f, err := pty.StartWithSize(cmd, size)
	if err != nil {
		return "", err
	}
	defer f.Close()
	copyOutputDone := make(chan struct{})
	go func() {
		io.Copy(w, f)
		copyOutputDone <- struct{}{}
	}()
	err = cmd.Wait()
	<-copyOutputDone

	output := buf.String()
	output = strings.ReplaceAll(output, "\x1b[0m", "")
	return strings.TrimSpace(output), err
}

// Remove all flags configuring this bazel_wrapper script, that should not be
// passed to the bazel command
func removeWrapperFlags(args []string) []string {
	out := args
	BazelWrapperFlagset.VisitAll(func(f *flag.Flag) {
		_, out = removeFlag(out, f.Name)
	})
	return out
}

func removeFlag(args []string, flag string) (string, []string) {
	arg, i, length := find(args, flag)
	if i < 0 {
		return "", args
	}
	return arg, append(args[:i], args[i+length:]...)
}

// Helper method for finding arguments by prefix within a list of arguments
func find(args []string, desiredArg string) (value string, index int, length int) {
	exact := fmt.Sprintf("--%s", desiredArg)
	prefix := fmt.Sprintf("--%s=", desiredArg)
	for i, arg := range args {
		// Handle "--name", "value" form
		if arg == exact && i+1 < len(args) {
			return args[i+1], i, 2
		}
		// Handle "--name=value" form
		if strings.HasPrefix(arg, prefix) {
			return strings.TrimPrefix(arg, prefix), i, 1
		}
	}
	return "", -1, 0
}

func appendStartupArgs(args []string) ([]string, error) {
	startupFlags, err := shlex.Split(*bazelStartupFlags)
	if err != nil {
		return nil, err
	}

	startupFlags = append(startupFlags, "--output_base="+filepath.Join(*rootPath, outputBaseDirName))
	startupFlags = append(startupFlags, "--bazelrc="+filepath.Join(*rootPath, buildbuddyBazelrcPath))

	// Bazel will treat the user's workspace .bazelrc file with lower precedence
	// than our --bazelrc, which is undesired. So instead, explicitly add the
	// workspace rc as a --bazelrc flag after ours, and also set --noworkspace_rc
	// to prevent the workspace rc from getting loaded twice.
	bazelWorkspacePath, err := runCommand(*bazelBinary, []string{"info", "workspace"}, io.Discard)
	if err != nil {
		return nil, err
	}
	workspaceRcPath := filepath.Join(bazelWorkspacePath, ".bazelrc")
	_, err = os.Stat(workspaceRcPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if exists := err == nil; exists {
		startupFlags = append(startupFlags, "--noworkspace_rc", "--bazelrc="+workspaceRcPath)
	}

	return append(startupFlags, args...), nil
}

// appendBazelArgs appends bazel arguments to a bazel command,
// *before* the arg separator ("--") if it exists, so that the arguments apply
// to the bazel subcommand ("build", "run", etc.) and not the binary being run
// (in the "bazel run" case).
func appendBazelArgs(args []string) ([]string, error) {
	extras, err := shlex.Split(*extraBazelArgs)
	if err != nil {
		return nil, err
	}

	splitIndex := len(args)
	for i, arg := range args {
		if arg == "--" {
			splitIndex = i
			break
		}
	}
	out := make([]string, 0, len(args)+len(extras))
	out = append(out, args[:splitIndex]...)
	out = append(out, extras...)
	out = append(out, args[splitIndex:]...)
	return out, nil
}
