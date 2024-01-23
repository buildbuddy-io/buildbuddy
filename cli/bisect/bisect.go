package bisect

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
)

var flags = flag.NewFlagSet("bisect", flag.ContinueOnError)

var (
	goodCommit   = flags.String("good", "", "The commit that is known to be good")
	badCommit    = flags.String("bad", "", "The commit that is known to be bad")
	bazelCommand = flags.String("bazel_command", "test", "The bazel command to run to test the commit")
	bazelConfig  = flags.String("bazel_config", "", "The bazel config to run to test the commit")
	bazelTarget  = flags.String("bazel_target", "//...", "The bazel target to run to test the commit")
)

const (
	usage = `
usage: bb bisect [flags]
`
)

func HandleBisect(args []string) (int, error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			log.Print("\nAll options:")
			flags.SetOutput(os.Stderr)
			flags.PrintDefaults()
			return 1, nil
		}
		return -1, err
	}

	good := *goodCommit
	if good == "" {
		good = "HEAD"
	}

	bad := *badCommit
	if bad == "" {
		lastBad, err := findLastKnownBadCommit()
		if err != nil {
			return 1, fmt.Errorf("Error finding last known bad commit: %s\n", err)
		}
		bad = lastBad
	}

	if err := runBisect(good, bad); err != nil {
		return 1, fmt.Errorf("bisect failed: %w", err)
	}

	return 0, nil
}

// TODO: make the strategy configurable
func findLastKnownBadCommit() (string, error) {
	steps := 1
	currentCommit := "HEAD"
	for {
		if steps > 100 {
			return "", fmt.Errorf("unable to find last known bad commit")
		}

		isGood, err := isGoodCommit(currentCommit)
		if err != nil {
			fmt.Printf("Error checking commit: %s\n", err)
			return "", err
		}

		if !isGood {
			return currentCommit, nil
		}
		currentCommit += fmt.Sprintf("~%d", steps)
		steps = steps * 2
	}
}

func runBisect(good, bad string) error {
	log, err := exec.Command("git", "log", "--oneline", fmt.Sprintf("%s..%s", bad, good)).Output()
	if err != nil {
		fmt.Printf("Error running git log: %s\n", err)
		return err
	}

	gitLog := strings.Split(strings.TrimSpace(string(log)), "\n")
	if len(gitLog) < 2 {
		return fmt.Errorf("Invalid git log: returned %d lines:\n%s", len(gitLog), log)
	}
	if len(gitLog) == 2 {
		fmt.Printf("Found the bad commit: %s\n", bad)
		return nil
	}

	// TODO: once we incorporate 'bb remote', we could run bisect remotely
	// and make it n-sect (configurable, automatically pick n depending on range size),
	// instead of just bisect (n := 2).
	bisectionPoint := len(gitLog) / 2
	bisectCommit := strings.Split(string(gitLog[bisectionPoint]), " ")[0]
	printRange(gitLog, bisectionPoint)

	isGood, err := isGoodCommit(bisectCommit)
	if err != nil {
		fmt.Printf("Error checking commit: %s\n", err)
		return err
	}

	// TODO: remove tail recursion here with a worker pool + queue
	if isGood {
		return runBisect(bisectCommit, bad)
	}
	return runBisect(good, bisectCommit)
}

// TODO: visualize the whole stack of commit with status for known commits
// TODO: find a way to visualize this better
func printRange(gitLog []string, bisectionPoint int) {
	fmt.Printf(`%s		<-- good
.
%s		<-- bisect commit
.
%s		<-- bad
`, gitLog[0], gitLog[bisectionPoint], gitLog[len(gitLog)-1])
}

func isGoodCommit(commit string) (bool, error) {
	if err := exec.Command("git", "checkout", commit).Run(); err != nil {
		fmt.Printf("Error checking out commit %s: %s\n", commit, err)
		return false, err
	}

	bzlCmd := []string{*bazelCommand, *bazelTarget}
	if *bazelConfig != "" {
		bzlCmd = []string{*bazelCommand, fmt.Sprintf("--config=%s", *bazelConfig), *bazelTarget}
	}

	fmt.Printf("Running bazel test on commit %s: %s\n", commit, strings.Join(bzlCmd, " "))

	// TODO: replace with bazelisk.Run() or bb remote
	if err := exec.Command("bazel", bzlCmd...).Run(); err != nil {
		fmt.Printf("Error running bazel test at commit %s: %s\n", commit, err)
		return false, nil
	}

	return true, nil
}
