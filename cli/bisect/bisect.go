package bisect

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
)

var flags = flag.NewFlagSet("bisect", flag.ContinueOnError)

var (
	goodCommit = flags.String("good", "", "The commit that is known to be good")
	badCommit  = flags.String("bad", "", "The commit that is known to be bad")
)

func HandleBisect(args []string) (int, error) {
	cmd, _ := arg.GetCommandAndIndex(args)
	if cmd != "bisect" {
		return -1, nil
	}

	good := *goodCommit
	if good == "" {
		good = "HEAD"
	}

	bad := *badCommit
	if bad == "" {
		lastBad, err := findLastKnownBadCommit()
		if err != nil {
			fmt.Printf("Error finding last known bad commit: %s\n", err)
			return 1, err
		}
		bad = lastBad
	}

	if err := runBisect(good, bad); err != nil {
		return 1, err
	}

	return 0, nil
}

// TODO: find the last known bad commit automatically
// This could be done by picking a range of commits to test: 1-week, 1-month, 6-months, etc.
func findLastKnownBadCommit() (string, error) {
	return "HEAD~10", nil
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

	// TODO: remove tail recursion here with a worker pool
	if isGood {
		return runBisect(bisectCommit, bad)
	}
	return runBisect(good, bisectCommit)
}

// TODO: visualize the whole stack of commit with status for known commits
func printRange(gitLog []string, bisectionPoint int) {
	fmt.Printf(`%s		<-- good
.
%s		<-- bisect commit
.
%s		<-- bad
`, gitLog[0], gitLog[bisectionPoint], gitLog[len(gitLog)-1])
}

// TODO: implement this
func isGoodCommit(commit string) (bool, error) {
	return true, nil
}
