package util

import (
	"fmt"
	"os/exec"
	"strings"
)

// GitListFilesWithExtensions lists files known to git that match the given
// extensions.
func GitListFilesWithExtensions(extensions []string) ([]string, error) {
	args := []string{"ls-files", "--"}
	for _, ext := range extensions {
		args = append(args, "*"+ext)
	}

	cmd := exec.Command("git", args...)
	b, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("run %q: %w: %s", strings.Join(cmd.Args, " "), err, exitErr.Stderr)
		}
		return nil, fmt.Errorf("run %q: %w", strings.Join(cmd.Args, " "), err)
	}
	return lines(string(b)), nil
}

func lines(s string) []string {
	s = strings.TrimSuffix(s, "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}
