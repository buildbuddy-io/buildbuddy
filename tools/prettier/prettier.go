package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

var filePatterns = []string{
	"*.css",
	"*.html",
	"*.js",
	"*.json",
	"*.jsx",
	"*.md",
	"*.ts",
	"*.tsx",
	"*.yaml",
}

func main() {
	if workspaceDir := os.Getenv("BUILD_WORKSPACE_DIRECTORY"); workspaceDir != "" {
		if err := os.Chdir(workspaceDir); err != nil {
			fmt.Fprintf(os.Stderr, "failed to change directory: %v\n", err)
			os.Exit(1)
		}
	}

	paths, err := getPathsToFormat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get paths to format: %v\n", err)
		os.Exit(1)
	}

	if len(paths) == 0 {
		os.Exit(0)
	}

	prettierPath := os.Getenv("PRETTIER_PATH")
	var cmd *exec.Cmd
	if prettierPath != "" {
		cmd = exec.Command(prettierPath, paths...)
	} else {
		rlocation, err := exec.Command("rlocation", "npm/prettier/bin/prettier.sh").Output()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get prettier location: %v\n", err)
			os.Exit(1)
		}
		cmd = exec.Command(strings.TrimSpace(string(rlocation)), append([]string{"--bazel_node_working_dir=" + os.Getenv("PWD")}, paths...)...)
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "prettier failed: %v\n", err)
		os.Exit(1)
	}
}

func getPathsToFormat() ([]string, error) {
	diffBase := os.Getenv("DIFF_BASE")
	if diffBase == "" {
		branch, err := exec.Command("git", "rev-parse", "--abbrev-ref", "HEAD").Output()
		if err != nil {
			return nil, fmt.Errorf("failed to get current branch: %w", err)
		}

		if strings.TrimSpace(string(branch)) == "master" {
			diffBase = "HEAD~1"
		} else {
			gitBaseBranch := os.Getenv("GIT_BASE_BRANCH")
			if gitBaseBranch == "" {
				gitBaseBranch = "master"
			}
			mergeBase, err := exec.Command("git", "merge-base", "HEAD", "origin/"+gitBaseBranch).Output()
			if err != nil {
				return nil, fmt.Errorf("failed to get merge base: %w", err)
			}
			diffBase = strings.TrimSpace(string(mergeBase))
		}
	}

	// Check if prettier config or version changed
	configChanged := false
	if out, err := exec.Command("git", "diff", diffBase, "--", "yarn.lock").Output(); err == nil {
		configChanged = strings.Contains(string(out), "prettier")
	}
	if !configChanged {
		if out, err := exec.Command("git", "diff", diffBase, "--", ".prettierrc", "tools/prettier/prettier.sh").Output(); err == nil {
			configChanged = len(out) > 0
		}
	}

	var paths []string
	if configChanged {
		// Get all files matching patterns
		for _, pattern := range filePatterns {
			out, err := exec.Command("git", "ls-files", "--", pattern).Output()
			if err != nil {
				return nil, fmt.Errorf("failed to list files: %w", err)
			}
			if len(out) > 0 {
				paths = append(paths, strings.Split(strings.TrimSpace(string(out)), "\n")...)
			}
		}
	} else {
		// Get only changed files matching patterns
		for _, pattern := range filePatterns {
			out, err := exec.Command("git", "diff", "--name-only", "--diff-filter=AMRCT", diffBase, "--", pattern).Output()
			if err != nil {
				return nil, fmt.Errorf("failed to get changed files: %w", err)
			}
			if len(out) > 0 {
				paths = append(paths, strings.Split(strings.TrimSpace(string(out)), "\n")...)
			}
		}
	}

	return paths, nil
}
