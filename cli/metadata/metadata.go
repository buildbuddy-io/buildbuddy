package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
)

func AppendBuildMetadata(args, originalArgs []string) ([]string, error) {
	originalArgs = append(
		// os.Args[0] might be something like /usr/local/bin/bb.
		// filepath.Base gets just the "bb" part.
		[]string{filepath.Base(originalArgs[0])},
		originalArgs[1:]...,
	)

	originalArgsJSON, err := json.Marshal(originalArgs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal original command arguments: %s", err)
	}

	metadataFlags := []string{
		"--build_metadata=EXPLICIT_COMMAND_LINE=" + string(originalArgsJSON),
	}

	// If the user already has a workspace status script, don't set any default
	// build_metadata flags since those would override the workspace status
	// script.
	// TODO: Maybe respect existing env vars as well, since those would also
	// get overridden by build metadata.
	if arg.Get(args, "workspace_status_command") == "" {
		gitMdFlags, err := gitMetadataFlags()
		if err != nil {
			log.Debugf("Failed to compute git metadata flags: %s", err)
		} else {
			metadataFlags = append(metadataFlags, gitMdFlags...)
		}
	}
	// Append default metadata just after the bazel command and before other
	// flags, so that it can be overridden by user metadata flags if needed.
	args = appendBazelCommandArgs(args, metadataFlags)
	// TODO: Add metadata to help understand how config files were expanded,
	// and how plugins modified args (maybe not as build_metadata, but
	// rather as some sort of artifact we attach to the invocation ID).
	return args, nil
}

func gitMetadataFlags() ([]string, error) {
	ws, err := workspace.Path()
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(ws, ".git")); err != nil {
		return nil, err
	}
	var flags []string

	repoURL, err := runGit(ws, "config", "--get", "remote.origin.url")
	if err != nil {
		log.Debug(err)
	} else if repoURL != "" {
		repoURL = git.StripRepoURLCredentials(repoURL)
		flags = append(flags, "--build_metadata=REPO_URL="+repoURL)
	}

	commitSHA, err := runGit(ws, "rev-parse", "HEAD")
	if err != nil {
		log.Debug(err)
	} else if commitSHA != "" {
		flags = append(flags, "--build_metadata=COMMIT_SHA="+commitSHA)
	}

	branch, err := runGit(ws, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		log.Debug(err)
	} else if branch != "" {
		flags = append(flags, "--build_metadata=BRANCH_NAME="+branch)
	}

	return flags, nil
}

func runGit(dir string, args ...string) (output string, err error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	b, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("command `git %s` failed: %s", strings.Join(args, " "), string(b))
	}
	return strings.TrimSpace(string(b)), nil
}

// appendBazelCommmandArgs appends the given command args just after the bazel
// command, so that the args have lower priority than existing command args.
func appendBazelCommandArgs(args, commandArgs []string) []string {
	_, commandIndex := parser.GetBazelCommandAndIndex(args)
	if commandIndex == -1 {
		log.Debugf("Failed to append metadata: not a bazel command")
		return args
	}
	var out []string
	out = append(out, args[:commandIndex+1]...)
	out = append(out, commandArgs...)
	out = append(out, args[commandIndex+1:]...)
	return out
}
