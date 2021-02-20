package workflowcmd

import (
	"log"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/google/shlex"
)

var (
	unsafeSet = regexp.MustCompile(`[^\w@%+=:,./-]`)
)

func sanitize(s string) string {
	if unsafeSet.MatchString(s) {
		// Single-quote the arg so that it can't be interpreted specially
		// by the shell.
		s = "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
	}
	return s
}

type workflowScript struct {
	commands []string
}

func newWorkflowScript() *workflowScript {
	return &workflowScript{
		commands: make([]string, 0),
	}
}
func (w *workflowScript) AddCommand(command string, args []string) {
	cmdLine := command
	for _, arg := range args {
		cmdLine += " " + sanitize(arg)
	}
	w.commands = append(w.commands, cmdLine)
}
func (w *workflowScript) Build() ([]byte, error) {
	log.Print("Assembled workflow script:")
	buf := "#!/bin/bash\n"
	log.Printf("  %s", buf)
	for _, c := range w.commands {
		buf += c
		buf += "\n"
		log.Printf("  %s", c)
	}
	return []byte(buf), nil
}

func (w *workflowScript) Checkout(repoURL, commitSHA string) {
	repoDir := filepath.Base(repoURL)
	if repoDir == "" {
		repoDir = "repo"
	}

	w.AddCommand("git", []string{"clone", "-q", repoURL, repoDir})
	w.AddCommand("cd", []string{repoDir})
	w.AddCommand("git", []string{"checkout", "-q", commitSHA})
}

func (w *workflowScript) Bazel(cmd string) error {
	tokens, err := shlex.Split(cmd)
	if err != nil {
		return err
	}
	if len(tokens) == 0 {
		return nil
	}
	w.AddCommand(cmd, tokens)
	return nil
}

type CommandInfo struct {
	RepoURL   string
	CommitSHA string
	// BazelFlags are passed to the bazel subcommand (i.e. run {{UserFlags...}} {{BazelFlags...}} )
	BazelFlags []string
	// BazelCommands is a list of commands where each one
	BazelCommands []string
}

// GenerateShellScript generates a build command for a repo at a given
// commit.
func GenerateShellScript(ci *CommandInfo) ([]byte, error) {
	script := newWorkflowScript()

	script.Checkout(ci.RepoURL, ci.CommitSHA)
	for _, bazelCmd := range ci.BazelCommands {
		if err := script.Bazel(bazelCmd, ci.BazelFlags); err != nil {
			return nil, err
		}
	}

	return script.Build()
}

func flat(slices ...[]string) []string {
	cap := 0
	for _, slice := range slices {
		cap += len(slice)
	}
	out := make([]string, 0, cap)
	for _, slice := range slices {
		out = append(out, slice...)
	}
	return out
}

func indexOf(slice []string, val string) int {
	for i, v := range slice {
		if v == val {
			return i
		}
	}
	return -1
}
