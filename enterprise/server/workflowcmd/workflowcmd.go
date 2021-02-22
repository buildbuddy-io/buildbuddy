package workflowcmd

import (
	"log"
	"path/filepath"
	"regexp"
	"strings"
)

var unsafeSet = regexp.MustCompile(`[^\w@%+=:,./-]`)

func sanitize(s string) string {
	if unsafeSet.MatchString(s) {
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

func (w *workflowScript) SetShellOptions() {
	// See https://www.gnu.org/software/bash/manual/html_node/The-Set-Builtin.html

	// Exit the script if any command fails.
	w.AddCommand("set", []string{"-o", "errexit"})
	w.AddCommand("set", []string{"-o", "pipefail"})
}

func (w *workflowScript) Checkout(repoURL, commitSHA string) {
	repoDir := filepath.Base(repoURL)
	if repoDir == "" {
		repoDir = "repo"
	}

	w.AddCommand("mkdir", []string{repoDir})
	w.AddCommand("cd", []string{repoDir})
	w.AddCommand("git", []string{"init"})
	w.AddCommand("git", []string{"remote", "add", "origin", repoURL})
	w.AddCommand("git", []string{"fetch", "origin", commitSHA})
	w.AddCommand("git", []string{"checkout", commitSHA})
}

func (w *workflowScript) Test(bazelFlags []string) {
	args := []string{"test"}
	args = append(args, bazelFlags...)
	args = append(args, "//...")

	w.AddCommand("bazelisk", args)
}

type CommandInfo struct {
	RepoURL    string
	CommitSHA  string
	BazelFlags []string
}

// GenerateShellScript generates a build command for a repo at a given
// commit.
func GenerateShellScript(ci *CommandInfo) ([]byte, error) {
	script := newWorkflowScript()

	// Keep it simple for now!
	script.SetShellOptions()
	script.Checkout(ci.RepoURL, ci.CommitSHA)
	script.Test(ci.BazelFlags)

	return script.Build()
}
