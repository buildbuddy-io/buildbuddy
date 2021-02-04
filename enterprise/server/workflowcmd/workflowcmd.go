package workflowcmd

import (
	"log"
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
func (w *workflowScript) Build() (string, error) {
	buf := ""
	log.Print("Assembled workflow script:")
	for _, c := range w.commands {
		buf += c
		buf += "\n"
		log.Printf("  %s", c)
	}
	return buf, nil
}

func appendCheckoutCmd(repoURL, commitSHA string, w *workflowScript) {
	w.AddCommand("git", []string{"clone", "-q", repoURL})
	w.AddCommand("git", []string{"checkout", "-q", commitSHA})
}

func appendTestCmd(w *workflowScript) {
	w.AddCommand("bazelisk", []string{"test", "//..."})
}

// GenerateShellScript generates a build command for a repo at a given
// commit.
func GenerateShellScript(repoURL, commitSHA string) (string, error) {
	script := newWorkflowScript()

	// Keep it simple for now!
	appendCheckoutCmd(repoURL, commitSHA, script)
	appendTestCmd(script)

	return script.Build()
}


