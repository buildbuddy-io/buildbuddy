package claude

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/terminal"
)

type response struct {
	Result    string `json:"result"`
	SessionID string `json:"session_id"`
}

// Run sends prompt to Claude Code and returns its response. allowedToolsJSON
// must be a JSON array of Claude tool permission rules, such as
// `["Read", "Bash(git diff *)"]`. Run in dontAsk mode so unapproved tool calls
// are denied rather than prompting.
func Run(prompt string, allowedTools []string) (string, error) {
	args, err := commandArgs(allowedTools)
	if err != nil {
		return "", err
	}
	if _, err := exec.LookPath("claude"); err != nil {
		return "", fmt.Errorf("claude is not installed or not in PATH")
	}

	cmd := exec.Command("claude", args...)
	cmd.Stdin = strings.NewReader(prompt)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		errMsg := "claude failed"
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			errMsg += ": " + msg
		}
		if msg := strings.TrimSpace(stdout.String()); msg != "" {
			errMsg += ": " + msg
		}
		return "", fmt.Errorf("%s: %w", errMsg, err)
	}
	return formatResponse(stdout.Bytes())
}

func commandArgs(allowedTools []string) ([]string, error) {
	args := []string{"--print", "--permission-mode", "dontAsk", "--output-format", "json"}
	if len(allowedTools) == 0 {
		return args, nil
	}
	args = append(args, "--allowedTools")
	return append(args, allowedTools...), nil
}

func formatResponse(output []byte) (string, error) {
	claudeResponse := &response{}
	if err := json.Unmarshal(output, claudeResponse); err != nil {
		return "", fmt.Errorf("parse claude response: %w", err)
	}
	if claudeResponse.SessionID == "" {
		return "", fmt.Errorf("parse claude response: session ID is missing")
	}
	if strings.TrimSpace(claudeResponse.Result) == "" {
		return "", fmt.Errorf("parse claude response: result is empty")
	}
	result := strings.TrimRight(claudeResponse.Result, "\n")
	resumeMessage := fmt.Sprintf(
		"%sResume this Claude session with:%s\n%sclaude --resume %s%s",
		terminal.Esc(90), terminal.Esc(),
		terminal.Esc(36), claudeResponse.SessionID, terminal.Esc(),
	)
	return result + "\n\n" + resumeMessage, nil
}
