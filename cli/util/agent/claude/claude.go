package claude

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

// Run sends prompt to Claude Code and returns its response. allowedToolsJSON
// must be a JSON array of Claude tool permission rules, such as
// `["Read", "Bash(git diff *)"]`. Run in dontAsk mode so unapproved tool calls
// are denied rather than prompting.
func Run(prompt, allowedToolsJSON string) (string, error) {
	args, err := commandArgs(allowedToolsJSON)
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
		if message := strings.TrimSpace(stderr.String()); message != "" {
			return "", fmt.Errorf("claude failed: %w: %s", err, message)
		}
		return "", fmt.Errorf("claude failed: %w", err)
	}
	return strings.TrimRight(stdout.String(), "\n"), nil
}

func commandArgs(allowedToolsJSON string) ([]string, error) {
	var allowedTools []string
	if err := json.Unmarshal([]byte(allowedToolsJSON), &allowedTools); err != nil {
		return nil, fmt.Errorf("parse allowed tools JSON: %w", err)
	}

	args := []string{"--print", "--permission-mode", "dontAsk"}
	if len(allowedTools) == 0 {
		return args, nil
	}
	args = append(args, "--allowedTools")
	return append(args, allowedTools...), nil
}
