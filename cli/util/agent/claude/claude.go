package claude

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/util/agent/agentutil"
)

type response struct {
	Result    string `json:"result"`
	SessionID string `json:"session_id"`
}

func Run(ctx context.Context, request *agentutil.RunRequest) (*agentutil.RunResponse, error) {
	if _, err := exec.LookPath("claude"); err != nil {
		return nil, fmt.Errorf("claude is not installed or not in PATH")
	}

	args := commandArgs(request)
	cmd := exec.CommandContext(ctx, "claude", args...)
	cmd.Stdin = strings.NewReader(request.Prompt)
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
		return nil, fmt.Errorf("%s: %w", errMsg, err)
	}
	return parseResponse(stdout.Bytes())
}

func commandArgs(request *agentutil.RunRequest) []string {
	// Run in dontAsk mode so unapproved tool calls are denied rather than prompting.
	args := []string{"--print", "--permission-mode", "dontAsk", "--output-format", "json"}
	if request.Model != "" {
		args = append(args, "--model", request.Model)
	}
	if request.ReasoningEffort != "" {
		args = append(args, "--effort", request.ReasoningEffort)
	}
	if len(request.AllowedTools) > 0 {
		args = append(args, "--allowedTools")
		args = append(args, request.AllowedTools...)
	}
	return args
}

func parseResponse(output []byte) (*agentutil.RunResponse, error) {
	claudeResponse := &response{}
	if err := json.Unmarshal(output, claudeResponse); err != nil {
		return nil, fmt.Errorf("parse claude response: %w", err)
	}
	if claudeResponse.SessionID == "" {
		return nil, fmt.Errorf("parse claude response: session ID is missing")
	}
	if strings.TrimSpace(claudeResponse.Result) == "" {
		return nil, fmt.Errorf("parse claude response: result is empty")
	}
	return &agentutil.RunResponse{
		Output:        claudeResponse.Result,
		SessionID:     claudeResponse.SessionID,
		ResumeCommand: fmt.Sprintf("claude --resume %s", claudeResponse.SessionID),
	}, nil
}
