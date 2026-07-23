package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/util/agent/agentutil"
)

type event struct {
	Type     string `json:"type"`
	ThreadID string `json:"thread_id"`
	Item     struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"item"`
}

func Run(ctx context.Context, request *agentutil.RunRequest) (*agentutil.RunResponse, error) {
	if _, err := exec.LookPath("codex"); err != nil {
		return nil, fmt.Errorf("codex is not installed or not in PATH")
	}

	args := commandArgs(request)
	cmd := exec.CommandContext(ctx, "codex", args...)
	cmd.Stdin = strings.NewReader(request.Prompt)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if message := strings.TrimSpace(stderr.String()); message != "" {
			return nil, fmt.Errorf("codex failed: %w: %s", err, message)
		}
		return nil, fmt.Errorf("codex failed: %w", err)
	}
	return parseResponse(stdout.Bytes())
}

func commandArgs(request *agentutil.RunRequest) []string {
	// Codex does not support a tool allowlist equivalent to Claude's
	// --allowedTools. Run without approvals in a read-only sandbox.
	args := []string{
		"exec",
		"--sandbox", "read-only",
		"--config", `approval_policy="never"`,
		"--json",
	}
	if request.Model != "" {
		args = append(args, "--model", request.Model)
	}
	if request.ReasoningEffort != "" {
		args = append(args, "--config", fmt.Sprintf("model_reasoning_effort=%q", request.ReasoningEffort))
	}
	return args
}

func parseResponse(rawOutput []byte) (*agentutil.RunResponse, error) {
	// In JSON mode, Codex emits a JSON event stream.
	var sessionID, output string
	decoder := json.NewDecoder(bytes.NewReader(rawOutput))
	for {
		e := &event{}
		if err := decoder.Decode(e); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("parse codex response: %w", err)
		}
		switch {
		case e.Type == "thread.started":
			// The thread.started event contains the session ID.
			sessionID = e.ThreadID
		case e.Type == "item.completed" && e.Item.Type == "agent_message":
			// The agent_message event contains the output.
			output = e.Item.Text
		}
	}
	if sessionID == "" {
		return nil, fmt.Errorf("parse codex response: session ID is missing")
	}
	if strings.TrimSpace(output) == "" {
		return nil, fmt.Errorf("parse codex response: result is empty")
	}
	return &agentutil.RunResponse{
		Output:        output,
		SessionID:     sessionID,
		ResumeCommand: fmt.Sprintf("codex exec resume %s", sessionID),
	}, nil
}
