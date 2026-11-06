package agent

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/cli/util/agent/agentutil"
	"github.com/buildbuddy-io/buildbuddy/cli/util/agent/claude"
	"github.com/buildbuddy-io/buildbuddy/cli/util/agent/codex"
)

// Run executes a request using the requested agent.
func Run(ctx context.Context, req *agentutil.RunRequest) error {
	switch req.Agent {
	case agentutil.Claude:
		return claude.Run(ctx, req)
	case agentutil.Codex:
		return codex.Run(ctx, req)
	default:
		return fmt.Errorf("unsupported agent %q", req.Agent)
	}
}
