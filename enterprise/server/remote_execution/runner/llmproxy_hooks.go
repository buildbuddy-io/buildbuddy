package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func prepareAgentRedactionHooks(workspacePath, proxyURL string, configureCodexProvider bool) (string, error) {
	configDir, err := os.MkdirTemp(workspacePath, ".buildbuddy-llm-proxy-")
	if err != nil {
		return "", status.WrapError(err, "create LLM proxy config directory")
	}
	configDirName := filepath.Base(configDir)
	guestConfigDir := "/tmp/" + configDirName
	hookPath := guestConfigDir + "/redact-hook"

	hookScript := `#!/bin/sh
if ! command -v curl >/dev/null 2>&1; then
  echo "BuildBuddy secret redaction unavailable; tool output withheld." >&2
  exit 2
fi
if ! curl --fail --silent --show-error --max-time 30 \
  -H "Content-Type: application/json" \
  --data-binary @- "$1"; then
  echo "BuildBuddy secret redaction unavailable; tool output withheld." >&2
  exit 2
fi
`
	if err := os.WriteFile(filepath.Join(configDir, "redact-hook"), []byte(hookScript), 0o555); err != nil {
		return "", status.WrapError(err, "write LLM redaction hook")
	}

	claudeSettings := map[string]any{
		"hooks": map[string]any{
			"PostToolUse": []any{
				map[string]any{
					"matcher": "*",
					"hooks": []any{
						map[string]any{
							"type":    "command",
							"command": fmt.Sprintf("%s %s/hooks/claude/post-tool-use", hookPath, proxyURL),
							"timeout": 30,
						},
					},
				},
			},
		},
	}
	claudeJSON, err := json.Marshal(claudeSettings)
	if err != nil {
		return "", status.WrapError(err, "marshal Claude hook settings")
	}
	if err := os.WriteFile(filepath.Join(configDir, "claude-settings.json"), claudeJSON, 0o444); err != nil {
		return "", status.WrapError(err, "write Claude hook settings")
	}

	var codexConfig strings.Builder
	if configureCodexProvider {
		fmt.Fprintf(&codexConfig, `model_provider = "buildbuddy"

[model_providers.buildbuddy]
name = "BuildBuddy OpenAI proxy"
base_url = %q
env_key = "CODEX_API_KEY"
wire_api = "responses"

`, proxyURL+"/openai/v1")
	}
	fmt.Fprintf(&codexConfig, `
[features]
hooks = true

[[hooks.PostToolUse]]
matcher = "*"

[[hooks.PostToolUse.hooks]]
type = "command"
command = %q
timeout = 30
statusMessage = "Redacting secrets from tool output"
`, hookPath+" "+proxyURL+"/hooks/codex/post-tool-use")
	if err := os.WriteFile(filepath.Join(configDir, "codex-config.toml"), []byte(codexConfig.String()), 0o444); err != nil {
		return "", status.WrapError(err, "write Codex hook config")
	}
	return configDirName, nil
}

func installAgentRedactionHooks(ctx context.Context, c *container.TracedCommandContainer, workspacePath, configDirName string) error {
	if configDirName == "" {
		return status.FailedPreconditionError("LLM proxy config was not prepared")
	}
	stagingDir := filepath.Join(workspacePath, configDirName)
	guestConfigDir := "/tmp/" + configDirName
	readConfig := func(name string) (string, error) {
		b, err := os.ReadFile(filepath.Join(stagingDir, name))
		if err != nil {
			return "", status.WrapErrorf(err, "read agent hook config %q", name)
		}
		return string(b), nil
	}
	hookScript, err := readConfig("redact-hook")
	if err != nil {
		return err
	}
	claudeSettings, err := readConfig("claude-settings.json")
	if err != nil {
		return err
	}
	codexConfig, err := readConfig("codex-config.toml")
	if err != nil {
		return err
	}
	commands := []string{
		"set -eu",
		fmt.Sprintf("mkdir -p %s %s", shlex.Quote(guestConfigDir+"/claude"), shlex.Quote(guestConfigDir+"/codex")),
		fmt.Sprintf("printf %%s %s > %s", shlex.Quote(hookScript), shlex.Quote(guestConfigDir+"/redact-hook")),
		fmt.Sprintf("chmod 0555 %s", shlex.Quote(guestConfigDir+"/redact-hook")),
		fmt.Sprintf("printf %%s %s > %s", shlex.Quote(claudeSettings), shlex.Quote(guestConfigDir+"/claude/settings.json")),
		fmt.Sprintf("printf %%s %s > %s", shlex.Quote(codexConfig), shlex.Quote(guestConfigDir+"/codex/config.toml")),
	}
	result := c.Exec(ctx, &repb.Command{
		Arguments: []string{"/bin/sh", "-c", strings.Join(commands, "\n")},
	}, &interfaces.Stdio{})
	stderr := strings.TrimSpace(string(result.Stderr))
	if result.Error != nil {
		if stderr != "" {
			return status.WrapErrorf(result.Error, "install agent redaction hooks: %s", stderr)
		}
		return status.WrapError(result.Error, "install agent redaction hooks")
	}
	if result.ExitCode != 0 {
		if stderr != "" {
			return status.FailedPreconditionErrorf("install agent redaction hooks: exited with code %d: %s", result.ExitCode, stderr)
		}
		return status.FailedPreconditionErrorf("install agent redaction hooks: exited with code %d", result.ExitCode)
	}
	return nil
}
