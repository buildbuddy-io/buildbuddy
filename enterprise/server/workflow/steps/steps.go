// Package steps provides reusable steps for backend-defined workflows.
package steps

import "github.com/buildbuddy-io/buildbuddy/proto/runner"

// InstallClaude installs Claude Code when it is not already available.
func InstallClaude() *runner.Step {
	return &runner.Step{Run: `set -euo pipefail
if ! command -v claude >/dev/null 2>&1; then
  curl -fsSL https://claude.ai/install.sh | bash
  if [ -f "$HOME/.local/bin/claude" ]; then
    sudo mv "$HOME/.local/bin/claude" /usr/local/bin/claude
  fi
fi
command -v claude
`}
}

// InstallCodex installs Codex when it is not already available.
func InstallCodex() *runner.Step {
	return &runner.Step{Run: `set -euo pipefail
if ! command -v codex >/dev/null 2>&1; then
  curl -fsSL https://chatgpt.com/codex/install.sh | CODEX_NON_INTERACTIVE=1 sh
  if [ -f "$HOME/.local/bin/codex" ]; then
    sudo mv "$HOME/.local/bin/codex" /usr/local/bin/codex
  fi
fi
command -v codex
`}
}

// InstallGH installs the GitHub CLI when it is not already available.
func InstallGH() *runner.Step {
	return &runner.Step{Run: `set -euo pipefail
if ! command -v gh >/dev/null 2>&1; then
  sudo apt-get update -q
  sudo apt-get install -y curl gpg
  curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
    | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg 2>/dev/null
  sudo chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
    | sudo tee /etc/apt/sources.list.d/github-cli.list >/dev/null
  sudo apt-get update -q
  sudo apt-get install -y gh
fi
command -v gh
`}
}
