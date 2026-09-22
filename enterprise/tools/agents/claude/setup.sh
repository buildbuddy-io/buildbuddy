#!/usr/bin/env bash
# setup.sh installs Claude Code, or updates it if the last install is over 24h old.
set -euo pipefail

if [[ -z "${ANTHROPIC_API_KEY:-}" ]]; then
  echo "Error: ANTHROPIC_API_KEY is not set." >&2
  exit 1
fi

# Workflow runners are recycled, so an installed claude binary would otherwise
# stay at whatever version the runner first installed. Record the install time
# so that later runs re-run the installer (which fetches the latest release)
# at most once per day.
INSTALL_STAMP="$HOME/.cache/claude-setup/last-install"

if command -v claude &>/dev/null && [[ -n "$(find "$INSTALL_STAMP" -mmin -1440 2>/dev/null)" ]]; then
  echo "==> claude already installed and up to date: $(command -v claude)" >&2
  exit 0
fi

echo "==> Installing latest Claude Code..." >&2
curl -fsSL https://claude.ai/install.sh | bash

# Move to a directory already on PATH so callers don't need to modify PATH.
if [[ -f "$HOME/.local/bin/claude" ]]; then
  sudo mv "$HOME/.local/bin/claude" /usr/local/bin/claude
fi

if ! command -v claude &>/dev/null; then
  echo "Error: Claude Code installation failed." >&2
  exit 1
fi

mkdir -p "$(dirname "$INSTALL_STAMP")"
touch "$INSTALL_STAMP"

echo "==> Claude Code installed: $(command -v claude) ($(claude --version))" >&2
