#!/usr/bin/env bash
# setup.sh — Install Claude Code if not already present.
set -euo pipefail

if [[ -z "${ANTHROPIC_API_KEY:-}" ]]; then
  echo "Error: ANTHROPIC_API_KEY is not set." >&2
  exit 1
fi

if command -v claude &>/dev/null; then
  echo "==> claude already installed: $(command -v claude)" >&2
  exit 0
fi

echo "==> Installing Claude Code..." >&2
curl -fsSL https://claude.ai/install.sh | bash

# Move to a directory already on PATH so callers don't need to modify PATH.
if [[ -f "$HOME/.local/bin/claude" ]]; then
  sudo mv "$HOME/.local/bin/claude" /usr/local/bin/claude
fi

if ! command -v claude &>/dev/null; then
  echo "Error: Claude Code installation failed." >&2
  exit 1
fi

echo "==> Claude Code installed: $(command -v claude)" >&2
