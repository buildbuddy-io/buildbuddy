#!/usr/bin/env bash
# setup.sh — Install Codex if not already present.
set -euo pipefail

if [[ -z "${CODEX_API_KEY:-}" ]]; then
  echo "Error: CODEX_API_KEY is not set." >&2
  exit 1
fi

if command -v codex &>/dev/null; then
  echo "==> codex already installed: $(command -v codex)" >&2
  exit 0
fi

echo "==> Installing Codex..." >&2
curl -fsSL https://chatgpt.com/codex/install.sh | CODEX_NON_INTERACTIVE=1 sh

# Move to a directory already on PATH so callers don't need to modify PATH.
if [[ -f "$HOME/.local/bin/codex" ]]; then
  sudo mv "$HOME/.local/bin/codex" /usr/local/bin/codex
fi

if ! command -v codex &>/dev/null; then
  echo "Error: Codex installation failed." >&2
  exit 1
fi

echo "==> Codex installed: $(command -v codex)" >&2
