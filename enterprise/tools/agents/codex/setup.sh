#!/usr/bin/env bash
# setup.sh installs Codex, or updates it if the last install is over 24h old.
set -euo pipefail

if [[ -z "${CODEX_API_KEY:-}" ]]; then
  echo "Error: CODEX_API_KEY is not set." >&2
  exit 1
fi

# Workflow runners are recycled, so an installed codex binary would otherwise
# stay at whatever version the runner first installed. Record the install time
# so that later runs re-run the installer (which fetches the latest release)
# at most once per day.
INSTALL_STAMP="$HOME/.cache/codex-setup/last-install"

if command -v codex &>/dev/null && [[ -n "$(find "$INSTALL_STAMP" -mmin -1440 2>/dev/null)" ]]; then
  echo "==> codex already installed and up to date: $(command -v codex)" >&2
  exit 0
fi

echo "==> Installing latest Codex..." >&2
curl -fsSL https://chatgpt.com/codex/install.sh | CODEX_NON_INTERACTIVE=1 sh

# Move to a directory already on PATH so callers don't need to modify PATH.
if [[ -f "$HOME/.local/bin/codex" ]]; then
  sudo mv "$HOME/.local/bin/codex" /usr/local/bin/codex
fi

if ! command -v codex &>/dev/null; then
  echo "Error: Codex installation failed." >&2
  exit 1
fi

mkdir -p "$(dirname "$INSTALL_STAMP")"
touch "$INSTALL_STAMP"

echo "==> Codex installed: $(command -v codex) ($(codex --version))" >&2
