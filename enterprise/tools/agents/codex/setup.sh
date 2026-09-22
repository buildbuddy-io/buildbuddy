#!/usr/bin/env bash
# setup.sh installs Codex, or updates it if the last install is older than
# AGENT_AUTO_UPDATE_CHECK_INTERVAL seconds (default 86400, one day).
#
# Workflow runners are recycled, so without updates an installed codex binary
# would stay at whatever version the runner first installed. Set
# AGENT_AUTO_UPDATE=0 to keep the installed version instead.
set -euo pipefail

if [[ -z "${CODEX_API_KEY:-}" ]]; then
  echo "Error: CODEX_API_KEY is not set." >&2
  exit 1
fi

AUTO_UPDATE="${AGENT_AUTO_UPDATE:-1}"
AUTO_UPDATE_CHECK_INTERVAL="${AGENT_AUTO_UPDATE_CHECK_INTERVAL:-86400}"
if [[ ! "$AUTO_UPDATE_CHECK_INTERVAL" =~ ^[0-9]+$ ]]; then
  echo "Error: AGENT_AUTO_UPDATE_CHECK_INTERVAL must be a number of seconds." >&2
  exit 1
fi

INSTALL_STAMP="$HOME/.cache/codex-setup/last-install"

if command -v codex &>/dev/null; then
  if [[ "$AUTO_UPDATE" != 1 ]]; then
    echo "==> codex already installed: $(command -v codex)" >&2
    exit 0
  fi
  if (( $(date +%s) - $(cat "$INSTALL_STAMP" 2>/dev/null || echo 0) < AUTO_UPDATE_CHECK_INTERVAL )); then
    echo "==> codex already installed and updated within the last ${AUTO_UPDATE_CHECK_INTERVAL}s: $(command -v codex)" >&2
    exit 0
  fi
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
date +%s > "$INSTALL_STAMP"

echo "==> Codex installed: $(command -v codex) ($(codex --version))" >&2
