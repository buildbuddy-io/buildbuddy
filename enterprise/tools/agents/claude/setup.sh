#!/usr/bin/env bash
# setup.sh installs Claude Code, or updates it if the last install is older than
# AGENT_AUTO_UPDATE_CHECK_INTERVAL seconds (default 86400, one day).
#
# Workflow runners are recycled, so without updates an installed claude binary
# would stay at whatever version the runner first installed. Set
# AGENT_AUTO_UPDATE=false (or 0) to keep the installed version instead.
set -euo pipefail

if [[ -z "${ANTHROPIC_API_KEY:-}" ]]; then
  echo "Error: ANTHROPIC_API_KEY is not set." >&2
  exit 1
fi

case "${AGENT_AUTO_UPDATE:-true}" in
  true | 1) AUTO_UPDATE=1 ;;
  false | 0) AUTO_UPDATE=0 ;;
  *)
    echo "Error: AGENT_AUTO_UPDATE must be true, false, 1, or 0." >&2
    exit 1
    ;;
esac
AUTO_UPDATE_CHECK_INTERVAL="${AGENT_AUTO_UPDATE_CHECK_INTERVAL:-86400}"
if [[ ! "$AUTO_UPDATE_CHECK_INTERVAL" =~ ^[0-9]+$ ]]; then
  echo "Error: AGENT_AUTO_UPDATE_CHECK_INTERVAL must be a number of seconds." >&2
  exit 1
fi

INSTALL_STAMP="$HOME/.cache/claude-setup/last-install"

if command -v claude &>/dev/null; then
  if [[ "$AUTO_UPDATE" == 0 ]]; then
    echo "==> claude already installed: $(command -v claude)" >&2
    exit 0
  fi
  # A missing or corrupt stamp counts as stale, so the install below rewrites it.
  last_install="$(cat "$INSTALL_STAMP" 2>/dev/null || true)"
  if [[ "$last_install" =~ ^[0-9]+$ ]] && (( $(date +%s) - last_install < AUTO_UPDATE_CHECK_INTERVAL )); then
    echo "==> claude already installed and updated within the last ${AUTO_UPDATE_CHECK_INTERVAL}s: $(command -v claude)" >&2
    exit 0
  fi
fi

echo "==> Installing latest Claude Code..." >&2
curl -fsSL https://claude.ai/install.sh | bash

# Check for the installer's output rather than using command -v, since during
# an update the previously installed binary is still on PATH.
if [[ ! -f "$HOME/.local/bin/claude" ]]; then
  echo "Error: Claude Code installation failed: $HOME/.local/bin/claude not found." >&2
  exit 1
fi

# Move to a directory already on PATH so callers don't need to modify PATH.
sudo mv "$HOME/.local/bin/claude" /usr/local/bin/claude

mkdir -p "$(dirname "$INSTALL_STAMP")"
date +%s > "$INSTALL_STAMP"

echo "==> Claude Code installed: $(command -v claude) ($(claude --version))" >&2
