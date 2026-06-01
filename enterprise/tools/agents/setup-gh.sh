#!/usr/bin/env bash
# setup-gh.sh — Install the GitHub CLI if not already present.
set -euo pipefail

if command -v gh &>/dev/null; then
  echo "==> gh already installed: $(command -v gh)" >&2
  exit 0
fi

echo "==> Installing GitHub CLI..." >&2

# gh is not in the default apt repos; use the official apt source.
sudo apt-get update -q
sudo apt-get install -y curl gpg
curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
  | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg 2>/dev/null
sudo chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
  | sudo tee /etc/apt/sources.list.d/github-cli.list >/dev/null
sudo apt-get update -q
sudo apt-get install -y gh

if ! command -v gh &>/dev/null; then
  echo "Error: GitHub CLI installation failed." >&2
  exit 1
fi

echo "==> GitHub CLI installed: $(command -v gh)" >&2
