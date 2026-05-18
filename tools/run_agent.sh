#!/bin/bash
set -euo pipefail

usage() {
  echo "Usage: $0 -q <question> [-f <context-file>]" >&2
  echo "  -q  Question to ask the agent (required)" >&2
  echo "  -f  File whose contents are prepended as context (optional)" >&2
  exit 1
}

QUESTION=""
CONTEXT_FILE=""

while getopts "q:f:" opt; do
  case $opt in
    q) QUESTION="$OPTARG" ;;
    f) CONTEXT_FILE="$OPTARG" ;;
    *) usage ;;
  esac
done

[[ -z "$QUESTION" ]] && usage

if [[ -z "${ANTHROPIC_API_KEY:-}" ]]; then
  echo "Error: ANTHROPIC_API_KEY is not set" >&2
  exit 1
fi

if ! command -v claude &>/dev/null; then
  curl -fsSL https://claude.ai/install.sh | bash
  export PATH="$HOME/.local/bin:$PATH"
fi

if [[ -n "$CONTEXT_FILE" ]]; then
  PROMPT="$(cat "$CONTEXT_FILE")

$QUESTION"
else
  PROMPT="$QUESTION"
fi

claude --print --bare --no-session-persistence "$PROMPT"
