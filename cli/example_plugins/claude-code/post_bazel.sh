#!/usr/bin/env bash

# This plugin sends failing build output to Claude Code for analysis
# It receives the bazel output file path as the first argument

OUTPUT_FILE="$1"
EXIT_CODE="${BAZEL_EXIT_CODE:-0}"

# Only proceed if the build failed (non-zero exit code)
if [ "$EXIT_CODE" -eq 0 ]; then
    exit 0
fi

# Check if Claude Code integration is enabled (default: enabled)
if [ "${BB_CLAUDE_CODE_ENABLED:-1}" != "1" ]; then
    exit 0
fi

# Get the Claude command (default: 'claude')
CLAUDE_CMD="${CLAUDE_CODE_CMD:-claude}"

# Check if claude CLI is available
if ! command -v "$CLAUDE_CMD" &> /dev/null; then
    echo -e "\x1b[31m[claude-code] Error: Claude CLI not found. Please ensure 'claude' is in your PATH or set CLAUDE_CODE_CMD environment variable.\x1b[0m" >&2
    exit 0
fi

# Check if auto-send is enabled
AUTO_SEND="${BB_CLAUDE_CODE_AUTO_SEND:-0}"

if [ "$AUTO_SEND" = "1" ]; then
    # Auto-send without prompting
    echo -e "\x1b[33m[claude-code] Build failed with exit code $EXIT_CODE. Auto-sending to Claude Code...\x1b[0m" >&2
    
    # Launch Claude Code with the build output
    $CLAUDE_CMD "$(cat "$OUTPUT_FILE")"
    
    echo -e "\x1b[32m[claude-code] Build failure sent to Claude Code\x1b[0m" >&2
else
    # Ask user if they want to send to Claude Code
    echo -e "\x1b[33m[claude-code] Build failed with exit code $EXIT_CODE.\x1b[0m" >&2
    echo -n -e "\x1b[36mWould you like Claude Code to analyze and help fix this error? [Y/n]: \x1b[0m" >&2
    
    # Read user input
    read -r response < /dev/tty
    
    # Default to 'yes' if user just presses enter
    if [[ -z "$response" || "$response" =~ ^[Yy]$ ]]; then
        echo -e "\x1b[33m[claude-code] Sending output to Claude Code for analysis...\x1b[0m" >&2
        
        # Launch Claude Code with the build output
        $CLAUDE_CMD "$(cat "$OUTPUT_FILE")"
        
        echo -e "\x1b[32m[claude-code] Build failure sent to Claude Code\x1b[0m" >&2
    else
        echo -e "\x1b[90m[claude-code] Skipping Claude Code analysis\x1b[0m" >&2
    fi
fi