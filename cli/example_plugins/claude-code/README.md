# Claude Code Plugin for BuildBuddy CLI

This plugin automatically sends failing build output to Claude Code for analysis and fix suggestions.

## Features

- Automatically detects build failures using the exit code
- Asks for user confirmation before sending to Claude Code
- Sends the complete, unmodified build output to Claude Code
- Claude analyzes the errors and suggests fixes
- Non-intrusive - only activates on build failures

## Installation

Install the plugin using the BuildBuddy CLI:

```bash
# Install for current workspace
bb install :cli/example_plugins/claude-code

# Or install globally for your user
bb install :cli/example_plugins/claude-code --user
```

## Prerequisites

- Claude CLI must be installed and available in your PATH
- You can install it from: https://claude.ai/download

## Configuration

### Environment Variables

- `BB_CLAUDE_CODE_ENABLED`: Set to `0` to disable the plugin (default: `1`)
- `BB_CLAUDE_CODE_AUTO_SEND`: Set to `1` to auto-send without prompting (default: `0`)
- `CLAUDE_CODE_CMD`: Path to the claude CLI command (default: `claude`)

### Disable the plugin temporarily

```bash
export BB_CLAUDE_CODE_ENABLED=0
```

### Enable auto-send (skip confirmation prompt)

```bash
export BB_CLAUDE_CODE_AUTO_SEND=1
```

### Use a custom Claude CLI path

```bash
export CLAUDE_CODE_CMD=/path/to/claude
```

## How it Works

1. The plugin uses the `post_bazel.sh` hook to run after every bazel command
2. It checks the exit code to determine if the build failed
3. If the build failed, it asks the user for confirmation (unless auto-send is enabled)
4. Upon confirmation, it sends the complete build output to Claude Code
5. Claude analyzes the errors and provides fix suggestions

## Example Usage

When a build fails:

```bash
bb build //my/target:failing_target
# Build fails...
# [claude-code] Build failed with exit code 1.
# Would you like Claude Code to analyze and help fix this error? [Y/n]: y
# [claude-code] Sending output to Claude Code for analysis...
# [claude-code] Build failure sent to Claude Code
# Claude Code window opens with the error analysis
```

With auto-send enabled:

```bash
export BB_CLAUDE_CODE_AUTO_SEND=1
bb build //my/target:failing_target
# Build fails...
# [claude-code] Build failed with exit code 1. Auto-sending to Claude Code...
# [claude-code] Build failure sent to Claude Code
# Claude Code window opens with the error analysis
```

## Privacy Note

This plugin sends your build output to Claude when builds fail. Ensure you're comfortable with this before enabling the plugin in projects with sensitive information.