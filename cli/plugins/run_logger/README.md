# Run Logger Plugin

Automatically streams `bazel run` output to BuildBuddy by adding `--run_under` with the run_logger wrapper.

## What it does

When you run:
```bash
bb run //my:binary
```

The plugin automatically transforms it to:
```bash
bb run --run_under=<workspace>/bazel-bin/tools/run_logger/run_logger_/run_logger //my:binary
```

This means all your `bazel run` output is automatically streamed to BuildBuddy and viewable in the UI!

## Installation

### Option 1: Workspace-level (recommended for teams)

Add to your workspace's `buildbuddy.yaml`:

```bash
bb plugin install --path=cli/plugins/run_logger
```

Or manually edit `buildbuddy.yaml`:
```yaml
plugins:
  - path: cli/plugins/run_logger
```

### Option 2: User-level (for personal use)

Add to your personal config (`~/.buildbuddy.yaml`):

```bash
bb plugin install --path=cli/plugins/run_logger --user
```

## Prerequisites

The `run_logger` tool must be built before using this plugin:

```bash
bazel build //tools/run_logger:run_logger
```

The plugin will attempt to build it automatically if it's missing, but for best performance, build it once ahead of time.

## Usage

Just use `bb run` normally:

```bash
# Automatically streams to BuildBuddy
bb run //my:server

# With arguments
bb run //my:cli -- --help

# Works with all run commands
bb run //tools/run_logger:hello
```

After the command completes, you'll see:
```
Logs streamed successfully to BuildBuddy
View at: https://app.buildbuddy.io/invocation/{id}?attempt={attempt}#run-log
```

## Configuration

### Disable for a specific run

If you want to skip the logger for a specific run, manually specify `--run_under`:

```bash
# Empty --run_under disables it
bb run --run_under= //my:binary

# Or use a different wrapper
bb run --run_under="/usr/bin/strace -c" //my:binary
```

The plugin respects existing `--run_under` flags and won't override them.

### Environment variables

The run_logger respects these environment variables:

- `BUILDBUDDY_API_KEY`: Your BuildBuddy API key
- `BAZEL_INVOCATION_ID`: Automatically set by Bazel

## How it works

1. Plugin intercepts `bb run` commands in the `pre_bazel` hook
2. Checks if command is `run` (not `build`, `test`, etc.)
3. Verifies `--run_under` isn't already set
4. Adds `--run_under=<path-to-run_logger>`
5. Bazel runs your binary through the wrapper
6. Output streams to both your terminal and BuildBuddy

## Troubleshooting

**Plugin not working?**
- Verify it's installed: `bb plugin list`
- Check the plugin is loaded: look for "✓ Added run_logger" message

**run_logger not found?**
- Build it: `bazel build //tools/run_logger:run_logger`
- Or let the plugin build it automatically on first use

**Logs not appearing in BuildBuddy?**
- Set your API key: `export BUILDBUDDY_API_KEY=your-key`
- Check BuildBuddy server is reachable

**Want to disable temporarily?**
- Use `--run_under=` to override: `bb run --run_under= //my:binary`
- Or uninstall the plugin: `bb plugin uninstall cli/plugins/run_logger`

## Uninstall

```bash
bb plugin uninstall cli/plugins/run_logger
```

Or remove from `buildbuddy.yaml`:
```yaml
plugins:
  # Remove this line:
  # - path: cli/plugins/run_logger
```

## See Also

- [Run Logger Tool](../../../tools/run_logger/README.md)
- [BB CLI Plugin Documentation](https://www.buildbuddy.io/docs/cli-plugins)
- [Example Plugins](../../example_plugins/)
