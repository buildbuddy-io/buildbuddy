# Quick Install Guide

## 1. Build the run_logger tool

```bash
bazel build //tools/run_logger:run_logger
```

## 2. Install the plugin

### For your workspace (team-wide)

```bash
# From the buildbuddy repo root
bb plugin install --path=cli/plugins/run_logger
```

This adds to `buildbuddy.yaml`:
```yaml
plugins:
  - path: cli/plugins/run_logger
```

### For your user only

```bash
bb plugin install --path=cli/plugins/run_logger --user
```

This adds to `~/.buildbuddy.yaml`.

## 3. Test it!

```bash
# Build and run the test binary
bb run //tools/run_logger:hello
```

You should see:
1. The normal "HELLO WORLD" output
2. A message: "✓ Added run_logger to bazel run command"
3. After completion: "Logs streamed successfully to BuildBuddy"
4. A URL to view the logs

## 4. View logs in BuildBuddy

Click the URL or go to:
```
https://app.buildbuddy.io/invocation/{your-invocation-id}#run-log
```

## Manual Installation

If you don't want to use `bb plugin install`, manually edit `buildbuddy.yaml`:

```yaml
# Add this to your buildbuddy.yaml or ~/.buildbuddy.yaml
plugins:
  - path: cli/plugins/run_logger
```

Or use an absolute path:
```yaml
plugins:
  - path: /Users/yourusername/bb/buildbuddy/cli/plugins/run_logger
```

## Verify Installation

```bash
# List installed plugins
bb plugin list

# Run a test
bb run //tools/run_logger:hello
```

## Uninstall

```bash
bb plugin uninstall cli/plugins/run_logger
```

Or manually remove from `buildbuddy.yaml`.

## Troubleshooting

**Error: "run_logger not found"**
```bash
bazel build //tools/run_logger:run_logger
```

**Plugin not loading**
- Check file permissions: `ls -la cli/plugins/run_logger/pre_bazel.sh`
- Should be executable (`-rwxr-xr-x`)
- Run: `chmod +x cli/plugins/run_logger/pre_bazel.sh`

**No output to BuildBuddy**
- Set API key: `export BUILDBUDDY_API_KEY=your-key-here`
- Check server: `echo $BUILDBUDDY_API_KEY`
