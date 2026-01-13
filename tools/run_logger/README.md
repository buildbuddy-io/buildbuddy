# Run Logger

A wrapper tool that streams `bazel run` output to BuildBuddy's event log API.

## Usage

### Basic Usage

```bash
# Build the tool first
bazel build //tools/run_logger

# Use with bazel run
bazel run --run_under=tools/run_logger/run_logger //my:target
```

### With API Key

```bash
# Set API key in environment
export BUILDBUDDY_API_KEY=your-api-key

# Or pass as flag
bazel run \
  --run_under="tools/run_logger/run_logger --api_key=your-api-key" \
  //my:target
```

### Custom BuildBuddy Server

```bash
bazel run \
  --run_under="tools/run_logger/run_logger --target=grpcs://your-buildbuddy.com" \
  //my:target
```

### View Logs

After running, logs will be available at:
```
https://app.buildbuddy.io/invocation/{invocation_id}?attempt={attempt_id}#run-log
```

The tool will print the URL when it completes.

## Flags

- `--target`: BuildBuddy server address (default: `grpcs://remote.buildbuddy.io`)
- `--api_key`: BuildBuddy API key (default: `$BUILDBUDDY_API_KEY`)
- `--invocation_id`: Invocation ID (default: `$BAZEL_INVOCATION_ID` or auto-generated)
- `--attempt_id`: Attempt ID (default: timestamp)
- `--use_pty`: Use PTY to capture ANSI codes (default: `true`)
- `--buffer_size`: Buffer size for streaming (default: `32768`)

## Features

- ✅ Captures stdout and stderr
- ✅ Preserves ANSI color codes and cursor movements (PTY mode)
- ✅ Streams in real-time to BuildBuddy
- ✅ Preserves exit codes
- ✅ Handles stdin passthrough
- ✅ Graceful degradation if BuildBuddy is unreachable

## How It Works

1. Intercepts the command execution using `--run_under`
2. Starts the command with a PTY to capture all output
3. Streams output to both:
   - Local stdout (so you see it in your terminal)
   - BuildBuddy's WriteEventLog API
4. Logs are chunked, compressed, and stored in BuildBuddy
5. Logs are viewable in the BuildBuddy UI with syntax highlighting

## Environment Variables

- `BAZEL_INVOCATION_ID`: Automatically set by Bazel, used to link logs to builds
- `BUILDBUDDY_API_KEY`: Your BuildBuddy API key for authentication

## Examples

### Running Tests
```bash
bazel run \
  --run_under=tools/run_logger/run_logger \
  //my:integration_test
```

### Running with Arguments
```bash
bazel run \
  --run_under=tools/run_logger/run_logger \
  //my:server -- --port=8080
```

### Debugging
```bash
# Disable PTY mode if you encounter issues
bazel run \
  --run_under="tools/run_logger/run_logger --use_pty=false" \
  //my:target
```

## Troubleshooting

**Logs not appearing?**
- Verify your API key is set correctly
- Check BuildBuddy server is reachable
- Look for error messages in stderr

**ANSI codes not working?**
- Ensure `--use_pty=true` (default)
- Some binaries detect non-TTY and disable colors

**Exit codes not preserved?**
- This is a bug - please file an issue

## Development

Build and test locally:
```bash
# Build
bazel build //tools/run_logger

# Run directly
bazel-bin/tools/run_logger/run_logger_/run_logger echo "Hello World"

# Test with a real target
bazel run --run_under=$(bazel info bazel-bin)/tools/run_logger/run_logger_/run_logger //my:target
```
