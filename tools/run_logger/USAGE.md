# Quick Start Guide

## 1. Build the tools

```bash
bazel build //tools/run_logger:run_logger //tools/run_logger:hello
```

## 2. Test the hello binary directly

```bash
# Run normally
bazel run //tools/run_logger:hello
```

Output:
```
HELLO WORLD
Starting countdown...
  5...
  4...
  3...
  2...
  1...
Done!
```

## 3. Run with the logger (local output only)

If BuildBuddy server isn't configured, the logger will still show output locally:

```bash
bazel run \
  --run_under=$(pwd)/bazel-bin/tools/run_logger/run_logger_/run_logger \
  //tools/run_logger:hello
```

You'll see the same output, plus the logger will attempt to stream to BuildBuddy.

## 4. Run with BuildBuddy streaming

Set your API key and run:

```bash
# Set API key
export BUILDBUDDY_API_KEY=your-api-key-here

# Run with logging
bazel run \
  --run_under=$(pwd)/bazel-bin/tools/run_logger/run_logger_/run_logger \
  //tools/run_logger:hello
```

At the end, you'll see:
```
Logs streamed successfully to BuildBuddy
View at: https://app.buildbuddy.io/invocation/{id}?attempt={attempt}#run-log
```

## 5. Alternative: Using wrapper script

```bash
# Build the wrapper
bazel build //tools/run_logger:run_logger_wrapper

# Run with the wrapper
bazel run \
  --run_under=$(pwd)/bazel-bin/tools/run_logger/run_logger_wrapper \
  //tools/run_logger:hello
```

## 6. Test with your own binaries

```bash
# Example: run your own target
bazel run \
  --run_under=$(pwd)/bazel-bin/tools/run_logger/run_logger_/run_logger \
  //your/package:your_binary -- --your-args here
```

## Configuration

### Use local BuildBuddy instance

```bash
bazel run \
  --run_under="$(pwd)/bazel-bin/tools/run_logger/run_logger_/run_logger --target=grpc://localhost:1985" \
  //tools/run_logger:hello
```

### Disable PTY mode (if you have issues)

```bash
bazel run \
  --run_under="$(pwd)/bazel-bin/tools/run_logger/run_logger_/run_logger --use_pty=false" \
  //tools/run_logger:hello
```

### Custom invocation ID

```bash
bazel run \
  --run_under="$(pwd)/bazel-bin/tools/run_logger/run_logger_/run_logger --invocation_id=my-custom-id" \
  //tools/run_logger:hello
```

## Troubleshooting

**Error: "No such file or directory"**
- Make sure you've built the run_logger first: `bazel build //tools/run_logger`
- Use the full path from bazel-bin

**No output streaming to BuildBuddy**
- Check your API key is set: `echo $BUILDBUDDY_API_KEY`
- Verify the target is correct (default: grpcs://remote.buildbuddy.io)
- Check logs for connection errors

**Exit code is wrong**
- File a bug! Exit codes should be preserved

**Colors not showing**
- Make sure `--use_pty=true` (default)
- Some programs detect they're not in a real terminal and disable colors
