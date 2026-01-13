# Run Logger Plugin - Example Usage

## Before Installing the Plugin

```bash
$ bb run //tools/run_logger:hello
INFO: Analyzed target //tools/run_logger:hello (0 packages loaded, 0 targets configured).
INFO: Found 1 target...
Target //tools/run_logger:hello up-to-date:
  bazel-bin/tools/run_logger/hello_/hello
INFO: Elapsed time: 0.123s, Critical Path: 0.01s
INFO: 1 process: 1 internal.
INFO: Build completed successfully, 1 total action
INFO: Running command line: bazel-bin/tools/run_logger/hello_/hello
HELLO WORLD
Starting countdown...
  5...
  4...
  3...
  2...
  1...
Done!
```

**Result:** Logs are only visible in your terminal. Not saved to BuildBuddy.

## After Installing the Plugin

```bash
$ bb run //tools/run_logger:hello
✓ Added run_logger to bazel run command
INFO: Analyzed target //tools/run_logger:hello (0 packages loaded, 0 targets configured).
INFO: Found 1 target...
Target //tools/run_logger:hello up-to-date:
  bazel-bin/tools/run_logger/hello_/hello
INFO: Elapsed time: 0.123s, Critical Path: 0.01s
INFO: 1 process: 1 internal.
INFO: Build completed successfully, 1 total action
INFO: Running command line: .../run_logger .../hello
HELLO WORLD
Starting countdown...
  5...
  4...
  3...
  2...
  1...
Done!
Logs streamed successfully to BuildBuddy
View at: https://app.buildbuddy.io/invocation/abc123?attempt=1234567890#run-log
```

**Result:** Logs are saved to BuildBuddy AND visible in terminal. You get a URL to view them later!

## What the Plugin Does

The plugin transforms this:
```bash
bb run //tools/run_logger:hello
```

Into this:
```bash
bb run \
  --run_under=$(pwd)/bazel-bin/tools/run_logger/run_logger_/run_logger \
  //tools/run_logger:hello
```

But you don't have to type all that every time!

## Real-World Example

### Running a Server

```bash
$ bb run //server:my_server -- --port=8080
✓ Added run_logger to bazel run command
INFO: Running command line: .../run_logger .../my_server -- --port=8080
Starting server on port 8080...
[2026-01-13 10:30:00] INFO: Server listening
[2026-01-13 10:30:05] INFO: Request from 127.0.0.1
[2026-01-13 10:30:10] INFO: Request from 127.0.0.1
^C
Logs streamed successfully to BuildBuddy
View at: https://app.buildbuddy.io/invocation/def456?attempt=1234567891#run-log
```

Now you can:
- Share the URL with your team
- Debug issues by reviewing the logs later
- See the exact output from a specific run
- Compare runs side-by-side

### Running Tests Manually

```bash
$ bb run //test:integration_test -- --verbose
✓ Added run_logger to bazel run command
INFO: Running command line: .../run_logger .../integration_test -- --verbose
Running integration tests...
✓ Test database connection... PASS
✓ Test API endpoints... PASS
✓ Test authentication... PASS
All tests passed!
Logs streamed successfully to BuildBuddy
View at: https://app.buildbuddy.io/invocation/ghi789?attempt=1234567892#run-log
```

## Disable for a Specific Run

Sometimes you don't want logging (e.g., interactive programs):

```bash
# Disable the plugin for this run
$ bb run --run_under= //tools:interactive_tool
```

## Advanced: Custom Flags

```bash
# Use custom BuildBuddy server
$ bb run \
  --run_under="$(pwd)/bazel-bin/tools/run_logger/run_logger_/run_logger --target=grpc://localhost:1985" \
  //my:binary
```

The plugin respects existing `--run_under` flags, so this works!

## Comparison

| Feature | Without Plugin | With Plugin |
|---------|---------------|-------------|
| Terminal output | ✅ | ✅ |
| BuildBuddy logs | ❌ | ✅ |
| Shareable URL | ❌ | ✅ |
| Searchable history | ❌ | ✅ |
| ANSI colors preserved | ✅ | ✅ |
| Typing required | Minimal | Minimal |
| Setup | None | One-time install |

## Next Steps

1. Install the plugin (see [INSTALL.md](INSTALL.md))
2. Try it with `bb run //tools/run_logger:hello`
3. Check BuildBuddy UI for your logs
4. Use it for all your `bazel run` commands!
