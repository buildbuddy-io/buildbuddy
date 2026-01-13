# Run Logger - Quick Start

## What is this?

A tool that captures `bazel run` output and streams it to BuildBuddy for viewing in the UI.

## Try it now!

### Step 1: Build everything

```bash
cd /Users/maggielou/bb/buildbuddy
bazel build //tools/run_logger:run_logger //tools/run_logger:hello
```

### Step 2: Run the hello binary normally

```bash
bazel run //tools/run_logger:hello
```

Expected output:
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

### Step 3: Run with the logger

```bash
bazel run \
  --run_under=$(pwd)/bazel-bin/tools/run_logger/run_logger_/run_logger \
  //tools/run_logger:hello
```

You should see:
- The same "HELLO WORLD" output
- A message about streaming to BuildBuddy
- A URL to view the logs

### Step 4: Set API key (optional, for production)

```bash
export BUILDBUDDY_API_KEY=your-key-here

bazel run \
  --run_under=$(pwd)/bazel-bin/tools/run_logger/run_logger_/run_logger \
  //tools/run_logger:hello
```

## What it does

1. **Intercepts execution**: The `--run_under` flag runs your binary through the wrapper
2. **Captures output**: Uses a PTY to capture all stdout/stderr (including ANSI codes)
3. **Streams to BuildBuddy**: Sends data in real-time via the WriteEventLog RPC
4. **Preserves behavior**: Exit codes and stdin/stdout/stderr work normally

## Architecture

```
bazel run --run_under=run_logger //my:binary
    ↓
run_logger starts
    ↓
Opens gRPC stream to BuildBuddy
    ↓
Sends metadata (invocation ID, attempt ID)
    ↓
Forks PTY and runs //my:binary
    ↓
Captures all output → streams to:
  - Local stdout (you see it)
  - BuildBuddy (stored for later)
    ↓
Binary exits → close stream → show URL
```

## Files created

```
tools/run_logger/
├── main.go              # Main wrapper implementation
├── BUILD                # Bazel build config
├── wrapper.sh           # Shell wrapper (alternative)
├── testdata/
│   └── hello.go         # Simple test binary
├── README.md            # Full documentation
├── USAGE.md             # Detailed usage guide
└── QUICKSTART.md        # This file
```

## Next steps

- Try with your own binaries
- View logs in BuildBuddy UI
- Customize flags (see README.md)
- Report bugs if you find any!
