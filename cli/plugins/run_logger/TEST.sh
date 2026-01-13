#!/bin/bash
# Quick test script for the run_logger plugin

set -e

echo "======================================"
echo "Run Logger Plugin - Test Script"
echo "======================================"
echo

# Check if bb command exists
if ! command -v bb &> /dev/null; then
    echo "❌ 'bb' command not found"
    echo "   Install BuildBuddy CLI first"
    exit 1
fi

echo "✓ BB CLI found"

# Check if we're in the buildbuddy repo
if [[ ! -f "WORKSPACE" ]] || [[ ! -d "tools/run_logger" ]]; then
    echo "❌ Must run from buildbuddy repo root"
    exit 1
fi

echo "✓ In buildbuddy repo"

# Build run_logger if needed
echo
echo "Building run_logger tool..."
if bazel build //tools/run_logger:run_logger 2>&1 | grep -q "Build completed"; then
    echo "✓ run_logger built successfully"
else
    echo "❌ Failed to build run_logger"
    exit 1
fi

# Check if plugin is installed
echo
echo "Checking plugin installation..."
if bb plugin list 2>&1 | grep -q "run_logger"; then
    echo "✓ Plugin already installed"
else
    echo "Installing plugin..."
    if bb plugin install --path=cli/plugins/run_logger; then
        echo "✓ Plugin installed"
    else
        echo "❌ Failed to install plugin"
        exit 1
    fi
fi

# Run test
echo
echo "======================================"
echo "Running test: bb run //tools/run_logger:hello"
echo "======================================"
echo

bb run //tools/run_logger:hello

echo
echo "======================================"
echo "Test Complete!"
echo "======================================"
echo
echo "If you saw:"
echo "  ✓ '✓ Added run_logger' message"
echo "  ✓ 'HELLO WORLD' output"
echo "  ✓ 'Logs streamed successfully' message"
echo "  ✓ A BuildBuddy URL"
echo
echo "Then the plugin is working correctly!"
echo
echo "Try it yourself:"
echo "  bb run //your/target:here"
