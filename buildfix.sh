#!/bin/bash
set -e

# buildifier format all BUILD files
echo "Formatting WORKSPACE files..."
find . -name WORKSPACE -exec buildifier {} \;
echo "Formatting BUILD files..."
find . -name BUILD -exec buildifier {} \;

# go fmt all .go files
echo "Formatting .go files..."
find . -name "*.go" -exec go fmt {} \;

echo "All Done!"
