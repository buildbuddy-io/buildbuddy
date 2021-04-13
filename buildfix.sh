#!/bin/bash
set -e

# buildifier format all BUILD files
echo "Formatting WORKSPACE/BUILD files..."
buildifier -r .

echo "Formatting .go files..."
# go fmt all .go files
gofmt -w .

echo "All Done!"
