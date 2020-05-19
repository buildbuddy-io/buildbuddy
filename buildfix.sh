#!/bin/bash
set -e

# buildifier format all BUILD files
echo "Formatting WORKSPACE files..."
find . -name WORKSPACE -exec buildifier {} \;
echo "Formatting BUILD files..."
find . -name BUILD -exec buildifier {} \;

# go fmt all .go files
echo "Formatting .go files..."
tldirs=$(find . -name "*.go" | cut -d"/" -f2 | uniq)
for dir in $tldirs; do
    gofmt -w "$dir/";
done;

echo "All Done!"
