#!/usr/bin/env bash
set -e

# Look for lines like:
#     foo/bar/baz.go:10:15: syntax error: blah
# And highlight the "path:line:column" part in orange.
python3 -c '
import re
import sys

for line in sys.stdin:
    m = re.search(r"^(.*?\.go:\d+:\d+:)(.*)", line)
    if m:
        print("\x1b[33m" + m.group(1) + "\x1b[0m" + m.group(2))
' <"$1" >&2
