#!/usr/bin/env bash
set -eu

# If the "invocation_url.txt" file was written by the pre-bazel hook,
# open the URL in that file.

[[ ! -e "$PLUGIN_TEMPDIR/invocation_url.txt" ]] && exit 0

_gray="\x1b[90m"
_underline="\x1b[4m"
_normal="\x1b[m"

invocation_url=$(cat "$PLUGIN_TEMPDIR/invocation_url.txt")

echo -e "${_gray}> Opening ${_underline}${invocation_url}${_normal}"
open_command=$( (which xdg-open open | head -n1) || true)
"$open_command" "$invocation_url" &>/dev/null
