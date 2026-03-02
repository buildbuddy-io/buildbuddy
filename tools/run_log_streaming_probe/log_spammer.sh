#!/usr/bin/env bash
set -euo pipefail

line_count="${1:-3000}"
line_width="${2:-120}"

if ! [[ "${line_count}" =~ ^[0-9]+$ ]] || (( line_count < 1 )); then
  echo "line_count must be a positive integer (got: ${line_count})" >&2
  exit 1
fi
if ! [[ "${line_width}" =~ ^[0-9]+$ ]] || (( line_width < 1 )); then
  echo "line_width must be a positive integer (got: ${line_width})" >&2
  exit 1
fi

payload="$(printf '%*s' "${line_width}" '' | tr ' ' 'x')"

echo "run-log-streaming probe start: line_count=${line_count}, line_width=${line_width}"
for ((i = 1; i <= line_count; i++)); do
  printf 'stdout line=%06d payload=%s\n' "${i}" "${payload}"
  if (( i % 3 == 0 )); then
    printf 'stderr line=%06d payload=%s\n' "${i}" "${payload}" >&2
  fi
done
echo "run-log-streaming probe complete"

exit 3
