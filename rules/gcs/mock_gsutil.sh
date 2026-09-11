#!/usr/bin/env bash
# Stateful fake: distinguish a completed payload copy from a partial upload,
# require completion before accepting a marker, and record every operation.
set -euo pipefail
[[ ! -v PYTHONSAFEPATH ]]
printf '%s\n' "$*" >> "$MOCK_LOG"
while [[ "$1" == -* ]]; do
  case "$1" in
    -m) shift ;;
    -h) [[ "$2" == Cache-Control:no-store ]]; shift 2 ;;
    *) exit 90 ;;
  esac
done
operation="$1"
shift
case "$operation" in
  cp)
    destination="${!#}"
    if [[ "$destination" == */_SUCCESS ]]; then
      [[ "$#" == 2 && -f "$1" && ! -s "$1" && -f "$MOCK_STATE/complete" ]]
      [[ "${MOCK_FAIL_MARKER:-0}" == 0 ]] || exit 8
      touch "$MOCK_STATE/marker"
    else
      [[ "$1" == -r ]]; shift
      if [[ "$1" == -Z ]]; then shift; fi
      [[ "$#" -ge 2 ]]
      while [[ "$#" -gt 1 ]]; do
        [[ -f "$1" ]]
        shift
      done
      touch "$MOCK_STATE/partial"
      [[ "${MOCK_FAIL_COPY:-0}" == 0 ]] || exit 7
      touch "$MOCK_STATE/complete"
    fi
    ;;
  stat)
    [[ "$#" == 1 && "$1" == gs://test-bucket/release/abc123/_SUCCESS ]]
    [[ "${MOCK_STAT_ERROR:-0}" == 0 ]] || exit 9
    [[ -f "$MOCK_STATE/marker" ]]
    ;;
  rm)
    [[ "$#" == 2 && "$1" == -r ]]
    ;;
  *) exit 91 ;;
esac
