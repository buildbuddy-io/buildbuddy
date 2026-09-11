#!/usr/bin/env bash
# Stateful fake: distinguish a completed payload copy from a partial upload,
# require completion before accepting a marker, and record every operation.
set -euo pipefail
fail() { echo >&2 "Mock gsutil: $*"; exit 1; }
[[ ! -v PYTHONSAFEPATH ]] || fail 'PYTHONSAFEPATH was not cleared'
printf '%s\n' "$*" >> "$MOCK_LOG"
while [[ "$1" == -* ]]; do
  case "$1" in
    -m) shift ;;
    -h) [[ "$2" == Cache-Control:no-store ]] || fail "Unexpected header: $2"; shift 2 ;;
    *) fail "Unexpected option: $1" ;;
  esac
done
operation="$1"
shift
case "$operation" in
  cp)
    destination="${!#}"
    if [[ "$destination" == */_SUCCESS ]]; then
      [[ "$#" == 2 && -f "$1" && ! -s "$1" && -f "$MOCK_STATE/complete" ]] || fail 'Marker copy must use an empty file after payload completion'
      [[ "${MOCK_FAIL_MARKER:-0}" == 0 ]] || exit 8
      touch "$MOCK_STATE/marker"
    else
      [[ "$1" == -r ]] || fail 'Payload copy must be recursive'; shift
      if [[ "$1" == -Z ]]; then shift; fi
      [[ "$#" -ge 2 ]] || fail 'Payload copy needs sources and a destination'
      while [[ "$#" -gt 1 ]]; do
        [[ -f "$1" ]] || fail "Payload source does not exist: $1"
        shift
      done
      touch "$MOCK_STATE/partial"
      [[ "${MOCK_FAIL_COPY:-0}" == 0 ]] || exit 7
      touch "$MOCK_STATE/complete"
    fi
    ;;
  stat)
    [[ "$#" == 1 && "$1" == gs://test-bucket/release/abc123/_SUCCESS ]] || fail 'Stat used the wrong marker path'
    [[ "${MOCK_STAT_ERROR:-0}" == 0 ]] || { echo >&2 'Simulated stat error'; exit 9; }
    [[ -f "$MOCK_STATE/marker" ]] || exit 1
    echo 'Content-Length: 0'
    ;;
  rm)
    [[ "$#" == 2 && "$1" == -r ]] || fail 'Delete used unexpected arguments'
    ;;
  *) fail "Unexpected operation: $operation" ;;
esac
