#!/usr/bin/env bash

set -euo pipefail

namespace="${NAMESPACE:-executor-prod}"
dry_run="${DRY_RUN:-0}"

usage() {
  echo "Usage: $0 <executor-id> [executor-id ...]" >&2
  echo "   or:  printf '%s\n' <executor-id> ... | $0" >&2
  echo >&2
  echo "Env vars:" >&2
  echo "  NAMESPACE=<namespace>   Override namespace (default: executor-prod)" >&2
  echo "  DRY_RUN=1               Print commands without executing them" >&2
}

delete_pod() {
  local id="$1"
  id="${id#"${id%%[![:space:]]*}"}"
  id="${id%"${id##*[![:space:]]}"}"

  if [[ -z "$id" || "$id" == \#* ]]; then
    return 0
  fi

  if [[ "$dry_run" == "1" ]]; then
    echo "kubectl delete pod -n ${namespace} ${id}"
    return 0
  fi

  echo "Deleting pod ${id} from namespace ${namespace}..." >&2
  kubectl delete pod -n "${namespace}" "${id}"
}

if [[ $# -gt 0 ]]; then
  for id in "$@"; do
    delete_pod "$id"
  done
  exit 0
fi

if [[ -t 0 ]]; then
  usage
  exit 1
fi

while IFS= read -r id; do
  delete_pod "$id"
done
