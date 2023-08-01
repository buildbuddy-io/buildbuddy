#!/usr/bin/env bash
set -e

export USER=root
export HOME=/root

export PATH="$PATH:/usr/local/go/bin"
export PATH="$PATH:$(go env GOPATH)/bin"

MODULES=($GO_MODULES)

# Generate a Makefile so we can use Make to download in parallel (make -j<n>)
[[ -e Makefile ]] && rm Makefile
for i in $(seq 1 "${#MODULES[@]}"); do
  idx=$((i - 1))
  {
    printf '%d:\n' "$i"
    printf '\tgo mod -race download %s\n\n' "${MODULES[$idx]}"
  } >>Makefile
done
{
  printf 'all: '
  printf '%s ' $(seq 1 "${#MODULES[@]}")
  printf '\n\n'
} >>Makefile

go clean -modcache
make -j$((CPU_COUNT * 2)) all >&2
