#!/usr/bin/env bash
set -eu

perl <go.mod -nle 'if (/^[^=]+v[^=]+$/) { print $_ }' |
  perl -pe 's/^\s+([^\s]+)\s+([^\s]+)( \/\/ indirect)?$/\1@\2/' |
  grep -v 'firecracker-go-sdk'
