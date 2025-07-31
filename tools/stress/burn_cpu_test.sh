#!/usr/bin/env bash

# Read 100GB of zeroes from /dev/zero, piping to /dev/null.
# This should fully saturate a single core.
exec head -c 100000000000 </dev/zero >/dev/null
