#!/usr/bin/env bash

# Generate a 10MB file filled with zeroes.
head -c 10000000 </dev/zero > 10mb.bin

# Copy it to the current directory
cp 10mb.bin 10mb.bin.copy
