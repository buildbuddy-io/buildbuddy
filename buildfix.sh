#!/bin/bash
exec bazel run -- //tools/lint -fix "$@"
