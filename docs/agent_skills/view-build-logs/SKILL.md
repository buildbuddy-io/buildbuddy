---
name: view-build-logs
description: Fetch BuildBuddy invocation logs. Use when you need to inspect or debug Bazel build logs for a specific invocation ID or invocation URL.
---

## Overview

Fetch logs for a BuildBuddy invocation by calling `bb view <invocation-id>`

To view failing test output, pass a target label and/or --test_filter: `bb view <invocation-id> //target:label --test_filter=TestName`
`--test_filter` is a regular expression (matching bazel's --test_filter semantics). If no target is specified, output from every failing test target is searched for the filter expression.

To view only the first build error, pass --errors: `bb view <invocation-id> --errors`
