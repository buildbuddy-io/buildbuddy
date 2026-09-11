---
title: "Fix failing builds with bb agent fix"
date: 2026-09-11T10:00:00
authors: maggie
tags: [AI, debugging]
---

`bb agent fix` reproduces a failure from a previous invocation and fixes it by editing the current working tree.

The agent reruns the invocation's original Bazel command to reproduce the failure, inspects the failure output and relevant source code, applies a minimal fix, and reruns the command to verify the fix is valid.

When run locally, the changes are applied to the current working tree.
When run remotely, the changes are uploaded to the 'Artifacts' tab of the invocation.

Run it from the workspace that produced the failure, with an invocation ID or URL:

```bash
# Fix every failing target in the invocation.
bb agent fix <INVOCATION_ID>

# Fix only a single failing target, or only the test cases matching a filter.
bb agent fix <INVOCATION_ID> //foo:bar_test --test_filter=TestBaz
```

See the [documentation](/docs/cli-commands#bb-agent-fix) for more details.
