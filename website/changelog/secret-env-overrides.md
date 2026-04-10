---
title: "Short-lived secrets for remote execution"
date: 2026-04-10T10:00:00
authors: dan
tags: [bazel]
---

BuildBuddy now supports `secret-env-overrides` and `secret-env-overrides-base64` platform properties for passing short-lived secrets to remote actions.

Unlike [organization-level secrets](https://www.buildbuddy.io/docs/secrets) which are configured ahead of time, these properties let you inject ephemeral credentials — like CI OIDC tokens or temporary cloud sessions — at invocation time via remote exec headers:

```bash
bazel build //my:target \
  --remote_exec_header=x-buildbuddy-platform.secret-env-overrides=API_KEY=sk-abc123,OTHER_KEY=val
```

For values containing commas or special characters, base64-encode each `KEY=VALUE` pair:

```bash
bazel build //my:target \
  --remote_exec_header=x-buildbuddy-platform.secret-env-overrides-base64=$(echo -n 'CREDS={"token": "abc"}' | base64)
```

Because these properties are sent as remote exec headers, they are:

- **Redacted** from action cache entries and workflow logs — secret values are never persisted in plain text.
- **Excluded from the action hash** — rotating credentials won't invalidate your cache or prevent reuse of warm [Remote Bazel](https://www.buildbuddy.io/docs/remote-bazel) workspaces.

See the [Secrets docs](https://www.buildbuddy.io/docs/secrets) and [RBE platform properties reference](https://www.buildbuddy.io/docs/rbe-platforms) for more details.
