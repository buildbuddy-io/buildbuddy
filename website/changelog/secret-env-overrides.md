---
title: "Improved handling of short-lived secrets in remote execution"
date: 2026-04-10T10:00:00
authors: dan
tags: [bazel]
---

BuildBuddy has supported passing short-lived secrets to remote actions via the `env-overrides` platform property, which redacts values from action cache entries. The new `secret-env-overrides` and `secret-env-overrides-base64` properties extend this protection by also **redacting values from workflow logs**.

Pass secrets via remote exec headers so they're injected at invocation time without affecting the action hash:

```bash
bazel build //my:target \
  --remote_exec_header=x-buildbuddy-platform.secret-env-overrides=API_KEY=sk-abc123,OTHER_KEY=val
```

For values containing commas or special characters, base64-encode each `KEY=VALUE` pair:

```bash
bazel build //my:target \
  --remote_exec_header=x-buildbuddy-platform.secret-env-overrides-base64=$(echo -n 'CREDS={"token": "abc"}' | base64)
```

See the [Secrets docs](https://www.buildbuddy.io/docs/secrets) and [RBE platform properties reference](https://www.buildbuddy.io/docs/rbe-platforms) for more details.
