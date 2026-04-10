---
slug: secret-env-overrides
title: "Short-Lived Secrets for Remote Execution"
description: "Introducing secret-env-overrides: a way to pass short-lived secrets to remote actions without storing them in the action cache."
authors: dan
date: 2026-04-10:12:00:00
tags: [product, security]
---

We're excited to announce `secret-env-overrides` and `secret-env-overrides-base64`, two new [platform properties](https://www.buildbuddy.io/docs/rbe-platforms) that make it easy to pass short-lived secrets to remote execution actions without exposing them in the action cache or workflow logs.

<!-- truncate -->

## The problem

Remote builds sometimes need access to credentials — an API key, a deploy token, a cloud provider session — to do their job. BuildBuddy already supports [organization-level secrets](https://www.buildbuddy.io/docs/secrets), but those are long-lived and configured ahead of time. For ephemeral, short-lived credentials (think CI OIDC tokens or temporary cloud credentials that rotate every few minutes) you need a way to inject them at invocation time.

Using the existing `env-overrides` property works, but the values are stored in **plain text** in the action cache and are visible in the BuildBuddy UI. That's not ideal for secrets.

## `secret-env-overrides`

The new `secret-env-overrides` property works the same way as `env-overrides` — it sets environment variables on the action — but with an important difference: **the values are automatically redacted** from action cache entries and workflow logs. The secret material is never persisted in plain text.

Pass secrets via a remote exec header so they travel with the request and are never written to disk:

```bash
bazel build //my:target \
  --remote_exec_header=x-buildbuddy-platform.secret-env-overrides=API_KEY=sk-abc123
```

Multiple variables are comma-separated:

```bash
--remote_exec_header=x-buildbuddy-platform.secret-env-overrides=VAR_A=value_a,VAR_B=value_b
```

## `secret-env-overrides-base64`

If your secret values contain commas, equals signs, or other special characters, use `secret-env-overrides-base64`. Each entry is a **base64-encoded** `KEY=VALUE` pair, comma-separated:

```bash
# Encode the full KEY=VALUE pair:
$ echo -n 'CREDENTIALS={"token": "abc", "expiry": 1234}' | base64
Q1JFREVOVElBTFM9eyJ0b2tlbiI6ICJhYmMiLCAiZXhwaXJ5IjogMTIzNH0=

# Pass it in:
bazel build //my:target \
  --remote_exec_header=x-buildbuddy-platform.secret-env-overrides-base64=Q1JFREVOVElBTFM9eyJ0b2tlbiI6ICJhYmMiLCAiZXhwaXJ5IjogMTIzNH0=
```

## Remote Bazel

These properties work great with [`bb remote`](https://www.buildbuddy.io/docs/remote-bazel) too. Use `--remote_run_header` to pass secrets into the remote runner:

```bash
bb remote \
  --remote_run_header=x-buildbuddy-platform.secret-env-overrides=DEPLOY_TOKEN=tok-xyz \
  --script='bazel run :deploy --token=$DEPLOY_TOKEN'
```

Because the secrets are sent via headers rather than platform properties, they don't affect the snapshot key. This means frequently rotating credentials won't prevent you from reusing warm, recycled workspaces — a significant performance advantage over `--runner_exec_properties`.

## Security details

- **Redacted from the action cache.** The values of `secret-env-overrides` and `secret-env-overrides-base64` are scrubbed from cached action results and auxiliary metadata before they are stored.
- **Redacted from workflow logs.** The CI runner knows which environment variable names are secrets and scrubs their values from log output.
- **Not subject to `include-secrets` gating.** Unlike organization-level secrets, these properties are applied to every action in the invocation without requiring `include-secrets=true`. This makes them suitable for invocation-scoped credentials that every action needs.
- **`secret-env-overrides-base64` takes priority.** If the same variable name appears in both properties, the base64 variant wins.

:::caution
As with any secret, avoid printing secret values to stdout or stderr in your build actions. Redaction covers BuildBuddy's own storage and logs, but cannot scrub values that your action writes to its own output files.
:::

## Migrating from `env-overrides`

If you're currently using `env-overrides` to pass sensitive values, switching to `secret-env-overrides` is a drop-in replacement — just change the property name. Your actions will behave identically, but the values will now be redacted.

For the full reference, see the [RBE Platforms documentation](https://www.buildbuddy.io/docs/rbe-platforms) and the [Secrets documentation](https://www.buildbuddy.io/docs/secrets).

If you have questions or feedback, come chat with us on [Slack](https://community.buildbuddy.io/) or reach out at [hello@buildbuddy.io](mailto:hello@buildbuddy.io).
