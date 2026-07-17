---
id: secrets
title: Secrets
sidebar_label: RBE Secrets
---

Secrets are encrypted environment variables associated with your
BuildBuddy organization. Secrets can be used in actions executed with
[remote execution](remote-build-execution) as well as [BuildBuddy remote runners](remote-runner-introduction).

BuildBuddy encrypts secrets with a
[libsodium sealed box](https://libsodium.gitbook.io/doc/public-key_cryptography/sealed_boxes).
Secrets are encrypted before they are sent to BuildBuddy's
servers, and stay encrypted until they are used.

## Why use secrets?

Builds that are executed remotely on BuildBuddy's servers may occasionally
need access to an API key or other credentials. For example, you may want
to [pass credentials for a Docker image](rbe-platforms#passing-credentials-for-docker-images).

Storing these sensitive parameters as plain environment variables is
undesirable because those keys would be stored unencrypted in BuildBuddy's
Action Cache. While BuildBuddy's cache requires authorization and is secured
using TLS, storing so many copies of the secret in cache increases attack
surface and increases the chance of accidentally exposing your own
credentials.

Secrets solve this problem by allowing sensitive keys to be stored in
encrypted format separately from the actions themselves.

## Defining secrets

Secrets can be added to your organization using the Secrets page in
[settings](https://app.buildbuddy.io/settings/org/secrets).

Secrets can be edited or deleted using the Secrets page. Once a secret
is saved, its currently stored value cannot be viewed using the Secrets
page.

## Getting secret values

### Bazel actions

To allow a target's remotely executed actions to access secrets, use the
`env-secrets` execution property, which accepts a comma-separated list of
secrets to pass as environment variables into the action.

It is recommended to set the `env-secrets` execution property per-target, rather
than globally via `--remote_default_exec_properties` or via execution platforms.

When applicable, it is also recommended to narrow these properties by [execution
group](https://bazel.build/extending/exec-groups) to further restrict the
actions within a target that can access the secret. Most commonly, the `test`
execution group can be used to allow a test to access a secret value, but not
the action that builds the test.

Example:

```python title="BUILD"
foo_test(
    # ...
    exec_properties = {
        # foo_test builds the test (action 1) then runs the test (action 2).
        # The "test." prefix here only exposes the secrets to the action that
        # runs the test (action 2).
        "test.env-secrets": "API_KEY,DB_PASSWORD",
    }
)
```

To make _all_ secrets available in the action environment, the `include-secrets`
execution property can be set to the string `"true"`. However, it is recommended
to use `env-secrets` instead, so that the action only receives the secret values
that it needs.

### Workflows

[BuildBuddy workflows](workflows-setup) do not need additional
configuration to use secrets; they receive secrets by default as long as
the workflow is being triggered on behalf of a trusted collaborator in the
repository.

Workflow secrets are accessed via environment variables, in the same way
as normal Bazel actions shown above.

## Short-lived secrets

For secrets that have a short Time To Live (TTL), BuildBuddy supports setting
environment variables via special platform properties that are automatically
redacted from the action cache and workflow logs. These properties can be set
via remote execution headers so they are never stored in plain text.

```bash title="Simple Secrets"
--remote_exec_header=x-buildbuddy-platform.secret-env-overrides=VAR_A=value_a,VAR_B=val_b

## At execution time:
> echo $VAR_A
value_a
> echo $VAR_B
val_b

```

```bash title="Complex Secrets"
## First encode the secrets using base64,
## making sure to include the entire 'KEY=VALUE' pair
> echo -n 'VAR_C={"a": 1, "b", 2}' | base64
> echo -n 'VAR_D=asdfa!@@C,+{}' | base64

## then use the base64-encoded strings in the `secret-env-overrides-base64` header, comma separated.
--remote_exec_header=x-buildbuddy-platform.secret-env-overrides-base64=VkFSX0M9eyJhIjogMSwgImIiLCAyfQ==,VkFSX0Q9YXNkZmEhQCNDLCt7fQ==

## At execution time:
> echo $VAR_C
{"a": 1, "b", 2}
> echo $VAR_D
asdfa!@@C,+{}
```

:::note

If multiple values are given with the same variable name, the last value will be used.
If a variable is specified in both `secret-env-overrides` and `secret-env-overrides-base64`,
`secret-env-overrides-base64` will take priority.

:::

These secrets will be set as environment variables at action execution time,
overriding the default environment variables on your container image as well as
the environment variables set by Bazel as part of the action configuration.

The values of `secret-env-overrides` and `secret-env-overrides-base64` properties
are automatically **redacted** from action cache entries and workflow logs, so
the secret material is never persisted in plain text.

Secrets that are passed through `secret-env-overrides` or `secret-env-overrides-base64`
headers are not subjected to `include-secrets` control documented above.

:::note

The older `env-overrides` and `env-overrides-base64` properties still work
but do **not** redact values from the action cache. Prefer
`secret-env-overrides` / `secret-env-overrides-base64` for any sensitive data.

:::

## Security notes

Secrets are encrypted on the client-side using
[libsodium](https://doc.libsodium.org/), which is based on
[NaCl](http://nacl.cr.yp.to/).

The public key used to encrypt secrets is unique to each organization. The
private key used to decrypt secrets is stored encrypted and only decrypted
when used to unseal secrets.

Secrets are only stored in their encrypted form and are decrypted
on-demand for actions that opt in to receiving secrets.

:::caution

Avoid printing secret values to your build logs or action outputs.

:::
