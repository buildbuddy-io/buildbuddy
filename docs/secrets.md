---
id: secrets
title: Secrets
sidebar_label: RBE Secrets
---

Secrets are encrypted environment variables associated with your
BuildBuddy organization. Secrets can be used in actions executed with
[remote execution](remote-build-execution) as well as [BuildBuddy
Workflows](workflows-introduction).

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

To opt a specific action into secrets, you can define the remote exec
property `include-secrets=true`. We recommend doing this per-action to
avoid exposing secrets to actions that do not need them.

Example:

```python
foo_library(
    # ...
    exec_properties = {
        "include-secrets": "true",
    }
)
```

### Workflows

[BuildBuddy workflows](workflows-introduction) do not need additional
configuration to use secrets; they receive secrets by default as long as
the workflow is being triggered on behalf of a trusted collaborator in the
repository.

Workflow secrets are accessed via environment variables, in the same way
as normal Bazel actions shown above.

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
