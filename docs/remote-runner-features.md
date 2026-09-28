---
id: remote-runner-features
title: Remote Runner Features
sidebar_label: Remote Runner Features
---

The following features are supported for both BuildBuddy Workflows and Remote Bazel.
Check individual docs for more product-specific features.

## Attaching artifacts to remote runs

To provide easy access to artifacts generated during remote runs, BuildBuddy supports a special
directory called the **artifacts directory**. If you write files
to this directory, BuildBuddy will automatically upload those files and
show them in the UI for the remote run. You can get the path to the
artifacts directory using the environment variable
`$BUILDBUDDY_ARTIFACTS_DIRECTORY`.

For example, Bazel supports several flags such as `--remote_grpc_log` that allow
writing additional debug logs and metadata files associated with an
invocation. To automatically upload these to our UI, you could run a command like
`bazel test //... --remote_grpc_log=$BUILDBUDDY_ARTIFACTS_DIRECTORY/grpc.log`.

BuildBuddy creates a new artifacts directory for each step executed on the remote
runner, and recursively uploads all files in the directory after the step exits.

#### Downloading artifacts

Download all artifacts attached to a remote run with the BuildBuddy CLI:

```bash
bb download artifacts <INVOCATION_ID_OR_URL>
```

By default, the command creates a temporary directory and prints its path. To
choose the destination, pass `--output_directory`:

```bash
bb download artifacts <INVOCATION_ID_OR_URL> --output_directory=/tmp/artifacts
```

If the downloaded artifacts include Git patches, the command also prints a
`git apply` command that can be used to apply them.

You can also fetch artifacts using the BuildBuddy API:

1. Using the [`GetInvocation` API](https://www.buildbuddy.io/docs/enterprise-api#getinvocation)
   with the `include_artifacts` field set to fetch the invocation for the remote run

- In the response, the `artifacts` field will include bytestream URLs for artifacts
  that were uploaded to BuildBuddy

2. Use the [`GetFile` API](https://www.buildbuddy.io/docs/enterprise-api#getfile) to fetch the desired artifacts
