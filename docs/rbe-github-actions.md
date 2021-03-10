---
id: rbe-github-actions
title: RBE with GitHub Actions
sidebar_label: RBE with GitHub Actions
---

Using BuildBuddy RBE with Github Actions is the simplest way to get started using BuildBuddy with a CI system.

## Setup instructions

There are three steps:

1. Create a workflow file
1. Update your `.bazelrc`
1. Set up cert Github secrets

### Workflow file

All you have to do is create a file `.github/workflows/main.yaml`

```
name: CI

on:
  push:
    branches:
      - master

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
    - name: Checkout
      uses: actions/checkout@v1

    - name: Install bazelisk
      run: |
        curl -LO "https://github.com/bazelbuild/bazelisk/releases/download/v1.1.0/bazelisk-linux-amd64"
        mkdir -p "${GITHUB_WORKSPACE}/bin/"
        mv bazelisk-linux-amd64 "${GITHUB_WORKSPACE}/bin/bazel"
        chmod +x "${GITHUB_WORKSPACE}/bin/bazel"
    - name: Create certs
      run: |
        echo "${{ secrets.BUILDBUDDY_ORG_CERT }}">buildbuddy-cert.pem
        echo "${{ secrets.BUILDBUDDY_ORG_KEY }}">buildbuddy-key.pem
    - name: Build
      run: |
        "${GITHUB_WORKSPACE}/bin/bazel" build --config=ci //...
    - name: Test
      run: |
        "${GITHUB_WORKSPACE}/bin/bazel" test --config=ci //...

```

### Updating your .bazelrc

You'll then need to add the following configuration to your `.bazelrc`

```
build:ci --build_metadata=ROLE=CI
build:ci --build_metadata=VISIBILITY=PUBLIC
build:ci --tls_client_certificate=buildbuddy-cert.pem
build:ci --tls_client_key=buildbuddy-key.pem
```

### Github secrets

Finally, you'll need to create Github secrets with the contents of your `buildbuddy-cert.pem` and `buildbuddy-key.pem` files.

You can download these files by logging in to your [BuildBuddy account](https://app.buildbuddy.io) and visiting your [Setup instructions](https://app.buildbuddy.io/docs/setup/). You can then click `Download buildbuddy-cert.pem` and `Download buildbuddy-key.pem`.

You can then open these two files in a text editor, and add them as Github Secrets named `BUILDBUDDY_ORG_CERT` and `BUILDBUDDY_ORG_KEY`. For more information on setting up Github Secrets, [click here](https://docs.github.com/en/actions/configuring-and-managing-workflows/creating-and-storing-encrypted-secrets).

## Github commit statuses

If you'd like BuildBuddy to publish commit statuses to your repo, you can do so by [logging in](https://app.buildbuddy.io) and clicking `Link Github Account` in the user menu in the top right.

## Visibility

By default, authenticated builds are only visible to members of your BuildBuddy organization. If you'd like your BuildBuddy results pages to be visible to members outside of your organization, you can add the following line to your `.bazelrc`:

```
build:ci --build_metadata=VISIBILITY=PUBLIC
```

## Remote build execution

If you'd like to use BuildBuddy's Remote Build Execution capabilities in your CI workflow, you can add the following lines to your `.bazelrc`:

```
build:remote --bes_results_url=https://app.buildbuddy.io/invocation/
build:remote --bes_backend=grpcs://cloud.buildbuddy.io
build:remote --remote_cache=grpcs://cloud.buildbuddy.io
build:remote --remote_executor=grpcs://cloud.buildbuddy.io
build:remote --remote_upload_local_results
build:remote --host_platform=@buildbuddy_toolchain//:platform
build:remote --platforms=@buildbuddy_toolchain//:platform
build:remote --crosstool_top=@buildbuddy_toolchain//:toolchain
build:remote --jobs=100

build:ci --config=remote
```

And the following lines to your `WORKSPACE` file:

```
http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "9055a3e6f45773cd61931eba7b7cf35d6477ab6ad8fb2f18bf9815271fc682fe",
    strip_prefix = "buildbuddy-toolchain-52aa5d2cc6c9ba7ee4063de35987be7d1b75f8e2",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/52aa5d2cc6c9ba7ee4063de35987be7d1b75f8e2.tar.gz"],
)

load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")

buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "buildbuddy")

buildbuddy(name = "buildbuddy_toolchain")
```

If you're using Java, or have a complex project - you'll likely need to configure the toolchain flags a bit. For more information, see our [Remote Build Execution guide](rbe-setup.md).
