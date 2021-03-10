---
id: troubleshooting-rbe
title: Troubleshooting RBE Failures
sidebar_label: RBE Failures
---

## Remote connection/protocol failed with: execution failed

This error is often a sign that a cache write is timing out. By default, bazel's `remote_timeout` [flag](https://docs.bazel.build/versions/master/command-line-reference.html#flag--remote_timeout) limits all remote execution calls to 60 seconds.

We recommend using the following flag to increase this remote timeout:

```
--remote_timeout=600
```

These expensive writes should only happen once when artifacts are initially written to the cache, and shouldn't happen on subsequent builds.

## Remote connection/protocol failed with: execution failed DEADLINE_EXCEEDED: deadline exceeded after 59999899500ns

This error is a sign that a cache write is timing out. By default, bazel's `remote_timeout` [flag](https://docs.bazel.build/versions/master/command-line-reference.html#flag--remote_timeout) limits all remote execution calls to 60 seconds.

We recommend using the following flag to increase this remote timeout:

```
--remote_timeout=600
```

## exec user process caused "exec format error"

This error occurs when your build is configured for darwin (Mac OSX) CPUs. BuildBuddy Cloud currently doesn't run Mac executors, but plan to in the coming months.

In the meantime, you can configure your toolchains to target k8 - or execute your builds from a linux host using either Docker or a CI system of your choice.

## rpc error: code = Unavailable desc = No registered executors.

This error occurs when your build is configured for darwin (Mac OSX) CPUs. BuildBuddy Cloud currently doesn't run Mac executors, but plan to in the coming months.

In the meantime, you can configure your toolchains to target k8 - or execute your builds from a linux host using either Docker or a CI system of your choice.
