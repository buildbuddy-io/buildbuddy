---
name: remote-run
description: Run bazel commands in a remote runner. Use when the user asks to run `bazel build`, `bazel test` or `bazel run`.
---

`bb remote` runs commands in a remote runner. These runners can have more compute power, and workspaces are snapshotted between
runs, speeding up subsequent runs.

It also uploads any local git diffs to the remote runner.

## Drop-in bazel replacement

Instead of running bazel commands locally with `bazel`, run them on a remote runner using the `bb remote` command.

For example, you should replace the command `bazel build :target` with `bb remote build :target`.
`bazel run :target` should be replaced with `bb remote run :target`.

You should preserve any bazel startup flags and options.
`bazel --host_jvm_args=-Xmx4g test :target --config=X` should become `bb --host_jvm_args=-Xmx4g remote test :target --config=X`.

## Run any bash command remotely

You can also use `bb remote` to run commands that would be expensive to run locally. For non-bazel commands, you can run them with
`--script`.

For example, you could run `bb remote --script="echo HELLO!"`.

## Failure

On failure, you should warn the user and print the invocation URL for the remote run. This will be at the top of the logs, and will look like
`Streaming remote runner logs to: https://app.buildbuddy.io/invocation/d70a1719-4cb6-4568-a19e-9b6f8e2b3ec3`

You should then ask the user if you should run the command locally.

## Configuration

You can configure the OS and architecture of the remote runner with the flags `--os` and `--arch`.
Valid values of `os` are: `linux` and `darwin`.
Valid values of `arch` are: `amd64` and `arm64`.
By default, the remote runner will match the local architecture.

You can configure the image of the remote runner with `--container_image=docker://<URL>`.

You can request more resources for the remote runner with `--runner_exec_properties=EstimatedCPU=X`, `--runner_exec_properties=EstimatedFreeDiskBytes=XGB`
and `EstimatedMemory=XGB`. We strongly recommend not setting these and using the defaults unless absolutely necessary. Remote runners are billed based on
resource requests, and requesting more resources is more expensive. Only suggest increasing resources if the logs explicitly report OOM errors or disk exhaustion.

If a remote runner is corrupted and you want a clean one, you can set `--runner_exec_properties=salt=<VALUE>`. Just make sure
you preserve this flag if you want future runs to resume from this new runner.

## Auth

The cleanest way to authorize requests is to run `bb login` and encourage the user to follow the login flow in the UI.
An API key will be stored in .git/config under:

```
[buildbuddy]
  api-key = <API_KEY_HERE>
```

If you have the user's API key, you can also add it to the end of commands with `--remote_header=x-buildbuddy-api-key=<API_KEY_HERE>`.

## Pre-requisites

If `bb` is not installed, it can be installed with `curl -fsSL https://install.buildbuddy.io | bash`.
