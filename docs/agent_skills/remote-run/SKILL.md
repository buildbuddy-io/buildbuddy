---
name: remote-run
description: Run bazel commands in a remote runner. Use when the user asks to run `bazel build`, `bazel test` or `bazel run`.
---

`bb remote` runs commands in a remote runner. These runners can have more compute power, and workspaces are snapshotted between
runs, speeding up subsequent runs.

It also uploads any local git diffs to the remote runner.

## Getting help

If you don't know how to accomplish a task, you should consult the docs at https://www.buildbuddy.io/docs/remote-bazel. Do not try to run `bb remote --help` or `bb help`.

## Pre-run gate

Before running any `bb remote` command, you should check if the user asked to add a flag to the command (either configuring Remote Bazel or bazel).
If they did, follow the flag persistence steps below.

## Drop-in bazel replacement

Instead of running bazel commands locally with `bazel`, run them on a remote runner using the `bb remote` command.

For example, you should replace the command `bazel build :target` with `bb remote build :target`.
`bazel run :target` should be replaced with `bb remote run :target`.

You should preserve any bazel startup flags and options.
`bazel --output_base=X test :target --config=Y` should become `bb remote --output_base=X test :target --config=Y`.

## Flag persistence

Whenever the user provides flags not already in the persistent-flags.md file, follow this workflow before running the command:

1. Ask: `Do you want me to persist <FLAG> in this SKILL file so it is automatically applied to all future remote runs?`
2. Wait for explicit confirmation before editing this file.
3. Ask the user to confirm the syntax of the flag via an example.
4. If the user confirms, edit only the persistent-flags.md file.
5. Ensure the flag is not already present.
6. Preserve alphabetical ordering.
7. Add one example command per flag.

Entry format:

- `--flag=value`: Example: `bb remote build :target --flag=value`

## Run any bash command remotely

You can also use `bb remote` to run commands that would be expensive to run locally. For non-bazel commands, you can run them with
`--script`.

For example, you could run `bb remote --script="echo HELLO!"`.

## Failure

On failure, you should warn the user and print the invocation URL for the remote run. This will be at the top of the logs, and will look like
`Streaming remote runner logs to: https://app.buildbuddy.io/invocation/d70a1719-4cb6-4568-a19e-9b6f8e2b3ec3`

## Configuration

You can configure the OS and architecture of the remote runner with the flags `--os` and `--arch`.
Valid values of `os` are: `linux` and `darwin`.
Valid values of `arch` are: `amd64` and `arm64`.
By default, the remote runner will match the local architecture.

You can configure the image of the remote runner with `--container_image=docker://<URL>`.

If a remote runner is corrupted and you want a clean one, you can set `--runner_exec_properties=salt=<VALUE>`. Just make sure
you preserve this flag if you want future runs to resume from this new runner.

## Requesting more resources

We strongly recommend not setting these and using the defaults unless absolutely necessary. Only suggest increasing resources if the logs explicitly report OOM errors or disk exhaustion.

You can request more resources for the remote runner with `--runner_exec_properties=EstimatedCPU=X`, `--runner_exec_properties=EstimatedFreeDiskBytes=XGB` and `EstimatedMemory=XGB`.

## Auth

If the request is not authorized, tell the user to run `bb login` and follow the login flow in the UI. Don't try to run `bb login` yourself because some manual steps are required.

An API key will be stored in .git/config under:

```
[buildbuddy]
  api-key = <API_KEY_HERE>
```

If running in non-interactive mode, the user should save the API key as a secret, and you can pass it with `--remote_header=x-buildbuddy-api-key=<API_KEY_HERE>`.

## Pre-requisites

If `bb` is not installed, it can be installed with `curl -fsSL https://install.buildbuddy.io | bash`.
