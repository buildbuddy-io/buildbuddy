---
name: remote-run-curl
description: Run bazel commands in a remote runner. Use when the user asks to run `bazel build`, `bazel test` or `bazel run`.
---

The `Run` API runs commands in a remote runner. These runners can have more compute power, and workspaces are snapshotted between runs, speeding up subsequent runs.

Any bash command can be run in a remote runner, but bazel commands may benefit the most from proximity
to our cache servers and workspace snapshotting.

## Getting help

If you don't know how to accomplish a task, you should consult the docs at https://www.buildbuddy.io/docs/remote-bazel. Do not try to run `bb remote --help` or `bb help`.

## Pre-run gate

Before initiating a remote run, you should check if the user asked to add any fields to the request.

If they did, follow the field persistence steps below.

## Initiating a remote run

The `Run` API can be made with a curl request.

For bazel commands, make sure to preserve the entire command, including startup flags and options, in the `steps` field.

Also make sure to always add the `--remote_cache` flag to bazel commands. For example if you are intending to run `bazel build :target`, the request should be: `{"run": "bazel build :target --remote_cache=grpcs://remote.buildbuddy.io"}`.

To clone a GitHub repo and run `steps` within that workspace, set the fields `repo`, `branch` and/or `commit_sha`. This is optional if a GitHub workspace is not needed for the steps.

By default, the `Run` API will return once the remote runner has started. You can poll for completion using the `GetInvocation` API. If you want the `Run` API to wait until the commands are finished, add `"wait_until": "COMPLETED"` to the request.

Multi-line steps are supported.

Sample request:

```
curl --data '{
  "repo": "git@github.com:<REPO_NAME>.git",
  "branch": "<BRANCH_NAME>",
  "steps": [{"run": "bazel build :target"}],
  "wait_until": "COMPLETED"
}' \
--header "x-buildbuddy-api-key: ${BUILDBUDDY_API_KEY?}" \
--header "Content-Type: application/json" \
https://app.buildbuddy.io/api/v1/Run
```

Parse `invocationId` from the response and share `https://app.buildbuddy.io/invocation/<INVOCATION_ID>` with the user so they can view logs for the remote run.

## Reading remote logs

Logs for the remote run can be fetched with the `GetLog` API. Logs are streamed live and can be fetched in parallel with the remote run.

The invocation ID should be parsed from the results of the `Run` API.

Sample request:

```
curl -d '{
  "selector": {"invocation_id":"<INVOCATION_ID>"}
}' \
-H "x-buildbuddy-api-key: ${BUILDBUDDY_API_KEY}" \
-H "Content-Type: application/json" \
https://app.buildbuddy.io/api/v1/GetLog
```

If `next_page_token` is present in the GetLog response, keep calling GetLog and passing the token to the `page_token` field until all log chunks are fetched.

## Fetching invocation metadata

Invocation metadata can be fetched with the `GetInvocation` API. Metadata includes the exit code of the run after it's finished and whether it completed successfully.

All steps must complete successfully for the invocation to be considered a success. If any steps fail,
subsequent steps will not be run.

```
curl -d '{
  "selector": {"invocation_id":"<INVOCATION_ID>"}
}' \
-H "x-buildbuddy-api-key: ${BUILDBUDDY_API_KEY}" \
-H "Content-Type: application/json" \
https://app.buildbuddy.io/api/v1/GetInvocation
```

## Configuration

Environment variables can be set on the remote runner with the `env` field. It expects a map as input.

Other configuration can be set in the `platform_properties` field, which is also a map.
Valid values of `OSFamily` are `linux` and `darwin`.
Valid values of `Arch` are `amd64` and `arm64`.
The `container-image` field expects a docker URL like `docker://<URL>`.

If a remote runner is corrupted and you want a clean one, you can set the property `salt` to a new value. Just make sure you preserve this flag if you want future runs to resume from this new runner.

## Requesting more resources

We strongly recommend not setting these and using the defaults unless absolutely necessary. Only suggest increasing resources if the logs explicitly report OOM errors or disk exhaustion.

More resources can be requested for the remote runner with the properties `EstimatedCPU`, `EstimatedFreeDiskBytes` and `EstimatedMemory`. The disk and memory fields expect values in the syntax `XGB`.

## Auth

Users can get an API key from the `Settings` page of the BuildBuddy UI. If running in non-interactive mode, the user should save the API key as a secret.

## Field persistence

Whenever the user provides fields not already in the persistent-fields.md file, follow this workflow before initiating the remote run:

1. Ask: `Do you want me to persist <FIELD> in this SKILL file so it is automatically applied to all future remote runs?`
1. Wait for explicit confirmation before editing this file.
1. If the user confirms, edit only the persistent-fields.md file.
1. Ensure the field is not already present.
1. Preserve alphabetical ordering.

Entry format:

- `{ "KEY": "VALUE" }`: Example: `{"wait_until": "COMPLETED"}`
